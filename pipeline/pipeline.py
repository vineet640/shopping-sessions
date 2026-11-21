#!/usr/bin/env python3
"""Shopping Sessions pipeline (Polars + numpy/scipy). See SPEC.md / reference/rules.md.

Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/part-0.parquet.
All CSV parsing is done by hand on raw lines (no CSV sniffing / lenient casts).
"""
import glob
import json
import os
import sys
import time

NCPU = os.environ.get("NCPU")
if NCPU:
    os.environ.setdefault("POLARS_MAX_THREADS", NCPU)

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
from scipy.sparse import coo_matrix  # noqa: E402
from scipy.sparse.csgraph import connected_components  # noqa: E402

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
LATE_MS = DAY_MS


def read_lines(files):
    """All data lines (header skipped) of the given files, as one String Series 'line'."""
    parts = []
    for path in files:
        with open(path, "rb") as f:
            raw = f.read()
        text = raw.decode("utf-8")
        s = pl.Series("line", [text]).str.split("\n").explode()
        n = s.len()
        end = n - 1 if text.endswith("\n") else n
        if end > 1:
            parts.append(s.slice(1, end - 1))
    if not parts:
        return pl.DataFrame({"line": pl.Series([], dtype=pl.String)})
    return pl.DataFrame({"line": pl.concat(parts)})


def parse_csv(df, names):
    """Split lines into fields per the rules' dialect. Adds 'ok' (well-formed) column."""
    n = len(names)
    line = pl.col("line")
    ok = (line.str.len_bytes() > 0) & (line.str.count_matches(",", literal=True) == n - 1)
    df = df.with_columns(
        line.str.split_exact(",", n - 1).struct.rename_fields(names).alias("_s"), ok.alias("ok")
    ).unnest("_s")
    exprs = []
    bad = pl.lit(False)
    for name in names:
        f = pl.col(name)
        quoted = f.str.starts_with('"') & f.str.ends_with('"') & (f.str.len_bytes() >= 2)
        v = pl.when(quoted).then(f.str.slice(1, f.str.len_chars() - 2)).otherwise(f)
        exprs.append(v.alias(name))
    df = df.with_columns(exprs)
    for name in names:
        bad = bad | pl.col(name).str.contains('"', literal=True).fill_null(False)
    df = df.with_columns((pl.col("ok") & ~bad).alias("ok"))
    df = df.with_columns([pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in names])
    return df.drop("line")


TS_RE = (r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})"
         r"(?:Z|([+-])([0-9]{2}):([0-9]{2}))$")


def days_from_civil(y, m, d):
    y2 = pl.when(m <= 2).then(y - 1).otherwise(y)
    era = y2 // 400
    yoe = y2 - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    return era * 146_097 + yoe * 365 + yoe // 4 - yoe // 100 + doy - 719_468


def ts_expr(col):
    """Expression: string column -> epoch ms Int64 or null if bad timestamp."""
    c = pl.col(col)
    # F3: digits only
    is_f3 = c.str.contains(r"^[0-9]+$")
    stripped = c.str.strip_chars_start("0")
    f3 = (
        pl.when(stripped.str.len_bytes() == 0).then(pl.lit(0, pl.Int64))
        .when(stripped.str.len_bytes() > 19).then(pl.lit(None, pl.Int64))
        .otherwise(stripped.cast(pl.Int64, strict=False))
    )
    g = c.str.extract_groups(TS_RE)
    y = g.struct.field("1").cast(pl.Int64)
    mo = g.struct.field("2").cast(pl.Int64)
    d = g.struct.field("3").cast(pl.Int64)
    h = g.struct.field("4").cast(pl.Int64)
    mi = g.struct.field("5").cast(pl.Int64)
    s = g.struct.field("6").cast(pl.Int64)
    sign = g.struct.field("7")
    oh = g.struct.field("8").cast(pl.Int64).fill_null(0)
    om = g.struct.field("9").cast(pl.Int64).fill_null(0)
    leap = ((y % 4 == 0) & (y % 100 != 0)) | (y % 400 == 0)
    dim = (
        pl.when(mo == 2).then(pl.when(leap).then(29).otherwise(28))
        .when(mo.is_in([4, 6, 9, 11])).then(30)
        .otherwise(31)
    )
    valid = (
        (mo >= 1) & (mo <= 12) & (d >= 1) & (d <= dim) & (h <= 23) & (mi <= 59) & (s <= 59)
        & (oh <= 23) & (om <= 59)
    )
    off = pl.when(sign == "-").then(-(oh * 60 + om)).otherwise(oh * 60 + om) * 60_000
    ms = (days_from_civil(y, mo, d) * DAY_MS + h * 3_600_000 + mi * 60_000 + s * 1000) - off
    f12 = pl.when(valid).then(ms).otherwise(None)
    return pl.when(is_f3).then(f3).otherwise(f12)


def parse_iso_utc(s):
    df = pl.DataFrame({"t": [s]}).select(ts_expr("t"))
    v = df.item()
    assert v is not None, s
    return v


def main():
    data = os.environ.get("DATA_DIR", "/data")
    out = os.environ.get("OUTPUT_DIR", "/output")
    with open(os.path.join(data, "manifest.json")) as f:
        manifest = json.load(f)
    ws = parse_iso_utc(manifest["window_start"])
    we = parse_iso_utc(manifest["window_end"])
    cws = ws - WEEK_MS
    log(f"window {ws}..{we}; threads {pl.thread_pool_size()}")

    def files(sub):
        return sorted(glob.glob(os.path.join(data, sub, "*.csv")))

    # ---------- small tables ----------
    products = parse_csv(read_lines([os.path.join(data, "products.csv")]), ["product_id", "category"])
    products = products.filter(pl.col("ok") & pl.col("product_id").is_not_null()).select(
        "product_id", "category").unique("product_id")
    campaigns = parse_csv(read_lines([os.path.join(data, "campaigns.csv")]), ["campaign_id", "channel"])
    campaigns = campaigns.filter(pl.col("ok") & pl.col("campaign_id").is_not_null()).select(
        "campaign_id", "channel").unique("campaign_id")

    # ---------- links ----------
    links = parse_csv(read_lines(files("links")), ["a", "b"])
    links = links.filter(pl.col("ok") & pl.col("a").is_not_null() & pl.col("b").is_not_null()).select("a", "b")
    ids = pl.concat([links["a"], links["b"]]).unique().sort().alias("id")
    idmap = pl.DataFrame({"id": ids}).with_row_index("ix")
    la = links.join(idmap, left_on="a", right_on="id", how="left")["ix"].to_numpy()
    lb = links.join(idmap, left_on="b", right_on="id", how="left")["ix"].to_numpy()
    nid = idmap.height
    if nid:
        g = coo_matrix((np.ones(len(la), dtype=np.int8), (la, lb)), shape=(nid, nid))
        _, comp = connected_components(g, directed=False)
    else:
        comp = np.zeros(0, dtype=np.int64)
    # ids are sorted, so the smallest id of a component is its first occurrence
    idmap = idmap.with_columns(pl.Series("comp", comp))
    names = idmap.group_by("comp").agg(pl.col("id").min().alias("name"))
    idmap = idmap.join(names, on="comp").select("id", "name")
    log(f"links: {links.height} lines, {nid} ids")

    # ---------- events ----------
    ev = parse_csv(read_lines(files("events")),
                   ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"])
    log(f"events read: {ev.height}")
    ev = ev.filter(pl.col("ok") & pl.col("event_id").is_not_null()).drop("ok")
    ev = ev.with_columns(ts_expr("ingest_ts").alias("ing"))
    ev = ev.filter(pl.col("ing") == pl.col("ing").min().over("event_id")).unique("event_id", keep="any")
    ev = ev.with_columns(ts_expr("ts").alias("t"))
    price_ok = pl.col("price_cents").str.contains(r"^[0-9]+$").fill_null(False)
    price_v = pl.col("price_cents").str.strip_chars_start("0")
    price_num = pl.when(price_v.str.len_bytes() > 9).then(pl.lit(10**9, pl.Int64)).otherwise(
        price_v.cast(pl.Int64, strict=False).fill_null(0))
    ev = ev.with_columns(pl.when(price_ok).then(price_num).otherwise(None).alias("price"))
    is_purchase = pl.col("event_type") == "purchase"
    valid = (
        pl.col("user_id").is_not_null()
        & pl.col("t").is_not_null() & (pl.col("t") >= ws) & (pl.col("t") < we)
        & pl.col("event_type").is_in(["view", "cart", "purchase"]).fill_null(False)
        & pl.col("product_id").is_not_null()
        & (~is_purchase | (pl.col("price").is_not_null() & (pl.col("price") >= 1) & (pl.col("price") <= 10_000_000)))
        & ((pl.col("ing") - pl.col("t")) <= LATE_MS)
    )
    ev = ev.filter(valid.fill_null(False)).select(
        "user_id", "t", "product_id",
        pl.when(is_purchase).then(pl.col("price")).otherwise(0).alias("price"),
        is_purchase.alias("is_p"),
    )
    ev = ev.join(idmap, left_on="user_id", right_on="id", how="left").with_columns(
        pl.coalesce("name", "user_id").alias("person")).drop("name", "user_id")
    log(f"events valid: {ev.height}")

    # ---------- persons ----------
    persons = ev.select(pl.col("person").unique().sort()).with_row_index("pid").with_columns(
        pl.col("pid").cast(pl.Int64))
    ev = ev.join(persons, on="person").drop("person")
    ev = ev.sort(["pid", "t"])
    new = (pl.col("pid") != pl.col("pid").shift(1)).fill_null(True) | (
        (pl.col("t") - pl.col("t").shift(1)) > GAP_MS).fill_null(True)
    ev = ev.with_columns(new.cum_sum().alias("sid"))
    sessions = ev.group_by("sid").agg(
        pl.col("pid").first(), pl.col("t").min().alias("start"), pl.len().alias("events"),
        pl.col("price").sum().alias("revenue"), pl.col("is_p").any().alias("has_p"),
    ).sort("sid")
    sessions = sessions.with_columns((pl.col("start") // DAY_MS).alias("dayn"))
    ev = ev.join(sessions.select("sid", "pid", "dayn"), on=["sid", "pid"])
    log(f"sessions: {sessions.height}")

    # top category per (pid, day)
    purch = ev.filter(pl.col("is_p")).join(products, on="product_id", how="left").with_columns(
        pl.col("category").fill_null("unknown"))
    topcat = purch.group_by(["pid", "dayn", "category"]).agg(pl.col("price").sum().alias("amt")).sort(
        ["pid", "dayn", "amt", "category"], descending=[False, False, True, False]
    ).group_by(["pid", "dayn"]).agg(pl.col("category").first().alias("top_category"))

    user_daily = sessions.group_by(["pid", "dayn"]).agg(
        pl.len().alias("sessions"), pl.col("events").sum(), pl.col("revenue").sum().alias("revenue_cents"))
    user_daily = user_daily.join(topcat, on=["pid", "dayn"], how="left").join(persons, on="pid").select(
        pl.col("person").alias("user_id"),
        pl.col("dayn").cast(pl.Int32).cast(pl.Date).alias("day"),
        pl.col("sessions").cast(pl.Int64), pl.col("events").cast(pl.Int64),
        pl.col("revenue_cents").cast(pl.Int64), "top_category")

    # ---------- clicks ----------
    ck = parse_csv(read_lines(files("clicks")), ["click_id", "user_id", "campaign_id", "ts"])
    log(f"clicks read: {ck.height}")
    ck = ck.filter(pl.col("ok") & pl.col("click_id").is_not_null()).drop("ok").unique("click_id", keep="any")
    ck = ck.with_columns(ts_expr("ts").alias("t"))
    ck = ck.filter((pl.col("user_id").is_not_null() & pl.col("t").is_not_null()
                    & (pl.col("t") >= cws) & (pl.col("t") < we)).fill_null(False))
    ck = ck.join(campaigns.select("campaign_id"), on="campaign_id", how="semi")
    ck = ck.join(idmap, left_on="user_id", right_on="id", how="left").with_columns(
        pl.coalesce("name", "user_id").alias("person")).drop("name", "user_id")
    ck = ck.join(persons, on="person").select("pid", "t", "click_id", "campaign_id")
    log(f"clicks valid (with sessions): {ck.height}")

    camp_ids = campaigns.select(pl.col("campaign_id").sort()).with_row_index("cix").with_columns(
        pl.col("cix").cast(pl.Int64))
    ck = ck.join(camp_ids, on="campaign_id")

    skey = (sessions["pid"].to_numpy().astype(np.int64) << 32) | (sessions["start"].to_numpy() - cws)
    # sessions sorted by sid == sorted by (pid, start), so skey is sorted
    # credit
    best = ck.sort(["pid", "t", "click_id"]).unique(["pid", "t"], keep="first", maintain_order=True)
    ckey = (best["pid"].to_numpy().astype(np.int64) << 32) | (best["t"].to_numpy() - cws)
    bcix = best["cix"].to_numpy()
    idx = np.searchsorted(ckey, skey, side="right") - 1
    idc = np.clip(idx, 0, None)
    okc = (idx >= 0) & ((ckey[idc] >> 32) == (skey >> 32)) & ((skey - ckey[idc]) <= WEEK_MS) if len(ckey) else np.zeros(len(skey), bool)
    credit = np.where(okc, bcix[idc] if len(ckey) else -1, -1)
    sessions = sessions.with_columns(pl.Series("cix", credit, dtype=pl.Int64))

    # assists: merge each (pid, campaign) click list into intervals
    cc = ck.select("pid", "cix", "t").unique().sort(["pid", "cix", "t"])
    newi = ((pl.col("pid") != pl.col("pid").shift(1)) | (pl.col("cix") != pl.col("cix").shift(1))
            | ((pl.col("t") - pl.col("t").shift(1)) > WEEK_MS)).fill_null(True)
    iv = cc.with_columns(newi.cum_sum().alias("g")).group_by("g").agg(
        pl.col("pid").first(), pl.col("cix").first(), pl.col("t").min().alias("lo"), pl.col("t").max().alias("hi"))
    ipid = iv["pid"].to_numpy().astype(np.int64) << 32
    lo = np.searchsorted(skey, ipid | (iv["lo"].to_numpy() - cws), side="left")
    hi = np.searchsorted(skey, ipid | (iv["hi"].to_numpy() + WEEK_MS - cws), side="right")
    lens = hi - lo
    total = int(lens.sum())
    starts = np.repeat(lo - np.concatenate([[0], np.cumsum(lens)[:-1]]), lens)
    sidx = np.arange(total, dtype=np.int64) + starts
    acix = np.repeat(iv["cix"].to_numpy(), lens)
    sday = sessions["dayn"].to_numpy()
    assists = pl.DataFrame({"cix": acix, "dayn": sday[sidx]}).group_by(["cix", "dayn"]).agg(
        pl.len().alias("assisted_sessions"))
    log(f"assist pairs: {total}")

    credited = sessions.group_by(["cix", "dayn"]).agg(
        pl.len().alias("sessions"), pl.col("has_p").sum().alias("purchasing_sessions"),
        pl.col("revenue").sum().alias("revenue_cents"))
    cd = credited.join(assists, on=["cix", "dayn"], how="full", coalesce=True).with_columns(
        pl.col("sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions").fill_null(0))
    camp_full = camp_ids.join(campaigns, on="campaign_id").vstack(
        pl.DataFrame({"cix": [-1], "campaign_id": ["organic"], "channel": ["organic"]},
                     schema={"cix": pl.Int64, "campaign_id": pl.String, "channel": pl.String}))
    campaign_daily = cd.join(camp_full, on="cix").select(
        "campaign_id", "channel", pl.col("dayn").cast(pl.Int32).cast(pl.Date).alias("day"),
        pl.col("sessions").cast(pl.Int64), pl.col("purchasing_sessions").cast(pl.Int64),
        pl.col("revenue_cents").cast(pl.Int64), pl.col("assisted_sessions").cast(pl.Int64))

    for name, df in (("user_daily", user_daily), ("campaign_daily", campaign_daily)):
        folder = os.path.join(out, name)
        os.makedirs(folder, exist_ok=True)
        df.write_parquet(os.path.join(folder, "part-0.parquet"))
    log(f"wrote user_daily {user_daily.height} rows, campaign_daily {campaign_daily.height} rows")


if __name__ == "__main__":
    main()
