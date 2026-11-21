#!/usr/bin/env python3
"""
Reference ("oracle") implementation of reference/rules.md.

Written only from rules.md, the fixtures and scripts/run_fixtures.py.
Pure standard library for all logic; pyarrow only to write the Parquet outputs.

Usage: DATA_DIR=... OUTPUT_DIR=... python oracle.py [DATA_DIR OUTPUT_DIR]
(the last two argv, when given, override the environment).

All values are kept as raw bytes, so every comparison ("byte-wise smallest",
exact ID equality, no normalization) is literally a byte comparison. Strings
are decoded as UTF-8 only when writing the output.

AMBIGUITIES NOTICED AND HOW THEY ARE RESOLVED
---------------------------------------------
1. Header line: the first line of every CSV is skipped unconditionally, whatever
   it holds. A later data line equal to the header is ordinary data (fixture L3:
   `user_id_a,user_id_b` as a data row links those two literal IDs).
2. Trailing newline: a file ending in "\n" yields a final empty piece that is not
   a line. Any other empty piece (including an empty last line before a final
   "\n", i.e. "\n\n") is an empty line and malformed. A file with no trailing
   newline has its last piece treated as a line.
3. Quote handling: a field is "quoted" iff it has length >= 2 and starts and ends
   with '"', and the inner part has no '"'. A lone `"` field, or a quote anywhere
   else, makes the whole line malformed. `""` is an empty field (= null).
4. Wrong field count: exactly 7 / 4 / 2 fields for events / clicks / links. An
   empty line splits into one empty field and is malformed (explicitly so per spec).
5. products.csv and campaigns.csv are described as clean; they are parsed with
   the same dialect anyway (quotes stripped, malformed lines skipped). If a
   product_id/campaign_id repeated, the first occurrence would win (never happens).
6. F1/F2 years: exactly 4 ASCII digits; year 0000 is accepted as a proleptic
   Gregorian year (it is far out of any window anyway). Leap year = Gregorian rule.
7. F3: one or more ASCII digits, any length, arbitrary precision; a value beyond
   int64 is simply out of window (Python ints compare exactly, so no overflow).
8. ingest_ts is "always valid F1". It is parsed with the general timestamp
   parser; if it were ever unparsable it would sort after every parsable copy in
   dedup (treated as +infinity) and the event would be treated as not late. This
   never arises in fixtures/dev.
9. Dedup ties on equal ingest_ts: "copies are identical", so the first copy read
   (files in sorted name order, lines in file order) is kept.
10. Click dedup "keep any one": the first copy read is kept (copies are identical
    after quote stripping, so it does not matter).
11. Dedup happens on the parsed (quote-stripped) event_id / click_id; `"e1"` and
    `e1` are the same ID, `e1 ` and `e3\r` are different IDs.
12. Late events: dropped iff ingest_ts - ts > 86,400,000 ms. Negative is kept.
13. Sessionization with equal timestamps: events are grouped per distinct ts
    (sorted ascending); a new session starts when the gap between consecutive
    distinct timestamps exceeds 1,800,000 ms. Exactly 1,800,000 stays.
14. purchasing_sessions counts credited sessions that hold at least one surviving
    purchase. Every surviving purchase has price >= 1, so such a session also has
    revenue > 0 and the person-day has a non-null top_category.
15. top_category is computed over all purchases of all sessions of the person-day
    (summing price_cents per category), max sum, tie -> byte-wise smallest
    category; `unknown` (product not in products.csv, including empty product,
    though empty product_id is already dropped by validation) competes normally.
16. Assists use the same qualifying set as credit: same person, valid click,
    session_start - 7d <= click.ts <= session_start; each campaign at most once per
    session. A campaign that only assists on a day still gets a row with zeros in
    the credited columns; channel comes from campaigns.csv. `organic` never assists.
17. Links: a line with either side empty is dropped; self-links and repeats are
    no-ops. The person name is the byte-wise min over the whole connected
    component of link IDs (including IDs that never appear in events/clicks).
    An ID never in a surviving link line is its own person.
18. A click's person is resolved via links just like an event's; clicks by a
    person with no sessions simply contribute nothing.
19. Output strings are decoded as UTF-8 (with replacement for any invalid byte,
    which the generator never produces). Integer columns are int64; `day` is
    date32 (Parquet DATE); top_category is nullable string. Empty tables are
    still written (zero rows, correct schema).
20. Input files: every `*.csv` directly in events/, clicks/, links/ (sorted by
    name; order does not affect results except for ambiguity 9/10, which do not
    change values). A missing directory is treated as holding no files.
"""
import glob
import json
import os
import sys
from bisect import bisect_left, bisect_right

DAY_MS = 86_400_000
GAP_MS = 1_800_000
WEEK_MS = 7 * DAY_MS
LATE_MS = DAY_MS
QUOTE = 0x22  # '"'


# --------------------------------------------------------------------------
# CSV dialect
# --------------------------------------------------------------------------

def read_lines(path):
    """Yield the data lines (bytes) of a file: split on b'\\n' only, skip the
    header, drop the final empty piece after a trailing newline."""
    with open(path, "rb") as f:
        data = f.read()
    pieces = data.split(b"\n")
    if pieces and pieces[-1] == b"":
        pieces.pop()  # final empty piece after a trailing newline is not a line
    return pieces[1:]  # first line is the header


def parse_line(line, nfields):
    """Return a list of nfields byte values, or None if malformed."""
    if line == b"":
        return None
    fields = line.split(b",")
    if len(fields) != nfields:
        return None
    out = []
    for f in fields:
        if QUOTE in f:
            if len(f) >= 2 and f[0] == QUOTE and f[-1] == QUOTE and QUOTE not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def read_table(paths, nfields):
    for p in paths:
        for line in read_lines(p):
            row = parse_line(line, nfields)
            if row is not None:
                yield row


# --------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------

def is_digits(b):
    # ASCII digits only; bytes.isdigit() is ASCII-only for bytes objects
    return len(b) > 0 and all(0x30 <= c <= 0x39 for c in b)


def is_leap(y):
    return (y % 4 == 0 and y % 100 != 0) or y % 400 == 0


def days_in_month(y, m):
    if m == 2:
        return 29 if is_leap(y) else 28
    if m in (4, 6, 9, 11):
        return 30
    return 31


def days_from_civil(y, m, d):
    """Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant)."""
    y -= m <= 2
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468


def parse_ts(b):
    """Parse F1/F2/F3 bytes to epoch ms (Python int, may be huge), or None if bad."""
    n = len(b)
    if n == 0:
        return None
    if is_digits(b):  # F3
        return int(b)
    if n not in (20, 25):
        return None
    # common prefix YYYY-MM-DDTHH:MM:SS
    if b[4] != 0x2D or b[7] != 0x2D or b[10] != 0x54 or b[13] != 0x3A or b[16] != 0x3A:
        return None
    parts = (b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19])
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, hh, mi, ss = (int(p) for p in parts)
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= days_in_month(y, mo)):
        return None
    if hh > 23 or mi > 59 or ss > 59:
        return None
    local = ((days_from_civil(y, mo, d) * 24 + hh) * 60 + mi) * 60 + ss
    if n == 20:
        if b[19] != 0x5A:  # 'Z' (uppercase only)
            return None
        offset_s = 0
    else:
        sign = b[19]
        if sign not in (0x2B, 0x2D):  # '+' or '-'
            return None
        if b[22] != 0x3A:
            return None
        oh, om = b[20:22], b[23:25]
        if not (is_digits(oh) and is_digits(om)):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        offset_s = (oh * 60 + om) * 60
        if sign == 0x2D:
            offset_s = -offset_s
    return (local - offset_s) * 1000


def day_of(ms):
    return ms // DAY_MS  # days since epoch (UTC date), floor


# --------------------------------------------------------------------------
# Union-find for links
# --------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        parent = self.parent
        if x not in parent:
            return x
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:  # path compression
            parent[x], x = root, parent[x]
        return root

    def add(self, x):
        if x not in self.parent:
            self.parent[x] = x

    def union(self, a, b):
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # keep the byte-wise smaller ID as the root, so the root is the name
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra


# --------------------------------------------------------------------------
# Main pipeline
# --------------------------------------------------------------------------

def csvs(data_dir, sub):
    return sorted(p for p in glob.glob(os.path.join(data_dir, sub, "*.csv")) if os.path.isfile(p))


def main():
    args = sys.argv[1:]
    if len(args) >= 2:
        data_dir, out_dir = args[-2], args[-1]
    else:
        data_dir, out_dir = os.environ["DATA_DIR"], os.environ["OUTPUT_DIR"]

    with open(os.path.join(data_dir, "manifest.json"), "rb") as f:
        manifest = json.loads(f.read().decode("utf-8"))
    w_start = parse_ts(manifest["window_start"].encode("utf-8"))
    w_end = parse_ts(manifest["window_end"].encode("utf-8"))
    if w_start is None or w_end is None:
        sys.exit("bad manifest window")
    c_start = w_start - WEEK_MS

    # products / campaigns
    products = {}
    for pid, cat in read_table([os.path.join(data_dir, "products.csv")], 2):
        products.setdefault(pid, cat)
    campaigns = {}
    for cid, ch in read_table([os.path.join(data_dir, "campaigns.csv")], 2):
        campaigns.setdefault(cid, ch)

    # ---- Links
    uf = UnionFind()
    for a, b in read_table(csvs(data_dir, "links"), 2):
        if a == b"" or b == b"":
            continue
        uf.union(a, b)  # self-link / repeats are harmless
    name_cache = {}

    def person(uid):
        p = name_cache.get(uid)
        if p is None:
            p = uf.find(uid)  # root is the byte-wise smallest of the component
            name_cache[uid] = p
        return p

    # ---- Events: parse + dedup (smallest ingest_ts, first read wins ties)
    INF = float("inf")
    kept = {}  # event_id -> (ingest_ms, row)
    for row in read_table(csvs(data_dir, "events"), 7):
        eid = row[0]
        if eid == b"":
            continue
        ing = parse_ts(row[3])
        ing_key = INF if ing is None else ing
        prev = kept.get(eid)
        if prev is None or ing_key < prev[0]:
            kept[eid] = (ing_key, row)

    # ---- Validate + late filter
    VALID_TYPES = (b"view", b"cart", b"purchase")
    per_person = {}  # person -> list of (ts, is_purchase, price, category)
    for ing_key, row in kept.values():
        _eid, uid, ts_b, _ing_b, etype, pid, price_b = row
        if uid == b"":
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (w_start <= ts < w_end):
            continue
        if etype not in VALID_TYPES:
            continue
        if pid == b"":
            continue
        price = 0
        is_purchase = etype == b"purchase"
        if is_purchase:
            if not is_digits(price_b):
                continue
            price = int(price_b)
            if not (1 <= price <= 10_000_000):
                continue
        if ing_key != INF and ing_key - ts > LATE_MS:
            continue
        cat = products.get(pid, b"unknown")
        per_person.setdefault(person(uid), []).append((ts, is_purchase, price, cat))
    del kept

    # ---- Clicks: parse, dedup, validate
    seen_clicks = set()
    clicks_by_person = {}  # person -> list of (ts, click_id, campaign_id)
    for cid_click, uid, camp, ts_b in read_table(csvs(data_dir, "clicks"), 4):
        if cid_click == b"":
            continue
        if cid_click in seen_clicks:
            continue
        seen_clicks.add(cid_click)
        if uid == b"":
            continue
        ts = parse_ts(ts_b)
        if ts is None or not (c_start <= ts < w_end):
            continue
        if camp not in campaigns:
            continue
        clicks_by_person.setdefault(person(uid), []).append((ts, cid_click, camp))
    del seen_clicks

    # ---- Sessionize, attribute, aggregate
    user_daily = {}      # (person, day) -> [sessions, events, revenue, {cat: sum}]
    camp_daily = {}      # (campaign, day) -> [sessions, purchasing, revenue, assisted]
    ORGANIC = b"organic"

    for p, evs in per_person.items():
        evs.sort(key=lambda e: e[0])
        clicks = clicks_by_person.get(p, [])
        clicks.sort(key=lambda c: (c[0], c[1]))
        click_ts = [c[0] for c in clicks]

        # split into sessions: gap measured between distinct timestamps
        sessions = []
        cur = None
        prev_ts = None
        for e in evs:
            if cur is None or e[0] - prev_ts > GAP_MS:
                cur = []
                sessions.append(cur)
            cur.append(e)
            prev_ts = e[0]

        for sess in sessions:
            start = sess[0][0]
            day = day_of(start)
            n_events = len(sess)
            revenue = 0
            has_purchase = False
            cats = {}
            for ts, is_p, price, cat in sess:
                if is_p:
                    has_purchase = True
                    revenue += price
                    cats[cat] = cats.get(cat, 0) + price

            ud = user_daily.get((p, day))
            if ud is None:
                ud = [0, 0, 0, {}]
                user_daily[(p, day)] = ud
            ud[0] += 1
            ud[1] += n_events
            ud[2] += revenue
            for c, v in cats.items():
                ud[3][c] = ud[3].get(c, 0) + v

            # qualifying clicks: start - 7d <= ts <= start
            lo = bisect_left(click_ts, start - WEEK_MS)
            hi = bisect_right(click_ts, start)
            qual = clicks[lo:hi]
            if qual:
                latest = max(c[0] for c in qual)
                winner = min(c[1] for c in qual if c[0] == latest)
                credited = next(c[2] for c in qual if c[0] == latest and c[1] == winner)
                assists = {c[2] for c in qual}
            else:
                credited = ORGANIC
                assists = set()

            cd = camp_daily.get((credited, day))
            if cd is None:
                cd = [0, 0, 0, 0]
                camp_daily[(credited, day)] = cd
            cd[0] += 1
            cd[1] += 1 if has_purchase else 0
            cd[2] += revenue
            for a in assists:
                ad = camp_daily.get((a, day))
                if ad is None:
                    ad = [0, 0, 0, 0]
                    camp_daily[(a, day)] = ad
                ad[3] += 1

    write_outputs(out_dir, user_daily, camp_daily, campaigns)


def dec(b):
    return b.decode("utf-8", errors="replace")


def write_outputs(out_dir, user_daily, camp_daily, campaigns):
    import datetime
    import pyarrow as pa
    import pyarrow.parquet as pq

    epoch = datetime.date(1970, 1, 1)

    def to_date(d):
        return epoch + datetime.timedelta(days=d)

    u_user, u_day, u_sess, u_ev, u_rev, u_top = [], [], [], [], [], []
    for (p, day), (s, ev, rev, cats) in sorted(user_daily.items()):
        top = None
        if cats:
            best = max(cats.values())
            top = dec(min(c for c, v in cats.items() if v == best))
        u_user.append(dec(p))
        u_day.append(to_date(day))
        u_sess.append(s)
        u_ev.append(ev)
        u_rev.append(rev)
        u_top.append(top)
    ut = pa.table({
        "user_id": pa.array(u_user, pa.string()),
        "day": pa.array(u_day, pa.date32()),
        "sessions": pa.array(u_sess, pa.int64()),
        "events": pa.array(u_ev, pa.int64()),
        "revenue_cents": pa.array(u_rev, pa.int64()),
        "top_category": pa.array(u_top, pa.string()),
    })

    c_id, c_ch, c_day, c_s, c_ps, c_rev, c_as = [], [], [], [], [], [], []
    for (cid, day), (s, ps, rev, asst) in sorted(camp_daily.items()):
        c_id.append(dec(cid))
        c_ch.append("organic" if cid == b"organic" else dec(campaigns[cid]))
        c_day.append(to_date(day))
        c_s.append(s)
        c_ps.append(ps)
        c_rev.append(rev)
        c_as.append(asst)
    ct = pa.table({
        "campaign_id": pa.array(c_id, pa.string()),
        "channel": pa.array(c_ch, pa.string()),
        "day": pa.array(c_day, pa.date32()),
        "sessions": pa.array(c_s, pa.int64()),
        "purchasing_sessions": pa.array(c_ps, pa.int64()),
        "revenue_cents": pa.array(c_rev, pa.int64()),
        "assisted_sessions": pa.array(c_as, pa.int64()),
    })

    for name, table in (("user_daily", ut), ("campaign_daily", ct)):
        d = os.path.join(out_dir, name)
        os.makedirs(d, exist_ok=True)
        pq.write_table(table, os.path.join(d, "part-0.parquet"))


if __name__ == "__main__":
    main()
