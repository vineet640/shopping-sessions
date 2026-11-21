//! Shopping Sessions engine (Rust). See ../SPEC.md and ../reference/rules.md.
//!
//! Phases:
//!  1. mmap + parallel parse of all CSVs (strict dialect). IDs are interned into a lock-free
//!     table keyed by a 128-bit xxh3 hash; events go to hash(event_id) buckets.
//!  2. union-find over link edges (table slots); person name = byte-wise smallest ID.
//!  3. per event-id bucket: dedup (min ingest_ts) -> surviving valid events to person buckets.
//!  4. per person bucket: sort, sessionize, credit, assists, user_daily rows, campaign counters.
//!  5. write Parquet.
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use xxhash_rust::xxh3::xxh3_128;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const DB_BITS: u32 = 8; // dedup buckets
const NDB: usize = 1 << DB_BITS;
const PB_BITS: u32 = 9; // person buckets
const NPB: usize = 1 << PB_BITS;
const NO_CAT: u16 = u16::MAX;

static DD_A: AtomicU64 = AtomicU64::new(0);
static DD_B: AtomicU64 = AtomicU64::new(0);
static T0: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
macro_rules! log {
    ($($a:tt)*) => {{
        let t = T0.get().unwrap().elapsed().as_secs_f64();
        eprintln!("[{:7.3}s] {}", t, format!($($a)*));
    }};
}

// ---------------------------------------------------------------- files

enum Bytes {
    Map(Mmap),
    Empty,
}
impl Bytes {
    #[inline]
    fn get(&self) -> &[u8] {
        match self {
            Bytes::Map(m) => m,
            Bytes::Empty => &[],
        }
    }
}

fn open(path: &Path) -> Bytes {
    let f = fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let len = f.metadata().unwrap().len();
    if len == 0 {
        return Bytes::Empty;
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", path.display()));
    #[cfg(unix)]
    let _ = m.advise(memmap2::Advice::Sequential);
    Bytes::Map(m)
}

fn csvs(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false) && p.is_file())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

// ---------------------------------------------------------------- CSV dialect

/// Split `line` into exactly N fields per the rules' dialect; quotes stripped.
/// Returns false if malformed (wrong count, stray quote, empty line).
#[inline]
fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    let mut k = 0usize;
    let mut s = 0usize;
    let n = line.len();
    let mut i = 0usize;
    loop {
        // find end of field
        let mut q = false;
        while i < n {
            let c = line[i];
            if c == b',' {
                break;
            }
            q |= c == b'"';
            i += 1;
        }
        if k == N {
            return false;
        }
        let f = &line[s..i];
        out[k] = if q {
            let l = f.len();
            if l < 2 || f[0] != b'"' || f[l - 1] != b'"' {
                return false;
            }
            let inner = &f[1..l - 1];
            if inner.contains(&b'"') {
                return false;
            }
            inner
        } else {
            f
        };
        k += 1;
        if i >= n {
            break;
        }
        i += 1; // skip comma
        s = i;
        if i == n {
            // trailing comma: one more empty field
            if k == N {
                return false;
            }
            out[k] = &line[n..n];
            k += 1;
            break;
        }
    }
    k == N
}

#[inline]
fn all_digits(f: &[u8]) -> bool {
    !f.is_empty() && f.iter().all(|c| c.is_ascii_digit())
}

#[inline]
fn d2(f: &[u8], i: usize) -> Option<i64> {
    let a = f[i].wrapping_sub(b'0');
    let b = f[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        return None;
    }
    Some((a * 10 + b) as i64)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// F1 / F2 / F3 -> epoch ms. None = bad timestamp (F3 beyond i64 included).
#[inline]
fn parse_ts(f: &[u8]) -> Option<i64> {
    let n = f.len();
    if n == 0 {
        return None;
    }
    if f[0].is_ascii_digit() && (n < 5 || f[4] != b'-') {
        // F3 candidate
        if !all_digits(f) {
            return None;
        }
        let mut i = 0;
        while i < n && f[i] == b'0' {
            i += 1;
        }
        let r = &f[i..];
        if r.len() > 19 {
            return None;
        }
        let mut v: u64 = 0;
        for &c in r {
            v = v * 10 + (c - b'0') as u64;
        }
        if v > i64::MAX as u64 {
            return None;
        }
        return Some(v as i64);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if f[4] != b'-' || f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
        return None;
    }
    let y = d2(f, 0)? * 100 + d2(f, 2)?;
    let mo = d2(f, 5)?;
    let d = d2(f, 8)?;
    let h = d2(f, 11)?;
    let mi = d2(f, 14)?;
    let s = d2(f, 17)?;
    let off = if n == 20 {
        if f[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match f[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if f[22] != b':' {
            return None;
        }
        let oh = d2(f, 20)?;
        let om = d2(f, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 60 + om) * 60_000
    };
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some((days_from_civil(y, mo, d) * DAY + h * 3_600_000 + mi * 60_000 + s * 1000) - off)
}

/// purchase price: digits only, 1..=10_000_000
#[inline]
fn parse_price(f: &[u8]) -> Option<u32> {
    if !all_digits(f) {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let r = &f[i..];
    if r.is_empty() || r.len() > 8 {
        return None;
    }
    let mut v: u32 = 0;
    for &c in r {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- ID table

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct SPtr {
    off: u64, // file << 40 | offset
    len: u32,
}

struct Files {
    b: Vec<Bytes>,
}
impl Files {
    #[inline]
    fn s(&self, p: SPtr) -> &[u8] {
        let fi = (p.off >> 40) as usize;
        let o = (p.off & ((1 << 40) - 1)) as usize;
        &self.b[fi].get()[o..o + p.len as usize]
    }
}

#[inline]
fn sptr(fi: usize, base: &[u8], f: &[u8]) -> SPtr {
    let o = f.as_ptr() as usize - base.as_ptr() as usize;
    SPtr { off: ((fi as u64) << 40) | o as u64, len: f.len() as u32 }
}

struct Arenas {
    a: Vec<Vec<u8>>,
    _f: Files,
}
impl Arenas {
    #[inline]
    fn s(&self, p: SPtr) -> &[u8] {
        let o = (p.off & ((1 << 40) - 1)) as usize;
        &self.a[(p.off >> 40) as usize][o..o + p.len as usize]
    }
}

struct IdTable {
    mask: usize,
    kv: Vec<AtomicU64>, // [2i] = lo, [2i+1] = hi (same cache line)
    ptr: Vec<AtomicU64>,
    len: Vec<std::sync::atomic::AtomicU32>,
    count: AtomicUsize,
    limit: usize,
    overflow: AtomicBool,
}

impl IdTable {
    fn new(cap: usize) -> Self {
        let mk = |n: usize| -> Vec<AtomicU64> {
            let mut v = std::mem::ManuallyDrop::new(vec![0u64; n]);
            unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
        };
        IdTable {
            mask: cap - 1,
            kv: mk(2 * cap),
            ptr: mk(cap),
            len: {
                let mut v = std::mem::ManuallyDrop::new(vec![0u32; cap]);
                unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut std::sync::atomic::AtomicU32, v.len(), v.capacity()) }
            },
            count: AtomicUsize::new(0),
            limit: cap / 10 * 6,
            overflow: AtomicBool::new(false),
        }
    }
    #[inline]
    fn insert(&self, s: &[u8], arena: &mut Vec<u8>, w: usize) -> u32 {
        let h = xxh3_128(s);
        let lo = (h as u64) | 1;
        let hi = ((h >> 64) as u64) | 1;
        let mut i = (hi as usize) & self.mask;
        let mut probes = 0usize;
        loop {
            let mut k = self.kv[2 * i].load(Ordering::Acquire);
            if k == 0 {
                match self.kv[2 * i].compare_exchange(0, lo, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        self.ptr[i].store(((w as u64) << 40) | arena.len() as u64, Ordering::Relaxed);
                        self.len[i].store(s.len() as u32, Ordering::Relaxed);
                        arena.extend_from_slice(s);
                        self.kv[2 * i + 1].store(hi, Ordering::Release);
                        let c = self.count.fetch_add(1, Ordering::Relaxed);
                        if c >= self.limit {
                            self.overflow.store(true, Ordering::Relaxed);
                        }
                        return i as u32;
                    }
                    Err(a) => k = a,
                }
            }
            if k == lo {
                let mut h2 = self.kv[2 * i + 1].load(Ordering::Acquire);
                while h2 == 0 {
                    std::hint::spin_loop();
                    h2 = self.kv[2 * i + 1].load(Ordering::Acquire);
                }
                if h2 == hi {
                    return i as u32;
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            if probes > self.mask {
                self.overflow.store(true, Ordering::Relaxed);
                return 0;
            }
        }
    }
    #[inline]
    fn sptr(&self, i: u32) -> SPtr {
        SPtr {
            off: self.ptr[i as usize].load(Ordering::Relaxed),
            len: self.len[i as usize].load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
struct Ev {
    h: u128,
    ing: i64,
    ts: i64,
    user: u32,
    price: u32, // 0 unless purchase
    cat: u16,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Ck {
    user: u32,
    camp: u16,
    ts: i64,
    pre: u64,
    idh: u64,
    id: SPtr,
}

#[derive(Clone, Copy)]
struct PEv {
    key: u64,
    price: u32,
    cat: u16,
}

#[derive(Clone, Copy)]
struct PCk {
    key: u64,
    pre: u64, // first 8 bytes of click_id, big-endian, zero padded
    idh: u64, // xxh3_64(click_id): equal => same click (copies are identical)
    camp: u16,
    id: SPtr,
}

#[inline]
fn prefix8(s: &[u8]) -> u64 {
    let mut b = [0u8; 8];
    let n = s.len().min(8);
    b[..n].copy_from_slice(&s[..n]);
    u64::from_be_bytes(b)
}

#[derive(Default)]
struct ParseOut {
    arena: Vec<u8>,
    evb: Vec<Vec<Ev>>,
    cks: Vec<Ck>,
    links: Vec<(u32, u32)>,
    nlines: [u64; 3],
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Ev,
    Ck,
    Ln,
}

struct Chunk {
    kind: Kind,
    fi: usize,
    s: usize,
    e: usize,
}

struct Ctx<'a> {
    files: &'a Files,
    tab: &'a IdTable,
    products: &'a FxHashMap<&'a [u8], u16>,
    unknown_cat: u16,
    camps: &'a FxHashMap<&'a [u8], u16>,
    ws: i64,
    we: i64,
    cws: i64,
}

fn parse_chunk(cx: &Ctx, c: &Chunk, w: usize, out: &mut ParseOut) {
    let base = cx.files.b[c.fi].get();
    let data = &base[c.s..c.e];
    let mut pos = 0usize;
    let n = data.len();
    match c.kind {
        Kind::Ev => {
            let mut f: [&[u8]; 7] = [&[]; 7];
            while pos < n {
                let end = memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(n);
                let line = &data[pos..end];
                pos = end + 1;
                out.nlines[0] += 1;
                if !split::<7>(line, &mut f) || f[0].is_empty() {
                    continue;
                }
                let h = xxh3_128(f[0]);
                let ing = parse_ts(f[3]).unwrap_or(i64::MAX);
                let mut ok = true;
                let ts = match parse_ts(f[2]) {
                    Some(t) if t >= cx.ws && t < cx.we => t,
                    _ => {
                        ok = false;
                        0
                    }
                };
                let et = f[4];
                let is_p = et == b"purchase";
                if !(is_p || et == b"view" || et == b"cart") {
                    ok = false;
                }
                if f[1].is_empty() || f[5].is_empty() {
                    ok = false;
                }
                let mut price = 0u32;
                if ok && is_p {
                    match parse_price(f[6]) {
                        Some(p) => price = p,
                        None => ok = false,
                    }
                }
                if ok && ing - ts > LATE {
                    ok = false;
                }
                let mut user = 0u32;
                let mut cat = 0u16;
                if ok {
                    user = cx.tab.insert(f[1], &mut out.arena, w);
                    cat = *cx.products.get(f[5]).unwrap_or(&cx.unknown_cat);
                }
                let b = (h >> (128 - DB_BITS)) as usize;
                out.evb[b].push(Ev { h, ing, ts, user, price, cat, ok });
            }
        }
        Kind::Ck => {
            let mut f: [&[u8]; 4] = [&[]; 4];
            while pos < n {
                let end = memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(n);
                let line = &data[pos..end];
                pos = end + 1;
                out.nlines[1] += 1;
                if !split::<4>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    continue;
                }
                let ts = match parse_ts(f[3]) {
                    Some(t) if t >= cx.cws && t < cx.we => t,
                    _ => continue,
                };
                let camp = match cx.camps.get(f[2]) {
                    Some(&c) => c,
                    None => continue,
                };
                let user = cx.tab.insert(f[1], &mut out.arena, w);
                out.cks.push(Ck {
                    user,
                    camp,
                    ts,
                    pre: prefix8(f[0]),
                    idh: xxhash_rust::xxh3::xxh3_64(f[0]),
                    id: sptr(c.fi, base, f[0]),
                });
            }
        }
        Kind::Ln => {
            let mut f: [&[u8]; 2] = [&[]; 2];
            while pos < n {
                let end = memchr(b'\n', &data[pos..]).map(|x| pos + x).unwrap_or(n);
                let line = &data[pos..end];
                pos = end + 1;
                out.nlines[2] += 1;
                if !split::<2>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                    continue;
                }
                let a = cx.tab.insert(f[0], &mut out.arena, w);
                let b = cx.tab.insert(f[1], &mut out.arena, w);
                if a != b {
                    out.links.push((a, b));
                }
            }
        }
    }
}

#[inline]
fn memchr(c: u8, s: &[u8]) -> Option<usize> {
    memchr::memchr(c, s)
}

/// Data lines of a small clean file (header skipped), split into N fields.
fn small_table<const N: usize>(bytes: &[u8]) -> Vec<[&[u8]; N]> {
    let mut v = Vec::new();
    let mut it = bytes.split(|&c| c == b'\n');
    it.next(); // header
    let body_ends_nl = bytes.last() == Some(&b'\n');
    let lines: Vec<&[u8]> = it.collect();
    let nl = if body_ends_nl && !lines.is_empty() { lines.len() - 1 } else { lines.len() };
    for line in &lines[..nl] {
        let mut f: [&[u8]; N] = [&[]; N];
        if split::<N>(line, &mut f) && !f[0].is_empty() {
            v.push(f);
        }
    }
    v
}

// ---------------------------------------------------------------- output rows

struct UdRow {
    name: SPtr,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u16,
}

struct CampAcc {
    // [(camp) * ndays + d] ; camp == ncamp is organic
    sessions: Vec<u64>,
    purch: Vec<u64>,
    revenue: Vec<i64>,
    assisted: Vec<u64>,
}
impl CampAcc {
    fn new(n: usize) -> Self {
        CampAcc { sessions: vec![0; n], purch: vec![0; n], revenue: vec![0; n], assisted: vec![0; n] }
    }
}

struct Worker {
    acc: CampAcc,
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    purch: Vec<(u16, u32)>,
    ev_buf: Vec<PEv>,
    ck_buf: Vec<PCk>,
}

// ---------------------------------------------------------------- main

fn env_usize(k: &str) -> Option<usize> {
    std::env::var(k).ok().and_then(|v| v.trim().parse().ok())
}

fn manifest_ts(text: &str, key: &str) -> i64 {
    let k = format!("\"{key}\"");
    let i = text.find(&k).unwrap_or_else(|| panic!("manifest missing {key}")) + k.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes()).unwrap_or_else(|| panic!("bad manifest ts {key}"))
}

fn main() {
    T0.set(Instant::now()).ok();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nthreads = env_usize("NCPU").unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)).clamp(1, 30);
    rayon::ThreadPoolBuilder::new().num_threads(nthreads).build_global().unwrap();

    let man = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");
    let cws = ws - WEEK;
    assert!(we > ws, "empty window");

    // ---- files
    let mut paths: Vec<(Kind, PathBuf)> = vec![];
    for p in csvs(&data.join("events")) {
        paths.push((Kind::Ev, p));
    }
    for p in csvs(&data.join("clicks")) {
        paths.push((Kind::Ck, p));
    }
    for p in csvs(&data.join("links")) {
        paths.push((Kind::Ln, p));
    }
    let prod_i = paths.len();
    let mut all: Vec<PathBuf> = paths.iter().map(|x| x.1.clone()).collect();
    all.push(data.join("products.csv"));
    all.push(data.join("campaigns.csv"));
    let files = Files { b: all.par_iter().map(|p| open(p)).collect() };
    let total: usize = files.b.iter().map(|b| b.get().len()).sum();
    log!("{} csv files, {:.1} MB, {} threads", all.len(), total as f64 / 1e6, nthreads);

    // ---- products / campaigns
    let prows = small_table::<2>(files.b[prod_i].get());
    let mut catnames: Vec<&[u8]> = vec![];
    let mut catidx: FxHashMap<&[u8], u16> = FxHashMap::default();
    let mut products: FxHashMap<&[u8], u16> = FxHashMap::default();
    for r in &prows {
        let ci = *catidx.entry(r[1]).or_insert_with(|| {
            catnames.push(r[1]);
            (catnames.len() - 1) as u16
        });
        products.entry(r[0]).or_insert(ci);
    }
    let unknown_cat = match catidx.get(&b"unknown"[..]) {
        Some(&c) => c,
        None => {
            catnames.push(b"unknown");
            (catnames.len() - 1) as u16
        }
    };
    // rank of each category by bytes (for top_category ties)
    let mut order: Vec<u16> = (0..catnames.len() as u16).collect();
    order.sort_by(|&a, &b| catnames[a as usize].cmp(catnames[b as usize]));
    let mut cat_rank = vec![0u16; catnames.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }
    let crows = small_table::<2>(files.b[prod_i + 1].get());
    let mut camps: FxHashMap<&[u8], u16> = FxHashMap::default();
    let mut camp_names: Vec<(&[u8], &[u8])> = vec![];
    for r in &crows {
        if !camps.contains_key(r[0]) {
            camps.insert(r[0], camp_names.len() as u16);
            camp_names.push((r[0], r[1]));
        }
    }
    let ncamp = camp_names.len();

    // ---- chunks
    const CH: usize = 4 << 20;
    let mut chunks: Vec<Chunk> = vec![];
    for (fi, (kind, _)) in paths.iter().enumerate() {
        let b = files.b[fi].get();
        let mut s = match memchr(b'\n', b) {
            Some(i) => i + 1,
            None => continue, // header only (or nothing)
        };
        while s < b.len() {
            let mut e = (s + CH).min(b.len());
            if e < b.len() {
                e = match memchr(b'\n', &b[e..]) {
                    Some(i) => e + i + 1,
                    None => b.len(),
                };
            }
            // chunk [s, e): lines; strip the final '\n' so it does not create an empty line
            let ee = if b[e - 1] == b'\n' { e - 1 } else { e };
            chunks.push(Chunk { kind: *kind, fi, s, e: ee });
            s = e;
        }
    }
    // big chunks first for balance: events then clicks then links is fine since sizes equal
    let mut cap = (total / 500).next_power_of_two().max(1 << 16);
    let nworkers = nthreads;
    let ev_bytes: usize = paths.iter().enumerate().filter(|(_, p)| p.0 == Kind::Ev).map(|(i, _)| files.b[i].get().len()).sum();
    // expected events per (thread, bucket), +25% slack; rows are ~70 bytes
    let per_bucket = ev_bytes / 70 / nworkers / NDB * 5 / 4 + 16;
    let (tab, outs) = loop {
        let tab = IdTable::new(cap);
        let outs: Vec<Mutex<ParseOut>> = (0..nworkers + 1)
            .map(|_| {
                Mutex::new(ParseOut {
                    evb: (0..NDB).map(|_| Vec::with_capacity(per_bucket)).collect(),
                    ..Default::default()
                })
            })
            .collect();
        {
            let cx = Ctx {
                files: &files,
                tab: &tab,
                products: &products,
                unknown_cat,
                camps: &camps,
                ws,
                we,
                cws,
            };
            chunks.par_iter().for_each(|c| {
                let w = rayon::current_thread_index().unwrap_or(nworkers);
                let mut o = outs[w].lock().unwrap();
                parse_chunk(&cx, c, w, &mut o);
            });
        }
        if tab.overflow.load(Ordering::Relaxed) {
            log!("id table overflow at cap {cap}; retrying");
            cap *= 4;
            continue;
        }
        break (tab, outs);
    };
    let mut outs: Vec<ParseOut> = outs.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let arenas = Files { b: vec![] };
    let arenas = Arenas { a: outs.iter_mut().map(|o| std::mem::take(&mut o.arena)).collect(), _f: arenas };
    let nids = tab.count.load(Ordering::Relaxed);
    let nl: [u64; 3] = outs.iter().fold([0; 3], |a, o| [a[0] + o.nlines[0], a[1] + o.nlines[1], a[2] + o.nlines[2]]);
    log!("parsed: {} event lines, {} click lines, {} link lines; {} ids (cap {})", nl[0], nl[1], nl[2], nids, cap);

    // ---- union-find
    let mut parent: Vec<u32> = (0..cap as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    let links: Vec<(u32, u32)> = outs.iter_mut().flat_map(|o| std::mem::take(&mut o.links)).collect();
    log!("links collected: {}", links.len());
    for &(a, b) in &links {
        let ra = find(&mut parent, a);
        let rb = find(&mut parent, b);
        if ra != rb {
            if ra < rb {
                parent[rb as usize] = ra;
            } else {
                parent[ra as usize] = rb;
            }
        }
    }
    for i in 0..cap {
        let p = parent[i];
        parent[i] = parent[p as usize]; // parents have smaller index => already final
    }
    let root = parent;
    log!("unions done");
    // person name = byte-wise smallest id in component
    let name: Vec<std::sync::atomic::AtomicU32> = (0..cap as u32).map(std::sync::atomic::AtomicU32::new).collect();
    links.par_iter().for_each(|&(a, b)| {
        for x in [a, b] {
            let r = root[x as usize] as usize;
            let sx = arenas.s(tab.sptr(x));
            let mut cur = name[r].load(Ordering::Relaxed);
            while cur != x && sx < arenas.s(tab.sptr(cur)) {
                match name[r].compare_exchange_weak(cur, x, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(c) => cur = c,
                }
            }
        }
    });
    let name: Vec<u32> = name.into_iter().map(|a| a.into_inner()).collect();
    drop(links);
    log!("union-find done");

    // ---- key layout
    let span = (we - cws) as u64;
    let tbits = 64 - span.leading_zeros();
    let pbits = 64 - (cap as u64).leading_zeros();
    assert!(tbits + pbits <= 64, "window too long for key packing");
    let tmask = (1u64 << tbits) - 1;
    #[inline]
    fn pbucket(r: u32) -> usize {
        (r.wrapping_mul(0x9E37_79B1) >> (32 - PB_BITS)) as usize
    }

    // ---- dedup + route events to person buckets
    let evbs: Vec<Vec<Vec<Ev>>> = outs.iter_mut().map(|o| std::mem::take(&mut o.evb)).collect();
    let per_pb = ev_bytes / 70 / nworkers / NPB * 5 / 4 + 16;
    let routed: Vec<Mutex<Vec<Vec<PEv>>>> =
        (0..nworkers + 1).map(|_| Mutex::new((0..NPB).map(|_| Vec::with_capacity(per_pb)).collect())).collect();
    let nsurv = AtomicUsize::new(0);
    let dd_scratch: Vec<Mutex<Vec<u64>>> = (0..nworkers + 1).map(|_| Mutex::new(Vec::new())).collect();
    let cks: Vec<Vec<Ck>> = outs.iter_mut().map(|o| std::mem::take(&mut o.cks)).collect();
    let ck_total: usize = cks.iter().map(|v| v.len()).sum();
    let per_pbc = ck_total / nworkers / NPB * 5 / 4 + 16;
    let routed_ck: Vec<Mutex<Vec<Vec<PCk>>>> =
        (0..nworkers + 1).map(|_| Mutex::new((0..NPB).map(|_| Vec::with_capacity(per_pbc)).collect())).collect();
    let route_clicks = || {
        cks.par_iter().flat_map(|v| v.par_chunks(1 << 16)).for_each(|part| {
            let w = rayon::current_thread_index().unwrap_or(nworkers);
            let mut r = routed_ck[w].lock().unwrap();
            for c in part {
                let p = root[c.user as usize];
                r[pbucket(p)].push(PCk {
                    key: ((p as u64) << tbits) | (c.ts - cws) as u64,
                    pre: c.pre,
                    idh: c.idh,
                    camp: c.camp,
                    id: c.id,
                });
            }
        });
    };
    rayon::join(route_clicks, || (0..NDB).into_par_iter().for_each(|b| {
        // group copies by event_id hash with an L2-sized open-addressing table:
        // slot = tag32 << 32 | t << 27 | index; keeps the copy with the smallest ingest_ts
        let t0 = Instant::now();
        let n: usize = evbs.iter().map(|t| t[b].len()).sum();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let w = rayon::current_thread_index().unwrap_or(nworkers);
        let mut tabg = dd_scratch[w].lock().unwrap();
        tabg.clear();
        tabg.resize(cap, 0);
        let tab = &mut *tabg;
        let get = |s: u64| -> &Ev { &evbs[((s >> 27) & 31) as usize][b][(s & ((1 << 27) - 1)) as usize] };
        for (t, part) in evbs.iter().enumerate() {
            for (k, e) in part[b].iter().enumerate() {
                let tag = (((e.h >> 32) as u32) | 1) as u64;
                let me = (tag << 32) | ((t as u64) << 27) | k as u64;
                let mut i = (e.h as usize) & mask;
                loop {
                    let s = tab[i];
                    if s == 0 {
                        tab[i] = me;
                        break;
                    }
                    if s >> 32 == tag {
                        let o = get(s);
                        if o.h == e.h {
                            if e.ing < o.ing {
                                tab[i] = me;
                            }
                            break;
                        }
                    }
                    i = (i + 1) & mask;
                }
            }
        }
        let t1 = Instant::now();
        DD_A.fetch_add((t1 - t0).as_nanos() as u64, Ordering::Relaxed);
        let mut r = routed[w].lock().unwrap();
        let mut cnt = 0;
        // second pass in source order (sequential reads): keep a record iff it is its group's winner
        for (t, part) in evbs.iter().enumerate() {
            for (k, e) in part[b].iter().enumerate() {
                if !e.ok {
                    continue;
                }
                let tag = (((e.h >> 32) as u32) | 1) as u64;
                let me = (tag << 32) | ((t as u64) << 27) | k as u64;
                let mut i = (e.h as usize) & mask;
                let win = loop {
                    let s = tab[i];
                    if s == me {
                        break true;
                    }
                    if s >> 32 == tag && get(s).h == e.h {
                        break false;
                    }
                    i = (i + 1) & mask;
                };
                if win {
                    let p = root[e.user as usize];
                    r[pbucket(p)].push(PEv {
                        key: ((p as u64) << tbits) | (e.ts - cws) as u64,
                        price: e.price,
                        cat: e.cat,
                    });
                    cnt += 1;
                }
            }
        }
        nsurv.fetch_add(cnt, Ordering::Relaxed);
        DD_B.fetch_add(t1.elapsed().as_nanos() as u64, Ordering::Relaxed);
    }));
    std::mem::forget(cks);
    log!("dd build {:.3}s route {:.3}s (thread-sum)", DD_A.load(Ordering::Relaxed) as f64 / 1e9, DD_B.load(Ordering::Relaxed) as f64 / 1e9);
    std::mem::forget(evbs);
    log!("dedup: {} surviving events", nsurv.load(Ordering::Relaxed));

    let routed: Vec<Vec<Vec<PEv>>> = routed.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let routed_ck: Vec<Vec<Vec<PCk>>> = routed_ck.into_iter().map(|m| m.into_inner().unwrap()).collect();
    log!("routed");

    // ---- per person bucket
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1) as usize;
    let ncs = (ncamp + 1) * ndays;
    let workers: Vec<Mutex<Worker>> = (0..nworkers + 1)
        .map(|_| {
            Mutex::new(Worker {
                acc: CampAcc::new(ncs),
                cnt: vec![0; ncamp],
                pos: vec![0; ncamp],
                active: vec![],
                purch: vec![],
                ev_buf: vec![],
                ck_buf: vec![],
            })
        })
        .collect();
    let files_r = &files;
    let tab_r = &tab;
    let name_r = &name;
    let cat_rank_r = &cat_rank;
    let ud_parts: Vec<Vec<UdRow>> = (0..NPB)
        .into_par_iter()
        .map(|b| {
            let w = rayon::current_thread_index().unwrap_or(nworkers);
            let mut wk = workers[w].lock().unwrap();
            let wk = &mut *wk;
            // per-thread scratch buffers, reused across buckets (no fresh-page faults)
            let mut ev = std::mem::take(&mut wk.ev_buf);
            let mut ck = std::mem::take(&mut wk.ck_buf);
            ev.clear();
            ck.clear();
            for t in &routed {
                ev.extend_from_slice(&t[b]);
            }
            ev.sort_unstable_by_key(|e| e.key);
            for t in &routed_ck {
                ck.extend_from_slice(&t[b]);
            }
            ck.sort_unstable_by(|x, y| {
                x.key.cmp(&y.key).then_with(|| {
                    x.pre.cmp(&y.pre).then_with(|| {
                        if x.idh == y.idh {
                            std::cmp::Ordering::Equal
                        } else {
                            files_r.s(x.id).cmp(files_r.s(y.id))
                        }
                    })
                })
            });
            let mut rows: Vec<UdRow> = Vec::new();
            let mut i = 0usize;
            let mut cj = 0usize;
            while i < ev.len() {
                let p = ev[i].key >> tbits;
                let mut iend = i + 1;
                while iend < ev.len() && (ev[iend].key >> tbits) == p {
                    iend += 1;
                }
                while cj < ck.len() && (ck[cj].key >> tbits) < p {
                    cj += 1;
                }
                let mut cend = cj;
                while cend < ck.len() && (ck[cend].key >> tbits) == p {
                    cend += 1;
                }
                person(
                    &ev[i..iend],
                    &ck[cj..cend],
                    tmask,
                    cws,
                    ws_day,
                    ndays,
                    ncamp,
                    tab_r.sptr(name_r[p as usize]),
                    cat_rank_r,
                    wk,
                    &mut rows,
                );
                i = iend;
                cj = cend;
            }
            wk.ev_buf = ev;
            wk.ck_buf = ck;
            rows
        })
        .collect();
    std::mem::forget(routed);
    std::mem::forget(routed_ck);
    let nud: usize = ud_parts.iter().map(|r| r.len()).sum();
    log!("sessions done: {} user_daily rows", nud);

    // ---- campaign_daily
    let mut acc = CampAcc::new(ncs);
    for w in workers {
        let w = w.into_inner().unwrap();
        for k in 0..ncs {
            acc.sessions[k] += w.acc.sessions[k];
            acc.purch[k] += w.acc.purch[k];
            acc.revenue[k] += w.acc.revenue[k];
            acc.assisted[k] += w.acc.assisted[k];
        }
    }

    write_outputs(&out, &arenas, ud_parts, &catnames, &acc, &camp_names, ndays, ws_day);
    log!("done");
}

#[allow(clippy::too_many_arguments)]
fn person(
    ev: &[PEv],
    ck: &[PCk],
    tmask: u64,
    cws: i64,
    ws_day: i64,
    ndays: usize,
    ncamp: usize,
    name: SPtr,
    cat_rank: &[u16],
    wk: &mut Worker,
    rows: &mut Vec<UdRow>,
) {
    let t = |k: u64| (k & tmask) as i64 + cws;
    let n = ev.len();
    let m = ck.len();
    let mut i = 0usize;
    let mut jhi = 0usize;
    let mut jlo = 0usize;
    // current user_daily accumulation
    let mut cur_day = i64::MIN;
    let mut ud = UdRow { name, day: 0, sessions: 0, events: 0, revenue: 0, top: NO_CAT };
    wk.purch.clear();
    let flush = |ud: &mut UdRow, purch: &mut Vec<(u16, u32)>, rows: &mut Vec<UdRow>| {
        if ud.sessions == 0 {
            return;
        }
        let mut top = NO_CAT;
        if !purch.is_empty() {
            purch.sort_unstable_by_key(|x| x.0);
            let mut best_sum = -1i64;
            let mut k = 0;
            while k < purch.len() {
                let c = purch[k].0;
                let mut s = 0i64;
                while k < purch.len() && purch[k].0 == c {
                    s += purch[k].1 as i64;
                    k += 1;
                }
                if s > best_sum || (s == best_sum && cat_rank[c as usize] < cat_rank[top as usize]) {
                    best_sum = s;
                    top = c;
                }
            }
            purch.clear();
        }
        rows.push(UdRow { top, ..*ud });
        ud.sessions = 0;
        ud.events = 0;
        ud.revenue = 0;
    };
    while i < n {
        let start = t(ev[i].key);
        let mut k = i + 1;
        let mut prev = start;
        let mut revenue = 0i64;
        let mut has_p = false;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            flush(&mut ud, &mut wk.purch, rows);
            cur_day = day;
            ud.day = day as i32;
        }
        {
            let e = ev[i];
            if e.price > 0 {
                revenue += e.price as i64;
                has_p = true;
                wk.purch.push((e.cat, e.price));
            }
        }
        while k < n {
            let tk = t(ev[k].key);
            if tk - prev > GAP {
                break;
            }
            prev = tk;
            let e = ev[k];
            if e.price > 0 {
                revenue += e.price as i64;
                has_p = true;
                wk.purch.push((e.cat, e.price));
            }
            k += 1;
        }
        let cnt = (k - i) as u32;
        ud.sessions += 1;
        ud.events += cnt;
        ud.revenue += revenue;
        // clicks: add ts <= start, remove ts < start - WEEK
        while jhi < m && t(ck[jhi].key) <= start {
            let c = ck[jhi].camp as usize;
            if wk.cnt[c] == 0 {
                wk.pos[c] = wk.active.len() as u32;
                wk.active.push(c as u16);
            }
            wk.cnt[c] += 1;
            jhi += 1;
        }
        while jlo < jhi && t(ck[jlo].key) < start - WEEK {
            let c = ck[jlo].camp as usize;
            wk.cnt[c] -= 1;
            if wk.cnt[c] == 0 {
                let p = wk.pos[c] as usize;
                let last = *wk.active.last().unwrap();
                wk.active[p] = last;
                wk.pos[last as usize] = p as u32;
                wk.active.pop();
            }
            jlo += 1;
        }
        let d = (day - ws_day) as usize;
        let credit = if jhi > jlo {
            let tt = ck[jhi - 1].key;
            let mut q = jhi - 1;
            while q > jlo && ck[q - 1].key == tt {
                q -= 1;
            }
            ck[q].camp as usize
        } else {
            ncamp
        };
        let ix = credit * ndays + d;
        wk.acc.sessions[ix] += 1;
        wk.acc.revenue[ix] += revenue;
        if has_p {
            wk.acc.purch[ix] += 1;
        }
        for &c in &wk.active {
            wk.acc.assisted[c as usize * ndays + d] += 1;
        }
        i = k;
    }
    flush(&mut ud, &mut wk.purch, rows);
    // reset active set
    for &c in &wk.active {
        wk.cnt[c as usize] = 0;
    }
    wk.active.clear();
}

#[allow(clippy::too_many_arguments)]
fn write_outputs(
    out: &Path,
    files: &Arenas,
    ud_parts: Vec<Vec<UdRow>>,
    catnames: &[&[u8]],
    acc: &CampAcc,
    camp_names: &[(&[u8], &[u8])],
    ndays: usize,
    ws_day: i64,
) {
    use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let props = || {
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .set_max_row_group_row_count(Some(1 << 20))
            .build()
    };
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    fs::create_dir_all(&cd_dir).unwrap();

    let ud_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]));
    // group buckets into ~nthreads files
    let nfiles = rayon::current_num_threads().max(1);
    let per = ud_parts.len().div_ceil(nfiles);
    let groups: Vec<&[Vec<UdRow>]> = ud_parts.chunks(per.max(1)).collect();
    groups.par_iter().enumerate().for_each(|(gi, g)| {
        let n: usize = g.iter().map(|r| r.len()).sum();
        let nb: usize = g.iter().flat_map(|r| r.iter()).map(|r| r.name.len as usize).sum();
        let mut uid = StringBuilder::with_capacity(n, nb);
        let mut day = Date32Builder::with_capacity(n);
        let mut ses = Int64Builder::with_capacity(n);
        let mut evs = Int64Builder::with_capacity(n);
        let mut rev = Int64Builder::with_capacity(n);
        let mut top = StringBuilder::with_capacity(n, n * 8);
        for r in g.iter().flat_map(|r| r.iter()) {
            uid.append_value(unsafe { std::str::from_utf8_unchecked(files.s(r.name)) });
            day.append_value(r.day);
            ses.append_value(r.sessions as i64);
            evs.append_value(r.events as i64);
            rev.append_value(r.revenue);
            if r.top == NO_CAT {
                top.append_null();
            } else {
                top.append_value(unsafe { std::str::from_utf8_unchecked(catnames[r.top as usize]) });
            }
        }
        let cols: Vec<ArrayRef> = vec![
            Arc::new(uid.finish()),
            Arc::new(day.finish()),
            Arc::new(ses.finish()),
            Arc::new(evs.finish()),
            Arc::new(rev.finish()),
            Arc::new(top.finish()),
        ];
        let batch = RecordBatch::try_new(ud_schema.clone(), cols).unwrap();
        let f = fs::File::create(ud_dir.join(format!("part-{gi:05}.parquet"))).unwrap();
        let mut w = ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 22, f), ud_schema.clone(), Some(props())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    });
    if groups.is_empty() {
        let f = fs::File::create(ud_dir.join("part-00000.parquet")).unwrap();
        let w = ArrowWriter::try_new(f, ud_schema.clone(), Some(props())).unwrap();
        w.close().unwrap();
    }

    let cd_schema = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Utf8, false),
        Field::new("channel", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("purchasing_sessions", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("assisted_sessions", DataType::Int64, false),
    ]));
    let mut cid = StringBuilder::new();
    let mut chn = StringBuilder::new();
    let mut day = Date32Builder::new();
    let mut ses = Int64Builder::new();
    let mut pur = Int64Builder::new();
    let mut rev = Int64Builder::new();
    let mut ast = Int64Builder::new();
    let ncamp = camp_names.len();
    for c in 0..=ncamp {
        let (id, ch): (&[u8], &[u8]) = if c == ncamp { (b"organic", b"organic") } else { camp_names[c] };
        for d in 0..ndays {
            let k = c * ndays + d;
            if acc.sessions[k] == 0 && acc.assisted[k] == 0 {
                continue;
            }
            cid.append_value(unsafe { std::str::from_utf8_unchecked(id) });
            chn.append_value(unsafe { std::str::from_utf8_unchecked(ch) });
            day.append_value((ws_day + d as i64) as i32);
            ses.append_value(acc.sessions[k] as i64);
            pur.append_value(acc.purch[k] as i64);
            rev.append_value(acc.revenue[k]);
            ast.append_value(acc.assisted[k] as i64);
        }
    }
    let cols: Vec<ArrayRef> = vec![
        Arc::new(cid.finish()),
        Arc::new(chn.finish()),
        Arc::new(day.finish()),
        Arc::new(ses.finish()),
        Arc::new(pur.finish()),
        Arc::new(rev.finish()),
        Arc::new(ast.finish()),
    ];
    let batch = RecordBatch::try_new(cd_schema.clone(), cols).unwrap();
    let f = fs::File::create(cd_dir.join("part-00000.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, cd_schema, Some(props())).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}
