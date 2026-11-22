//! Shopping Sessions engine v2 (Rust). See ../SPEC.md and ../reference/rules.md.
//!
//! Phases:
//!  A. parse links (parallel), intern IDs in a lock-free table, union-find -> person root per ID,
//!     person name = byte-wise smallest ID of the component.
//!  B. parse events + clicks (parallel). Every row already knows its person root. Events go to
//!     event-id buckets (for dedup); valid clicks go straight to person buckets.
//!  C. per event-id bucket: keep the copy with the smallest ingest_ts; valid winners -> person buckets.
//!  D. per person bucket: sort, sessionize, credit, assists, user_daily rows, campaign counters.
//!  E. write Parquet.
//!
//! IDs of <= 15 bytes are keyed by their exact bytes (16-byte key incl. length): no hashing, exact.
//! Longer IDs use a 128-bit xxh3 hash (marked so they never equal a short key).
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use memmap2::Mmap;
use rayon::prelude::*;
use xxhash_rust::xxh3::xxh3_128;

const DAY: i64 = 86_400_000;
const GAP: i64 = 1_800_000;
const WEEK: i64 = 7 * DAY;
const LATE: i64 = DAY;
const DB_BITS: u32 = 8; // event-id (dedup) buckets
const NDB: usize = 1 << DB_BITS;
const PB_BITS: u32 = 9; // person buckets
const NPB: usize = 1 << PB_BITS;
const NO_CAT: u16 = u16::MAX;
const CH: usize = 1 << 20; // parse chunk bytes
const PF: usize = 8; // rows in flight between id-slot prefetch and insert (hides DRAM latency)

static STATS: AtomicBool = AtomicBool::new(false);
static SI: [AtomicU64; 4] = [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)];
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PopMode {
    Map,   // MAP_POPULATE whole files at open (per-file, serial within a file)
    Chunk, // MADV_POPULATE_READ per parse chunk (parallel)
    None,  // plain page faults
}
fn popmode() -> PopMode {
    static M: std::sync::OnceLock<PopMode> = std::sync::OnceLock::new();
    *M.get_or_init(|| match std::env::var("SS_POPMODE").as_deref() {
        Ok("chunk") => PopMode::Chunk,
        Ok("none") => PopMode::None,
        Ok("map") => PopMode::Map,
        _ => {
            // measured on the scorer: populating buys nothing (none 1.69 s vs map 1.72 s at 20M)
            PopMode::None
        }
    })
}

fn open(path: &Path) -> Bytes {
    let f = fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    let len = f.metadata().unwrap().len();
    if len == 0 {
        return Bytes::Empty;
    }
    let mut o = memmap2::MmapOptions::new();
    #[cfg(target_os = "linux")]
    if popmode() == PopMode::Map {
        o.populate();
    }
    let m = unsafe { o.map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", path.display()));
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

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct SPtr {
    off: u64, // file-or-thread << 40 | offset
    len: u32,
}

struct Files {
    b: Vec<Bytes>,
}
impl Files {
    #[inline]
    fn s(&self, p: SPtr) -> &[u8] {
        let o = (p.off & ((1 << 40) - 1)) as usize;
        &self.b[(p.off >> 40) as usize].get()[o..o + p.len as usize]
    }
}

struct Arenas {
    a: Vec<Vec<u8>>,
}
impl Arenas {
    #[inline]
    fn s(&self, p: SPtr) -> &[u8] {
        let o = (p.off & ((1 << 40) - 1)) as usize;
        &self.a[(p.off >> 40) as usize][o..o + p.len as usize]
    }
}

// ---------------------------------------------------------------- keys

#[derive(Clone, Copy, PartialEq, Eq, Default)]
struct Key {
    lo: u64,
    hi: u64, // top byte = length (1..=15) for exact keys, 0xFF for hashed long keys
}

#[inline]
fn mask_bytes(k: usize) -> u64 {
    if k >= 8 {
        u64::MAX
    } else if k == 0 {
        0
    } else {
        u64::MAX >> (64 - 8 * k)
    }
}

/// Key of a non-empty field `f`, where `avail` = bytes readable from f.as_ptr() (>= f.len()).
#[inline]
fn key_at(f: &[u8], avail: usize) -> Key {
    let n = f.len();
    if n <= 15 {
        if avail >= 16 {
            let p = f.as_ptr();
            let w0 = unsafe { (p as *const u64).read_unaligned() }.to_le();
            let w1 = unsafe { (p.add(8) as *const u64).read_unaligned() }.to_le();
            let lo = w0 & mask_bytes(n);
            let hi = if n > 8 { w1 & mask_bytes(n - 8) } else { 0 };
            Key { lo, hi: hi | ((n as u64) << 56) }
        } else {
            let mut b = [0u8; 16];
            b[..n].copy_from_slice(f);
            Key {
                lo: u64::from_le_bytes(b[0..8].try_into().unwrap()),
                hi: u64::from_le_bytes(b[8..16].try_into().unwrap()) | ((n as u64) << 56),
            }
        }
    } else {
        let h = xxh3_128(f);
        Key { lo: h as u64, hi: ((h >> 64) as u64 & 0x00FF_FFFF_FFFF_FFFF) | (0xFFu64 << 56) }
    }
}

#[inline]
fn key(f: &[u8]) -> Key {
    key_at(f, 0)
}

#[inline]
fn mix(k: Key) -> u64 {
    let x = k.lo.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ k.hi.rotate_left(31);
    let x = (x ^ (x >> 29)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^ (x >> 32)
}

/// Small read-only exact-key map (products, campaigns).
/// Read-only exact-key map (products, campaigns): one entry array (key + value in one line),
/// 2 MB-aligned and THP-advised so random lookups stay TLB-friendly. Leaked: lives for the run.
#[derive(Clone, Copy, Default)]
struct SmEntry {
    k: Key,
    v: u16,
}
struct SmallMap {
    mask: usize,
    e: &'static mut [SmEntry],
}
fn huge_slice<T: Copy + Default + Send>(n: usize) -> &'static mut [T] {
    const HP: usize = 2 << 20;
    let bytes = (n * std::mem::size_of::<T>()).max(1).div_ceil(HP) * HP;
    let layout = std::alloc::Layout::from_size_align(bytes, HP).unwrap();
    let p = unsafe { std::alloc::alloc(layout) } as *mut T;
    assert!(!p.is_null(), "alloc");
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(p as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
    }
    struct P<T>(*mut T);
    unsafe impl<T> Sync for P<T> {}
    let pp = P(p);
    let pp = &pp;
    const CHK: usize = 1 << 16;
    (0..n.div_ceil(CHK)).into_par_iter().for_each(|c| {
        for i in c * CHK..((c + 1) * CHK).min(n) {
            unsafe { pp.0.add(i).write(T::default()) };
        }
    });
    unsafe { std::slice::from_raw_parts_mut(p, n) }
}
impl SmallMap {
    fn new(items: &[(Key, u16)]) -> Self {
        let cap = (items.len() * 2).next_power_of_two().max(16);
        let m = SmallMap { mask: cap - 1, e: huge_slice::<SmEntry>(cap) };
        for &(k, v) in items {
            let mut i = mix(k) as usize & m.mask;
            loop {
                if m.e[i].k.hi == 0 {
                    m.e[i] = SmEntry { k, v };
                    break;
                }
                if m.e[i].k == k {
                    break; // first wins
                }
                i = (i + 1) & m.mask;
            }
        }
        m
    }
    #[inline(always)]
    fn prefetch(&self, k: Key) -> usize {
        let i = mix(k) as usize & self.mask;
        prefetch_ptr(self.e.as_ptr().wrapping_add(i) as *const i8);
        i
    }
    #[inline]
    fn get(&self, k: Key) -> Option<u16> {
        self.get_at(k, mix(k) as usize & self.mask)
    }
    #[inline]
    fn get_at(&self, k: Key, i0: usize) -> Option<u16> {
        let mut i = i0;
        loop {
            let s = &self.e[i];
            if s.k == k {
                return Some(s.v);
            }
            if s.k.hi == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

#[inline(always)]
fn prefetch_ptr(p: *const i8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch(p, std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
}

// ---------------------------------------------------------------- timestamps

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

/// "YYYY-MM-DD" -> epoch day, validated.
fn parse_date(f: &[u8]) -> Option<i64> {
    let y = d2(f, 0)? * 100 + d2(f, 2)?;
    let mo = d2(f, 5)?;
    let d = d2(f, 8)?;
    if !(1..=12).contains(&mo) || d < 1 {
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
    Some(days_from_civil(y, mo, d))
}

/// 8 ASCII digits (little-endian load) -> value, or None if any byte is not a digit.
#[inline]
fn p8(b: &[u8]) -> Option<u64> {
    let x = u64::from_le_bytes(b[..8].try_into().unwrap());
    if (x & 0xF0F0_F0F0_F0F0_F0F0) != 0x3030_3030_3030_3030
        || (x.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) != 0x3030_3030_3030_3030
    {
        return None;
    }
    let mut v = x - 0x3030_3030_3030_3030;
    v = (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v.wrapping_mul(100) + (v >> 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v.wrapping_mul(10000) + (v >> 32)) & 0xFFFF_FFFF;
    Some(v)
}

#[derive(Default, Clone)]
struct DateCache {
    ybase: i64,
    days: Vec<i32>, // [(y - ybase) * 372 + (m - 1) * 31 + (d - 1)] = epoch day, or i32::MIN if impossible
}
const DC_YEARS: i64 = 6;
impl DateCache {
    /// Day table for the years around `ws` (other years take the general path).
    fn new(ws: i64) -> Self {
        let ybase = 1970 + ws.div_euclid(DAY) * 400 / 146_097 - 3;
        let mut days = vec![i32::MIN; (DC_YEARS * 372) as usize];
        for yi in 0..DC_YEARS {
            for m in 1..=12i64 {
                for d in 1..=31i64 {
                    let y = ybase + yi;
                    let s = format!("{:04}-{:02}-{:02}", y, m, d);
                    if let Some(v) = parse_date(s.as_bytes()) {
                        days[(yi * 372 + (m - 1) * 31 + (d - 1)) as usize] = v as i32;
                    }
                }
            }
        }
        DateCache { ybase, days }
    }
}

/// 8 ASCII digits? (little-endian word)
#[inline(always)]
fn digits8(x: u64) -> bool {
    (x & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
        && (x.wrapping_add(0x0606_0606_0606_0606) & 0xF0F0_F0F0_F0F0_F0F0) == 0x3030_3030_3030_3030
}
/// 8 digits -> four 2-digit values in 16-bit lanes (lane 0 = first two characters)
#[inline(always)]
fn pairs8(x: u64) -> u64 {
    let v = x - 0x3030_3030_3030_3030;
    (v.wrapping_mul(10) + (v >> 8)) & 0x00FF_00FF_00FF_00FF
}

/// F1 / F2 / F3 -> epoch ms. None = bad timestamp (F3 beyond i64 included).
#[inline]
fn parse_ts(f: &[u8], dc: &mut DateCache) -> Option<i64> {
    let n = f.len();
    if n == 0 {
        return None;
    }
    if n == 20 || n == 25 {
        if f[4] == b'-' {
            let w0 = u64::from_le_bytes(f[0..8].try_into().unwrap());
            let w1 = u64::from_le_bytes(f[8..16].try_into().unwrap());
            let w2 = u32::from_le_bytes(f[16..20].try_into().unwrap()) as u64;
            if (w0 >> 56) as u8 != b'-' || (w1 >> 16) as u8 != b'T' || (w1 >> 40) as u8 != b':' || w2 as u8 != b':' {
                return None;
            }
            // YYYYMMDD and hhmmss00 as digit words
            let da = (w0 & 0xFFFF_FFFF) | (((w0 >> 40) & 0xFFFF) << 32) | ((w1 & 0xFFFF) << 48);
            let ta = ((w1 >> 24) & 0xFFFF) | (((w1 >> 48) & 0xFFFF) << 16) | (((w2 >> 8) & 0xFFFF) << 32) | (0x3030u64 << 48);
            if !digits8(da) || !digits8(ta) {
                return None;
            }
            let pd = pairs8(da);
            let pt = pairs8(ta);
            let y = ((pd & 0xFF) * 100 + ((pd >> 16) & 0xFF)) as i64;
            let mo = ((pd >> 32) & 0xFF) as i64;
            let d = ((pd >> 48) & 0xFF) as i64;
            let h = (pt & 0xFF) as i64;
            let mi = ((pt >> 16) & 0xFF) as i64;
            let s = ((pt >> 32) & 0xFF) as i64;
            if h > 23 || mi > 59 || s > 59 {
                return None;
            }
            let yi = y - dc.ybase;
            let day = if (0..DC_YEARS).contains(&yi) && (1..=12).contains(&mo) && (1..=31).contains(&d) && !dc.days.is_empty() {
                let v = dc.days[(yi * 372 + (mo - 1) * 31 + (d - 1)) as usize];
                if v == i32::MIN {
                    return None;
                }
                v as i64
            } else {
                parse_date(f)?
            };
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
            return Some(day * DAY + h * 3_600_000 + mi * 60_000 + s * 1000 - off);
        }
    }
    // F3: digits only
    let mut i = 0;
    while i < n && f[i] == b'0' {
        i += 1;
    }
    let r = &f[i..];
    let m = r.len();
    if m > 19 {
        // too long for i64 if all digits; either way bad
        return None;
    }
    let v: u64 = if (9..=16).contains(&m) {
        // two overlapping 8-digit SWAR loads cover every digit (typical 13-digit epoch ms)
        const P10: [u64; 9] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];
        let hi = p8(&r[..8])?;
        let lo = p8(&r[m - 8..])?;
        let k = m - 8;
        hi * P10[k] + lo % P10[k]
    } else {
        let mut v: u64 = 0;
        let mut j = 0;
        while j + 8 <= m {
            v = v * 100_000_000 + p8(&r[j..j + 8])?;
            j += 8;
        }
        while j < m {
            let c = r[j].wrapping_sub(b'0');
            if c > 9 {
                return None;
            }
            v = v.wrapping_mul(10).wrapping_add(c as u64);
            j += 1;
        }
        v
    };
    // m <= 19 digits: at most 9_999_999_999_999_999_999 < u64::MAX; i64 range checked below
    if v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

/// purchase price: digits only, 1..=10_000_000
#[inline]
fn parse_price(f: &[u8]) -> Option<u32> {
    if f.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < f.len() && f[i] == b'0' {
        i += 1;
    }
    let r = &f[i..];
    if r.is_empty() || r.len() > 8 {
        // all zeros (value 0) or too large -- but still must be digits-only: both invalid anyway
        return None;
    }
    let mut v: u32 = 0;
    for &c in r {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ---------------------------------------------------------------- CSV dialect

/// Calls `f(fields)` for every well-formed line (exactly N fields, quotes only around whole
/// fields, stripped) of `data` (lines separated by '\n'; `data` has no trailing '\n').
#[inline]
fn for_lines<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(data: &'a [u8], mut f: F) -> u64 {
    // Structural iteration over ',' and '\n' only; quotes are counted per line with popcount and
    // validated at the end of the line: a line is well-formed iff every field that starts with '"'
    // is >= 2 bytes and ends with '"', and the line holds exactly 2 quotes per such field (so no
    // quote appears anywhere else).
    let mut bounds = [(0usize, 0usize); N];
    let mut nf = 0usize;
    let mut fstart = 0usize;
    let mut lq = 0u32; // quotes seen in the current line
    let mut lines = 0u64;
    macro_rules! finish {
        ($end:expr) => {{
            let end = $end;
            lines += 1;
            if nf + 1 == N {
                bounds[nf] = (fstart, end);
                let mut fs: [&'a [u8]; N] = [&[]; N];
                // positions come from the delimiter masks of `data`: always in range, s <= e
                if lq == 0 {
                    for k in 0..N {
                        fs[k] = unsafe { data.get_unchecked(bounds[k].0..bounds[k].1) };
                    }
                    f(&fs);
                } else {
                    let mut ok = true;
                    let mut qf = 0u32;
                    for k in 0..N {
                        let v = unsafe { data.get_unchecked(bounds[k].0..bounds[k].1) };
                        if !v.is_empty() && v[0] == b'"' {
                            let l = v.len();
                            if l < 2 || v[l - 1] != b'"' {
                                ok = false;
                                break;
                            }
                            qf += 1;
                            fs[k] = &v[1..l - 1];
                        } else {
                            fs[k] = v;
                        }
                    }
                    if ok && qf * 2 == lq {
                        f(&fs);
                    }
                }
            }
            nf = 0;
            lq = 0;
            fstart = end + 1;
        }};
    }
    let n = data.len();
    let mut blk = 0usize;
    let mut tail = [0u8; 64];
    while blk < n {
        let (sm, nlm, qm) = if blk + 64 <= n {
            delim_masks(&data[blk..blk + 64])
        } else {
            tail = [0u8; 64];
            tail[..n - blk].copy_from_slice(&data[blk..]);
            delim_masks(&tail)
        };
        let mut s = sm;
        let mut qrem = qm;
        while s != 0 {
            let i = s.trailing_zeros();
            s &= s - 1;
            let p = blk + i as usize;
            if (nlm >> i) & 1 == 0 {
                if nf < N {
                    unsafe { *bounds.get_unchecked_mut(nf) = (fstart, p) };
                }
                nf += 1;
                fstart = p + 1;
            } else {
                // quotes of this line that sit in this block (earlier blocks already counted)
                let below = (1u64 << i) - 1;
                lq += (qrem & below).count_ones();
                qrem &= !below;
                finish!(p);
            }
        }
        lq += qrem.count_ones();
        blk += 64;
    }
    let _ = &tail;
    if fstart < data.len() || (fstart == data.len() && nf > 0) {
        finish!(data.len());
    }
    lines
}

/// (',' | '\n', '\n', '"') position masks of a 64-byte block (bit i = byte i).
#[inline(always)]
fn delim_masks(b: &[u8]) -> (u64, u64, u64) {
    debug_assert!(b.len() >= 64);
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm256_set1_epi8(b',' as i8);
        let q = _mm256_set1_epi8(b'"' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let (mut ms, mut mn, mut mq) = (0u64, 0u64, 0u64);
        for k in 0..2 {
            let v = _mm256_loadu_si256(b.as_ptr().add(32 * k) as *const __m256i);
            let en = _mm256_cmpeq_epi8(v, nl);
            let es = _mm256_or_si256(_mm256_cmpeq_epi8(v, c), en);
            ms |= (_mm256_movemask_epi8(es) as u32 as u64) << (32 * k);
            mn |= (_mm256_movemask_epi8(en) as u32 as u64) << (32 * k);
            mq |= (_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, q)) as u32 as u64) << (32 * k);
        }
        (ms, mn, mq)
    }
    #[cfg(all(target_arch = "x86_64", not(target_feature = "avx2")))]
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm_set1_epi8(b',' as i8);
        let q = _mm_set1_epi8(b'"' as i8);
        let nl = _mm_set1_epi8(b'\n' as i8);
        let (mut ms, mut mn, mut mq) = (0u64, 0u64, 0u64);
        for k in 0..4 {
            let v = _mm_loadu_si128(b.as_ptr().add(16 * k) as *const __m128i);
            let en = _mm_cmpeq_epi8(v, nl);
            let es = _mm_or_si128(_mm_cmpeq_epi8(v, c), en);
            ms |= (_mm_movemask_epi8(es) as u32 as u64) << (16 * k);
            mn |= (_mm_movemask_epi8(en) as u32 as u64) << (16 * k);
            mq |= (_mm_movemask_epi8(_mm_cmpeq_epi8(v, q)) as u32 as u64) << (16 * k);
        }
        (ms, mn, mq)
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use std::arch::aarch64::*;
        const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let w = vld1q_u8(W.as_ptr());
        let p = b.as_ptr();
        let v = [vld1q_u8(p), vld1q_u8(p.add(16)), vld1q_u8(p.add(32)), vld1q_u8(p.add(48))];
        let red = |e: [uint8x16_t; 4]| -> u64 {
            let s0 = vpaddq_u8(vandq_u8(e[0], w), vandq_u8(e[1], w));
            let s1 = vpaddq_u8(vandq_u8(e[2], w), vandq_u8(e[3], w));
            let s = vpaddq_u8(s0, s1);
            let s = vpaddq_u8(s, s);
            vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
        };
        let c = vdupq_n_u8(b',');
        let q = vdupq_n_u8(b'"');
        let nl = vdupq_n_u8(b'\n');
        let en = [vceqq_u8(v[0], nl), vceqq_u8(v[1], nl), vceqq_u8(v[2], nl), vceqq_u8(v[3], nl)];
        let es = [
            vorrq_u8(vceqq_u8(v[0], c), en[0]),
            vorrq_u8(vceqq_u8(v[1], c), en[1]),
            vorrq_u8(vceqq_u8(v[2], c), en[2]),
            vorrq_u8(vceqq_u8(v[3], c), en[3]),
        ];
        let eq = [vceqq_u8(v[0], q), vceqq_u8(v[1], q), vceqq_u8(v[2], q), vceqq_u8(v[3], q)];
        (red(es), red(en), red(eq))
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        let (mut ms, mut mn, mut mq) = (0u64, 0u64, 0u64);
        for (i, &x) in b[..64].iter().enumerate() {
            ms |= ((x == b',' || x == b'\n') as u64) << i;
            mn |= ((x == b'\n') as u64) << i;
            mq |= ((x == b'"') as u64) << i;
        }
        (ms, mn, mq)
    }
}

#[allow(dead_code)] // used by the tokenizer fuzz test (previous tokenizer)
/// Bitmask of the positions of ',', '"' and '\n' in a 64-byte block (bit i = byte i).
#[inline(always)]
fn delim_mask(b: &[u8]) -> u64 {
    debug_assert!(b.len() >= 64);
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let c = _mm_set1_epi8(b',' as i8);
        let q = _mm_set1_epi8(b'"' as i8);
        let nl = _mm_set1_epi8(b'\n' as i8);
        let mut m = 0u64;
        for k in 0..4 {
            let v = _mm_loadu_si128(b.as_ptr().add(16 * k) as *const __m128i);
            let e = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(v, c), _mm_cmpeq_epi8(v, q)), _mm_cmpeq_epi8(v, nl));
            m |= (_mm_movemask_epi8(e) as u32 as u64) << (16 * k);
        }
        m
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use std::arch::aarch64::*;
        const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
        let w = vld1q_u8(W.as_ptr());
        let c = vdupq_n_u8(b',');
        let q = vdupq_n_u8(b'"');
        let nl = vdupq_n_u8(b'\n');
        let p = b.as_ptr();
        let m = |k: usize| {
            let v = vld1q_u8(p.add(16 * k));
            let e = vorrq_u8(vorrq_u8(vceqq_u8(v, c), vceqq_u8(v, q)), vceqq_u8(v, nl));
            vandq_u8(e, w)
        };
        let s0 = vpaddq_u8(m(0), m(1));
        let s1 = vpaddq_u8(m(2), m(3));
        let s = vpaddq_u8(s0, s1);
        let s = vpaddq_u8(s, s);
        vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        #[inline(always)]
        fn eq(x: u64, t: u64) -> u64 {
            let y = x ^ t;
            !(((y & 0x7F7F_7F7F_7F7F_7F7F) + 0x7F7F_7F7F_7F7F_7F7F) | y | 0x7F7F_7F7F_7F7F_7F7F)
        }
        let mut m = 0u64;
        for k in 0..8 {
            let x = u64::from_le_bytes(b[8 * k..8 * k + 8].try_into().unwrap());
            let e = eq(x, 0x2C2C_2C2C_2C2C_2C2C) | eq(x, 0x2222_2222_2222_2222) | eq(x, 0x0A0A_0A0A_0A0A_0A0A);
            // gather the high bit of each byte into 8 bits
            let bits = ((e >> 7).wrapping_mul(0x0102_0408_1020_4080)) >> 56;
            m |= bits << (8 * k);
        }
        m
    }
}

/// Data lines of a small clean file (header skipped).
fn small_table<const N: usize>(bytes: &[u8]) -> Vec<[&[u8]; N]> {
    let mut v = Vec::new();
    let s = match memchr::memchr(b'\n', bytes) {
        Some(i) => i + 1,
        None => return v,
    };
    let mut e = bytes.len();
    if e > s && bytes[e - 1] == b'\n' {
        e -= 1;
    }
    if s >= bytes.len() {
        return v;
    }
    for_lines::<N, _>(&bytes[s..e], |f| {
        if !f[0].is_empty() {
            v.push(*f);
        }
    });
    v
}

// ---------------------------------------------------------------- ID table

const READY: u64 = 1 << 63;

/// Lock-free open-addressing table. Slot i = words [4i..4i+4]:
///   0: key.hi (claim word, never 0)   1: key.lo
///   2: READY | len << 32 | root        3: arena pointer (thread << 40 | offset)
struct IdTable {
    mask: usize,
    w: BigBuf,
    limit: usize,
    count: AtomicUsize,
    overflow: AtomicBool,
}

/// Large zeroed array of atomics: 2 MB aligned, transparent huge pages requested (Linux),
/// pre-faulted by all threads in parallel instead of by random first touches later.
struct BigBuf {
    ptr: *mut AtomicU64,
    len: usize,
    layout: std::alloc::Layout,
}
unsafe impl Send for BigBuf {}
unsafe impl Sync for BigBuf {}
impl std::ops::Deref for BigBuf {
    type Target = [AtomicU64];
    fn deref(&self) -> &[AtomicU64] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}
impl Drop for BigBuf {
    fn drop(&mut self) {
        unsafe { std::alloc::dealloc(self.ptr as *mut u8, self.layout) }
    }
}
fn big_zeroed(n: usize) -> BigBuf {
    const HP: usize = 2 << 20;
    let bytes = (n * 8).div_ceil(HP) * HP;
    let layout = std::alloc::Layout::from_size_align(bytes, HP).unwrap();
    let p = unsafe { std::alloc::alloc(layout) };
    assert!(!p.is_null(), "alloc");
    #[cfg(target_os = "linux")]
    unsafe {
        libc::madvise(p as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
    }
    struct P(*mut u8);
    unsafe impl Sync for P {}
    let pp = P(p);
    let pp = &pp;
    (0..bytes / HP).into_par_iter().for_each(|k| unsafe { std::ptr::write_bytes(pp.0.add(k * HP), 0, HP) });
    BigBuf { ptr: p as *mut AtomicU64, len: n, layout }
}

#[allow(dead_code)]
fn zeroed_atomics(n: usize) -> Vec<AtomicU64> {
    let mut v = std::mem::ManuallyDrop::new(vec![0u64; n]);
    unsafe { Vec::from_raw_parts(v.as_mut_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
}

impl IdTable {
    fn new(cap: usize) -> Self {
        IdTable {
            mask: cap - 1,
            w: big_zeroed(4 * cap),
            limit: cap / 10 * 7,
            count: AtomicUsize::new(0),
            overflow: AtomicBool::new(false),
        }
    }
    /// Prefetch the first probe slot of `k` (hides the cache miss of a later insert).
    #[inline(always)]
    fn prefetch(&self, k: Key) -> usize {
        let i = mix(k) as usize & self.mask;
        prefetch_ptr(self.w.as_ptr().wrapping_add(4 * i) as *const i8);
        i
    }
    /// -> (slot, root)
    #[inline]
    fn insert(&self, k: Key, s: &[u8], arena: &mut Vec<u8>, w: usize, newc: &mut usize) -> (u32, u32) {
        self.insert_at(k, mix(k) as usize & self.mask, s, arena, w, newc)
    }
    /// insert starting the probe at slot `i0` (= the value prefetch() returned for k)
    #[inline]
    fn insert_at(&self, k: Key, i0: usize, s: &[u8], arena: &mut Vec<u8>, w: usize, newc: &mut usize) -> (u32, u32) {
        let mut i = i0;
        let mut probes = 0usize;
        loop {
            let b = 4 * i;
            let mut h = self.w[b].load(Ordering::Acquire);
            if h == 0 {
                match self.w[b].compare_exchange(0, k.hi, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => {
                        self.w[b + 1].store(k.lo, Ordering::Relaxed);
                        self.w[b + 3].store(((w as u64) << 40) | arena.len() as u64, Ordering::Relaxed);
                        arena.extend_from_slice(s);
                        self.w[b + 2].store(READY | ((s.len() as u64) << 32) | i as u64, Ordering::Release);
                        *newc += 1;
                        return (i as u32, i as u32);
                    }
                    Err(a) => h = a,
                }
            }
            if h == k.hi {
                let mut m = self.w[b + 2].load(Ordering::Acquire);
                while m & READY == 0 {
                    std::hint::spin_loop();
                    m = self.w[b + 2].load(Ordering::Acquire);
                }
                if self.w[b + 1].load(Ordering::Relaxed) == k.lo {
                    return (i as u32, m as u32);
                }
            }
            i = (i + 1) & self.mask;
            probes += 1;
            if probes > 1 << 14 {
                self.overflow.store(true, Ordering::Relaxed);
                return (0, 0);
            }
        }
    }
    #[inline]
    fn sptr(&self, i: u32) -> SPtr {
        let b = 4 * i as usize;
        SPtr {
            off: self.w[b + 3].load(Ordering::Relaxed),
            len: ((self.w[b + 2].load(Ordering::Relaxed) >> 32) & 0x7FFF_FFFF) as u32,
        }
    }
    #[inline]
    fn used(&self, i: usize) -> bool {
        self.w[4 * i].load(Ordering::Relaxed) != 0
    }
    #[inline]
    fn set_root(&self, i: usize, r: u32) {
        let b = 4 * i + 2;
        let m = self.w[b].load(Ordering::Relaxed);
        self.w[b].store((m & !0xFFFF_FFFF) | r as u64, Ordering::Relaxed);
    }
}

// ---------------------------------------------------------------- records

#[derive(Clone, Copy)]
/// Dedup record (every well-formed event with an event_id): key, ingest_ts and where its
/// person-bucket row lives (NONE if the copy is invalid).
struct Ev {
    klo: u64,
    khi: u64,
    ing: i64,
    r: u64, // thread << 56 | person bucket << 40 | index, or NONE
}
const NONE: u64 = u64::MAX;

#[derive(Clone, Copy)]
struct PEv {
    key: u64,
    price: u32,
    cat: u16,
    dead: bool, // set by dedup when a copy with a smaller ingest_ts exists
}

#[derive(Clone, Copy)]
struct PCk {
    key: u64,
    b0: u64,  // click_id bytes 0..8, big-endian, zero padded (fast tie order)
    off: u64, // click_id location: file << 40 | offset (exact tie order)
    len: u32,
    camp: u16,
}

#[inline]
/// first 8 bytes, big-endian, zero padded (lexicographic order of the prefix)
#[inline]
fn be8(s: &[u8]) -> u64 {
    if s.len() >= 8 {
        u64::from_be_bytes(s[..8].try_into().unwrap())
    } else {
        let mut b = [0u8; 8];
        b[..s.len()].copy_from_slice(s);
        u64::from_be_bytes(b)
    }
}

#[allow(dead_code)]
fn be16(s: &[u8]) -> (u64, u64) {
    let mut b = [0u8; 16];
    let n = s.len().min(16);
    b[..n].copy_from_slice(&s[..n]);
    (u64::from_be_bytes(b[0..8].try_into().unwrap()), u64::from_be_bytes(b[8..16].try_into().unwrap()))
}

/// Per-thread bucketed append storage: one 2 MB-aligned slab (THP-advised on Linux) carved into
/// fixed-capacity regions, so the thread's many write streams sit in a few huge pages instead of
/// thousands of 4 KB pages (TLB). A bucket that outgrows its region moves to a heap Vec.
struct Buckets<T: Copy> {
    base: *mut T,
    cap: usize,
    lens: Vec<u32>, // u32::MAX = spilled
    spill: Vec<Vec<T>>,
    layout: Option<std::alloc::Layout>,
}
unsafe impl<T: Copy + Send> Send for Buckets<T> {}
unsafe impl<T: Copy + Sync> Sync for Buckets<T> {}
impl<T: Copy> Default for Buckets<T> {
    fn default() -> Self {
        Buckets { base: std::ptr::null_mut(), cap: 0, lens: vec![], spill: vec![], layout: None }
    }
}
impl<T: Copy> Buckets<T> {
    fn new(nb: usize, cap: usize) -> Self {
        const HP: usize = 2 << 20;
        let bytes = (nb * cap * std::mem::size_of::<T>()).max(1).div_ceil(HP) * HP;
        let layout = std::alloc::Layout::from_size_align(bytes, HP).unwrap();
        let p = unsafe { std::alloc::alloc(layout) };
        assert!(!p.is_null(), "alloc");
        #[cfg(target_os = "linux")]
        unsafe {
            libc::madvise(p as *mut libc::c_void, bytes, libc::MADV_HUGEPAGE);
        }
        Buckets { base: p as *mut T, cap, lens: vec![0; nb], spill: (0..nb).map(|_| Vec::new()).collect(), layout: Some(layout) }
    }
    fn empty(nb: usize) -> Self {
        Buckets { base: std::ptr::null_mut(), cap: 0, lens: vec![0; nb], spill: (0..nb).map(|_| Vec::new()).collect(), layout: None }
    }
    fn is_empty(&self) -> bool {
        self.lens.is_empty()
    }
    /// append to bucket b, returning the row's index within the bucket
    #[inline(always)]
    fn push(&mut self, b: usize, x: T) -> usize {
        let l = self.lens[b];
        if (l as usize) < self.cap {
            unsafe { self.base.add(b * self.cap + l as usize).write(x) };
            self.lens[b] = l + 1;
            l as usize
        } else {
            self.push_slow(b, x)
        }
    }
    #[cold]
    fn push_slow(&mut self, b: usize, x: T) -> usize {
        let l = self.lens[b];
        if l != u32::MAX {
            // region full: move it to the heap
            let region = unsafe { std::slice::from_raw_parts(self.base.add(b * self.cap), l as usize) };
            let mut v = Vec::with_capacity((l as usize * 2).max(64));
            v.extend_from_slice(region);
            self.spill[b] = v;
            self.lens[b] = u32::MAX;
        }
        let v = &mut self.spill[b];
        v.push(x);
        v.len() - 1
    }
    #[inline]
    fn slice(&self, b: usize) -> &[T] {
        let l = self.lens[b];
        if l == u32::MAX {
            &self.spill[b]
        } else if l == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.base.add(b * self.cap), l as usize) }
        }
    }
    fn as_mut_ptr(&mut self, b: usize) -> *mut T {
        if self.lens[b] == u32::MAX {
            self.spill[b].as_mut_ptr()
        } else {
            unsafe { self.base.add(b * self.cap) }
        }
    }
}
impl<T: Copy> Drop for Buckets<T> {
    fn drop(&mut self) {
        if let Some(l) = self.layout {
            unsafe { std::alloc::dealloc(self.base as *mut u8, l) }
        }
    }
}

#[derive(Default)]
struct ParseOut {
    arena: Vec<u8>,
    evb: Buckets<Ev>,
    pev: Buckets<PEv>,
    ckb: Buckets<PCk>,
    links: Vec<(u32, u32)>,
    nlines: [u64; 3],
    newids: usize,
    dc: DateCache,
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
    first: bool,
}

struct Ctx<'a> {
    files: &'a Files,
    tab: &'a IdTable,
    products: &'a SmallMap,
    unknown_cat: u16,
    camps: &'a SmallMap,
    ws: i64,
    we: i64,
    cws: i64,
    tbits: u32,
    pshift: u32,
}

#[inline]
fn pbucket(r: u32, pshift: u32) -> usize {
    // top bits of the slot index: slots are hash positions, so buckets are balanced, and the
    // key's high bits are constant within a bucket (the radix sort skips those digits)
    (r >> pshift) as usize
}

fn parse_chunk(cx: &Ctx, c: &Chunk, w: usize, out: &mut ParseOut) {
    let base = cx.files.b[c.fi].get();
    #[cfg(target_os = "linux")]
    if popmode() == PopMode::Chunk {
        if let Bytes::Map(m) = &cx.files.b[c.fi] {
            let a = c.s & !4095;
            let e = (c.e + 4096).min(m.len());
            let _ = m.advise_range(memmap2::Advice::PopulateRead, a, e - a);
        }
    }
    let (rs, re) = chunk_region(base, c);
    if re <= rs {
        return;
    }
    let data = &base[rs..re];
    let bend = base.as_ptr() as usize + base.len();
    let avail = |f: &[u8]| bend - f.as_ptr() as usize;
    let ParseOut { arena, evb, pev, ckb, links, nlines, newids, dc } = out;
    match c.kind {
        Kind::Ev => {
            let mut ring: [Option<(Key, i64, u64, u32, Key, usize, Key, usize, &[u8])>; PF] = [None; PF];
            let mut rpos = 0usize;
            let tbits = cx.tbits;
            macro_rules! finish_ev {
                ($pp:expr) => {{
                    let (k, ing, tsr, price, pk, pi, uk, ui, us): (Key, i64, u64, u32, Key, usize, Key, usize, &[u8]) = $pp;
                    let cat = cx.products.get_at(pk, pi).unwrap_or(cx.unknown_cat);
                    let root = cx.tab.insert_at(uk, ui, us, arena, w, newids).1;
                    let pb = pbucket(root, cx.pshift);
                    let i = pev.push(pb, PEv { key: ((root as u64) << tbits) | tsr, price, cat, dead: false });
                    let r = ((w as u64) << 56) | ((pb as u64) << 40) | i as u64;
                    evb.push((mix(k) >> (64 - DB_BITS)) as usize, Ev { klo: k.lo, khi: k.hi, ing, r });
                }};
            }
            nlines[0] += for_lines::<7, _>(data, |f| {
                if f[0].is_empty() {
                    return;
                }
                let k = key_at(f[0], avail(f[0]));
                let ing = parse_ts(f[3], dc).unwrap_or(i64::MAX);
                let mut ok = !f[1].is_empty() && !f[5].is_empty();
                let mut ts = 0i64;
                if ok {
                    match parse_ts(f[2], dc) {
                        Some(t) if t >= cx.ws && t < cx.we => ts = t,
                        _ => ok = false,
                    }
                }
                let et = f[4];
                let mut price = 0u32;
                if ok {
                    if et == b"purchase" {
                        match parse_price(f[6]) {
                            Some(p) => price = p,
                            None => ok = false,
                        }
                    } else if et != b"view" && et != b"cart" {
                        ok = false;
                    }
                }
                if ok && ing - ts > LATE {
                    ok = false;
                }
                if ok {
                    // software pipeline: prefetch this row's id slot, finish the previous row's insert
                    let uk = key_at(f[1], avail(f[1]));
                    let ui = cx.tab.prefetch(uk);
                    let pk = key_at(f[5], avail(f[5]));
                    let pi = cx.products.prefetch(pk);
                    let slot = rpos % PF;
                    rpos += 1;
                    if let Some(pp) = ring[slot].take() {
                        finish_ev!(pp);
                    }
                    ring[slot] = Some((k, ing, (ts - cx.cws) as u64, price, pk, pi, uk, ui, f[1]));
                } else {
                    evb.push((mix(k) >> (64 - DB_BITS)) as usize, Ev { klo: k.lo, khi: k.hi, ing, r: NONE });
                }
            });
            for i in 0..PF {
                if let Some(pp) = ring[(rpos + i) % PF].take() {
                    finish_ev!(pp);
                }
            }
        }
        Kind::Ck => {
            // same software pipeline as events: prefetch the user's id slot, insert one row later
            let mut ring: [Option<(Key, usize, &[u8], u64, u16, &[u8])>; PF] = [None; PF];
            let mut rpos = 0usize;
            macro_rules! finish_ck {
                ($pp:expr) => {{
                    let (uk, ui, us, tsr, camp, id): (Key, usize, &[u8], u64, u16, &[u8]) = $pp;
                    let root = cx.tab.insert_at(uk, ui, us, arena, w, newids).1;
                    let b0 = be8(id);
                    let o = id.as_ptr() as usize - base.as_ptr() as usize;
                    ckb.push(pbucket(root, cx.pshift), PCk {
                        key: ((root as u64) << cx.tbits) | tsr,
                        b0,
                        off: ((c.fi as u64) << 40) | o as u64,
                        len: id.len() as u32,
                        camp,
                    });
                }};
            }
            nlines[1] += for_lines::<4, _>(data, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let ts = match parse_ts(f[3], dc) {
                    Some(t) if t >= cx.cws && t < cx.we => t,
                    _ => return,
                };
                let camp = match cx.camps.get(key_at(f[2], avail(f[2]))) {
                    Some(c) => c,
                    None => return,
                };
                let uk = key_at(f[1], avail(f[1]));
                let ui = cx.tab.prefetch(uk);
                let slot = rpos % PF;
                rpos += 1;
                if let Some(pp) = ring[slot].take() {
                    finish_ck!(pp);
                }
                ring[slot] = Some((uk, ui, f[1], (ts - cx.cws) as u64, camp, f[0]));
            });
            for i in 0..PF {
                if let Some(pp) = ring[(rpos + i) % PF].take() {
                    finish_ck!(pp);
                }
            }
        }
        Kind::Ln => {
            // pipelined: prefetch both slots, insert one line later
            let mut ring: [Option<(Key, usize, &[u8], Key, usize, &[u8])>; PF] = [None; PF];
            let mut rpos = 0usize;
            macro_rules! finish_ln {
                ($pp:expr) => {{
                    let (ka, ia, sa, kb, ib, sb): (Key, usize, &[u8], Key, usize, &[u8]) = $pp;
                    let a = cx.tab.insert_at(ka, ia, sa, arena, w, newids).0;
                    let b = cx.tab.insert_at(kb, ib, sb, arena, w, newids).0;
                    if a != b {
                        links.push((a, b));
                    }
                }};
            }
            nlines[2] += for_lines::<2, _>(data, |f| {
                if f[0].is_empty() || f[1].is_empty() {
                    return;
                }
                let ka = key_at(f[0], avail(f[0]));
                let kb = key_at(f[1], avail(f[1]));
                let ia = cx.tab.prefetch(ka);
                let ib = cx.tab.prefetch(kb);
                let slot = rpos % PF;
                rpos += 1;
                if let Some(pp) = ring[slot].take() {
                    finish_ln!(pp);
                }
                ring[slot] = Some((ka, ia, f[0], kb, ib, f[1]));
            });
            for i in 0..PF {
                if let Some(pp) = ring[(rpos + i) % PF].take() {
                    finish_ln!(pp);
                }
            }
        }
    }
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
    cred: Vec<[i64; 3]>, // credited: sessions, purchasing sessions, revenue (one cache line touch)
    assisted: Vec<u64>,
}
impl CampAcc {
    fn new(n: usize) -> Self {
        CampAcc { cred: vec![[0; 3]; n], assisted: vec![0; n] }
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
    ev_tmp: Vec<PEv>,
    ck_tmp: Vec<PCk>,
    since: Vec<u32>,
    sday: Vec<u16>,
    hist: Vec<u32>,
    udw: Option<PqWriter>,
    udrows: Vec<UdRow>,
}

/// Vec with the given capacity whose buffer is advised for transparent huge pages (Linux).
fn hp_vec<T>(cap: usize) -> Vec<T> {
    let v: Vec<T> = Vec::with_capacity(cap);
    #[cfg(target_os = "linux")]
    {
        let bytes = cap * std::mem::size_of::<T>();
        if bytes >= 4 << 20 {
            // madvise needs a page-aligned range: round inwards
            let start = (v.as_ptr() as usize + 4095) & !4095;
            let end = (v.as_ptr() as usize + bytes) & !4095;
            if end > start {
                unsafe {
                    libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_HUGEPAGE);
                }
            }
        }
    }
    v
}

/// Gathers the rows of `srcs` that pass `keep` into `out`, sorted by key (LSD radix, 12-bit
/// digits, one histogram pass for all digits, constant digits skipped, the first scatter reads
/// straight from the sources).
#[allow(clippy::too_many_arguments)]
fn radix_gather<T: Copy, P: Fn(&T) -> bool, K: Fn(&T) -> u64>(
    srcs: &[&[T]],
    keep: P,
    out: &mut Vec<T>,
    tmp: &mut Vec<T>,
    hist: &mut Vec<u32>,
    bits: u32,
    key: K,
) {
    // one pass when the key has <= 16 bits, else 12-bit digits
    let dd: u32 = if bits <= 16 { bits.max(1) } else { 12 };
    let rr: usize = 1 << dd;
    #[allow(non_snake_case)]
    let (D, R) = (dd, rr);
    let nd = bits.div_ceil(D).max(1) as usize;
    hist.clear();
    hist.resize(nd * R, 0);
    let mut n = 0usize;
    for s in srcs {
        for x in s.iter() {
            if keep(x) {
                let k = key(x);
                for d in 0..nd {
                    hist[d * R + ((k >> (D * d as u32)) as usize & (R - 1))] += 1;
                }
                n += 1;
            }
        }
    }
    out.clear();
    if n < 256 {
        for s in srcs {
            out.extend(s.iter().filter(|x| keep(x)).copied());
        }
        out.sort_unstable_by_key(|x| key(x));
        return;
    }
    out.reserve(n);
    tmp.clear();
    tmp.reserve(n);
    unsafe {
        out.set_len(n);
        tmp.set_len(n);
    }
    let mut first = true;
    for d in 0..nd {
        let h = &mut hist[d * R..(d + 1) * R];
        if h.iter().any(|&c| c as usize == n) {
            continue;
        }
        let mut sum = 0u32;
        for c in h.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        let sh = D * d as u32;
        if first {
            for s in srcs {
                for x in s.iter() {
                    if keep(x) {
                        let g = (key(x) >> sh) as usize & (R - 1);
                        out[h[g] as usize] = *x;
                        h[g] += 1;
                    }
                }
            }
            first = false;
        } else {
            for x in out.iter() {
                let g = (key(x) >> sh) as usize & (R - 1);
                tmp[h[g] as usize] = *x;
                h[g] += 1;
            }
            std::mem::swap(out, tmp);
        }
    }
    if first {
        // every digit constant: all keys equal
        let mut i = 0;
        for s in srcs {
            for x in s.iter() {
                if keep(x) {
                    out[i] = *x;
                    i += 1;
                }
            }
        }
    }
}

// ---------------------------------------------------------------- main

fn env_usize(k: &str) -> Option<usize> {
    std::env::var(k).ok().and_then(|v| v.trim().parse().ok())
}

fn manifest_ts(text: &str, k: &str) -> i64 {
    let kk = format!("\"{k}\"");
    let i = text.find(&kk).unwrap_or_else(|| panic!("manifest missing {k}")) + kk.len();
    let rest = &text[i..];
    let a = rest.find('"').unwrap() + 1;
    let b = a + rest[a..].find('"').unwrap();
    parse_ts(rest[a..b].as_bytes(), &mut DateCache::default()).unwrap_or_else(|| panic!("bad manifest ts {k}"))
}

/// Fixed byte ranges [s, e) of each file's data (after the header line). A chunk owns the lines
/// that start inside it; parse_chunk snaps the range to line boundaries (in parallel).
fn make_chunks(files: &Files, paths: &[(Kind, PathBuf)], want: &[Kind]) -> Vec<Chunk> {
    let mut chunks = vec![];
    let ch = env_usize("SS_CHUNK").unwrap_or(CH).max(1); // override for boundary tests
    for (fi, (kind, _)) in paths.iter().enumerate() {
        if !want.contains(kind) {
            continue;
        }
        let b = files.b[fi].get();
        let ds = match memchr::memchr(b'\n', b) {
            Some(i) => i + 1,
            None => continue, // header only (or nothing)
        };
        let mut s = ds;
        while s < b.len() {
            let e = (s + ch).min(b.len());
            chunks.push(Chunk { kind: *kind, fi, s, e, first: s == ds });
            s = e;
        }
    }
    chunks
}

/// Lines starting in [c.s, c.e) as one region without its terminating '\n'.
#[inline]
fn chunk_region(base: &[u8], c: &Chunk) -> (usize, usize) {
    let start = if c.first {
        c.s
    } else {
        match memchr::memchr(b'\n', &base[c.s - 1..]) {
            Some(j) => c.s + j,
            None => return (0, 0),
        }
    };
    if start >= c.e {
        return (0, 0);
    }
    let end = match memchr::memchr(b'\n', &base[c.e - 1..]) {
        Some(j) => c.e - 1 + j,
        None => base.len(),
    };
    if end <= start {
        return (0, 0);
    }
    (start, end)
}

fn main() {
    T0.set(Instant::now()).ok();
    STATS.store(std::env::var_os("SS_STATS").is_some(), Ordering::Relaxed);
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let nthreads = env_usize("NCPU")
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8))
        .clamp(1, 30);
    rayon::ThreadPoolBuilder::new().num_threads(nthreads).build_global().unwrap();

    let man = fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = manifest_ts(&man, "window_start");
    let we = manifest_ts(&man, "window_end");
    let cws = ws - WEEK;
    assert!(we > ws, "empty window");
    let span = (we - cws) as u64;
    assert!(span < (1u64 << 32), "window too long for u32 offsets");
    let tbits = 64 - span.leading_zeros();

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
    let mut catidx: rustc_hash::FxHashMap<&[u8], u16> = Default::default();
    let mut pitems: Vec<(Key, u16)> = vec![];
    for r in &prows {
        let ci = *catidx.entry(r[1]).or_insert_with(|| {
            catnames.push(r[1]);
            (catnames.len() - 1) as u16
        });
        pitems.push((key(r[0]), ci));
    }
    let products = SmallMap::new(&pitems);
    let unknown_cat = match catidx.get(&b"unknown"[..]) {
        Some(&c) => c,
        None => {
            catnames.push(b"unknown");
            (catnames.len() - 1) as u16
        }
    };
    let mut order: Vec<u16> = (0..catnames.len() as u16).collect();
    order.sort_by(|&a, &b| catnames[a as usize].cmp(catnames[b as usize]));
    let mut cat_rank = vec![0u16; catnames.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c as usize] = r as u16;
    }
    let crows = small_table::<2>(files.b[prod_i + 1].get());
    let mut camp_names: Vec<(&[u8], &[u8])> = vec![];
    let mut citems: Vec<(Key, u16)> = vec![];
    {
        let mut seen: rustc_hash::FxHashSet<&[u8]> = Default::default();
        for r in &crows {
            if seen.insert(r[0]) {
                citems.push((key(r[0]), camp_names.len() as u16));
                camp_names.push((r[0], r[1]));
            }
        }
    }
    let camps = SmallMap::new(&citems);
    let ncamp = camp_names.len();

    let link_chunks = make_chunks(&files, &paths, &[Kind::Ln]);
    let main_chunks = make_chunks(&files, &paths, &[Kind::Ev, Kind::Ck]);
    let ev_bytes: usize = paths.iter().enumerate().filter(|(_, p)| p.0 == Kind::Ev).map(|(i, _)| files.b[i].get().len()).sum();
    let ck_bytes: usize = paths.iter().enumerate().filter(|(_, p)| p.0 == Kind::Ck).map(|(i, _)| files.b[i].get().len()).sum();
    let nworkers = nthreads;
    let per_db = ev_bytes / 70 / nworkers / NDB * 5 / 4 + 16;
    let per_pb = ev_bytes / 70 / nworkers / NPB * 5 / 4 + 16;
    let per_pbc = ck_bytes / 45 / nworkers / NPB * 5 / 4 + 16;

    let dc0 = DateCache::new(ws);
    let mut cap = (total / 600).next_power_of_two().max(1 << 16);
    let (tab, mut outs, root, name1) = loop {
        let pbits = 64 - (cap as u64).leading_zeros();
        assert!(tbits + pbits <= 64, "key packing");
        let tab = IdTable::new(cap);
        // per-thread buffers are allocated lazily by their own thread (parallel, no serial
        // allocation storm in the main thread)
        let outs: Vec<Mutex<ParseOut>> = (0..nworkers + 1).map(|_| Mutex::new(ParseOut::default())).collect();
        let ensure = |o: &mut ParseOut| {
            if o.evb.is_empty() {
                // SS_SLABCAP forces tiny regions (tests the spill path)
                let force = env_usize("SS_SLABCAP");
                o.evb = Buckets::new(NDB, force.unwrap_or(per_db));
                o.pev = Buckets::new(NPB, force.unwrap_or(per_pb));
                o.ckb = Buckets::new(NPB, force.unwrap_or(per_pbc));
                o.dc = dc0.clone();
            }
        };
        let cx = Ctx {
            files: &files,
            tab: &tab,
            products: &products,
            unknown_cat,
            camps: &camps,
            ws,
            we,
            cws,
            tbits,
            pshift: (cap.trailing_zeros()).saturating_sub(PB_BITS),
        };
        // ---- A: links
        link_chunks.par_iter().for_each(|c| {
            let w = rayon::current_thread_index().unwrap_or(nworkers);
            let mut g = outs[w].lock().unwrap();
            ensure(&mut g);
            parse_chunk(&cx, c, w, &mut g);
        });
        if tab.overflow.load(Ordering::Relaxed) {
            cap *= 4;
            log!("id table overflow in links; cap -> {cap}");
            continue;
        }
        let links: Vec<(u32, u32)> =
            outs.iter().flat_map(|o| std::mem::take(&mut o.lock().unwrap().links)).collect();
        // concurrent union-find: link the larger root under the smaller with CAS, path halving.
        // Invariant parent[x] <= x, so every walk terminates and the root is the smallest slot.
        // huge pages: random probes during union-find
        let parent: &'static [AtomicU32] = {
            let raw = huge_slice::<u32>(cap);
            raw.par_chunks_mut(1 << 16).enumerate().for_each(|(c, ch)| {
                for (j, x) in ch.iter_mut().enumerate() {
                    *x = (c * (1 << 16) + j) as u32;
                }
            });
            unsafe { std::slice::from_raw_parts(raw.as_ptr() as *const AtomicU32, cap) }
        };
        let find = |mut x: u32| -> u32 {
            loop {
                let p = parent[x as usize].load(Ordering::Relaxed);
                if p == x {
                    return x;
                }
                let g = parent[p as usize].load(Ordering::Relaxed);
                if g != p {
                    // path halving (benign race: g <= p, any stale value still respects the invariant)
                    let _ = parent[x as usize].compare_exchange_weak(p, g, Ordering::Relaxed, Ordering::Relaxed);
                }
                x = g;
            }
        };
        links.par_iter().for_each(|&(a, b)| {
            let (mut ra, mut rb) = (find(a), find(b));
            while ra != rb {
                let (lo, hi) = if ra < rb { (ra, rb) } else { (rb, ra) };
                match parent[hi as usize].compare_exchange(hi, lo, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(_) => {
                        ra = find(lo);
                        rb = find(hi);
                    }
                }
            }
        });
        let root: &'static [u32] = {
            let raw = huge_slice::<u32>(cap);
            raw.par_chunks_mut(1 << 16).enumerate().for_each(|(c, ch)| {
                for (j, x) in ch.iter_mut().enumerate() {
                    *x = find((c * (1 << 16) + j) as u32);
                }
            });
            raw
        };
        root.par_iter().enumerate().for_each(|(i, &r)| {
            if r as usize != i {
                tab.set_root(i, r);
            }
        });
        // names: name1[root] = smallest-id slot + 1 (0 = the root itself)
        let arenas_a: Vec<std::sync::MutexGuard<ParseOut>> = outs.iter().map(|o| o.lock().unwrap()).collect();
        let sref = |i: u32| -> &[u8] {
            let p = tab.sptr(i);
            let o = (p.off & ((1 << 40) - 1)) as usize;
            &arenas_a[(p.off >> 40) as usize].arena[o..o + p.len as usize]
        };
        // huge pages: read once per person, at random, in the sessions phase
        let name1: &'static [AtomicU32] = {
            let raw = huge_slice::<u32>(cap);
            unsafe { std::slice::from_raw_parts(raw.as_ptr() as *const AtomicU32, cap) }
        };
        links.par_iter().for_each(|&(a, b)| {
            for x in [a, b] {
                let r = root[x as usize] as usize;
                let sx = sref(x);
                let mut cur = name1[r].load(Ordering::Relaxed);
                loop {
                    let cs = if cur == 0 { r as u32 } else { cur - 1 };
                    if cs == x || sx >= sref(cs) {
                        break;
                    }
                    match name1[r].compare_exchange_weak(cur, x + 1, Ordering::Relaxed, Ordering::Relaxed) {
                        Ok(_) => break,
                        Err(c) => cur = c,
                    }
                }
            }
        });
        drop(arenas_a);
        log!("links: {} edges, union-find + names done", links.len());

        // ---- B: events + clicks
        main_chunks.par_iter().for_each(|c| {
            let w = rayon::current_thread_index().unwrap_or(nworkers);
            let mut g = outs[w].lock().unwrap();
            ensure(&mut g);
            parse_chunk(&cx, c, w, &mut g);
        });
        // threads that parsed nothing still need (empty) buckets for the later phases
        for o in &outs {
            let mut g = o.lock().unwrap();
            if g.evb.is_empty() {
                g.evb = Buckets::empty(NDB);
                g.pev = Buckets::empty(NPB);
                g.ckb = Buckets::empty(NPB);
            }
        }
        let newids: usize = outs.iter().map(|o| o.lock().unwrap().newids).sum();
        if tab.overflow.load(Ordering::Relaxed) || newids > tab.limit {
            cap *= 4;
            log!("id table overflow ({newids} ids); cap -> {cap}");
            continue;
        }
        let outs: Vec<ParseOut> = outs.into_iter().map(|m| m.into_inner().unwrap()).collect();
        break (tab, outs, root, name1);
    };
    let _ = &root;
    let nl: [u64; 3] = outs.iter().fold([0; 3], |a, o| [a[0] + o.nlines[0], a[1] + o.nlines[1], a[2] + o.nlines[2]]);
    let nids: usize = outs.iter().map(|o| o.newids).sum();
    log!("parsed: {} event lines, {} click lines, {} link lines; {} ids (cap {})", nl[0], nl[1], nl[2], nids, cap);
    if std::env::var_os("SS_STOP_AFTER_PARSE").is_some() {
        std::process::exit(0); // profiling aid
    }
    let arenas = Arenas { a: outs.iter_mut().map(|o| std::mem::take(&mut o.arena)).collect() };

    // ---- C: dedup. One pass per event-id bucket with an L2-sized table; when two copies of an
    // event meet, the one with the larger ingest_ts (or the later one on a tie: copies with equal
    // ingest_ts are identical) is killed in place in its person bucket.
    let evbs: Vec<Buckets<Ev>> = outs.iter_mut().map(|o| std::mem::take(&mut o.evb)).collect();
    let mut pevs: Vec<Buckets<PEv>> = outs.iter_mut().map(|o| std::mem::take(&mut o.pev)).collect();
    let ckbs: Vec<Buckets<PCk>> = outs.iter_mut().map(|o| std::mem::take(&mut o.ckb)).collect();
    struct Ptrs(Vec<Vec<*mut PEv>>);
    unsafe impl Sync for Ptrs {}
    unsafe impl Send for Ptrs {}
    let pptr = Ptrs(pevs.iter_mut().map(|t| (0..NPB).map(|b| t.as_mut_ptr(b)).collect()).collect());
    let pptr = &pptr;
    let kill = |r: u64| {
        if r != NONE {
            let t = (r >> 56) as usize;
            let pb = ((r >> 40) & 0xFFFF) as usize;
            let i = (r & ((1 << 40) - 1)) as usize;
            // distinct copies are distinct rows: no two threads write the same element
            unsafe { (*pptr.0[t][pb].add(i)).dead = true };
        }
    };
    // per-thread dedup tables on huge pages, sized for the largest bucket (random probes, TLB)
    let max_n: usize = (0..NDB).map(|b| evbs.iter().map(|t| t.slice(b).len()).sum::<usize>()).max().unwrap_or(0);
    let max_cap = (max_n * 2).next_power_of_two().max(16);
    // allocated here, outside the parallel loop: huge_slice uses rayon internally, and running
    // nested rayon work while holding a worker's lock can deadlock (work stealing re-enters)
    let dd_scratch: Vec<Mutex<&'static mut [u64]>> =
        (0..nworkers + 1).map(|_| Mutex::new(huge_slice::<u64>(max_cap))).collect();
    let ndup = AtomicUsize::new(0);
    (0..NDB).into_par_iter().for_each(|b| {
        let n: usize = evbs.iter().map(|t| t.slice(b).len()).sum();
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        let w = rayon::current_thread_index().unwrap_or(nworkers);
        let mut tabg = dd_scratch[w].lock().unwrap();
        let tab = &mut tabg[..cap];
        tab.fill(0);
        let get = |s: u64| -> &Ev { &evbs[((s >> 27) & 31) as usize].slice(b)[(s & ((1 << 27) - 1)) as usize] };
        let mut dups = 0usize;
        for (t, part) in evbs.iter().enumerate() {
            let part = part.slice(b);
            // hashes of the next 8 records: computed once, used for the prefetch then the probe
            let mut hr = [0u64; 8];
            for (k, f) in part.iter().take(8).enumerate() {
                hr[k] = mix(Key { lo: f.klo, hi: f.khi });
                prefetch_ptr(tab.as_ptr().wrapping_add(hr[k] as usize & mask) as *const i8);
            }
            for (k, e) in part.iter().enumerate() {
                let m = hr[k & 7];
                if let Some(f) = part.get(k + 8) {
                    // prefetch the table slot of a record 8 ahead (table can exceed L2)
                    let mf = mix(Key { lo: f.klo, hi: f.khi });
                    hr[k & 7] = mf;
                    prefetch_ptr(tab.as_ptr().wrapping_add(mf as usize & mask) as *const i8);
                }
                let tag = ((m >> 32) | 1) & 0xFFFF_FFFF;
                let me = (tag << 32) | ((t as u64) << 27) | k as u64;
                let mut i = m as usize & mask;
                loop {
                    let s = tab[i];
                    if s == 0 {
                        tab[i] = me;
                        break;
                    }
                    if s >> 32 == tag {
                        let o = get(s);
                        if o.klo == e.klo && o.khi == e.khi {
                            dups += 1;
                            if e.ing < o.ing {
                                kill(o.r);
                                tab[i] = me;
                            } else {
                                kill(e.r);
                            }
                            break;
                        }
                    }
                    i = (i + 1) & mask;
                }
            }
        }
        ndup.fetch_add(dups, Ordering::Relaxed);
    });
    std::mem::forget(evbs);
    log!("dedup: {} duplicate copies", ndup.load(Ordering::Relaxed));
    let routed = pevs;

    // ---- D: per person bucket
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
                ev_tmp: vec![],
                ck_tmp: vec![],
                since: vec![0; ncamp],
                sday: vec![],
                hist: vec![],
                udw: None,
                udrows: Vec::new(),
            })
        })
        .collect();
    let tmask = (1u64 << tbits) - 1;
    let _kbits = tbits + (64 - (cap as u64).leading_zeros());
    let pshift_s = cap.trailing_zeros().saturating_sub(PB_BITS);
    let pmask: u64 = (1u64 << pshift_s) - 1;
    let files_r = &files;
    let tab_r = &tab;
    let name_r = &name1;
    let cat_rank_r = &cat_rank;
    let ud_dir = out.join("user_daily");
    fs::create_dir_all(&ud_dir).unwrap();
    let arenas_r = &arenas;
    let catnames_r = &catnames;
    // per-worker sort buffers sized for the largest bucket, THP-advised (random radix scatter)
    let maxe: usize = (0..NPB).map(|b| routed.iter().map(|t| t.slice(b).len()).sum::<usize>()).max().unwrap_or(0);
    let maxc: usize = (0..NPB).map(|b| ckbs.iter().map(|t| t.slice(b).len()).sum::<usize>()).max().unwrap_or(0);
    let ud_parts: Vec<usize> = (0..NPB)
        .into_par_iter()
        .map(|b| {
            let w = rayon::current_thread_index().unwrap_or(nworkers);
            let mut wk = workers[w].lock().unwrap();
            let wk = &mut *wk;
            if wk.ev_buf.capacity() < maxe {
                wk.ev_buf = hp_vec(maxe);
                wk.ev_tmp = hp_vec(maxe);
            }
            if wk.ck_buf.capacity() < maxc {
                wk.ck_buf = hp_vec(maxc);
                wk.ck_tmp = hp_vec(maxc);
            }
            let mut ev = std::mem::take(&mut wk.ev_buf);
            let mut ck = std::mem::take(&mut wk.ck_buf);
            let st = STATS.load(Ordering::Relaxed);
            let t0 = if st { Some(Instant::now()) } else { None };
            // radix by person only (few varying bits), then sort each person's (small) run by time
            let esrc: Vec<&[PEv]> = routed.iter().map(|t| t.slice(b)).collect();
            // within a bucket only the low `pshift` bits of the person root vary
            radix_gather(&esrc, |e| !e.dead, &mut ev, &mut wk.ev_tmp, &mut wk.hist, pshift_s, |e| (e.key >> tbits) & pmask);
            let mut q = 0;
            while q < ev.len() {
                let p = ev[q].key >> tbits;
                let mut r = q + 1;
                while r < ev.len() && (ev[r].key >> tbits) == p {
                    r += 1;
                }
                if r - q > 2048 {
                    // whale person: radix on the time bits
                    let run: Vec<PEv> = ev[q..r].to_vec();
                    let mut out = Vec::new();
                    let mut tmp = Vec::new();
                    let mut h = Vec::new();
                    radix_gather(&[&run[..]], |_| true, &mut out, &mut tmp, &mut h, tbits, |e| e.key & tmask);
                    ev[q..r].copy_from_slice(&out);
                } else if r - q > 1 {
                    ev[q..r].sort_unstable_by_key(|e| e.key);
                }
                q = r;
            }
            let t1 = if st { Some(Instant::now()) } else { None };
            let csrc: Vec<&[PCk]> = ckbs.iter().map(|t| t.slice(b)).collect();
            radix_gather(&csrc, |_| true, &mut ck, &mut wk.ck_tmp, &mut wk.hist, pshift_s, |c| (c.key >> tbits) & pmask);
            // per person: by time, ties (same ms) by click_id bytes
            let cmp = |x: &PCk, y: &PCk| {
                x.key.cmp(&y.key).then_with(|| {
                    x.b0.cmp(&y.b0).then_with(|| {
                        if x.len <= 8 && y.len <= 8 {
                            x.len.cmp(&y.len)
                        } else {
                            files_r
                                .s(SPtr { off: x.off, len: x.len })
                                .cmp(files_r.s(SPtr { off: y.off, len: y.len }))
                        }
                    })
                })
            };
            let mut q = 0;
            while q < ck.len() {
                let p = ck[q].key >> tbits;
                let mut r = q + 1;
                while r < ck.len() && (ck[r].key >> tbits) == p {
                    r += 1;
                }
                if r - q > 2048 {
                    // big run: radix on time, then fix ties (same ms) by click_id
                    let run: Vec<PCk> = ck[q..r].to_vec();
                    let mut out = Vec::new();
                    let mut tmp = Vec::new();
                    let mut h = Vec::new();
                    radix_gather(&[&run[..]], |_| true, &mut out, &mut tmp, &mut h, tbits, |c| c.key & tmask);
                    let mut a = 0;
                    while a < out.len() {
                        let mut z = a + 1;
                        while z < out.len() && out[z].key == out[a].key {
                            z += 1;
                        }
                        if z - a > 1 {
                            out[a..z].sort_unstable_by(cmp);
                        }
                        a = z;
                    }
                    ck[q..r].copy_from_slice(&out);
                } else if r - q > 1 {
                    ck[q..r].sort_unstable_by(cmp);
                }
                q = r;
            }
            let t2 = if st { Some(Instant::now()) } else { None };
            let mut rows: Vec<UdRow> = Vec::new();
            // persons of this bucket, then their names resolved in tight independent loops
            // (random reads into name1 / the id table overlap instead of serializing per person)
            let mut pers: Vec<(u32, u32, u32)> = Vec::new();
            {
                let mut i = 0usize;
                while i < ev.len() {
                    let p = ev[i].key >> tbits;
                    let mut iend = i + 1;
                    while iend < ev.len() && (ev[iend].key >> tbits) == p {
                        iend += 1;
                    }
                    pers.push((i as u32, iend as u32, p as u32));
                    i = iend;
                }
            }
            let nslots: Vec<u32> = pers
                .iter()
                .map(|&(_, _, p)| {
                    let n1 = name_r[p as usize].load(Ordering::Relaxed);
                    if n1 == 0 { p } else { n1 - 1 }
                })
                .collect();
            let names: Vec<SPtr> = nslots.iter().map(|&ns| tab_r.sptr(ns)).collect();
            let mut cj = 0usize;
            for (pi, &(i, iend, p)) in pers.iter().enumerate() {
                let (i, iend, p) = (i as usize, iend as usize, p as u64);
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
                    names[pi],
                    cat_rank_r,
                    wk,
                    &mut rows,
                );
                cj = cend;
            }
            let t3 = if st { Some(Instant::now()) } else { None };
            wk.ev_buf = ev;
            wk.ck_buf = ck;
            // batch rows per worker: fewer, larger Arrow batches
            let nrows = rows.len();
            wk.udrows.extend(rows.drain(..));
            if wk.udrows.len() >= 1 << 16 {
                let pw = wk.udw.get_or_insert_with(|| ud_writer(&ud_dir, w));
                ud_write(pw, &wk.udrows, arenas_r, catnames_r);
                wk.udrows.clear();
            }
            if let (Some(t0), Some(t1), Some(t2), Some(t3)) = (t0, t1, t2, t3) {
                let t4 = Instant::now();
                for (k, d) in [(0, t1 - t0), (1, t2 - t1), (2, t3 - t2), (3, t4 - t3)] {
                    SI[k].fetch_add(d.as_nanos() as u64, Ordering::Relaxed);
                }
            }
            nrows
        })
        .collect();
    if STATS.load(Ordering::Relaxed) {
        let g = |k: usize| SI[k].load(Ordering::Relaxed) as f64 / 1e9;
        log!("stats sessions thread-s: ev-sort {:.3} ck-sort {:.3} person {:.3} parquet {:.3}", g(0), g(1), g(2), g(3));
    }
    std::mem::forget(routed);
    std::mem::forget(ckbs);
    let nud: usize = ud_parts.iter().sum();
    log!("sessions done: {} user_daily rows", nud);
    // close the per-worker user_daily files (in parallel); an empty table still gets one file
    // flush each worker's remaining rows and close its file (in parallel)
    let mut pend: Vec<(usize, Option<PqWriter>, Vec<UdRow>)> = workers
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let mut g = w.lock().unwrap();
            (i, g.udw.take(), std::mem::take(&mut g.udrows))
        })
        .filter(|(_, w, r)| w.is_some() || !r.is_empty())
        .collect();
    if pend.is_empty() {
        pend.push((0, None, Vec::new()));
    }
    pend.into_par_iter().for_each(|(i, w, rows)| {
        let mut w = w.unwrap_or_else(|| ud_writer(&ud_dir, i));
        if !rows.is_empty() {
            ud_write(&mut w, &rows, arenas_r, catnames_r);
        }
        w.close().unwrap();
    });

    // ---- campaign_daily
    let mut acc = CampAcc::new(ncs);
    for w in workers {
        let w = w.into_inner().unwrap();
        for k in 0..ncs {
            for j in 0..3 {
                acc.cred[k][j] += w.acc.cred[k][j];
            }
            acc.assisted[k] += w.acc.assisted[k];
        }
    }

    write_outputs(&out, &acc, &camp_names, ndays, ws_day);
    log!("done");
    std::process::exit(0);
}

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
    let mut si: u32 = 0; // session index within this person
    wk.sday.clear();
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
        // A campaign assists every session started while it has >= 1 click in [start-7d, start].
        // Track activity spans [since, now) over session indices and add them per day when the
        // span closes (O(clicks x days) instead of O(sessions x active campaigns)).
        while jhi < m && t(ck[jhi].key) <= start {
            let c = ck[jhi].camp as usize;
            if wk.cnt[c] == 0 {
                wk.pos[c] = wk.active.len() as u32;
                wk.active.push(c as u16);
                wk.since[c] = si;
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
                span_add(&mut wk.acc.assisted, &wk.sday, c, ndays, wk.since[c], si);
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
        let cr = &mut wk.acc.cred[ix];
        cr[0] += 1;
        cr[1] += has_p as i64;
        cr[2] += revenue;
        wk.sday.push(d as u16);
        si += 1;
        i = k;
    }
    flush(&mut ud, &mut wk.purch, rows);
    // close the still-open spans and reset the active set
    for idx in 0..wk.active.len() {
        let c = wk.active[idx] as usize;
        span_add(&mut wk.acc.assisted, &wk.sday, c, ndays, wk.since[c], si);
        wk.cnt[c] = 0;
    }
    wk.active.clear();
}

/// assisted[c][day(s)] += 1 for every session index s in [a, b) (session days are nondecreasing).
#[inline]
fn span_add(assisted: &mut [u64], sday: &[u16], c: usize, ndays: usize, a: u32, b: u32) {
    let (mut k, b) = (a as usize, b as usize);
    if b - k <= 16 {
        // short spans (the common case): linear
        while k < b {
            assisted[c * ndays + sday[k] as usize] += 1;
            k += 1;
        }
        return;
    }
    while k < b {
        let d = sday[k];
        let e = k + sday[k..b].partition_point(|&x| x <= d);
        assisted[c * ndays + d as usize] += (e - k) as u64;
        k = e;
    }
}

#[allow(clippy::too_many_arguments)]
type PqWriter = parquet::arrow::ArrowWriter<std::io::BufWriter<fs::File>>;

fn pq_props() -> parquet::file::properties::WriterProperties {
    parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
        .set_max_row_group_row_count(Some(1 << 20))
        .build()
}

fn ud_schema() -> Arc<arrow_schema::Schema> {
    use arrow_schema::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ]))
}

fn ud_writer(dir: &Path, idx: usize) -> PqWriter {
    let f = fs::File::create(dir.join(format!("part-{idx:05}.parquet"))).unwrap();
    parquet::arrow::ArrowWriter::try_new(std::io::BufWriter::with_capacity(1 << 22, f), ud_schema(), Some(pq_props())).unwrap()
}

/// Encode a bucket's user_daily rows into the worker's open Parquet file.
fn ud_write(w: &mut PqWriter, rows: &[UdRow], names: &Arenas, catnames: &[&[u8]]) {
    use arrow_array::builder::{Date32Builder, Int64Builder, StringBuilder};
    use arrow_array::{ArrayRef, RecordBatch};
    let n = rows.len();
    let nb: usize = rows.iter().map(|r| r.name.len as usize).sum();
    let mut uid = StringBuilder::with_capacity(n, nb);
    let mut day = Date32Builder::with_capacity(n);
    let mut ses = Int64Builder::with_capacity(n);
    let mut evs = Int64Builder::with_capacity(n);
    let mut rev = Int64Builder::with_capacity(n);
    let mut top = StringBuilder::with_capacity(n, n * 8);
    for r in rows {
        uid.append_value(unsafe { std::str::from_utf8_unchecked(names.s(r.name)) });
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
    w.write(&RecordBatch::try_new(ud_schema(), cols).unwrap()).unwrap();
}

fn write_outputs(
    out: &Path,
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
            if acc.cred[k][0] == 0 && acc.assisted[k] == 0 {
                continue;
            }
            cid.append_value(unsafe { std::str::from_utf8_unchecked(id) });
            chn.append_value(unsafe { std::str::from_utf8_unchecked(ch) });
            day.append_value((ws_day + d as i64) as i32);
            ses.append_value(acc.cred[k][0]);
            pur.append_value(acc.cred[k][1]);
            rev.append_value(acc.cred[k][2]);
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


#[cfg(test)]
mod ts_fuzz {
    use super::*;
    #[derive(Default)]
    struct OldCache {
        k0: u64,
        k1: u16,
        day: i64,
    }
    /// previous implementation (reference) -> epoch ms. None = bad timestamp (F3 beyond i64 included).
    #[inline]
    fn old_parse_ts(f: &[u8], dc: &mut OldCache) -> Option<i64> {
        let n = f.len();
        if n == 0 {
            return None;
        }
        if n == 20 || n == 25 {
            if f[4] == b'-' {
                if f[7] != b'-' || f[10] != b'T' || f[13] != b':' || f[16] != b':' {
                    return None;
                }
                let k0 = u64::from_le_bytes(f[0..8].try_into().unwrap());
                let k1 = u16::from_le_bytes(f[8..10].try_into().unwrap());
                let day = if k0 == dc.k0 && k1 == dc.k1 && dc.k0 != 0 {
                    dc.day
                } else {
                    let d = parse_date(f)?;
                    dc.k0 = k0;
                    dc.k1 = k1;
                    dc.day = d;
                    d
                };
                let h = d2(f, 11)?;
                let mi = d2(f, 14)?;
                let s = d2(f, 17)?;
                if h > 23 || mi > 59 || s > 59 {
                    return None;
                }
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
                return Some(day * DAY + h * 3_600_000 + mi * 60_000 + s * 1000 - off);
            }
        }
        // F3: digits only
        let mut i = 0;
        while i < n && f[i] == b'0' {
            i += 1;
        }
        let r = &f[i..];
        let m = r.len();
        if m > 19 {
            // too long for i64 if all digits; either way bad
            return None;
        }
        let mut v: u64 = 0;
        let mut j = 0;
        while j + 8 <= m {
            v = v * 100_000_000 + p8(&r[j..j + 8])?;
            j += 8;
        }
        while j < m {
            let c = r[j].wrapping_sub(b'0');
            if c > 9 {
                return None;
            }
            v = v.wrapping_mul(10).wrapping_add(c as u64);
            j += 1;
        }
        // m <= 19 digits: at most 9_999_999_999_999_999_999 which may exceed u64? no: u64 max ~1.8e19
        if m == 19 && r[0] > b'1' {
            // >= 2e18 .. 9.99e18 fits u64; check i64 below
        }
        if v > i64::MAX as u64 {
            return None;
        }
        Some(v as i64)
    }


    #[test]
    fn new_matches_old() {
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let ws = parse_ts(b"2028-02-20T00:00:00Z", &mut DateCache::default()).unwrap();
        let mut dc = DateCache::new(ws);
        let mut oc = OldCache::default();
        let bases: Vec<String> = vec![
            "2028-02-29T23:59:59Z".into(), "2027-02-29T10:00:00Z".into(), "2028-12-31T00:00:00+05:30".into(),
            "2026-01-01T00:00:00-00:00".into(), "2028-03-01T12:34:56-10:00".into(), "1999-07-04T01:02:03Z".into(),
            "2100-02-29T00:00:00Z".into(), "2028-02-30T00:00:00Z".into(), "1791071890000".into(), "0001791071890000".into(),
            "2028-04-31T00:00:00Z".into(), "2028-13-01T00:00:00Z".into(), "2028-00-10T00:00:00Z".into(), "2028-01-00T00:00:00Z".into(),
            "2028-06-15T24:00:00Z".into(), "2028-06-15T23:60:00Z".into(), "2028-06-15T23:00:60Z".into(), "2028-06-15T23:00:00+24:00".into(),
            "2028-06-15T23:00:00+05:60".into(), "2028-06-15T23:00:00+0500".into(), "2028-06-15 23:00:00Z".into(), "2028-06-15t23:00:00z".into(),
            "99999999999999999999".into(), "9223372036854775807".into(), "9223372036854775808".into(), "00000000000000000000".into(),
        ];
        let alphabet = b"0123456789-T:Z+ zt.e";
        let mut n = 0;
        for round in 0..3_000_000u64 {
            let mut s = bases[(rnd() % bases.len() as u64) as usize].clone().into_bytes();
            let muts = rnd() % 4;
            for _ in 0..muts {
                let op = rnd() % 10;
                if s.is_empty() { break; }
                let i = (rnd() % s.len() as u64) as usize;
                match op {
                    0..=5 => s[i] = b'0' + (rnd() % 10) as u8,
                    6 => s[i] = alphabet[(rnd() % alphabet.len() as u64) as usize],
                    7 => { s.remove(i); }
                    8 => s.insert(i, b'0' + (rnd() % 10) as u8),
                    _ => s[i] = (rnd() % 256) as u8,
                }
            }
            let a = parse_ts(&s, &mut dc);
            let b = old_parse_ts(&s, &mut oc);
            assert_eq!(a, b, "mismatch on {:?} (round {round})", String::from_utf8_lossy(&s));
            n += 1;
        }
        assert!(n > 0);
    }
}


#[cfg(test)]
mod tok_fuzz {
    use super::*;
    fn old_for_lines<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(data: &'a [u8], mut f: F) -> u64 {
        let mut bounds = [(0usize, 0usize); N];
        let mut nf = 0usize;
        let mut fstart = 0usize;
        let mut qmask = 0u32;
        let mut lines = 0u64;
        macro_rules! finish {
            ($end:expr) => {{
                let end = $end;
                lines += 1;
                if nf + 1 == N {
                    bounds[nf] = (fstart, end);
                    let mut fs: [&'a [u8]; N] = [&[]; N];
                    let mut ok = true;
                    for k in 0..N {
                        let (s, e) = bounds[k];
                        let v = &data[s..e];
                        if qmask & (1 << k) != 0 {
                            let l = v.len();
                            if l < 2 || v[0] != b'"' || v[l - 1] != b'"' || memchr::memchr(b'"', &v[1..l - 1]).is_some() {
                                ok = false;
                                break;
                            }
                            fs[k] = &v[1..l - 1];
                        } else {
                            fs[k] = v;
                        }
                    }
                    if ok {
                        f(&fs);
                    }
                }
                nf = 0;
                qmask = 0;
                fstart = end + 1;
            }};
        }
        let n = data.len();
        let mut blk = 0usize;
        let mut tail = [0u8; 64];
        while blk < n {
            let mut mask = if blk + 64 <= n {
                delim_mask(&data[blk..blk + 64])
            } else {
                tail = [0u8; 64];
                tail[..n - blk].copy_from_slice(&data[blk..]);
                delim_mask(&tail)
            };
            while mask != 0 {
                let p = blk + mask.trailing_zeros() as usize;
                mask &= mask - 1;
                match data[p] {
                    b',' => {
                        if nf < N {
                            bounds[nf] = (fstart, p);
                        }
                        nf += 1;
                        fstart = p + 1;
                    }
                    b'"' => {
                        qmask |= 1u32 << nf.min(31);
                    }
                    _ => finish!(p),
                }
            }
            blk += 64;
        }
        let _ = &tail;
        if fstart < data.len() || (fstart == data.len() && nf > 0) {
            finish!(data.len());
        }
        lines
    }


    fn collect<const N: usize>(d: &[u8], new: bool) -> (Vec<Vec<Vec<u8>>>, u64) {
        let mut v = Vec::new();
        let n = if new {
            for_lines::<N, _>(d, |f| v.push(f.iter().map(|x| x.to_vec()).collect()))
        } else {
            old_for_lines::<N, _>(d, |f| v.push(f.iter().map(|x| x.to_vec()).collect()))
        };
        (v, n)
    }
    #[test]
    fn new_matches_old() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut rnd = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alpha = b"aab,,,\"\"\n\r x";
        for round in 0..300_000u64 {
            let len = (rnd() % 300) as usize;
            let d: Vec<u8> = (0..len).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect();
            assert_eq!(collect::<2>(&d, true), collect::<2>(&d, false), "N=2 round {round}: {:?}", String::from_utf8_lossy(&d));
            assert_eq!(collect::<4>(&d, true), collect::<4>(&d, false), "N=4 round {round}");
            assert_eq!(collect::<7>(&d, true), collect::<7>(&d, false), "N=7 round {round}");
        }
    }
}

#[cfg(test)]
mod ts_bench {
    use super::*;
    #[test]
    #[ignore]
    fn bench_parse_ts() {
        let text = std::fs::read_to_string("../out/ts_sample.txt").unwrap();
        let v: Vec<&[u8]> = text.lines().map(|l| l.as_bytes()).collect();
        let ws = parse_ts(b"2027-12-01T00:00:00Z", &mut DateCache::default()).unwrap();
        let mut dc = DateCache::new(ws);
        let t = std::time::Instant::now();
        let mut acc = 0i64;
        for _ in 0..5 {
            for s in &v {
                acc = acc.wrapping_add(parse_ts(std::hint::black_box(s), &mut dc).unwrap_or(1));
            }
        }
        let ns = t.elapsed().as_nanos() as f64 / (5 * v.len()) as f64;
        eprintln!("parse_ts: {ns:.1} ns/ts over {} samples (acc {acc})", v.len());
        let t = std::time::Instant::now();
        for _ in 0..5 {
            for s in &v {
                acc = acc.wrapping_add(key(std::hint::black_box(s)).lo as i64);
            }
        }
        let ns = t.elapsed().as_nanos() as f64 / (5 * v.len()) as f64;
        eprintln!("key(): {ns:.1} ns (acc {acc})");
    }
}

#[cfg(test)]
mod tok_bench {
    use super::*;
    #[test]
    #[ignore]
    fn bench_for_lines() {
        let path = std::env::var("TOK_FILE").unwrap();
        let data = std::fs::read(&path).unwrap();
        let body = &data[data.iter().position(|&c| c == b'\n').unwrap() + 1..data.len() - 1];
        let mut lines = 0u64;
        let mut acc = 0usize;
        let t = std::time::Instant::now();
        for _ in 0..5 {
            lines += for_lines::<7, _>(body, |f| {
                acc = acc.wrapping_add(f[0].len() + f[6].len());
            });
        }
        let el = t.elapsed();
        eprintln!(
            "for_lines: {:.1} ns/line, {:.2} GB/s ({} lines, acc {acc})",
            el.as_nanos() as f64 / lines as f64,
            (5 * body.len()) as f64 / el.as_secs_f64() / 1e9,
            lines / 5
        );
    }
}
