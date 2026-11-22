//! Synthetic training data for profile-guided optimization (PGO) of the engine.
//! Written from the shape described in rules.md (formats, dirt rates); not exact, not scored.
//! Usage: traingen OUT_DIR EVENTS
use std::fs::{self, File};
use std::io::{BufWriter, Write};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1e6 < p
    }
}

fn b36(mut v: u64, len: usize) -> String {
    let a = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut s = vec![b'0'; len];
    for i in (0..len).rev() {
        s[i] = a[(v % 36) as usize];
        v /= 36;
    }
    String::from_utf8(s).unwrap()
}

fn civil(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + if m <= 2 { 1 } else { 0 }, m, d)
}

fn f1(ms: i64) -> String {
    let s = ms.div_euclid(1000);
    let (y, m, d) = civil(s.div_euclid(86_400));
    let r = s.rem_euclid(86_400);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", r / 3600, r / 60 % 60, r % 60)
}

fn ts(r: &mut Rng, ms: i64) -> String {
    let p = r.below(1000);
    if p < 5 {
        return ["", "2027-12-13 10:00:00Z", "2027-13-01T00:00:00Z", "1.79e12", "2027-12-13T10:00:00.5Z"][r.below(5) as usize].into();
    }
    if p < 585 {
        f1(ms)
    } else if p < 685 {
        let off = (r.below(23) as i64 - 10) * 60;
        let local = f1(ms + off * 60_000);
        let sign = if off < 0 { '-' } else { '+' };
        format!("{}{}{:02}:{:02}", &local[..19], sign, off.abs() / 60, off.abs() % 60)
    } else {
        format!("{}", ms)
    }
}

fn q(r: &mut Rng, f: &str) -> String {
    if r.chance(0.2) {
        format!("\"{f}\"")
    } else {
        f.to_string()
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let out = std::path::Path::new(&args[1]);
    let events: u64 = args[2].parse().unwrap();
    let mut r = Rng(0x2545_f491_4f6c_dd1d);
    for d in ["events", "clicks", "links"] {
        fs::create_dir_all(out.join(d)).unwrap();
    }
    let ws: i64 = 1_828_915_200_000; // 2027-12-15T00:00:00Z
    let we = ws + 14 * 86_400_000;
    fs::write(out.join("manifest.json"), format!("{{\"window_start\": \"{}\", \"window_end\": \"{}\"}}\n", f1(ws), f1(we))).unwrap();
    let mut p = BufWriter::new(File::create(out.join("products.csv")).unwrap());
    writeln!(p, "product_id,category").unwrap();
    for i in 1..=50_000 {
        writeln!(p, "p{i},cat{}", i % 20).unwrap();
    }
    let mut c = BufWriter::new(File::create(out.join("campaigns.csv")).unwrap());
    writeln!(c, "campaign_id,channel").unwrap();
    for i in 1..=2000 {
        writeln!(c, "c_{i},ch{}", i % 5).unwrap();
    }
    let people = (events / 30).max(10);
    let uid = |pi: u64, k: u64| format!("u{}", b36(pi * 7 + k, 8));
    let mut l = BufWriter::new(File::create(out.join("links/part-0.csv")).unwrap());
    writeln!(l, "user_id_a,user_id_b").unwrap();
    for pi in 0..people {
        let n = 1 + (r.below(100) < 55) as u64 + (r.below(100) < 25) as u64;
        for k in 1..n {
            let a = uid(pi, k - 1);
            let b = uid(pi, k);
            writeln!(l, "{},{}", q(&mut r, &a), q(&mut r, &b)).unwrap();
        }
    }
    let nf = 8;
    let mut ev: Vec<BufWriter<File>> = (0..nf)
        .map(|i| {
            let mut w = BufWriter::new(File::create(out.join(format!("events/part-{i}.csv"))).unwrap());
            writeln!(w, "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents").unwrap();
            w
        })
        .collect();
    for i in 0..events {
        let pi = (r.below(people) * r.below(people)) / people;
        let u = uid(pi, r.below(2));
        let t = ws + r.below((we - ws) as u64) as i64;
        let ing = t + if r.chance(0.01) { 90_000_000 } else { r.below(120_000) as i64 };
        let ty = match r.below(1000) {
            0 => "Purchase",
            1..=30 => "purchase",
            31..=330 => "cart",
            _ => "view",
        };
        let price = if ty == "purchase" { if r.chance(0.05) { "12.5".into() } else { format!("{}", 100 + r.below(50_000)) } } else { String::new() };
        let prod = if r.chance(0.005) { "P1".to_string() } else { format!("p{}", 1 + r.below(50_000) * r.below(50_000) / 50_000) };
        let id = format!("e{}", b36(i * 2_654_435_761 % 3_000_000_000_000, 9));
        let copies = if r.chance(0.03) { 2 } else { 1 };
        for cpy in 0..copies {
            let w = &mut ev[r.below(nf) as usize];
            if r.chance(0.0005) {
                writeln!(w, "{id},\"bad\"line").unwrap();
                continue;
            }
            let tsv = ts(&mut r, t);
            let line = format!(
                "{},{},{},{},{},{},{}",
                q(&mut r, &id),
                q(&mut r, &u),
                q(&mut r, &tsv),
                q(&mut r, &f1(ing + cpy * 60_000)),
                q(&mut r, ty),
                q(&mut r, &prod),
                q(&mut r, &price)
            );
            writeln!(w, "{line}").unwrap();
        }
    }
    let mut ck = BufWriter::new(File::create(out.join("clicks/part-0.csv")).unwrap());
    writeln!(ck, "click_id,user_id,campaign_id,ts").unwrap();
    for i in 0..events / 4 {
        let pi = (r.below(people) * r.below(people)) / people;
        let u = uid(pi, r.below(2));
        let t = ws - 7 * 86_400_000 + r.below((we - ws + 7 * 86_400_000) as u64) as i64;
        let camp = if r.chance(0.005) { "C_9".to_string() } else { format!("c_{}", 1 + r.below(2000)) };
        let id = format!("k{}", b36(i * 40_503 + 7, 9));
        let tsv = ts(&mut r, t);
        writeln!(ck, "{},{},{},{}", q(&mut r, &id), q(&mut r, &u), q(&mut r, &camp), q(&mut r, &tsv)).unwrap();
    }
}
