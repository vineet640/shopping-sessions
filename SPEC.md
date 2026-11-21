# SPEC — every rule from rules.md as a numbered testable statement

Source of truth: `reference/rules.md` (copy of the track's rules.md). Fixture IDs in brackets.

## Inputs
1. Read every `*.csv` in `events/`, `clicks/`, `links/` (any count, header-only files allowed). Plus `products.csv`, `campaigns.csv` (clean), `manifest.json` (window_start, window_end; F1 strings).
2. The first line of every CSV is the header and is skipped; any later line equal to the header is ordinary data [L3].

## CSV dialect
3. Lines end only at `\n`. `\r` is an ordinary character [E20]. A trailing `\n` at EOF does not create an extra line.
4. Fields split on every `,`. Wrong field count ⇒ malformed (dropped). Empty line ⇒ malformed.
5. A field wrapped in `"…"` (both ends) has the quotes stripped; `""` is empty. A `"` anywhere else (including a lone `"` field) ⇒ malformed [L3, E15].
6. Empty field (after unquoting) = null.
7. Spaces are part of values; no trimming; no case folding [E19, E11].
8. "Digits" = ASCII `0`–`9` only [E18].

## Timestamps
9. F1 `YYYY-MM-DDTHH:MM:SSZ`; F2 same with `+HH:MM`/`-HH:MM` (HH 00–23, MM 00–59, colon required), UTC = local − offset; `-00:00` valid [E7, E17].
10. F3 = one or more ASCII digits = epoch ms; leading zeros allowed; value beyond int64 ⇒ out of window (bad) [E18].
11. Anything else is bad: empty, fractional secs, lowercase `t`/`z`, space separator, no zone, impossible calendar (month 13, Feb 30, Feb 29 non-leap, hour 24, second 60, minute 60), signs/decimals/exponent in F3.
12. Event window: `window_start <= ts < window_end` [E12]. Click window: `window_start − 7d <= ts < window_end` [A10].
13. `ingest_ts` is always valid F1.

## Linked IDs
14. Link line kept iff well-formed (2 fields) and both fields non-empty [L3].
15. Kept lines union their two IDs, transitively across files/order; repeats and self-links are no-ops [L2].
16. Person name = byte-wise smallest ID in its component (may be an ID with no events) [L2, L3]. Unlinked ID = its own person.
17. Events/clicks map to person via their user_id; links apply at all times.

## Events (in order)
18. Drop malformed lines and empty `event_id` [E13].
19. Dedup on exact `event_id`: keep the copy with smallest `ingest_ts` (ties are identical) BEFORE validation [E4, E5].
20. Validate kept copy: non-empty user_id; ts valid and in event window; event_type ∈ {view, cart, purchase} exactly; product_id non-empty; purchase price_cents = ASCII digits only, value 1..10,000,000 (leading zeros ok) [E10, E18]. view/cart price ignored.
21. Drop if `ingest_ts − ts > 24h` (exactly 24h kept; negative kept) [E6].
22. Sessionize per person (all IDs) by ts; new session iff gap to previous distinct ts > 1,800,000 ms [E1, E2, E16, L1].
23. Session day = UTC date of session start; all its events count there [E3].
24. Category from products.csv; missing ⇒ `unknown` [E9].

## Clicks
25. Drop malformed and empty `click_id`.
26. Dedup on click_id (copies identical; keep any).
27. Valid iff non-empty user_id, ts valid and in click window, campaign_id in campaigns.csv (exact) [A6, A7].
28. Credit: among the person's valid clicks with `0 <= start − click.ts <= 604,800,000`, latest ts; tie ⇒ smallest click_id bytewise [A2–A5, L4].
29. No qualifying click ⇒ campaign `organic`, channel `organic`.
30. One click may credit many sessions [A8]; clicks never alter sessions [A9].
31. Assists: every campaign with ≥1 of the person's valid clicks in `[start − 7d, start]` assists the session once (including the crediting campaign). organic never assists [A11, A12].

## Outputs
32. `user_daily/`: per (person, day) with ≥1 session: user_id, day(DATE), sessions, events, revenue_cents (0 if none), top_category (max summed purchase price per category; tie ⇒ bytewise smallest; null if no purchases) [E8, E9].
33. `campaign_daily/`: per (campaign_id, day) with ≥1 credited or assisted session: campaign_id, channel, day, sessions (credited), purchasing_sessions (credited with ≥1 purchase), revenue_cents (credited), assisted_sessions (0 for organic).
34. Exactly these columns; any int width; `day` Parquet DATE; one or more `*.parquet` files directly in the folder; no `.`/`_` prefixed names relied upon.
35. Self-check: Σsessions and Σrevenue equal across tables; Σassisted ≥ Σ non-organic credited sessions.

## Runtime
36. Exit 0 only on success; logs to stderr; threads from NCPU, memory from MEM_GB; spills only to SCRATCH_DIR; no writes elsewhere (root fs ro, /tmp 64MB).
