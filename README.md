# Money Flow Map

A real-time, **heuristic** read of where money is moving across markets right now: stocks, bonds,
the dollar, gold, silver, oil, Bitcoin, Ether and stablecoins, plus whichever **single large-cap
stock** is moving most unusually at that moment (if any). It runs as **one
lightweight Python process** that listens to free, low-latency public data feeds. It serves the
read as a **local JSON API** (REST, WebSocket, SSE, webhooks) for other apps, plus an
**optional dashboard**.

![dashboard](docs/dashboard.jpg)

> **Heuristic, not fact.** Prices are public; *flows between assets are inferred* from
> simultaneous opposite moves. Nobody can see where each dollar actually goes. Nothing here is
> advice, and nothing here can place an order: it uses market data only.

---

## Contents
1. [Quick start](#1-quick-start)
2. [What it shows](#2-what-it-shows)
3. [How the flows are computed](#3-how-the-flows-are-computed)
4. [Sources, measured latency, terms](#4-sources-measured-latency-terms)
5. [Keys and configuration](#5-keys-and-configuration)
6. [Connecting other apps (API)](#6-connecting-other-apps-api)
7. [The dashboard](#7-the-dashboard)
8. [Performance](#8-performance)
9. [Recovery after sleep or network loss](#9-recovery-after-sleep-or-network-loss)
10. [Tests and tools](#10-tests-and-tools)
11. [Limits: what is real-time and what is not](#11-limits-what-is-real-time-and-what-is-not)
12. [Project layout](#12-project-layout)
13. [License](#13-license)

---

## 1. Quick start

```powershell
.\start.ps1
```
Or double-click **`start.bat`**. This finds Python 3.11+, installs `aiohttp` only if it is
missing, starts the service and opens the dashboard at <http://127.0.0.1:8765/>. Stop it with
Ctrl+C or by closing the window.

**It does not keep running in the background.** It stops by itself **5 minutes after the last
dashboard tab (or app) disconnects**, and also 5 minutes after a start if nobody opens it. A page
refresh is fine; the 5 minutes are there for that. Stopping is clean: the feeds stop, the port is
freed, the history cache is saved, open dashboard tabs and apps are told the service is going away,
and the process exits, within 2 s of Ctrl+C with a tab or an app connected (the port is free
during the save, so `start.bat` works again at once; a second Ctrl+C meanwhile waits for the save).
Closing the window, logging off or shutting Windows down saves the cache first, then Windows ends
the process (Windows allows a closing window about 4.5 s; the save takes about half a second, and
1-1.5 s with every core busy: see section 9). The log says why ("no dashboard
tab or app connected for 5 minutes: stopping"). Nothing restarts it: there is no scheduled task, startup
entry, service or watchdog. Start it again with `start.bat` when you want it.

- An app using the API keeps it alive: an open WebSocket/SSE stream, or any request (or webhook
  delivery) within the 5 minutes.
- A dashboard tab that is only minimized or in the background still counts as a viewer, so close
  the tab (or the browser) to let it stop.
- To keep it always on: `.\start.ps1 -StayRunning`, `python -m moneyflow run --stay-running`, or
  `idle_shutdown_minutes = 0` under `[server]` in `moneyflow.local.toml`. Another delay:
  `idle_shutdown_minutes = 15` or `--idle-minutes 15`.
- A webhook-only setup (nothing reads the API) should use the always-on option. Webhooks fire only
  on events, so the service could otherwise stop between them.

```powershell
.\start.ps1 -NoDashboard        # API only (headless)
.\start.ps1 -StayRunning        # do not stop when unused
python -m moneyflow run --no-dashboard --port 8800
python -m moneyflow read --tf 1h # print the current read in the terminal
python -m moneyflow keys         # which API keys were found (values are never shown)
python tools/selftest.py         # check a RUNNING service end to end (about 270 checks)
python -m moneyflow check        # measure every source's latency (with the service stopped)
```

What to expect:
- **First start:** about 20-30 s to download 35 days of 5-minute history (measured: 22 s, and 26 s on 25 Nov).
  Binance's request budget is shared by every program on your internet connection. If others use
  it in the same minute, the service pauses until the next minute rather than use more than half of
  it, and the log says so (one first start next to a second copy took 81 s).
- **Later starts:** about 7-25 s, because history is cached in `data/` and only the missing tail is downloaded (measured: 7-21 s).
- **While loading:** nodes show **WARMING UP** until their volatility history is in.

Requirements: Windows (also works on macOS/Linux), Python 3.11+ (developed on 3.13),
`aiohttp`. `orjson` and `psutil` are used when installed. `tzdata` provides the time zones on
Windows. Everything is in `requirements.txt`.

---

## 2. What it shows

| Element | Meaning |
|---|---|
| **Node** (sphere) | An asset. **Green glow = inflow** (bid), **red = outflow** (offered), grey = no clear move. Glow strength grows with the size of the move. |
| **σ** under a node | The window's move divided by that asset's **typical move for the same window** (its z-score). +2σ is an unusually strong rise for *that* asset; it lets bonds, coins and indices be compared. |
| **%** under a node | The plain percent change over the window. |
| **Sparkline** | The path of the move over the window. |
| **Badge** (e.g. `BNC-F* 144ms · 0.4s`) | The data source, its **median latency** (exchange timestamp to receipt) and the **age of its newest data**. Green = live, amber = delayed, grey = market closed, red = stale. `*` = a proxy (e.g. a perpetual standing in for the ETF). |
| **Curved edge with moving dots** | An **inferred** flow from an outflow asset to an inflow asset in the same window. Thicker means stronger. Hover over an asset to see its arrows, or, if it has none, why not (for example "it moved −0.4σ, inside ±0.5σ, too small to count"). |
| **Notable stock** (right of the map) | The large-cap US stock whose move is most unusual in the window, with why it was picked (section 3.8). It changes with the window and the moment; when no stock clears the threshold the spot shows "none stands out now". |
| **The Fed** (centre) | Days to the next FOMC rate decision. |
| **1H / TODAY / 1W / 1M** | The window: the last 60 minutes; since **17:00 New York** yesterday (the global trading-day roll, i.e. the US close); the last 7 days; the last 30 days (section 3.9). |
| **Risk-on / risk-off meter** | A −100…+100 composite of documented components (section 3). |
| **FEAR · VIX** | The VIX level. It is licensed and **15 minutes delayed**, and is labelled so. Beside it is a live 24/7 proxy, the UVXY perpetual. |
| **DOLLAR · DXY** | The ICE Dollar Index formula, computed live from six FX pairs. |
| **US 10Y YIELD** | FRED's daily close, with the date shown, plus the live TLT move as a hint. |
| **BTC LEVERAGE** | Bitcoin perpetual funding (per year) and the change in open interest over the window. |
| **Next Fed rate decision** | A countdown, shown in New York time and in your local time (this PC's time zone, or `local_timezone` in the config), plus the next high-impact releases. |
| **What the flow says** | Rule-based sentences. Each sentence carries the id of the rule that produced it (hover to see it). |
| **Strongest flows** | The top inferred edges with weight and correlation ρ. |
| **News wire** | Headlines from several feeds, newest first. Each shows its source and how long ago it was published. |
| **Status strip** | Every connector with its state, median latency and message rate. |
| **Footer** | Connection state, and "This program stops by itself 5 min after the last viewer leaves" (section 1). If the page loses the service for good it says the service may have stopped and to start it again with `start.bat`. |

Every asset reports one of these statuses:

| Status | Meaning |
|---|---|
| `live` | Receiving live data. |
| `delayed` | The source is delayed by design (the VIX level). |
| `closed` | Its market is shut. |
| `stale` | Its market is open but the feed has been silent too long: 2 min for most feeds, 10 min for IEX ETFs, which can go minutes between trades on that one exchange. A price loaded from the history cache also counts as stale until the first live message arrives. |
| `warming_up` | Not enough history for σ. A brand-new listing carries `history_from` instead. |
| `no_data` | Nothing received. |

**Delayed, cached or closed data is never shown as live.**

---

## 3. How the flows are computed

All maths is in `moneyflow/stats.py`, `moneyflow/flow.py` and `moneyflow/narrative.py`, and it is
unit-tested.

### 3.1 Price series
Every instrument keeps fixed-memory ring buffers:
- 1-minute closes for 1 day;
- 5-minute closes for 35 days;
- 5-minute traded volume, including the taker-buy part where the venue reports it.

The live price updates every tick. A step's `live` flag records whether that instrument's market
was open. History is seeded at start-up from REST and cached on disk.

### 3.2 The z-score (σ)
`z = log return over the window / σ`, where σ is the asset's **own typical move for that same kind
of window, on the days its market trades**:

| Window | σ estimated from |
|---|---|
| 1H | **Same clock hour**: RMS of the 60-minute moves ending within 30 minutes of this clock time on each of the previous 20 trading days (10 days for crypto), grouped by day. The pre-market hour is judged against past pre-market hours, the opening hour against past opening hours. An hour belongs to the session of its midpoint: at 17:00 the hour 16:00-17:00 is Friday's closing hour, not Saturday's. Only hours when the market was open count. Fallback (fewer than 30 such moves, e.g. a recent listing): every 60-minute move of the last 10 days. |
| TODAY | **Same-clock σ**: RMS, over the previous 20 trading days (30 days for crypto), of the move from that day's 17:00 New York roll to the *same clock time*. At 10:00 New York it compares with 10:00 on previous days, so overnight gaps and intraday volatility patterns are handled for every asset class. In the first hour after the roll, the yardstick is the typical move of that first hour; otherwise right after 17:00 every past window would be shorter than one 5-minute step and σ would read 0. In the first 5 minutes of a session, a past day's move is read from that day's opening 5-minute step (the step before it holds the previous close: σ read 0 and z was missing until 9:35, for every ETF); 1W's start the same way. Fallback (fewer than 8 such days, e.g. a recent listing): daily realized variance from 5-minute returns, scaled to the elapsed part of the day. |
| 1W | RMS of the daily (roll-to-roll) moves on the trading days of the last 30 days, × √k, where k is the number of trading sessions in the window: 5 in a normal week, 4 in Thanksgiving week (z would otherwise be 11% too small), 7 for crypto. Sessions cut by the window's ends count by the share of their 24 hours inside. |
| 1M | RMS of the daily moves of about the last year (daily closes, section 3.9), × √k, k the trading sessions in the 30 days: 30 for crypto, 22 on weekdays, 19.7-22 for NYSE days. Fallback (fewer than 30 daily moves: a recent listing, the stablecoin share): the last 30 days' roll-to-roll moves × √k, as 1W. |

- **Trading days only.** Each series has a `calendar`:
  - `all`: crypto and the stablecoin share, every day;
  - `weekdays`: the dollar and FX, gold, silver, oil and copper (Monday to Friday sessions);
  - `us_equity`: NYSE trading days, for the indices, bonds, the VIX proxy and the notable stock.

  A trading day is the session that ends at 17:00 New York, so Sunday evening's open counts as
  Monday. The Binance perpetuals trade around the clock, but on weekends and holidays the index
  under them hardly moves. Counting those quiet hours as "typical" made σ too small, so every
  weekday move looked bigger than it was. The S&P 500's TODAY σ was 0.45% with weekends counted;
  on trading days only it is 0.53%, in line with the cash index's own weekday range (0.51-0.62%).
- **One one-off day does not set the yardstick.** If a single day's moves are more than twice the
  size of any other day's (an earnings gap, a crash day), that day counts as the next-largest one.
  SHOP's −18.3% earnings gap had more than doubled its TODAY σ (3.47% instead of 1.44%), so a −2.5σ
  morning read −1.1σ. Several big days are all kept: that is the asset's real volatility.
- **The notable-stock screen uses the same functions**, on hourly bars (section 3.8); a stock the
  map already reads is ranked on the map's own numbers.
- **RMS, not standard deviation:** the mean of short-horizon returns is about 0, and estimating it adds noise.
- **No information is not a zero move:** a market that was **shut for the whole window** gets a missing z. HYG at 3 a.m. has no "1H" move.

### 3.3 Volume confirmation (where volume exists)
- **Relative volume:** volume in the window ÷ the median volume of the same window on the previous 20 trading days. For 1W it is the previous 4 weeks; for 1M the year's earlier 30-day stretches, from daily bars (section 3.9).
- **Net taker flow (USD):** taker-buy quote volume − taker-sell quote volume, from Binance 1-minute klines. It shows how much aggressive buying versus selling hit the book.
- **IEX volume** (Alpaca) is a *sample*, about 2-3% of US volume, so only relative volume is used from it.

### 3.4 Flow edges (inferred)
For every pair with **A offered** (`z_A ≤ −θ`) and **B bid** (`z_B ≥ +θ`) in the same window:

```
weight(A→B) = sqrt(|z_A| · z_B) · (1 − ρ_AB) / 2
```
- `ρ_AB` is the correlation of A's and B's step returns **inside the window**: 1-minute steps for 1H, 5-minute for TODAY, hourly for 1W.
- A rotation looks like one asset sold while the other is bought, step by step (ρ → −1). That keeps the full weight.
- Assets that merely moved together (ρ → +1) get **no edge**.
- Unknown ρ counts as 0 (factor 0.5).
- So does the stablecoin share against BTC and ETH. The share is computed from their prices, so its correlation with them (about −1) is arithmetic, not evidence of a rotation. Such an edge shows no ρ.
- **Which edges are drawn.** First, every asset that has a qualifying edge (weight ≥ 0.3) keeps its single strongest one. Then the strongest of the rest are added, up to 16 in all and 5 per asset. Until 25 Nov 2026 the caps applied to everything, so an asset sold into the same haven as five stronger sellers lost its only arrow. That was often gold (section 10).
- Defaults: θ = 0.5σ, minimum weight 0.3, at most 16 edges and 5 per asset beyond each asset's strongest.

**An asset without an arrow** is always explained (hover over it on the map, or `no_edge` in the API):

| Code | Plain words |
|---|---|
| `neutral` | It moved less than θ (±0.5σ): too small to count as money moving in or out. |
| `no_counterpart` | It was sold (or bought), but nothing on the map moved the other way by θ or more. |
| `co_moved` | Its best partner moved in step with it (ρ > 0): moving together is not a rotation, so the weight stays under 0.3. |
| `too_weak` | Its strongest possible edge weighs under 0.3. |
| `closed`, `warming_up`, `no_data`, `not_live` | No usable move in this window (market shut all window, too little history, no data, or stale data). |

The strongest edge is the "strongest flow". It is a statement about *simultaneous opposite
moves*, not about where any particular dollar went.

### 3.5 Risk-on / risk-off composite (−100…+100)
Each component is in σ units, clipped to ±4. Weights are in the config (`[risk]`):

| Component | Formula | Weight |
|---|---|---|
| Stocks vs Treasuries | z(S&P 500) − z(TLT) | 0.25 |
| Fear | −z(UVXY perpetual, live VIX proxy) | 0.20 |
| Crypto | mean(z(BTC), z(ETH)) | 0.15 |
| Credit | z(HYG) − z(IEF) | 0.10 |
| Dollar | −z(DXY) | 0.10 |
| Stablecoin share | −z(stablecoin share of crypto) | 0.10 |
| Gold (haven demand) | −z(gold) | 0.05 |
| Tech leadership | z(Nasdaq 100) − z(S&P 500) | 0.05 |

- `S` = the weighted mean of the available components. `score = round(100 · tanh(S / 1.5))`.
- Labels: ≤ −60 RISK-OFF, −60…−20 leaning risk-off, ±20 mixed, 20…60 leaning risk-on, ≥ 60 RISK-ON.
- Missing inputs, such as HYG/IEF outside US hours, drop out and the weights are renormalised. `coverage` reports the live share of weight.
- Because z-scores remove scale, a leveraged or futures-based proxy (UVXY ≈ 1.5× short-term VIX futures) gives a z close to that of what it tracks.

### 3.6 Special series
- **US Dollar (DXY).** `50.14348112 · EURUSD^−0.576 · USDJPY^0.136 · GBPUSD^−0.119 · USDCAD^0.091 · USDSEK^0.042 · USDCHF^0.036`, computed on every OANDA tick from Finnhub.
  - **Validated live:** over 16 minutes and 323 timestamp-matched quotes, the computed value minus ICE's DXY (via Yahoo, 10 min delayed) was **mean +0.0007, sd 0.0024, max 0.011**, at a DXY of about 103.9.
  - The history comes from ICE's DXY itself.
- **Stablecoins (cash in crypto).** The share of total crypto market cap held in USD stablecoins:
  `S / (S + NS₀ · B(t)/B₀)`.
  - `S` is the supply from DefiLlama. `NS₀` is the non-stable cap from CoinGecko.
  - `B` is a cap-weighted basket of live Binance prices (BTC, ETH, BNB, SOL, XRP).
  - The share moves **mechanically** when crypto prices fall, even with no new money. So the hover and narrative also show the **supply change** (mints − burns), which is the real "new cash" signal.
- **US Bonds (TLT).** A *stitched* series. While US markets are open it uses TLT on IEX. Outside them it uses the TMF perpetual's index ÷ 3 (TMF is 3× TLT). At the open it snaps back to the real price, so the opening gap is real.
  - **Without an Alpaca key** there is no live TLT. The service then downloads TLT's 5-minute (60 days) and 1-minute (2 days) bars from Yahoo's chart API at start-up, so the proxy is anchored on TLT's real last close. From then on the node follows TMF ÷ 3 around the clock, and the hover says so.
  - The 1-minute series (one day) and the 5-minute series (35 days) are kept consistent: the 1-minute chain starts from the 5-minute value at a 5-minute boundary, and a gap in the proxy's data never drops its move.

### 3.7 "What the flow says"
These are deterministic rules in `narrative.py`; each bullet carries its rule id. Examples:
- **Bonds bid while stocks fall** (z ≥ 1 vs ≤ −1): "classic safety trade".
- **Stocks and bonds both sold**: "Treasuries are not working as a haven".
- **Dollar bid while stocks fall.**
- **Gold sold with stocks and crypto**: "broad liquidation".
- **Fear spike**: VIX proxy ≥ 2σ.
- **Stablecoin share up while crypto is down**: "crypto traders moving to cash", qualified by the supply change.
- **Ether lagging Bitcoin.**
- **Open interest vs price**: deleveraging, new shorts, new longs or short covering.
- **Negative funding.**
- **Oil shocks.**
- **Tech lagging.**
- **Notable stock**: the stock in the slot, its move and how unusual it is.
- **Plus:** the risk regime, the Fed countdown, the next US high-impact release, and caveats about proxies and closed markets.

### 3.8 The notable-stock slot
The fixed nodes are asset classes. A single stock is not one: on the reference map "Anthropic" was
simply the stock that stood out at that moment. So one slot (`[stocks] slots`, 0 to 5) shows **the
large-cap stock whose move is most unusual in the window**, and only when one clears the rule. It
can be a different stock in 1H, TODAY, 1W and 1M (section 3.9), and it can be empty.

- **Universe.** The configured large caps (138 S&P 100 and Nasdaq-100 names plus big ADRs, editable
  in `[stocks] universe`) that have a Binance 24/7 TradFi perpetual: **78 of them** on 25 Nov 2026.
  The API lists the other 60 (`stocks.not_listed`: MA, PG, PEP, CVX, ...). The perpetual is what
  makes the chosen stock readable around the clock, like the Nasdaq-100 and S&P 500 nodes, with no key.
- **The screen** runs every 5 minutes and is cheap. Its prices are **Binance's index of each stock**,
  the same price the stock's node reads, not the perpetual's last trade. A thin perpetual's last
  trade can sit well off the index: MDB's sat 0.6% above it at the 17:00 roll on 25 Nov.
  - one bulk request returns every index price (all 817 perpetuals, weight 10, about 25 KB compressed);
  - each stock's hourly index closes and its perpetual's hourly volume are fetched once (31 days,
    weight 5 each) and topped up hourly (weight 1 each);
  - the scans' prices are kept for the last hour and a bit, by time. Whenever they do not reach
    back to a 1H window's start (after a start, a wake, or a gap in the scans), each such stock's
    last 70 minutes of 5-minute index closes are downloaded (weight 1 each), so a 1H move starts
    from a real price, not from a line between two hourly closes. If that download fails it is
    tried again after 5, 10, ... 30 minutes; within the hour the scans cover it anyway.
  - For each window and stock:

    ```
    r     = ln(price now / price at the window start)
    σ     = the map's own estimator (section 3.2) on hourly bars, over the previous 20 NYSE trading
            days: 1H the same clock hour; TODAY the same time since 17:00 New York; 1W the daily
            moves of the last 30 days × √(sessions in the window); 1M the same daily moves ×
            √(sessions in the 30 days). A one-off day is capped.
            Read the way the node reads it at this minute: for each of the node's window ends
            (now ± 30 min for 1H), the variance is interpolated between the whole hours either side.
    z     = r / σ
    rvol  = the window's quote volume / the median of the same on previous trading days (1M: none)
    score = |z| · √clip(rvol, 0.5, 4)      (unusual volume strengthens a move, thin volume weakens it)
    ```
  - **Rule:** a stock qualifies with **|z| ≥ 2 and score ≥ 2.5**; the highest scores fill the slots.
    The API shows the five best candidates in every case, picked or not (`stocks.candidates`).
- **The chosen stock becomes a full node.** Its 35-day history and its live 1-second index price
  are loaded within seconds of the pick. Its z on the map uses the same estimator as the screen,
  on 5-minute data, and its reason text (map, hover, panel, "what the flow says") quotes the node's
  own numbers. The screen's numbers, which chose it, stay in the API (`slot.screen`). A stock no
  longer picked keeps its live feed for 2 hours (`keep_s`) in case it comes back, then it is dropped.
- **From the next screen on, a stock the map shows is ranked on the map's own numbers** (its move,
  σ and volume ratio from 5-minute data; `screen.from` = `map`): the current picks, once their
  history has no gap left by a sleep or an outage. Hourly bars cannot place where in
  an hour the variance came (the 09:30 open sits inside the 09:00-10:00 bar), so for the other
  stocks the screen's σ differs from what a node would read: within ±10% as a median at every
  clock time tested, but 0.74 to 1.21 times on single days (section 10). For the pick itself, from
  the next screen on (within 5 minutes), they are the same numbers.
- **Map and screen agree** (25 Nov 2026, during US hours, a copy without keys). The first screen
  (15:55 UTC) used hourly bars; from the next one on (16:01 UTC), the picks were ranked on the
  map's own numbers:

  | Window, stock | First screen: screen / map | Next screen: screen / map |
  |---|---|---|
  | 1H, HD | +2.90σ / +2.59σ | +2.316σ / +2.321σ |
  | TODAY, AMZN | −2.21σ / −2.20σ | −2.224σ / −2.221σ |
  | 1W, PDD | +2.48σ / +2.47σ | +2.421σ / +2.422σ |

  Before the σ round (section 10), SHOP read −1.06σ on the map against −2.95σ on the screen.
- **A 1H move can fade between two screens** (they run every 5 minutes). When the node's own move
  has shrunk below 3/4 of the move the screen picked (or turned around) **and** its |z| is under the
  rule, the slot says so instead of calling it the most unusual move, for example "NKE −0.30% in the
  last hour (−0.9σ) on 2.7× its usual volume. At the screen 2 min ago it was the most unusual move
  among 75 large caps (−0.77%, −2.4σ); the move has eased since." That holds (no flicker at the
  threshold) until the next screen, or until the move is fully back: as big as the screen's, the
  same way, with |z| over the rule. Meanwhile "What the flow says" leaves the stock out (API:
  `slot.eased`). A move that did not shrink is never "eased", even if the node's σ puts its z a
  little under the rule.
- **At 17:00 New York** TODAY starts again: the old session's TODAY pick is cleared (its move is
  gone) and the screen runs again at once.
- **A pick whose history fails to download** (no network) is retried after 30 s, doubling to
  15 min (at once when a feed recovers), for as long as it is picked, instead of "loading" for good.
- **A stock dropped lately whose live feed is still kept** (2 h) is ranked on the map's own
  numbers again when it is among the five best: on hourly bars alone a borderline one went in and
  out of the slot at every screen with unchanged prices.
- **After a wake**, a shown pick whose history still has the sleep's gap is held, not dropped on
  hourly bars alone, until its back-fill is done; then it is judged again on the map's numbers.
- **A scan that began before 17:00 New York** and ended after it does not bring back the old
  session's TODAY pick; the screen runs again within seconds.
- **Seen live on 25 Nov 2026 (pre-market, with the first σ estimator):**
  - TODAY: **DELL**, −9.2% since 17:00 New York on 4.1× its usual volume (screen −2.9σ, map −3.3σ),
    after a 12.4% fall in Tuesday's session (Alpaca), an arrow into bonds, the dollar and stablecoins;
  - 1H: **WMT** (−3.0σ for that pre-market hour, 6.7× volume);
  - 1W: none (best: ZS +18% but only +1.4σ, its weekly swings being large).

### 3.9 The monthly view (1M)
**1M is the last 30 days, rolling:** the move from the price 30 days back (the 5-minute grid holds
35 days) to the live price, like 1W's over 7 days.

**Its σ comes from a year of daily closes.** From 35 days of 5-minute closes a month's yardstick
would rest on about 20 daily moves (an ETF's), a month of samples for a month's move. So each
instrument also keeps about a year of daily closes (`series.Daily`, complete days only), and σ_1M is
the robust RMS of their daily moves (one one-off day capped, as everywhere) × √k, where k is the
number of trading sessions in the 30 days: 30 for crypto, 22 on weekdays, 19.7 to 22 for NYSE days
(holidays left out; sessions cut by the window's ends count by their share), the same rule as 1W's
√5. With fewer than 30 daily moves (a recent listing; the stablecoin share, which has no daily
source) it falls back to the last 30 days' roll-to-roll moves from the 5-minute grid × √k, as 1W.

| Node | Daily closes from |
|---|---|
| Bitcoin, Ether, the basket coins | Binance spot, 1-day klines (with their volume) |
| Gold, silver, oil, copper | the futures on Yahoo (GC=F, SI=F, CL=F, HG=F): the perpetuals were listed in 2025-26, so they have only months of their own (gold since 10 Dec 2025, oil since 31 Mar 2026), and Binance has no copper pair |
| Nasdaq-100, S&P 500, the VIX proxy | QQQ, SPY, UVXY: Alpaca's daily bars with a key, Yahoo's without (the SPY and QQQ perpetuals have their own since 3 Apr 2026, UVXY's since 11 Jun 2026) |
| Bonds, HYG, IEF, the reference ETFs | the ETF itself (TLT for the bonds node), from Alpaca or Yahoo |
| The dollar | ICE's DXY (Yahoo DX-Y.NYB), its history as for the other windows |
| A picked stock | its ticker's daily bars (Alpaca) |

- **Light.** One small request per instrument 20 s after the start (the 5-minute history goes
  first; all ETFs in one Alpaca request), then a check every 30 minutes that downloads again only
  what is about a day old (20 hours), from three days before its newest close. No extra streams.
  They are kept in one file, `data/cache/daily.json` (95 KB in a keyless run), written after each
  download, so a restart downloads nothing new and the final save has none of it to write.
- **Measured on real data** (27 Nov 2026, keyless: Binance, Yahoo): σ_1M against the RMS of the
  year's actual 30-day moves, from the same daily closes:

  | | σ_1M | RMS of the 30-day moves | yearly volatility |
  |---|---|---|---|
  | S&P 500 (SPY) | 4.41% | 3.97% | 14.9% |
  | Nasdaq-100 (QQQ) | 6.98% | 6.50% | 23.3% |
  | Bitcoin | 13.52% | 15.61% | 44.2% |
  | Ether | 18.93% | 22.02% | 60.5% |
  | Dollar (ICE DXY) | 1.56% | 1.56% | 5.2% |
  | Gold (GC=F) | 9.01% | 7.71% | 29.1% |
  | Silver (SI=F) | 19.44% | 17.34% | 69.7% |
  | Oil (CL=F) | 18.67% | 18.72% | 57.8% |
  | Bonds (TLT) | 2.94% | 2.65% | 10.0% |
  | HYG / IEF | 1.26% / 1.58% | 1.13% / 1.48% | 4.3% / 5.4% |
  | VIX proxy (UVXY) | 33.14% | 22.94% | 99.0% |

  σ_1M came within 12% of the actual 30-day moves for the indices, the bonds, the dollar, silver
  and oil; it ran 17% above them for gold, 13-14% below for Bitcoin and Ether (trending months), and
  44% above for UVXY (it decays and swings back). A year holds only 12 separate months, so the
  right-hand column is rough itself.
- **Volume:** the last 30 complete days against the median of the year's earlier 30-day stretches,
  from the daily bars, and only where they are the instrument's own (the S&P 500 perpetual's are
  SPY's, another market's volume: no ratio). The taker split comes from the 5-minute volume.
- **The notable stock:** screened on its 31 days of hourly bars: the move from 30 days back, σ the
  daily moves of those bars × √k as above (fewer than 10: none), and no volume ratio (no earlier
  month in the bars). A stock the map reads is ranked on the node's own numbers, as in every window.
- **What the flow says** speaks of "this month", and quotes the stablecoin supply's change over
  30 days.
- **The risk meter and an asset without arrows** follow the same rules as in the other windows, on
  the 1M moves and z's: same components and weights, the same ±0.5σ threshold for an arrow, and a
  market shut for the whole window has no move (none is, over 30 days).
- **Cost:** a fourth window in every snapshot. With caches as in use (σ worked out every 5 minutes
  for 1M, its volume every 10), a snapshot a second took 5 ms instead of 4 as a median (9 ms
  instead of 8 on average); one with every cache cold, 0.20-0.31 s instead of 0.17-0.18 s.
- **API:** `tf=1m` wherever a window is taken (`/api/snapshot?tf=1m`, `/ws`, `/sse`, `tf=all`
  gives all four), and the JSON schema lists it.

---

## 4. Sources, measured latency, terms

### 4.1 What feeds each node

| Node | Real-time source (default) | Reference / fallback | Hours |
|---|---|---|---|
| Nasdaq 100 | Binance **QQQUSDT** perpetual, *index price* (Binance's fair price of QQQ, 1 s) | QQQ on Alpaca IEX | 24/7 (weekends = perp price discovery) |
| S&P 500 | Binance **SPYUSDT** perpetual, index | SPY on IEX | 24/7 |
| Notable stock (slot) | Binance **TradFi perpetual** of the picked stock, index (e.g. DELLUSDT) | | 24/7 |
| Gold | Binance **XAUUSDT** perpetual, index | GLD (IEX), PAXG (Binance spot) | 24/7 |
| Silver | Binance **XAGUSDT**, index | SLV | 24/7 |
| Crude oil (WTI) | Binance **CLUSDT**, index | USO | 24/7 |
| US Bonds (TLT) | **TLT on Alpaca IEX** (mid quote) in US hours | TMF perpetual index ÷ 3 otherwise | stitched 24/7 |
| US Dollar (DXY) | **Computed** from 6 OANDA FX pairs (Finnhub websocket) | UUP on IEX | FX hours (Sun 17:00 – Fri 17:00 NY) |
| Bitcoin, Ethereum | Binance spot trades (BTCUSDT, ETHUSDT) | Coinbase BTC-USD / ETH-USD (the "Coinbase premium") | 24/7 |
| Stablecoins | Computed: DefiLlama supply ÷ CoinGecko market cap, moved by live Binance prices | | 24/7 |
| VIX tile | Cboe delayed quote and Yahoo quote (both **15 min delayed**) | live proxy: Binance **UVXYUSDT** perpetual | VIX hours |
| Credit (risk input) | HYG and IEF on IEX | | US regular hours |

Context sources:
- **FRED:** DGS10, DGS2 and the HY spread (daily).
- **federalreserve.gov:** the FOMC calendar.
- **Forex Factory weekly export:** scheduled US releases.
- **Binance REST:** open interest.
- **Headlines:**
  - Alpaca news stream (real-time, filtered to the map's tickers and macro themes).
  - Finnhub news.
  - RSS: FinancialJuice, FT, Bloomberg, CNBC, FXStreet, CoinDesk, Cointelegraph, The Block and the Fed's press releases.

**Why perpetuals for indices and commodities?** In November 2026 Binance lists "TradFi
perpetuals" that trade around the clock. Measured on 24 Nov 2026:

| Perpetual | 24h turnover |
|---|---|
| QQQUSDT | $1.2B |
| XAUUSDT | $3.0B |
| ANTUSDT | $1.1B |
| CLUSDT | $0.7B |
| SPYUSDT | $0.34B |

On Saturday 21 November QQQUSDT still traded $192M and XAUUSDT $684M. Their index price keeps
moving at weekends and re-prices when CME futures reopen on Sunday evening. They are therefore the
best free 24/7, low-latency proxy for these markets. During US hours the hover shows the basis
against the real ETF on IEX.

How closely does the index track the real ETF? Measured against IEX trades on 24 Nov 2026:

| When | QQQ | SPY | ANT |
|---|---|---|---|
| First 3 minutes after the open (median) | +11 bp | +13 bp | +16 bp |
| 09:50 New York | −0.4 to +4.5 bp | about +6 bp | −2 to +19 bp |

At the open the index lagged a falling market. ANT, a new listing, is the noisiest. So **%
changes from the perpetuals can differ from the ETF's own by about 0.1% around the open**.

### 4.2 Measured latency
Latency is **receive time − the exchange's own timestamp**, measured on this PC (home connection,
Pakistan) with the clock corrected by NTP. Measured on 24 Nov 2026; marketing claims were not used.
> The PC clock ran **0.95-0.99 s fast**. Uncorrected, every source would look about 1 s slower.
> The service measures this offset itself (SNTP) and corrects all ages. It asks four servers and
> uses a reply only from a synchronised server answering its own request. Replies within a day win
> over any that correct by more; a correction over a day counts only when no reply is within a day,
> and then only where another server agrees (a PC clock days off is corrected; a lone reply 1024
> weeks off is not, nor two that agree on it beside a good one). Of the replies that agree (within
> 0.5 s of their median), the shortest round trip wins. Every reply from one box that intercepts NTP
> (all of them days off and agreeing) cannot be told from a PC clock days off, and is followed. A
> server's "kiss-of-death" (zero timestamps, when many copies start at once) once set the clock 127
> years back; a failed or doubtful sync now leaves the offset in use. The downloads leave out a bar
> still open by this corrected clock, not by the PC's own (two days behind, they dropped the last two
> days of closed bars as open; ahead, they kept an open one), and so does the stablecoin supply's
> 60-day cut. The clock's helpers that format or convert a time take one out of range (not a
> number, before 1970, after 3000) as the nearest end instead of raising.

| Source (what is used) | Median | p90 | Rate | Notes |
|---|---|---|---|---|
| Binance spot trades (BTC, ETH) | **89-126 ms** | ~140 ms | 13-17/s per coin | 95 ms in the first probe |
| Binance futures, liquid trades (BTC, XAU) | **95 ms** | 170-245 ms | 15-44/s | `/market/stream` path |
| Binance futures, TradFi perp trades | 110-245 ms | 245 ms | 1-6/s | pushed in 250 ms batches |
| Binance futures index/mark price (`markPrice@1s`) | **130-156 ms** | ~165 ms | 1/s per symbol | the price used for TradFi nodes |
| Alpaca IEX (quotes / trades), pre-market | **~110 ms** | ~113 ms | SPY quotes 11/s | |
| Alpaca IEX, US regular session (14:34 UTC) | **132-139 ms** | ~145 ms | TLT 52/s, IEF 27/s, HYG 17/s quotes at the open | every ETF symbol |
| Binance spot during the US session | **~106 ms** | ~108 ms | BTC 50/s, ETH 36/s | |
| Binance futures index/mark, US session | **144-154 ms** | ~160 ms | 1/s per symbol | |
| Finnhub OANDA FX, US session | **336-400 ms** | 536-632 ms | 1.5-3/s per pair | busier than pre-market |
| Evening check, 15:15 UTC (20:15 Pakistan) | Binance 215-230 ms, IEX ~120 ms, FX ~420 ms | | | from the running service's own badges |

Network latency varies through the day on a home connection: Binance's median doubled between
13:30 and 15:15 UTC. The dashboard shows each source's *current* median latency, so read the
table as typical ranges.
| Finnhub OANDA FX (6 pairs) | **260-345 ms** | 380-505 ms | 1-2/s per pair | DXY inputs |
| Coinbase full ticker | 120 ms | 130 ms | 19/s (BTC) | the service uses the batched channel, which updates every ~5 s by design |
| Hyperliquid trades (optional) | 300-450 ms | ~530 ms | | not used by default |
| Yahoo streamer FX (rejected) | 0.9-1.1 s | ~2 s | | unofficial |
| Yahoo `^VIX` | **902 s (15 min)** | | | licensing delay |
| Yahoo `DX-Y.NYB` (ICE DXY) | **602 s (10 min)** | | | used only for the DXY *history* |
| Cboe delayed VIX JSON | 15 min | | | shows the prior close until 09:30 New York |

| Polled source | Freshness when fetched | Poll interval |
|---|---|---|
| CoinGecko `/global` | data 5-7 min old | 5 min |
| DefiLlama stablecoins | live total; history daily | 60 min |
| FRED DGS10 | on 24 Nov the latest close was 20 Nov (1-2 business days) | 6 h |
| FinancialJuice RSS | newest headline 1-2 min old | 3 min |
| CNBC RSS | ~6 min | 3 min |
| Finnhub general news | 15-45 min | 2.5 min |
| Alpaca news stream | real-time | stream |

Run `python -m moneyflow check --seconds 120` to measure again. It writes a JSON report to `data/`.

### 4.3 Tested and rejected (and why)

| Candidate | Result |
|---|---|
| **Kraken** websocket (real FX pairs) | This network's DNS **refuses** `ws.kraken.com` / `api.kraken.com` ("Query refused"). Not usable here. |
| **Binance futures, legacy `/stream` path** | Connects but **delivers nothing** (Nov 2026). `/market/stream` (and `/public/stream` for book data) work. |
| **Hyperliquid HIP-3** `xyz:DXY`, `xyz:VIX` | Listed but **zero volume and open interest**: frozen prices. The index perps (`xyz:XYZ100`, `xyz:SP500`) are liquid. The connector is included as an optional fallback, off by default. |
| **VIX real-time** | The index is licensed. Every free quote found (Cboe, Yahoo) is 15 min delayed. The tile shows the delayed level, labelled with its age, and the live UVXY perpetual as the real-time direction. |
| **Yahoo streaming** | Unofficial. FX about 1 s (Finnhub is 3-4× faster). Not used for live prices; the chart API is used only for history (DXY, and TLT/HYG/IEF when there is no Alpaca key) and a second delayed VIX quote. |
| **JBlanked** (key present) | Every calendar endpoint answers **HTTP 401 "requires credits"** with this key. Not used. |
| **Finnhub** economic calendar and FX candles | HTTP 403 on the free plan. |
| **Spot BTC/ETH ETF flows** | Farside's pages answered this program's HTTP client with a **Cloudflare bot challenge** (403). Bot checks are respected, not worked around, so the connector is off. DefiLlama's ETF routes are 404; SoSoValue needs a key; CoinGlass is paid. No ETF flows by default. |
| **MT5 / Exness** | MetaTrader 5 is installed on this PC, but the other projects only hold Exness *web-terminal indicators* (no data connector). A live MT5 feed would need the terminal running, which is heavy, and it gives broker CFD quotes. Not used; it could be added as a connector. |
| **Binance spot EURUSDT** for FX | Only EUR exists (no GBP/JPY/CAD/SEK/CHF). Not enough for the DXY. |

### 4.4 Terms and rate limits (summary)
- **Binance**
  - Public market data needs no key.
  - REST weight limits: 6,000/min (spot) and 2,400/min (futures). The service watches the used-weight header and pauses at 50%.
  - Up to 1,024 streams per websocket; Binance closes each connection every 24 h, and the service reconnects.
  - The notable-stock screen adds about 280 weight an hour (one bulk index-price call every 5 min at weight 10, plus two small hourly-bar requests per stock an hour), and about 860 once at start-up (two 31-day requests per stock at weight 5, and one small 5-minute request each), after the map's own history has loaded; about 80 again after a wake. A picked stock joins the open stream with `SUBSCRIBE`, without reconnecting (measured).
  - Market data only. API Terms of Use: <https://www.binance.com/en/terms>
- **Alpaca Basic (free)**
  - Real-time IEX feed; **one** connection per stream type; 200 REST calls/min.
  - For personal, non-redistributed use.
  - If another app holds the stream (error 406), the service retries every 5 min and polls the latest trades over REST meanwhile.
- **Finnhub free**
  - 60 REST calls/min, 50 websocket symbols, attribution "Data provided by Finnhub".
  - Uses 6 symbols and 1 news call per 150 s.
- **CoinGecko public API:** 5-30 calls/min. Uses 1 per 5 min. Attribution: "Data provided by CoinGecko".
- **DefiLlama:** free, no key. Uses 1 call per hour, plus history twice a day.
- **FRED:** 120 calls/min per key. Uses 3 calls per 6 h.
- **Cboe delayed quotes and Yahoo:** delayed data for personal use. Yahoo's API is unofficial, so it is used sparingly: VIX once a minute while VIX is calculated, DXY history at start, and (only without an Alpaca key) two history requests per ETF at start.
- **federalreserve.gov:** public domain. Polled once a day.
- **Forex Factory weekly export:** polled every 2 h.
- **RSS:** publishers' public feeds. Each is polled every 3 min with ETag / If-Modified-Since. Headlines link back to the publisher.

---

## 5. Keys and configuration

Keys are optional; each one unlocks one source:

| Key | Unlocks | Without it |
|---|---|---|
| `ALPACA_KEY_ID`, `ALPACA_SECRET` | TLT/HYG/IEF and the ETF reference quotes; the real-time news stream | Bonds follow the TMF proxy around the clock, anchored on TLT's real close (Yahoo history at start-up); credit drops out of the risk meter |
| `FINNHUB_KEY` | the live DXY, Finnhub news | No dollar node |
| `FRED_API_KEY` | the 10Y yield tile and curve | No 10Y tile |

Put them in a git-ignored `.env` (copy `.env.example`), **or** point the config at an existing
`.env` instead of copying:

```toml
# moneyflow.local.toml (git-ignored)
[keys]
env_files = [".env", "../my-other-project/.env"]

[general]
local_timezone = "Asia/Karachi"   # the second clock; "" (the default) = this PC's own zone
```
Only the four key names above are read from those files. Other secrets in them, such as e-mail
passwords or exchange trading keys, are never loaded. Values are never printed, logged, cached or
served:
- `python -m moneyflow keys` shows only *set / missing*;
- every log line and every error string goes through a redaction filter;
- a test checks that no key appears in any API response.

**Configuration** is TOML:
- `moneyflow/default.toml` documents every option.
- Override it in `moneyflow.toml`, or in `moneyflow.local.toml` (git-ignored) for this PC.
- `[[series]]` entries merge by `id`. To add an asset (for example the disabled `copper`, `iwm`
  or `openai` series), set `enabled = true`, or add a new entry with a `source` and `symbol`.
- Each series' `calendar` (`all`, `weekdays` or `us_equity`) sets the days its σ is measured on
  (section 3.2). The default follows its trading hours.
- Connectors are switched in `[connectors.*]`.
- Auto-stop is set in `[server] idle_shutdown_minutes` (default 5; 0 = keep running), or per start
  with `--stay-running` / `--idle-minutes N` (`start.ps1 -StayRunning`).
- The notable-stock slot is set in `[stocks]`: `slots` (0 switches it off, up to 5), `z_min` and
  `score_min` (the rule), `scan_interval_s`, `keep_s`, `positions` (where each slot sits on the
  map) and `universe` (your own list of tickers replaces the default one).

```toml
[[series]]
id = "copper"
enabled = true

[connectors.hyperliquid]
enabled = true

[stocks]
slots = 2          # show up to two notable stocks
z_min = 2.5        # and only really unusual moves
```

---

## 6. Connecting other apps (API)

The service listens on `http://127.0.0.1:8765` (localhost only). Everything is read-only JSON.

| Endpoint | Returns |
|---|---|
| `GET /api/snapshot?tf=today` | Everything for one window (`1h`, `today`, `1w`, `1m`), or `tf=all` for all four |
| `GET /api/nodes?tf=1h` · `/api/nodes/{id}` | Assets (`bonds usd gold silver oil ndx spx btc eth stables`, plus `stock1` when a stock is picked; aux: `vix_proxy hyg ief`) |
| `GET /api/edges?tf=` · `/api/risk?tf=` · `/api/narrative?tf=` · `/api/tiles?tf=` | Parts of the read |
| `GET /api/history/{id}?grid=5m&hours=24` | Stored closes of one series (5m: 35 days, 1m: 1 day), each `[step end time, close, market open 1/0]`, for drawing your own charts. `stock1` gives the stock now in that slot (`&tf=` picks the window, default `today`) |
| `GET /api/news?limit=30` · `/api/fed` · `/api/events` | Headlines, next FOMC decision, scheduled releases |
| `GET /api/sources` · `/api/health` | Connector states and latency; process CPU/RAM, clock offset; `idle`: open streams, seconds idle and seconds until auto-stop |
| `GET /api/schema` | The JSON Schema (`moneyflow/schema/snapshot.schema.json`) |
| `WS /ws?tf=today` | A `{"type":"snapshot","tf":..,"data":{..}}` message every second. Send `{"tf":"1h"}` to switch. |
| `GET /sse?tf=today` | The same messages as Server-Sent Events (`event: snapshot`) |
| Webhooks | POSTs on `regime_change`, `strongest_flow_change`, `extreme_move` (|z| ≥ 3), optional `snapshot` |

Examples:

```powershell
Invoke-RestMethod "http://127.0.0.1:8765/api/risk?tf=1h"
(Invoke-RestMethod "http://127.0.0.1:8765/api/snapshot?tf=today").narrative.text
curl.exe -N "http://127.0.0.1:8765/sse?tf=today"
```

```python
from moneyflow.client import MoneyFlowClient          # standard library only
c = MoneyFlowClient()                                   # http://127.0.0.1:8765
snap = c.snapshot("today")
print(snap["risk"]["label"], snap["risk"]["score"])
for e in snap["edges"][:3]:
    print(e["from"], "->", e["to"], e["weight"])
```
See `examples/client.py` (REST and `--stream`) and `examples/webhook_receiver.py`.
`client.stream()` reconnects by itself (1 s doubling to 30 s) if the service restarts.

```js
const ws = new WebSocket("ws://127.0.0.1:8765/ws?tf=today");
ws.onmessage = (m) => { const s = JSON.parse(m.data).data; console.log(s.risk.label, s.edges[0]); };
```

**Snapshot shape** (abridged; the full schema is in `/api/schema`):

```json
{"schema": "moneyflow.snapshot/1", "generated_at": "2026-11-24T14:13:34.340Z", "timeframe": "today",
 "window": {"start": "...", "end": "...", "label": "since 17:00 New York Mon 23 Nov"},
 "nodes": [{"id": "ndx", "label": "Nasdaq 100", "price": 638.67, "change_pct": -2.10, "z": -2.01,
            "sigma_pct": 1.06, "sigma_method": "same-clock", "sigma_days": "NYSE trading days",
            "direction": "outflow", "status": "live", "age_s": 0.15, "latency_ms": 121,
            "source": {"id": "binance_fut", "name": "Binance USD-M perpetual", "symbol": "QQQUSDT", "proxy": true},
            "volume": {"rvol": 1.92, "net_taker": -15307890, "taker_buy_ratio": 0.48},
            "reference": [{"symbol": "QQQ", "source": "alpaca", "price": 635.76, "status": "closed"}],
            "spark": [-0.04, 0.03, "..."], "no_edge": null},
           {"id": "oil", "label": "Crude Oil (WTI)", "z": -0.4, "...": "...",
            "no_edge": {"code": "neutral", "text": "No arrow: it moved -0.4σ, inside ±0.5σ, too small to count as money moving in or out."}},
           {"id": "stock1", "label": "DELL (notable stock)", "short": "DELL", "z": -3.29, "change_pct": -9.22, "...": "...",
            "slot": {"index": 1, "symbol": "DELL", "perpetual": "DELLUSDT", "universe": 78, "screened": 77,
                     "rank": 1, "eased": false,
                     "why": "DELL -9.22% today (-3.3σ) on 4.1× its usual volume: the most unusual move among 77 large caps.",
                     "screen": {"z": -2.92, "rvol": 4.06, "score": 5.85, "from": "hourly bars"}}}],
 "edges": [{"from": "spx", "to": "stables", "weight": 1.53, "share": 0.18, "corr": -0.65}],
 "stocks": {"slots": 1, "universe": 78, "screened": 77, "rule": {"z_min": 2.0, "score_min": 2.5},
            "picks": [{"slot": 1, "symbol": "DELL", "why": "(in the node's own numbers)",
                       "screen": {"z": -2.92, "change_pct": -8.95, "rvol": 4.06, "score": 5.85, "from": "hourly bars"}}],
            "candidates": [{"symbol": "DELL", "screen": {"...": "..."}}, {"symbol": "MDB", "screen": {"...": "..."}}],
            "not_listed": ["MA", "PG", "..."],
            "note": "..."},
 "risk": {"score": -83, "label": "RISK-OFF", "coverage": 1.0, "components": ["..."]},
 "narrative": [{"id": "bonds_bid_safety", "text": "Bonds are bid (yields falling) while stocks fall: a classic safety trade.", "level": "down"}],
 "tiles": {"vix": {"level": 29.59, "status": "delayed"}, "dxy": {"level": 103.86}, "us10y": {"value": 4.92, "date": "2026-11-20"}},
 "fed": {"decision_at": "2026-12-09T19:00:00.000Z", "countdown": "15d 04h 55m", "when_ny": "Wed, Dec 9, 2:00 PM New York"},
 "news": [{"title": "...", "source": "FinancialJuice", "age_s": 105, "tags": ["FED"]}],
 "sources": [{"id": "binance_spot", "state": "connected", "latency_ms_median": 89.6, "msgs_per_s": 13.4}],
 "meta": {"heuristic": true, "clock": {"offset_ms": -983}}}
```

**Webhooks** (`moneyflow.toml`):
```toml
[webhooks]
targets = [{ url = "http://127.0.0.1:9000/hook", events = ["regime_change", "extreme_move"] }]
min_interval_s = 60
```

**Sharing on your LAN** is off by default. It needs both `server.host = "0.0.0.0"` and a
`server.token`; the config refuses one without the other. Requests whose Host header is not
localhost are refused, which protects against DNS rebinding.

---

## 7. The dashboard

A single page in `moneyflow/static/`: plain HTML, CSS and JavaScript, no framework, and **no
external requests** (no CDNs, no web fonts).

**Rendering**
- The network is drawn into an off-screen canvas **only when data changes** (once a second).
- The visible canvas copies it and draws the moving particles at **≤ 30 fps**.
- Animation **stops while the tab is hidden**, when MOTION is switched off, or when Windows asks
  for reduced motion. The news wire pauses too.

**Interaction**
- Hover a node (or **tap** it on a touch screen) to see its price, source, data age and latency, plus a larger chart of the move.
- On a **phone** the map re-flows into two columns with shorter labels and compact badges. The legend moves below the map.
- The news wire shows headlines immediately and loops seamlessly. New headlines join at the end of a loop, so it never jumps back.
- The hover card also shows:
  - the move and σ in all four windows, and the typical 1σ move;
  - relative volume and net taker flow;
  - funding and open interest;
  - the reference quotes with their basis;
  - the stablecoin supply change and the DXY's FX legs.
- The status strip shows every connector. The footer shows the PC clock correction, market hours,
  and the service's own CPU and RAM.
- The window choice and the motion setting are remembered in the browser.

**The page's own cost.** The page measures itself: type `__mfPerf.report()` in the browser
console. On 24 Nov 2026 it measured:

| Work | Time |
|---|---|
| One animation frame (blit + about 100 particles) | 0.25 ms |
| Redrawing the network when data arrives (once a second) | 1.4 ms |
| Updating the panel (once a second) | 1.1 ms |
| Parsing each ~87 KB update | 1.0 ms |

At the 30 fps cap that is about **11 ms of script per second, roughly 1% of one core**, plus the
browser's own compositing, which the page cannot see. With MOTION off it is about 3.5 ms/s. A
hidden tab only parses updates and draws nothing.

---

## 8. Performance

Measured on this laptop (Intel i7-10510U, 4 cores / 8 threads, 16 GB, Windows 10, Python 3.13)
with all default sources live:

CPU is given two ways: % of one logical core (Task Manager's *Details* tab) and % of the whole
machine (its *Processes* tab, 8 logical cores).

| Run (24 Nov 2026, US session) | CPU, one core: mean / median / p95 | CPU, whole machine | RAM (working set) | Private |
|---|---|---|---|---|
| **Headless, final config** (8 min) | **2.5% / 1.6% / 6.2%** | **0.31%** | **75-83 MB** | 62-67 MB |
| **+ 1 WebSocket client** (8 min; the load of one open dashboard: a full ~87 KB snapshot every second) | **3.7% / 3.1% / 7.8%** | **0.46%** | 78-82 MB | 61-65 MB |
| Headless, first config, first hour after the open (10 min; also streamed IEF/HYG bid/ask, since dropped) | 3.9% / 3.1% / 9.3% | 0.49% | 75-79 MB | 60-63 MB |
| **With the notable-stock slot** (25 Nov, pre-market, 10 min; 2 picked stocks live, 1 dashboard open) | **2.65%** / — / 6.2% | **0.33%** | 81-87 MB | — |

The large-cap screen costs no measurable CPU (one bulk request every 5 minutes, about 25 KB compressed, and a small hourly top-up) and about 5 MB of memory.

- **25-minute soak** (after the verification fixes, with the dashboard tab connected): 50 samples, 30 s apart.
  - Memory flat: 75.6 MB at the start, 74.2 MB at the end, 78.6 MB at most.
  - CPU averaged 3.6% of one core (at most 7.5%).
  - Every asset was live in every sample, with zero source errors.
- **Start to ready:** 9-21 s from the history cache.
- **Threads:** 18.
- **Peaks:** occasional single-second peaks of 12-22% of one core come from the periodic jobs:
  - parsing RSS (9 feeds every 3 min);
  - DefiLlama's 0.6 MB stablecoin list (hourly);
  - re-estimating σ (once a minute);
  - saving the history cache (every 10 min).
- **Load at the US open:** about 180 messages/s across all feeds. Binance BTC/ETH trades alone
  were ~90/s. `trade_stream = false` under `[connectors.binance_spot]` drops them in exchange for
  BTC/ETH prices up to ~2 s old.
- **Browser side:** see section 7 (measured in the page).

Why it is light:
- **One asyncio process.** No busy loops: every wait has a timeout.
- **Light streams.** Binance futures use 1 s index prices and 1-minute klines rather than every
  trade, Coinbase uses its 5-second batch channel, and Alpaca news is filtered.
- **Cached computation.** σ is recomputed once a minute, correlations every 10-30 s, volume
  baselines every 30-120 s.
- **Snapshots only when wanted.** They are built for the 1-second push only when a client is
  connected, otherwise on request.
- **Fixed memory.** Ring buffers of about 0.2 MB per instrument.
- **History cached on disk.**

Measure it yourself:
```powershell
python tools/measure_resources.py --minutes 10              # headless
python tools/measure_resources.py --minutes 10 --clients 1  # with one dashboard/WebSocket client
```

---

## 9. Recovery after sleep or network loss

- **Dropped or silent feeds.**
  - Every websocket reconnects with exponential back-off and jitter: 1 s doubling to 60 s, reset after a healthy minute.
  - A feed that goes silent beyond its idle limit counts as dead. That catches half-open TCP connections after Wi-Fi loss.
    - The limit is 45 s for Binance futures and 60 s for Binance spot. For FX it is 2 min and for IEX 10 min while their market is open.
    - It is an hour while their market is closed: silence is normal then.
  - aiohttp heartbeat pings check the peer.
- **Sleep or hibernation.**
  - A watchdog notices the wall clock jumping (more than 25 s in a 5 s tick).
  - It then clears the DNS cache, re-syncs the clock by NTP and **kicks every connector** into an immediate reconnect.
  - Then it **back-fills the missing history** from the exchanges (below).
- **What a sleep or a network outage leaves behind.**
  - A step counts as data only if its feed covered it: a message, or the feed's own heartbeat (a
    quiet market on a live feed is fine). A step that passed while the PC slept or the network was
    down is **empty**: no price, not live. It is never the last price carried forward as if it
    traded, which made σ too low and the history differ from the exchange (the 25 Nov night).
    Computed series (the dollar, bonds, the stablecoin share) are live only where their inputs are.
  - The first tick after a sleep takes nothing heard since the wake as covering the time asleep.
    After a wake every overdue timer fires at once, and when the Alpaca REST fallback's round came
    first (in about 1 of 100 to 120 simulated wakes with the stream up, the review found) the whole
    time asleep counted as covered: an ETF that had traded held its pre-sleep price there, flagged
    live, with no gap (1H +0.43% where the truth was 0; after a night asleep, TODAY +0.61% the next
    morning). Every round of the fallback counts as the ETFs' feed heard, standing by or asking (the
    stream up, the market shut, the stream connecting or refused): the fifteenth round counted no
    standby round, and with the stream refused (406: another copy holds the key's one stream) or
    unreachable nothing then told of the feed outside a session. Started then and asleep over the
    open, the slept session was carried, not back-filled; and a trading day with no trade at all
    (an unscheduled closure, IEX silent) ran a back-fill about every 67 s.
  - That holds before an ETF's first trade of the run too, once its feed has reported (any message:
    the stream's login counts). Started on a weekend or before the open, with the PC then asleep (or
    the network down) over one or more sessions, an ETF had those sessions carried from the cache's
    close: not live, but with no gap, so they were never back-filled; 1H read the night's move as
    the hour's, and after days away TODAY the whole move since the cache. An ETF with no feed at
    all (no Alpaca key: its history comes from Yahoo) has its sessions carried from its last close,
    as before.
  - After a session minute an ETF's feed missed, its close is unknown, and so are the shut steps after
    it (not live, no gap: the session's gap is what gets downloaded), until a session print or a
    download that covers the gap fills them with the close. They held the price from before the gap,
    and when the back-fill's answer had no bar for the ETF (an Alpaca anomaly, given up after that one
    attempt) they stayed: TODAY read the move since the gap began, status live, until 17:00 New York
    (+0.96% where the truth was -0.04%). A cache's empty shut steps are filled with the session's
    close when it is loaded, but not across an empty session step.
  - A **back-fill** downloads the empty steps. After a failure (DNS down after waking, no network)
    it retries after 30 s, doubling to 15 min, and 10 s after a feed delivers again (time for the
    feeds to settle; also when that happens during a download, on the first live price after a
    start whose download failed, and before an ETF's first trade when its stream reports in a
    session again). It stops only when nothing is missing, or when the exchange has no data there
    either (a gap that appeared during a download was not asked for, so it is the next attempt's; a
    gap older than the history starts at its oldest step). It runs
    in the background: it never holds up the auto-stop, and it stops with the app. Its state is
    under `/api/health` (`backfill`).
  - It downloads only the instruments with gaps, each from its own gap (after a sleep of many
    hours, from its newest value, not 35 days back). The dollar's six FX legs have no history of
    their own, so a gap in them re-downloads the dollar from ICE's DXY (Yahoo). The stale-close rule
    above holds for every market with shut hours, the FX legs too: one that slept over Friday's
    17:00 close holds an older price than the close all weekend, so its shut steps after the gap are
    unknown. While FX is shut and a leg is stale, the dollar's price is its own close (ICE's, once
    downloaded), held over the shut hours, instead of the formula on the legs' stale prices, until
    they print again on Sunday at 17:00. In the review's case (the legs stale from Friday 15:00, ICE
    0.3% higher at the close) the price read -0.30% against ICE's all weekend: TODAY -0.30% after a
    wake on Saturday at 1:00 (true 0), and 1W and 1M 0.30% off whenever the wake; now 0.00% and ICE's
    close. An attempt that got
    nothing does no more work; one that did rebuilds only the computed series it fed, each from its
    inputs' gap, and writes the cache off the event loop (each attempt used to stall the app for a
    second or more).
  - The time the app was off counts as a gap too, from the first moment its market was open (an
    ETF whose market stayed shut has nothing to download), in the 1-minute grid as well (a cache
    older than the day that grid holds leaves it empty; from its first open minute there too: at a
    closed one, a Sunday or 2:00, the download never covered it and every such start fetched each
    ETF again for nothing). If the first download at a start fails,
    the retry still fetches it, and meanwhile the cache's last close is not carried as data (it
    stood in as TODAY's start price for a whole session, and after 30-48 hours off TODAY read +44%
    and 1H +94%).
  - An instrument's price is the newest it knows: the cache's last data (not the close carried after
    it), a live price, or a downloaded bar newer than either. A sleep or an outage over an ETF's
    close used to leave the pre-outage price (live data had come, and after the close nothing else
    corrects it): the back-fill wrote the close into the history, the roll carried the old price over
    the night and weekend, and Monday's TODAY read +1.0% on an unchanged ETF. Without a key, an ETF's
    Yahoo history is carried on through the hours its market was shut since its last bar (not over
    open ones: a delayed feed's minutes are unknown).
  - **An ETF's history is its regular session**, as its live series is: Alpaca's IEX bars include
    pre- and post-market prints, which are kept aside (the latest as `ext_price`, like a live one)
    while the session's close is carried over those hours. Stored as closes, Friday evening's
    prints were carried all weekend and became Monday's TODAY reference (0.46% off in this PC's
    cache, with the price on the session). A cache written before this (format version 2 or older)
    is rewritten when loaded: each step outside the session takes the session's close (unknown
    before the first session step a grid holds; there a price is read from the 5-minute grid).
  - IEX has bars only for minutes that traded, so no bar between where a download began and its
    first bar means no trade: the last close is carried there, and no gap is left. The same holds
    for an answer with no bar at all for an ETF (a weekend, a holiday, a start before its first
    print) over shut hours, and over open ones up to the ETF's own limit: twice the longest stretch
    of 5-minute session steps without a trade (no traded volume, the close unchanged) in its last 35
    days, 15 minutes at least. A price that held while it traded is not such a stretch (six equal
    closes gave HYG 50 minutes, though it traded in five of them), nor is the app's own "no trade"
    fill of an empty answer, which is not live (counted, each one raised the limit: SPY's from 15 to
    20 and 30 minutes over two). On this PC's cache: SPY, QQQ, IWM, GLD, TLT, HYG 15 minutes; SLV
    20; VIXY 30; IEF 40; USO 50; UUP 210 (it often prints nothing on IEX for 50 minutes after the
    open). So an answer without SPY over a 30-minute outage (the review's case) is taken for the
    venue's gap: those minutes stay unknown and are fetched again (then given up as "no exchange
    data"), not filled with the last price as if nothing had traded. Unless the venue says otherwise:
    for an ETF with no session bar in an answer that spans session time, Alpaca's latest trade of
    it is asked (one request for all such ETFs, with a key; none at night or on a weekend), and a
    stretch from after that trade had no trade, whatever its length. An ETF whose close changed
    every 5 minutes lately (a limit of 15 minutes) and that first prints at 10:00 got a hole for
    9:30-9:50 at a 9:50 start, one more download and "no exchange data"; in the review's runs (5
    thin ETFs; starts at 9:50 on a Monday and the Friday after Thanksgiving, and 10:30 with the first
    print at 10:45) each had 25 to 65 unknown 1-minute steps since 9:30, now none, with 3 requests
    instead of 4. SPY missing from an answer while the venue's latest trade of it is recent is still
    the venue's gap. Other symbols' bars in the same answer were considered as the signal, but they
    cannot tell a thin ETF that did not trade from a liquid one whose bars the venue lost. A thin ETF's first trade after
    9:30 left a gap (fetched again, then given up), also with the start before that trade; a cache
    saved on a Saturday left the weekend empty until Monday's first bar, and with no bar at all
    since, the time off stayed empty (every ETF's TODAY unknown all day). A cache can hold such an
    empty night or weekend: filled with the session's close when loaded.
  - A back-fill's 1-minute download is one day long; the 5-minute one reaches back to the gap. So
    after a download the 1-minute grid's shut hours before its first bar take the 5-minute grid's
    close: after a wake in a session a day or more after an ETF's close (a weekend, a holiday) they
    kept the pre-sleep price, and TODAY read the move to the close slept over all session. The
    1-minute download begins on a 5-minute boundary: begun inside a 5-minute step (a day before a
    start at 10:01:30, so at 10:02:30), its carry up to the first bar came from the 5-minute grid's
    close before that step, and a quiet ETF's print in the step's first minutes was missed (10:02 to
    10:06 read the 10:00 price, not the 10:01 print).
  - Empty steps in the cache (a past outage) are downloaded at the next start. A cache written
    before this fix (format version 1) can hold a sleep's flat stretch flagged live, so its whole
    range is downloaded again once. So is an ETF's cache of format 4 or older: it can hold sessions
    slept over before the ETF's first trade of a run, carried from the close before (not live, no
    gap, never fetched). That download is a gap at the cache's first value, which nothing in the data
    shows: the gaps a download has yet to fill are saved with the cache, and one older than the
    history starts at its oldest step. A first start whose downloads failed for a few minutes (the
    network down for 6, an Alpaca error for 40 s) dropped it as older than the history, and one
    stopped before they succeeded saved the cache in the new format: either way the one-time download
    was lost for good, and a week later 1W read SPY +1.13%, HYG +0.73%, IEF -0.27% (true 0).
  - A picked stock whose history failed to download, and a kick a connector got while busy, are
    not left to their back-off either: a recovered feed retries the stock at once, and the kick
    ends the connector's next wait.
  - With no dashboard open, it stops by itself within 5 minutes of the PC waking (auto-stop,
    section 1). An open dashboard reconnects within that time and keeps it running.
- **Pollers** back off on errors (10 s doubling to 15 min) and recover by themselves.
- **Rate limits:** a `429 Too Many Requests` answer makes that source wait at least 5 minutes (or its `Retry-After`) before asking again.
- **Restarts reuse fresh data:** the slow sources are cached in `data/cache/`, so frequent restarts stay far from the sources' limits:
  - market cap 10 min, stablecoin supply 1 h, FRED 6 h, calendar 2 h;
  - the FOMC calendar is also cached.
- **No network at start-up:** history is retried every 1 min (then 2, 4 … up to 15 min) until every instrument has it.
- **Another app holds Alpaca's single stream:** the free plan allows one, and Alpaca refuses the second with code 406. The service waits 5 min between retries and meanwhile polls the latest IEX trades over REST every 20 s.
  - It waits for Alpaca's login answer before subscribing, so a refusal is never mistaken for bad keys.
  - The fallback's status says what it is doing ("standby (stream connected)", "standby (US market
    closed)", "polling (stream down)"); it was cleared right after each round, so the sources panel
    showed nothing.
- **Alpaca refuses the keys** (HTTP 401 or 403 from its REST API, 402 at the stream's login): the
  first refusal marks them, and from then on the ETFs are read as without a key: history from
  Yahoo, their steps carried, not covered by the stream's or the fallback's messages (an error
  message used to count as coverage), and nothing asks Alpaca again (every start-up retry and
  back-fill asked and failed, 32 requests over a morning in the review's run). The sources panel
  says why ("Alpaca refused the keys: ETFs are read without a key (Yahoo history, no live prints)").
  - Tested live: the bonds node stayed **live** through the REST fallback, at a data age of about 20 s.
- **Finnhub also allows one stream per key.** A second copy of this service, or another app using the same key, takes the slot, and Finnhub refuses the first with HTTP 429 (seen live on 24 Nov 2026).
  - Any websocket refused with 429 waits at least 2 minutes before retrying instead of hammering, and its status says why.
  - **Run one copy at a time;** other apps should read this service's API rather than open their own Finnhub or Alpaca streams.
- **Closing the window or Ctrl+C / Ctrl+Break:** a graceful stop that saves the history cache. Tested: exit code 0 in 1.2 s, cache written.
  - The feeds stop first, then the port is freed, then the cache is saved (the log says how long it
    took), and then open dashboard tabs and SSE apps are closed ("going away") and the server shuts
    down. The save is the stop's slow part: about half a second, 1-1.5 s with every core busy, since
    it writes 8 files at once (one at a time it took about a second, and 4-13 s on a busy PC in the
    reviews' runs; earlier it took twice that: no rounding of every value now, a slice of each ring
    buffer, `orjson` when installed).
  - Measured with the real `serve()` (33 instruments) and a tab or an SSE app connected: the save
    starts at once and the process ends 0.5-1.6 s after Ctrl+C. Before, the save waited for the
    open streams (41 s for a tab, 122 s for an SSE app, the process lingering as long), and a second
    Ctrl+C meanwhile, or Ctrl+Break then Ctrl+C, ended it with nothing saved.
  - **A second Ctrl+C during the save** is noted ("still saving the history cache...") and the stop
    goes on; once the save is done Ctrl+C acts again. It used to cut the save short (11-27 of 33
    files written in this round's runs, the server's shutdown skipped).
  - **Closing the console window:** Windows ends the process about 5 s after the window closes,
    whatever runs. The process was gone within 0.03 s with nothing saved: the C runtime's handler
    for that event returns at once. A handler of its own now starts the stop and holds the process
    until the cache is saved, first on this path: saved in 0.4-1.0 s, then Windows ends the process
    (its exit code is then 0xC000013A). Tested by closing the console window (WM_CLOSE, what its X
    button sends), through `start.bat` too, and during a stop already under way (after Ctrl+C,
    Ctrl+Break or the auto-stop: that save used to be skipped).
  - **The console is written from a thread of its own.** A console window with a text selection (a
    click in it with QuickEdit on, Windows' default, or Edit > Mark) holds every write to it until the
    selection ends: a log line written meanwhile froze the whole service (the API did not answer),
    and a window closed in that state saved nothing. The service now runs on, and the lines appear
    once the selection ends. At a stop under a selection the process waits up to 2 s for them and
    then ends normally (the log file has every line); it used to end in "Fatal Python error" once the
    selection ended. The log file also says when a window close started the stop ("the console
    window is closing: saving the history cache first...").
  - **The limit:** Windows ends the process about 4.5 s after the close, saved or not. Most of a
    save's time went into closing each file (Windows scans a file written to as it is closed: 1.1 of
    a save's 1.7 s), one file at a time, so the final save now writes 8 at once. Measured on an
    engine with every instrument's grids full (33 instruments, 12.1 MB), 5 saves each: 1.34-1.40 s
    before, 0.43-0.53 s now; with every core busy (8 processes spinning), 4.12-4.74 s before and
    0.94-1.52 s now. It still starts the files the next start reads first, and those a cut save
    does not reach keep their last save (every 10 minutes). Each file is written whole or not at
    all. The saves every 10 minutes stay one file at a time, in a worker thread: nothing waits for them.
  - **Logging off or shutting Windows down** ended it without the final save: Windows does not call
    the console handler in this process then (`user32.dll` is loaded by its imports, so it counts
    as a windowed program), and such a program hears of a session's end as window messages. A
    hidden window of its own, on a thread of its own, now gets them: `WM_QUERYENDSESSION` (may the
    session end?) is answered yes and changes nothing, since another program can still cancel it;
    `WM_ENDSESSION` (it ends: Windows ends the process once the message returns) starts the stop,
    which saves the cache first, and returns once it is saved, giving "Saving the history cache" as
    the reason Windows shows if that takes long (30 s at most). Tested on the real app (keyless, 23
    instruments) by sending its windows the messages Windows sends at a logoff, then ending the
    process as Windows does: 23 of 23 files saved before the message returned, 0.13 s after it was
    sent (0.94 s with every core busy); the log says "Windows is ending the session (logoff or
    shutdown): saving the history cache first...". Before, the process had no window of its own to
    tell: nothing was saved.
- **History** is saved to `data/cache/` every 10 minutes, after each download and on exit. The files
  are written in a worker thread, from the list of instruments taken on the event loop (which picks
  and drops stocks meanwhile: reading its live list from the thread failed), one save at a time. An
  error in a housekeeping round is logged and the next round runs (one failed save had ended wake
  detection, back-fills and saves for the rest of the run). Each process writes its own temporary
  files: a copy started while another still saves (the port is freed first) cannot write into its.
  Windows refuses to replace a file another process has open (a copy loading the cache at its start):
  the replace is retried for up to 0.75 s, then its temporary file is removed. A temporary file no
  save writes any more (its process ended, or ten minutes old) is removed at the next start.
- **Tests:** `tests/test_reconnect.py` checks:
  - reconnection after server drops, after silence and after a `kick()`;
  - poller back-off;
  - the resume path;
  - that the sleep gap is re-downloaded, not left flat, through the production path (housekeeping
    sees the wall clock jump and the wake starts the back-fill). That bug was caught here: the
    1-second tick had already filled the gap with the pre-sleep price;
  - a wake with DNS failing for the first two tries: the gap ends filled with the exchange's prices,
    and no step is left carried as live (it was empty, not live, meanwhile);
  - an outage while awake, back-filled once the feed is back; a recovered feed waking the back-off
    once, not every 5 s; a gap the exchange has no data for given up, not retried forever;
  - a quiet market on a live feed staying live (the heartbeat); and (`tests/test_series.py`) a
    computed series not live while an input was down (the bonds, and any computed series);
  - an old cache with a flat night flagged live, downloaded again; and (`tests/test_idle.py`) the
    auto-stop firing on time while a back-fill keeps failing;
  - the dollar re-downloaded from ICE when its FX legs have a gap (TODAY starts again); a failed
    first download keeping the time off as a gap (none for an ETF over a weekend); a long sleep
    downloaded from each instrument's own gap; an attempt that got nothing neither saving nor
    rebuilding; the wake and housekeeping entry points; a gap found mid-download fetched next; a
    recovery during a download, or a kick while busy, not lost; an ETF covered by its REST
    poller's heartbeat;
  - the cache written in a worker thread while stocks are picked and dropped, two saves taking
    turns, housekeeping going on after an error; a newer gap of the same instrument fetched, not
    given up; the minutes after a failed start unknown, also after more than a day off; the first
    live price waking the back-fill; the downloaded close as the price (an ETF from Alpaca and from
    Yahoo, the dollar, a perpetual), never a bar still open or a pre/post-market one; the cache's
    price dated at its last data; Yahoo's history carried over shut hours only;
  - an ETF's pre- and post-market bars kept aside (TODAY, 1H and 1W unchanged on an unchanged ETF,
    also with a cache saved on a Saturday), an older cache rewritten to its session closes, a thin
    ETF's late first trade covering its gap; the 1-minute gap from its first open minute and no
    back-fill after the start-up download; without a key, TODAY's start all weekend; one wake of
    the back-fill per recovery, not per live price; housekeeping's save in a worker thread; the
    port freed before the final save, and two processes saving the same file at once;
  - a download with no bar at all carrying the close (a Saturday cache, a start before a thin ETF's
    first trade), an older cache's empty night filled, a format-1 cache rewritten like a format-2 one;
    a sleep over the close: the back-fill's newer close replacing the stale live price and what the
    roll carried of it; the first five minutes of a session; a refused replace retried and stale
    temporary files removed; and (`tests/test_idle.py`) the stop with a tab and an SSE app connected
    (the port free during the save, the save first and at once, the streams told to go away) and
    the console-close handler saving first;
  - a logoff or a shutdown (the hidden window: a query changes nothing, a cancelled end nothing, the
    end returns only once the cache is saved, through the real `serve()` too); the final save's
    files written several at once, in the order the next start reads them; refused Alpaca keys (one
    request, then the ETFs as without a key); the downloads' bars closed by the corrected clock; an
    FX leg's unknown weekend after a gap over Friday's close and the dollar on ICE's close over it;
    the venue's latest trade for an answer without a bar (and none asked over a night).

**Sleep, tested live** (24 Nov 2026, US session). The running service was **frozen for 150 s**
(process suspended, which is what it experiences during sleep), then resumed:

| When | What happened |
|---|---|
| On resume | The log says `wall clock jumped 151s (sleep/hibernate?): resyncing and reconnecting`. The clock is re-synced by NTP and every stream reconnects. |
| 2 s later | Nodes honestly show **STALE**, with data ages of 126-133 s. |
| 20 s later | The gap's history was downloaded (the log line then read `history gap since 15:07:21Z refilled`; it now reads `history gap since ... back-filled (wake, attempt 1)`). |
| 40 s later | Every stream has reconnected (2 connects each) and all 11 nodes are **LIVE** with data ages of 0.1-5 s. |

The same test was repeated **in place on the running service** after the verification fixes.
It detected a 154 s jump and all six streams reconnected. **All 11 assets were live again 12 s
after waking**, and the gap's history was back-filled 15 s after.

**Sleep with DNS down, tested live** (25 Nov 2026, the 25 Nov morning reproduced on test copies
without keys). Inside each copy the resolver was made to fail, as getaddrinfo did that morning. The
process was then frozen for 12 minutes, which is what a sleep looks like to the app, and DNS stayed
down for 2.5 minutes after waking:

| | Before this fix (commit 66ace16) | Now |
|---|---|---|
| The sleep's 1-minute steps (16:48-16:57 UTC) | 10 of 10 held the pre-sleep price (58,270.01), flagged live, while Binance moved from 58,241.66 to 58,377.32 | empty while DNS was down, then 10 of 10 equal to Binance's |
| Back-fill | one try 10 s after waking, failed, never again | 3 tries failed while DNS was down; the 4th, 73 s after DNS came back, filled the gap |
| `tools/verify_against_exchange.py` (35 days × 6 series) | 6 mismatches: the 25 Nov night still flat in the cache (27-49 steps per series) | all 10,078 steps of all six series equal Binance's (the old cache was downloaded again once), and so do σ and z |

A real network cut needs admin rights and would cut your own connection, so the DNS outage was
simulated inside the test copy. The websocket drops it causes are covered by the drop and silence
tests and by the Finnhub relay test (section 10).
Closing the console window was tested too (above): the cache is saved first. So is a logoff or a
shutdown.

---

## 10. Tests and tools

```powershell
python -m pytest              # unit, API, reconnect and offline-replay tests (no network needed)
```
- **Unit tests:**
  - key loading and redaction;
  - New York trading-day roll across DST, NYSE holidays and early closes, FX and VIX hours;
  - the ring buffers;
  - the stitched bonds series: the 1- and 5-minute series agreeing, gaps in the proxy, and bonds without an Alpaca key in US hours;
  - σ estimators on exact synthetic series: trading days only (a quiet weekend or Thanksgiving does not shrink an index's σ), the same clock hour (a quiet pre-open hour is not judged by the opening hour), an hour belonging to the session of its midpoint, √(sessions in the window) for 1W (4 in Thanksgiving week), one one-off day capped through the real 1H, TODAY and 1W estimators (and as one day even when all five 1H windows see it), and the stock screen and the node giving the same σ;
  - every stream connector's **real** reconnect loop through a session that ends and one that fails, no connector attribute hiding a method of its class, and a crashed connector being logged and restarted (`tests/test_reconnect.py`);
  - edge weights (including series built from each other), which edges are kept, the plain-language reason for every asset without an arrow, and the risk composite;
  - **every node, gold and the stock slot included, as the strongest outflow and the strongest inflow, in all four windows**, through the real engine (`tests/test_every_node.py`: 88 forced cases);
  - auto-stop: the idle timer (a tab connects, disconnects, a refresh within the delay, two viewers, API polling, a webhook delivery, the opt-out, never opened), and the real `serve()` stopping by itself and freeing its port, or staying up while an app streams (`tests/test_idle.py`);
  - the notable-stock screen on exact synthetic hourly bars (σ, volume ratio, score, the rule, "may be none"), index prices rather than last trades, 5-minute start prices after a start, a stock the map reads ranked on the map's own numbers, "eased" only when the move itself faded (and held until the next screen), the universe filter, and the slot as a full node (snapshot, schema, narrative, history id);
  - narrative rules (including the reference screenshot's read);
  - Fed calendar parsing;
  - RSS/Atom and news de-duplication;
  - config merging and validation.
- **API:** REST endpoints validated against the JSON Schema, WebSocket and SSE streams, the Host
  guard and token, headless mode, and no keys in any response.
- **Offline replay:** `tests/data/replay_sample.jsonl.gz` holds real frames recorded on 24 Nov 2026
  around the US open, replayed through the real connector parsers, the engine and the snapshot
  builder. The test checks prices, the DXY formula, latency and the schema. The recording is raw
  provider data, which their terms do not allow republishing, so it is not in this repository:
  record your own with `tools/record.py` (the replay test skips without one).
- **Tools:**
  - `tools/selftest.py`: checks a **running** service end to end.
  - `tools/record.py`: record live frames for replay.
  - `tools/check_latency.py` (= `python -m moneyflow check`): latency and freshness report.
  - `tools/measure_resources.py`: CPU and RAM report.
  - `tools/replay_flows.py`: replays the flow map over the cached history (offline) and reports how often each asset had an arrow in each window, and when it had none, which rule left it out.

**What `tools/selftest.py` checks:**
- every endpoint and error code, and the JSON Schema for every window;
- each asset's status, data age, z and σ;
- that every edge runs from an outflow to an inflow;
- the risk range, the Fed date, news de-duplication, and that connectors are up;
- latency, CORS, the Host guard, and the WebSocket and SSE streams;
- **prices against independent sources:**
  - Binance REST (BTC, ETH, the index prices);
  - Yahoo's ICE DXY and VIX;
  - CoinGecko's stablecoin share;
  - Alpaca's latest TLT trade and the SPY basis.

**Second test round (24-25 Nov 2026):**
- **Independent recomputation** (`tools/verify_against_exchange.py [--url ...]`, which recomputes the README 3.2 formulas on its own) for BTC, ETH, Nasdaq 100, S&P 500, gold and oil, from 35 days of raw Binance 5-minute data:
  - stored history matched 10,079 steps (worst 0.0125%, on the still-forming latest step);
  - a 2-hour restart gap had been back-filled with real data;
  - σ (1H, TODAY, 1W), z-scores and % changes are identical to the recomputation;
  - the strongest edge's correlation matched (0.110 vs 0.109).
- **Derived series on real data:**
  - stitched bonds match TLT's 5-minute closes in US hours (max 0.006%);
  - overnight, each of 715 steps equals the TMF index move ÷ 3 exactly;
  - the computed DXY stayed within 0.012 of ICE's;
  - the stablecoin share stayed within ~0.5% (relative) of the share implied by CoinGecko's fresh totals, which lag 2.5-7.5 min.
- **Load:**
  - 20 WebSocket and 10 SSE clients plus 40 REST requests/s for a minute: 1,845/1,845 requests OK (p50 1.6 ms, p95 9.6 ms);
  - every client got its 1/s update, and CPU stayed at 4-11% of one core;
  - 5 minutes of streamed messages (299, validated for all three windows): 0 schema failures, 0 duplicates, max gap 1.3 s.
- **Security:**
  - path-traversal attempts are all 404, with nothing leaked;
  - foreign Host headers are refused;
  - write methods get 405;
  - malformed, binary and oversized WebSocket input neither crashes nor blocks anything, and a stalled client doesn't stall others;
  - no key appears in any response.
- **More recovery and calendar tests:**
  - the network down for a while, then back;
  - a dead webhook target;
  - a start with no data at all;
  - Thanksgiving (US shut, FX and crypto live), a Saturday (FX shut) and the 13:00 early close;
  - a real daily roll replayed from the cache;
  - token mode in the browser.
- **Live market transitions, 24 Nov 2026:**
  - **US close (21:00 UTC):** within 20 s bonds switched from TLT to TMF ÷ 3 and carried on from TLT's last price with no jump. HYG/IEF and the ETF reference quotes went to "closed", and the caveats appeared.
  - **Daily roll (17:00 New York, 22:00 UTC):** TODAY switched to "since 17:00 New York Tue 24 Nov" by 22:00:02, with z-scores for all 11 assets throughout (σ > 0, nothing "warming up"). A copy with keys and one without agreed on bonds after the roll (+0.027% vs +0.026%, same σ).
  - The dollar went "stale" for about 4 minutes after 17:00 New York, when FX quotes pause for the daily rollover. That is correct.
- **Optional parts tested live:**
  - the Hyperliquid connector, whose Nasdaq-100 perp read z −1.08 against −1.13 from Binance's QQQ contract;
  - the copper, Russell 2000 and OpenAI nodes;
  - `start.bat` (normal start, and a busy port).
- **Dashboard:**
  - phone, tablet and desktop layouts;
  - memory flat over hundreds of forced re-renders;
  - 0.9% of one core at the full 30 fps.
- **Fixed in this round:**
  - **TODAY's σ read 0 for ~5 minutes after every daily roll** (17:00 New York, 22:00 UTC in winter), blanking the z-scores. It was found by replaying a real roll from the history cache;
  - **quiet markets would have caused reconnect storms:** IEX is silent overnight and at weekends, and FX from Friday to Sunday, so the streams would have reconnected every 2-10 minutes. While their market is closed the silence limit is now an hour, and the heartbeat still checks the link. Finnhub pings every 60 s, measured;
  - the dashboard now says when a token is missing or wrong, instead of "stream lost";
  - Finnhub (one stream per key) refused a second copy with HTTP 429 and the service retried every few seconds; 429 refusals now wait ≥ 2 min;
  - the Host check was case-sensitive;
  - `client.stream()` now reconnects after a restart;
  - labels and badges now make room for same-row neighbours on tablets;
  - **bonds' TODAY move differed by 0.1% between two copies of the service.** Found by running one with keys and one without, side by side. The one-day 1-minute bonds series restarted from TLT's raw close at the start of its window, dropping the proxy's moves since the close. It now starts from the 5-minute series, at a 5-minute boundary where the proxy's own close is known, and a gap in the proxy's data no longer drops a move. The two series now agree within 0.0002% outside US hours (in US hours both are TLT's real bars);
  - **without an Alpaca key the bonds node was blank.** It now takes TLT's history from Yahoo's chart API and follows the TMF proxy from TLT's real close. With and without keys, the TODAY baselines agreed within 0.013% and the moves within 0.007 points;
  - **...and in US hours it would have stayed frozen at that close.** A price carried forward from history counted as "trading" during market hours, so the bonds series kept TLT's old close instead of following its proxy. A carried price now counts as trading only once live data has arrived;
  - **"Bitcoin → Stablecoins, ρ −1.00" was drawn as the strongest kind of rotation.** The stablecoin share is computed from Bitcoin's and Ether's prices, so that correlation is arithmetic. It now counts as unknown (section 3.4), which halves such an edge's weight;
  - **for up to a minute after the daily roll, σ from the previous day was still served** (cached), so z-scores were about 6× too small (Bitcoin's σ read 2.46% instead of 0.43% until 22:00:47). Seen in the live roll; everything cached is now dropped when the trading day changes;
  - after a restart in the 17:00 New York lull the dollar read "stale" for 11 minutes with no reason given. One FX pair had not ticked yet; the Finnhub source now names such pairs.

**Verification pass on 24 Nov 2026, during US hours.**

- **Self-test:** 268 of 268 checks passed.
- **Failure scenarios, run live:**

  | Scenario | Result |
  |---|---|
  | Busy port | Clear message, exit 1 |
  | `check` while the service runs | Refuses with an explanation |
  | Second instance (Alpaca stream refused) | REST fallback kept bonds live |
  | No API keys at all | Runs on the keyless sources, says why each key-based one is off |
  | Process frozen 150 s (sleep) | Recovered within 40 s |
  | Ctrl+Break | Graceful stop |
  | 25-minute soak | See section 8 |
- **Bugs this pass found and fixed, each now covered by a test:**
  - Cached history could count as "live".
  - A refused Alpaca login was taken for bad keys, which delayed retries by an hour.
  - REST-polled prices were counted as network latency.
  - The stablecoin share could start empty for 10 minutes.
  - Webhooks fired a false regime change at start-up.
  - A new listing showed "warming up" without saying why.
  - Thin IEX ETFs went stale every few minutes.
  - The phone layout overlapped.
  - The news wire jumped back on every headline.

**Third round (25 Nov 2026): "why is Anthropic always on the map, and why does gold never get an arrow?"**
- **Anthropic** was a fixed node, yet it was only the stock that stood out on the reference map. It
  is replaced by the notable-stock slot (section 3.8). On its first live screen (pre-market) it
  picked DELL for TODAY, −9.2% on 4.1× volume, cross-checked against Alpaca (DELL closed Tuesday
  −12.4%) and Binance's REST index (within 0.01%). It picked WMT for 1H, and nothing for 1W.
- **Gold, the evidence.** The flow map was replayed over the cached history with the dashboard's
  own engine (every 15 minutes for 24 h, every hour for 7 days):

  | Gold has an arrow | 1H (24 h) | TODAY (24 h) | TODAY (7 d) | 1W (7 d) |
  |---|---|---|---|---|
  | Before the fix | 37% | 47% | 46% | 100% |
  | After | 42% | 61% | 51% | 100% |

  - The data path was sound: gold's price matched Binance's REST index, and its z-score and status
    were normal.
  - Most arrow-less moments are **legitimate**: gold moved less than ±0.5σ (34 of 38 in TODAY over 24 h).
  - **A selection flaw** removed the rest. With 5 arrows per asset, the havens (bonds, stablecoins,
    the dollar) filled up with stronger sellers, and gold (often the weakest seller) lost its only
    arrow: 9 of 51 arrow-less TODAY moments in 24 h. Every clearly sold or bought asset now keeps its
    strongest arrow, as on the reference map, where bonds and stablecoins each take 6-7 arrows.
  - **Drawing**: the arrows were drawn (pixel-checked on the canvas), but a typical one was 2 px wide
    at half opacity. They are now thicker and brighter. On phones, arrows between neighbours in a
    column were hidden under the spheres and labels; they now bow clear of them.
  - Hovering an asset now lists its arrows or says exactly why it has none (section 3.4). On
    25 Nov 11:35 UTC gold had arrows in all three windows (1H: the window's strongest, gold → stablecoins).
  - `tests/test_every_node.py` proves every node, gold and the stock slot included, can be the
    strongest outflow and inflow in each window.
- **Also found and fixed in this round:**
  - the stock screen's weekly σ included the current day, which is the move being measured;
  - the hover mixed up two stocks when the slot held different ones in different windows;
  - the phone layout kept stale positions when the slot appeared on switching windows;
  - a newly picked stock appeared as "no data" for the few seconds its history was loading
    (seen when the pick switched from DELL to SHOP). It is now shown once its data is in, and
    meanwhile listed as `loading`;
  - **after a Finnhub server error (HTTP 502 on reconnecting) Finnhub refused all six FX pairs
    ("Invalid symbol") and kept the connection up with pings alone, so the dollar went stale for
    good** (seen live at 12:30 UTC, still stale 11 min later). While refused pairs stay silent the
    service now reconnects after 60 s, backing off to 15 min.

**Auto-stop, tested live on 25 Nov 2026.** A 1-minute delay was used unless noted. The service was
watched through the process list and open ports only, because a request would itself count as use.

| Scenario | What happened |
|---|---|
| Started, never opened (default 5 min; a copy without keys) | Started 12:03:46 UTC. At 12:08:49: "not opened for 5 minutes: stopping"; cache saved; exited at 12:08:50, code 0. |
| Dashboard opened, kept open, then closed | Opened 12:09:45 and still running 100 s later. Tab closed 12:11:41. At 12:12:43: "no dashboard tab or app connected for 1 minute: stopping"; exited at 12:12:45, code 0; port free 65 s after the close. |
| An app streaming the API for 150 s | 148 snapshots received; the service stayed up throughout, long past the delay. The app left at 12:15:47; the service stopped at 12:16:52 and exited at 12:16:54, code 0. |
| `--stay-running` (given together with `--idle-minutes 1`) | Still running 100 s after start with nobody connected ("auto-stop off"). |
| A hidden tab left open (port 8765, default 5 min) | The test browser kept a hidden dashboard tab open, so the service rightly stayed up. That tab was closed at 12:43:16. At 12:48:20: "no dashboard tab or app connected for 5 minutes: stopping"; exited at 12:48:24, code 0. The browser's idle keep-alive connections did not count. |
| Afterwards | No Python process left on the PC, no port listening. No scheduled task (of 196), Startup-folder item, Run/RunOnce registry entry or Windows service refers to the app. |

**Fourth round (25 Nov 2026): σ calibration, "the map and the stock screen disagree".** The map
showed SHOP at −1.1σ for TODAY while the stock screen ranked it at −2.9σ. There were two causes:
- **Two estimators.** The screen used 20 days of same-clock moves. The node used 30 calendar days
  (TODAY) and every 60-minute move of 10 days (1H). Both counted SHOP's −18.3% earnings day,
  which more than doubled the node's σ.
- **Weekends counted as trading.** The Binance perpetuals trade 24/7, so σ for the S&P 500, the
  Nasdaq 100, gold and the stocks included weekend and holiday hours, when the market under them
  hardly moves. That made σ too small.

One estimator now serves both (section 3.2): trading days only, the same clock hour for 1H, a cap
on a single one-off day. The slot's reason text quotes the node's numbers. The table shows the
same frozen data (25 Nov 13:04 UTC, 08:04 New York, before the US open) through the old and the
new code:

| z, before → after | 1H | TODAY | 1W |
|---|---|---|---|
| S&P 500 | −0.10 → −0.16 | −4.04 → −3.40 (σ 0.45% → 0.53%) | −1.07 → −1.04 |
| Nasdaq 100 | +0.17 → +0.26 | −3.17 → −2.66 (σ 0.90% → 1.07%) | −1.12 → −1.11 |
| Gold | +1.62 → +2.04 | −1.01 → −0.85 | −1.38 → −1.43 |
| SHOP on the map | −0.29 → −0.39 | **−1.06 → −2.53** (σ 3.47% → 1.44%) | −1.19 → −1.18 |
| SHOP on the screen | −1.08 → −0.60 | −2.95 → −2.47 | −1.38 → −1.16 |
| Bitcoin (every day counts) | +0.19 → +0.34 | −1.72 → −1.72 | −1.36 → −1.33 |

- **Live**, on a copy without keys: the map and the screen agree in all three windows (section 3.8,
  14:12 UTC). The reason text, the hover and the panel all quote the node's numbers, and the hover
  names the days σ is measured on ("same-clock on NYSE trading days (20)"; API: `sigma_days`).
- **Three more causes turned up while checking live**, and each is fixed:
  - **Last trade against index.** The screen priced stocks by the perpetual's last trade; the
    node uses Binance's index of the stock. MDB's perpetual traded about $1,000 in the hour to the
    17:00 roll. Its last trade sat 0.6% above the index then and 0.4% below at 09:00 New York, so the
    screen saw −5.3% where the map saw −4.4%. The screen now reads index prices too (one bulk
    request at weight 10 instead of 2).
  - **The screen's σ lagged by up to an hour.** Hourly bars give σ at whole hours only, so at 08:59
    New York the screen still used 08:00's σ. That left out a market-wide jump on Monday 23 Nov
    between 08:55 and 09:00: S&P futures +1.3% and gold +1.1% on Yahoo (ES=F, GC=F), Bitcoin +4%. The
    node counted it from 09:00, and PANW read −2.5σ on the screen against −2.0σ on the map. Between
    whole hours the screen now interpolates σ from the hours either side.
  - **A 1H move can fade in the minutes between two screens.** NKE read −2.35σ at the 13:52:40
    screen and −0.88σ on the map 2½ minutes later. The σ agreed (0.33% and 0.34%) but the move did
    not (−0.77% against −0.30%), because of the minutes and, then, the last-trade price. The slot
    now says when a move has eased since the screen picked it (section 3.8).
- **σ can step at a clock time.** A same-clock yardstick counts every past day's moves up to this
  clock time, so that Monday jump raised TODAY's σ for the S&P 500 from 0.64% at 08:43 New York to
  0.76% at 09:00. It is real volatility, not a fault.
- **The gold-arrow replay, again.** The engine ran over the same frozen history, once with the old
  code and once with the new:

  | Gold has an arrow | 1H (24 h) | TODAY (24 h) | TODAY (7 d) | 1W (7 d) |
  |---|---|---|---|---|
  | Before | 47% | 69% | 50% | 100% |
  | After | 59% | 64% | 45% | 100% |

  TODAY lost a few arrows, and that is intended. Without quiet weekends diluting it, gold's σ is
  larger, so more of its small moves now read as neutral (inside ±0.5σ). Every arrow-less moment
  still has a stated reason: in TODAY over 24 h, 32 were neutral and 3 had no counterpart. None
  was crowded out. `tests/test_every_node.py` (66 forced cases) still passes.
- **A first version was too aggressive.** It capped every outlier at 3× a median scale. That made
  the map and the screen disagree on ZS's week: daily σ 3.5% on the map, 2.5% on the screen. A stock
  with a run of big days is volatile, not unlucky. Now only a single one-off day is capped, and ZS
  reads 4.5% on the map and 4.35% on the screen. For crypto the 1H lookback stays at 10 days, so its
  yardstick follows the current regime.


**Fifth round (25 Nov 2026): a review of the σ round.**
- **Finnhub could lose the dollar for good on any disconnect** (since commit 60e9614). An attribute
  named `retry_delay` hid the base class's `retry_delay()` method. The first reconnect, after a
  network blip, a sleep or the refusal retry, raised a TypeError, the feed's task died, and the
  dollar stayed stale until a restart (seen twice in the log that day). Fixed:
  - the attribute is renamed;
  - every stream connector's real reconnect loop is now tested;
  - a test fails if any connector attribute hides a method;
  - the engine restarts a crashed connector, logged with its traceback (keys redacted), after 5 s
    doubling to 5 min.

  Live, through a local relay that cut the Finnhub connection without a close frame (a network drop):

  | After the cut | Code before this round (a2f3856) | Now |
  |---|---|---|
  | Reconnect | never: the relay saw no new connection | after 1 s |
  | FX ticks | stopped | 645 trades in the next 2 minutes |
  | The dollar | stale at +122 s, while its source still read "connected" | never stale (data age 1-4 s) |

- **"The move has eased since" appeared when nothing had eased.** It was decided on the node's |z|
  alone, so the σ gap between hourly bars and 5-minute data flipped picks whose move was identical.
  "Eased" now needs the move itself to shrink below 3/4 (or turn), and it holds until the next
  screen. On the review's 30 cases (screen z −2.05 or −2.10, the very same move, through the real
  engine), 19 were flagged before and none now. The node's z in those cases ran from −1.60 to −2.38;
  the difference was σ only.
- **The screen's σ keeps closer to the node's.**
  - For each of the node's window ends (±30 min for 1H), it interpolates between the whole hours.
  - 1H is no longer pinned to the 15:00-16:00 hour from 16:00 on.
  - An hour belongs to the session of its midpoint.
  - A stock the map already reads is ranked on the map's own numbers. Live at 16:01 UTC: HD 1H
    +2.316σ on the screen and +2.321σ on the map, AMZN TODAY −2.224σ and −2.221σ, PDD 1W +2.421σ
    and +2.422σ.

  On the review's synthetic days (12 seeds), the node's σ ÷ the screen's, as median [range]:

  | Window, New York time | Before | After |
  |---|---|---|
  | 1H 03:00 | 0.99 [0.84-1.23] | 1.03 [0.91-1.13] |
  | 1H 09:45 | 0.88 [0.83-0.94] | 0.93 [0.88-0.99] |
  | 1H 10:45 | 1.12 [0.96-1.23] | 1.09 [0.96-1.18] |
  | 1H 13:45 | 1.02 [0.93-1.26] | 1.02 [0.91-1.21] |
  | 1H 16:30 | **0.74** [0.65-0.88] | **0.99** [0.88-1.17] |
  | TODAY 09:10 | 0.90 [0.74-0.99] | 0.90 [0.74-0.99] |
  | TODAY 09:45 | 1.06 [0.94-1.18] | 1.06 [0.94-1.18] |

  What is left is where in an hour the variance came (the opening burst inside the 09:00 bar); it
  only decides which stock is picked first.
- **Smaller fixes:**
  - 1H starts from 5-minute index closes after a start or a wake, not a line between hourly closes;
  - 1W uses √(sessions in the window), 4 in Thanksgiving week (z was 11% too small);
  - the screen's 1W start is interpolated between the hourly closes around it, not the hour's
    close (the node's start is to the minute);
  - "what the flow says" counts and ranks like the slot's text (75 screened, not 78; a runner-up in
    slot 1 is "number 2");
  - in the API every screen number sits under `screen` (with `from`: `map` or `hourly bars`), and
    `why` is the map's own text;
  - the default config's weight note is corrected;
  - the calendar helpers are memoised: one full screen (78 stocks, 3 windows) now takes 52-64 ms
    of CPU, against 118-192 ms before, although it does more (measured alternately, same machine).
- **`tools/verify_against_exchange.py`** recomputes the new formulas on its own (and takes `--url`).
  Against a test copy at 16:00 UTC, σ for BTC, ETH, the Nasdaq 100, the S&P 500, gold and oil, in
  all three windows, matched to the fourth decimal, and so did every z and % change.
- **Mutation check:** the four changes the review listed as missed are now caught (1W ignoring the
  calendar, NYSE holidays ignored, the cap not wired into TODAY and 1W, 1H samples not grouped by
  day), as are 12 for this round's changes. Seven others that the review found later (the
  screen's calendar, DST, the 1H live filter, the volume baselines) were caught only by the tests
  of the seventh round.
- **Not fixed** (section 11): the screen's 1H volume ratio is the last whole hour's; the one-off
  cap can miss an event that straddles the 17:00 roll.


**Sixth round (25 Nov 2026): the sleep and outage gap.** The cache held the night of 25 Nov
(00:30-06:00 UTC) as flat prices flagged live: Bitcoin did not move for 26 five-minute steps. The PC
had woken from sleep with DNS down. Every history download failed, and the one-shot refill never
tried again (it even logged "refilled"). Fixed (section 9):
- a step its feed did not cover is empty, never the old price as live;
- the back-fill retries until it succeeds;
- an old cache's range is downloaded again once.

Live, the before and after are in section 9. The tests cover the wake with DNS failing first, an
outage while awake, the auto-stop while back-fills keep failing, and an old cache. A mutation check
of this fix (10 changes, from carried steps flagged live again to a back-fill that never retries) is
caught by them all.


**Seventh round (25 Nov 2026): a review of the fifth round.**
- **The 1H start after a short gap** (a regression of the fifth round). After a 15-40 minute sleep
  or outage, the 5-minute start prices and the scans since shared a ring of 16 points (about 40
  minutes), so the prices near the hour's start were dropped. The 1H start then stayed on the line
  between two hourly closes for the rest of the session, and every scan downloaded 5-minute prices
  for all 78 stocks again. The ring now keeps the last hour by time, and a failed download backs
  off (5 to 30 minutes). On the review's reproduction (a 15, 20 or 30-minute gap at 09:32 New
  York):

  | | Before | Now |
  |---|---|---|
  | Later scans whose 1H start fell back to the line | 21 | 0 |
  | Scans that downloaded 5-minute prices | 22 | 2: the start, and once an hour later, when the hour's start fell in the gap |

- **Smaller fixes:**
  - a pick whose history fails to download is retried (30 s, doubling to 15 min) instead of
    staying "loading";
  - only the stocks the map shows are ranked on its numbers, and only once their history has no
    gap from a sleep (the first screen after a wake ranked them on pre-sleep prices). With five
    more stocks kept on the map, the screen's re-reads take 15 ms instead of 84 ms;
  - "eased" lets go when the move is fully back;
  - at 17:00 New York the old session's TODAY pick is cleared and the screen runs again at once;
  - a crashed connector's "restarting" shows red, like "reconnecting";
  - the screen's 1W start is now described as what it is (interpolated);
  - the API schema describes `eased` by its rule.
- **Tests.** `tests/test_calendar_rules.py` checks every place a calendar or a clock rule enters
  σ and the volume baselines, on the nodes and on the screen, against values computed
  independently: NYSE trading days, the midpoint rule, DST, the 1H live filter, the TODAY stop at
  the roll, and √(sessions). Other new tests cover:
  - the ring after a gap and with a failing download;
  - `poll()` seeding the ring;
  - each condition of the "eased" rule;
  - the roll, the retry and which stocks are re-read.

  The guard against attributes that hide methods now imports every connector first; run alone, it
  fails on the old Finnhub file.
- **Found while testing this round, in the sixth round's back-fill.** A start from an old-format
  cache showed two flaws:
  - the back-fill re-downloaded every instrument from the earliest hole of any of them: one
    stock's 35-day-old gap meant 35 days of everything, and Binance's half-budget pause twice;
  - a gap at the very first step of the 1-day grid could never count as filled, which set off one
    needless back-fill.

  Now it downloads only the instruments with holes, each from its own first hole, and a gap older
  than the grid holds counts as filled once the data starts at the grid's beginning. On a fresh
  old-format cache: history ready in 27 s, no back-fill needed, and `verify_against_exchange` found
  all 10,078 steps of six series equal to Binance's (the 25 Nov night repaired).
- **Mutation check: 40 of 40 changes are caught.** They include the seven the review found
  surviving, the review's list for the fifth round, the conditions of "eased", this round's seven
  and the two back-fill flaws above.

**Eighth round (26 Nov 2026): a review of the sixth and seventh rounds.** Each finding was checked
again on the review's own reproductions (offline: fake exchanges and a simulated clock), on the code
before this round (fe74248) and now:

| | Before | Now |
|---|---|---|
| **The dollar after a night's sleep over 17:00 New York** (a regression of the seventh round: the back-fill downloaded only what had gaps, and the dollar's six FX legs have no history of their own) | 93 of 93 five-minute steps of the sleep empty; TODAY "warming up" for 19 hours, until the next roll | 2 of 93 empty (ICE's own 10-minute delay); TODAY live from waking, as when everything was downloaded again |
| **A start with the network down for 45 s**, 9 hours after the cache was written | 520 of 539 one-minute steps since the cache stayed empty; TODAY and 1H "warming up" on every node | none empty; every window live at the exchange's prices |
| The same with the network down for 25 minutes and Bitcoin up 3% overnight | for 10 minutes Bitcoin's 1H read +2.45%, z 11.2, flagged live (the real move: +0.08%) | every read equal to the exchange's |
| **A 6-hour sleep** with five stocks loaded | the back-fill asked for 35 days: 288 futures requests (weight 1,440), 328,302 bars, one pause at Binance's half budget | 36 requests (weight 180), 9,060 bars, no pause |
| **One failed back-fill attempt** (DNS down) | the event loop stalled 1.4-1.6 s on the review's reproduction, 0.9 s on a copy of this PC's cache (every cache file written, 35 days of computed series rebuilt) | under 50 ms: nothing came, so nothing is saved or rebuilt. A good attempt writes the cache off the loop and rebuilds each computed series only from its own inputs' gap (35 days of 13 instruments from an old-format cache: 0.27 s instead of 0.93 s) |
| **A borderline stock picked lately** (1H: z −2.41 on the map, −2.65 on hourly bars) | the 1H slot alternated between the stock and empty at every screen, prices unchanged | empty at every screen: ranked on the map's numbers |

- **Smaller fixes:**
  - a gap that appears during a download is fetched by the next attempt (it was given up as "no
    exchange data");
  - a feed that recovers during a download still cuts the back-off short, and a picked stock whose
    download failed is retried when a feed recovers (it waited up to 15 minutes);
  - after a wake, the shown pick is held while its history still has the sleep's gap (it was
    dropped for good);
  - a scan begun before 17:00 New York that ended after it no longer brings back the old session's
    TODAY pick, and a kick a connector gets while busy is kept (the roll's re-screen waited a full
    interval);
  - section 9 matches the code again: the log wording, the 10 s settle, Binance spot's 60 s idle
    limit, which file tests what.
- **Tests: 177 pass.** The new ones cover each finding above. The old sleep test now runs through
  the production path (housekeeping sees the clock jump, the wake starts the back-fill), and
  `test_config_and_flags` reads the defaults only, not this PC's git-ignored settings and key files.
- **Mutation check: 36 of 36 changes are caught.** They include the review's survivors
  (the midpoint rule off the whole hour, the volume baselines, "eased" released by a move the other
  way, the seed back-off, the retry of a dropped pick, the wake and housekeeping entry points) and
  this round's changes.

**Ninth round (26 Nov 2026): a review of the eighth round.** Checked on the review's reproductions
(offline), on the code before this round (ea819fb) and now:

| | Before | Now |
|---|---|---|
| **The cache written in a worker thread while a stock is picked** (in housekeeping since the first version; after each download since the eighth round) | "dictionary changed size during iteration" after 6 of 26 files. In housekeeping nothing caught it: wake detection, back-fills, hole checks, NTP and saves stopped for the rest of the run | the thread writes the list taken on the event loop, one save at a time; housekeeping logs an error and goes on |
| **An ETF's price after a Saturday start** (cache from Thursday at 600, Friday's session downloaded at 612) | 600, dated Thursday: TODAY −1.96% all weekend, until the first live trade | 612, dated Friday 16:00; TODAY 0.00% |
| **A failed download after 30 or 48 hours off**, live prices from 20 or 10 minutes after the start | TODAY +43.6% and 1H +94.3%, read from the 30-48 hour old close | unknown until the data is back |
| **Live, a copy without keys** started from a cache that ended in a stale price carried after its last data | SPY 704.12 (that carried value) instead of its 716.71 close; TODAY −1.76% | SPY 716.71, its session close, dated 16:00; TODAY +0.00% |

- **Also:**
  - the first live price after a start whose download failed wakes the back-fill (it waited out its
    back-off, up to 15 minutes);
  - without a key, an ETF's Yahoo history is carried over the hours its market was shut (a Saturday
    start had no TODAY start price);
  - a downloaded ETF price is its regular session's close, as for live prints.
- **Tests: 195 pass.** The new ones cover each item above, and three changes the suite let through:
  the 1-minute half of the eighth round's time-off fix, the save in a worker thread, and the
  give-up rule for a newer gap of the same instrument.
- **Mutation check: 53 of 53 changes are caught:** the eighth round's 36, the review's three
  and this round's 14.

**Tenth round (26 Nov 2026): a review of the ninth round.** Checked on the review's reproductions
(offline), on the code before this round (a9b7c3e) and now:

| | Before | Now |
|---|---|---|
| **An ETF's TODAY after a weekend, with an Alpaca key** (IEX bars include pre- and post-market prints; an unchanged ETF at Monday 10:00) | −0.50%: the reference was Friday evening's last print, carried over the weekend (1H too, in the first hour). This PC's cache: Monday's reference 693.89 against a 690.68 close | 0.00%: the session's close, carried over those hours. Older caches are rewritten on loading |
| **The review's before/after harness** (23 scenarios, 204 ETF readings against the ideal) | 111 off, most by the 1% its extended-hours prints are off | 25 off, all after a failed download, a sleep or without live ETF data (each the same as before or closer by those 1%). The perpetual's readings are unchanged |
| **A cache saved on a Saturday**, started on Monday with a pre-market bar in the download | TODAY unknown all Monday: the weekend was left empty until Monday's first bar | 0.00% (with no bar at all in the download, still unknown: fixed in the eleventh round) |
| **The 1-minute gap after more than a day off** (Wednesday 2:00 after Monday noon, a Sunday, Monday before the open, a thin ETF's first trade at 9:45) | at a closed minute or before the first trade, never covered by the download: a back-fill fetched all 10 ETFs again and logged "no exchange data … left empty" for each | from the first open minute (none on a Sunday or before the open), covered by the start-up download: no back-fill |
| **Stopping** | 8-13 s on a busy PC in the review's runs (17 s in this round's, right after the PC woke from a long sleep), with the port held: `start.bat` right after an auto-stop found it busy. All of it was the final cache save | the port is freed before the save, which takes a third to a half of the time (the same values) |
| **Live, with keys, from copies of this PC's cache** (Thanksgiving: TODAY since Wednesday 17:00) | SPY TODAY and 1H +0.009%, from a post-market print; right after the start a back-fill fetched all 10 ETFs again, "no exchange data" for each | SPY 0.000%, no back-fill; after the session the cache holds only its 716.735 close. Port free at "stopping", process gone 1.0 s later |

- **Also:** without a key, Yahoo's last bar of a day is the 16:00 closing print; it is kept aside too
  (it made TODAY +0.001% on a closed day).
- **Tests: 211 pass.** The new ones cover each item above and the review's two changes the suite
  let through: the 5-minute half of the keyless carry over shut hours (read on a weekend at 16:45),
  and only the first live price waking the back-fill. They also cover three more changes its
  mutation run let through: the 1-minute half of that carry, housekeeping's save in a worker thread,
  and where the 1-minute gap starts.
- **Mutation check: 76 of 76 changes are caught:** the ninth round's (rewritten where this
  round changed the code), the review's two and its three other survivors, and this round's. Three
  of this round's got through at first and now have tests: a pre- or post-market bar older than the
  grid taken as the close to carry, the carry from the grid's own close, and the 5-minute download
  without where it began. The tests had cached one ETF, so the shared download started 35 days back.

**Eleventh round (26 Nov 2026): a review of the tenth round.** Checked on the review's reproductions
and harnesses (offline, the real `serve()` where it says so), on the code before this round (ce3a174)
and now:

| | Before | Now |
|---|---|---|
| **A download with no bar for any ETF** (a Saturday or Thanksgiving cache, a start at 7:30 or 2:00 before IEX's first print) | every ETF's TODAY unknown all day (10 of 10), HYG and IEF "warming up", the risk meter without its credit input | TODAY within 0.004 percentage points of the oracle for all 10 |
| **A sleep or an outage over an ETF's close** (SPY 600 until 15:30, closed at 606) | price 600 all night and weekend; Monday 10:30 TODAY and 1H +1.0% (true 0%) | price 606 after the back-fill; Monday 0.0% |
| **Stopping with a dashboard tab or an SSE app open** (Ctrl+C) | the save started after 41 s (tab) or 122 s (SSE), the process lingering as long; a second Ctrl+C meanwhile, or Ctrl+Break then Ctrl+C: nothing saved | saved at once (33 of 33 files, also after a second Ctrl+C that comes once the save is done; one during the save still cut it short: fixed in the twelfth round), the process gone after 0.8-1.6 s |
| **Closing the console window** | the process gone within 0.03 s, nothing saved | the cache saved first (0.3-0.9 s on a quiet PC), then Windows ends the process; also through `start.bat` (a save over about 4.5 s is cut short: see the twelfth round) |
| **The first 5 minutes of a session** (an ETF at 9:30-9:35) | TODAY σ 0, z missing ("warming up"); 1W +0.56% (true -0.006%) | σ 0.66%, z shown (at 9:30:05 in the test); 1W -0.025% |
| **A start between the open and a thin ETF's first trade** (9:40, first trade 9:45) | a gap kept for each such ETF, a second download and "no exchange data" for each (10 of 10) | no gap, one download |
| **A save refused while another process reads the file** (a copy started at once, 30 trials) | 2 save errors, 2 temporary files left for good | 0 errors (retried), none left; a killed save's file is removed at the next start |

- **Also:** a format-1 cache is now tested like a format-2 one, and the stop's test checks what the
  stop does (the port bound from inside the save; the save before the server's shutdown, with a tab
  and an SSE app connected), not the order of three lines of its source.
- **Not changed:** an outage that begins before the open (9:20-9:50 in the review's harness) is not
  taken for a gap: until an ETF's first print of the session its steps count as carried, not missing,
  so that session's first minutes keep the previous close and no back-fill runs (as before). A
  1-minute history that begins inside a session's 5-minute step can miss that step's first minutes
  (low, left as it was).
- **Tests: 223 pass.** The new ones cover each item above.
- **Mutation check: 104 of 104 changes are caught:** the tenth round's (rewritten where this
  round changed the code) and this round's. Three of this round's were caught at first only by a
  webhook test that fails under load (the price rewritten in live steps or before the download's
  end, the close handler not waiting): their own tests now catch them.

**Twelfth round (26 Nov 2026): a review of the eleventh round.** Checked on the review's reproductions
and harnesses (offline; the real `serve()` in a console window of its own where it says so), on the
code before this round (10246f7) and now:

| | Before | Now |
|---|---|---|
| **A sleep over an ETF's close that wakes a day or more later** (SPY 600 until 15:30, closed at 606; awake Monday 8:00, Saturday 17:30, or Friday 8:00 after Thanksgiving) | price 606, but every 1-minute step held 600: TODAY and 1H +1.0% while the market was shut (true 0), TODAY +1.0% all through the next session | 0.0% at every read, and after a restart, for these wakes before the next open (a wake during the next session still read +1.0%: fixed in the thirteenth round); in the review's random lifecycles the five multi-day outages went from 900-1,320 wrong 1-minute steps each to 0 (89 in one, the same 89 as before; 90 long-sleep runs: none worse) |
| **The computed dollar after a weekend wake** (FX legs stale since Friday 15:00, ICE 0.3% higher at 17:00, awake Saturday 18:00) | TODAY and 1H -0.30% while FX was shut (true 0) | 0.0% |
| **An answer with no bar for an ETF that did trade** (SPY, a 30-minute outage while awake) | the 30 minutes written as the 9:59 price, flagged live; the hole cleared for good, also in the cache | left unknown (30 of 30), given up as "no exchange data", downloaded again at the next start |
| **A window closed as a stop begins** (during the feeds' stop, after Ctrl+Break or Ctrl+C) | 0 of 33 files saved | 33 of 33 (saved within a second) |
| **A start whose first downloads fail past the first 5-minute roll** (Monday 7:30, network up at 7:33:30 or 7:36) | the weekend left empty (521 steps), TODAY unknown for 10 of 10 ETFs all Monday, the same after a restart, 1W unknown the next Saturday | none empty, TODAY and 1W known; a format-4 cache with an empty weekend: 521 empty steps to 0 on loading |
| **1H in a session's first minute an hour on** (10:30:00-10:31, an ETF that opened 1% up and stayed) | +1.000% (the night's gap) | 0.000% (at 10:30:00 exactly +1.000%: before the first print) |
| **A second Ctrl+C during the final save** | 27 of 33 files (a tab open, the second 0.5 s after the first); 11 of 33 with real key presses during a 2.6 s save; the server's shutdown skipped (the tab got 1006) | 33 of 33, the shutdown complete (1001) |
| **A window closed while the console has a text selection** (Edit > Mark) | nothing saved, the process ended 4.5 s after the close; with the service logging meanwhile, the API did not answer | 33 of 33 saved 0.4-0.7 s after the close; the API answered in 0.1-0.2 s |
| **A window closed during a slow save** (7 s, as on a busy PC) | 19 of 33 files, 13 of them among the 25 the next start reads | 19 of 33, all among those 25; the rest at most 10 minutes old (was 30) |

- **Also:** logging off or shutting Windows down is not covered (Windows does not call the
  handler in this process: `user32.dll` is loaded), now said in the code and in section 9.
  Loading the cache fills empty shut hours in any format (10 full ETF caches: 144 to 164 ms). A
  test that runs `main()` no longer leaves the selector event loop policy set for every later test:
  the webhook test's two deliveries then arrived the other way round whenever it ran after that file
  (put down to load in the last two rounds).
- **Not changed:** a final save cut short by Windows on a busy PC (section 9: the files the next
  start reads go first, and the cache is saved every 10 minutes). A save of only what changed
  would not help: the roll writes every instrument's new step each minute. The computed dollar's
  TODAY after a wake between Friday 17:00 and Saturday 17:00 still compares the legs' stale price
  with ICE's close at the roll (-0.30% for a wake on Saturday at 1:00 in the review's case), as before.
- **Tests: 238 pass.** The new ones cover each item above.
- **Mutation check: 129 of 130 changes are caught:** the eleventh round's (rewritten where this
  round changed the code) and this round's. The one left is equivalent now: without a key, seeding
  Yahoo's 5-minute bars only up to their last one leaves nothing empty, as the first roll after the
  start carries the same close over the same shut hours (with that carry off too, the test catches
  it). Two older ones (an empty answer not seeded, an older cache's empty night not filled) were
  caught at first only by that webhook test: new test cases catch them.

**Thirteenth round (27 Nov 2026): a review of the twelfth round.** Checked on the review's
reproductions and harnesses (offline; the real `python -m` start in a console window of its own where
it says so), on the code before this round (60e7442) and now:

| | Before | Now |
|---|---|---|
| **A wake in a session a day or more after an ETF's close** (SPY 600 until Friday 15:30, closed at 606; awake Monday 10:00, or Friday 10:00 after Thanksgiving; a live trade before the back-fill or not) | TODAY +1.0% all session (true 0), 1H +1.0% until 10:30; the 1-minute grid held 600 at TODAY's start, the 5-minute one 606. Random lifecycle 7401 (an outage from Friday 9:54 to Monday 11:49): 1,001 wrong 1-minute steps | 0.0% at every read, also after a restart the same day; 7401: 1 (a live step, the same before) |
| **UUP's quiet opening** (its first IEX print 50, 40 or 27 minutes after the open; a start at 10:15, 10:05 or 9:50) | 4 Alpaca requests instead of 2; UUP's 1H blank at 11, 1 and 0 reads an hour later | 2 requests; no blank read |
| **An open stretch of 16-19 minutes with no bar** (a start at 9:46-9:49) | the 1-minute grid unknown with a hole, the 5-minute one carried and flagged live | both grids the same |
| **A stop with a text selection in the console window** (the auto-stop, run as `python -m`) | the cache saved (33 of 33), then "Fatal Python error: _enter_buffered_busy" (exit code 0xC0000409) once the selection ended | the cache saved, exit code 0 two seconds later, the selection still held |
| **The first roll in a later minute of a shut 5-minute step** (a start at 15:36) | the 15:35 minute left empty for good | filled |

- **The quiet limit on this PC's cache** (twice the longest stretch of unchanged 5-minute closes in
  35 days of sessions): SPY, QQQ, IWM 15 minutes; GLD, TLT 20; SLV 30; HYG, IEF, USO 50; VIXY 60;
  UUP 260. An empty answer over a longer open stretch is the venue's gap; over a shorter one, no
  trade. (The fourteenth round measures steps without a trade instead.)
- **Not changed:** an ETF whose history shows a new close every 5 minutes and that then prints
  nothing for 20 minutes after the open still gets a hole, one more download and "no exchange
  data" (the review's thin_0950 case, whose thin ETFs traded every minute before: the same on
  60e7442). In the review's long-sleep lifecycles, 12 of 60 still read TODAY wrong, the same on
  60e7442; the two looked at slept from before the run's first session print, so the sessions slept
  through count as carried and are not back-filled (the eleventh round's "Not changed").
- **Tests: 249 pass.** The new ones cover each item above.
- **Mutation check: 141 of 142 changes are caught:** the twelfth round's (rewritten where this
  round changed the code) and this round's. The one left is the twelfth round's equivalent one (the
  carry of Yahoo's 5-minute bars, done by the first roll's carry as well). One older one (an empty
  1-minute grid's carry from the 5-minute grid) got through at first: the shut-hours sync came
  before that carry was taken, which also let an empty grid carry an older close over a download's
  first open minutes. The sync now comes after it, with a test.

**Fourteenth round (27 Nov 2026): a sleep or an outage from before an ETF's first print, the quiet
limit, and the clock.** Checked on engine tests, the review's reproductions (offline: the real engine,
only the network stubbed), its lifecycle harness, the quiet-limit cases on a copy of this PC's cache and
a faked NTP exchange, on the code before this round (ea4dbdb) and now:

| | Before | Now |
|---|---|---|
| **Started on a Saturday, asleep from noon to Monday 10:00** (SPY 600 on Friday, 606 from Monday's open) | Monday's 9:30-10:00 carried at 600, not live, with no gap, never back-filled: 1H +1.0% at 10:30 (true 0) | a gap from 9:30, back-filled: 1H 0.0% |
| **Started on a Monday before the open, asleep to the Friday after Thanksgiving** (606, 612, 618 on the three sessions slept over) | the three sessions carried at 600: TODAY and 1H +3.0% at 10:30 (true 0) | back-filled: 0.0% |
| **Started before the open, the network down 9:20-9:50** | 1H +1.0% at 10:30 (true 0) | 0.0% |
| **The review's seven cases** (a Thanksgiving start asleep over Friday's session, woken on Monday at 10:00 or on Saturday at noon; a Sunday start asleep over Monday's session; asleep, or the network down, from before the open to 11:00; two controls) | 33 wrong reads: TODAY +1.0% all Monday after Thanksgiving and after a restart that evening (true 0); a week on, 1W +1.0% for SPY, +0.6% for HYG and -0.4% for IEF (true 0: the risk meter's credit input); woken on the Saturday, 1W -0.03% (true +0.97%) | none: every session slept over back-filled, also after a restart |
| **Such a sleep with the back-fill failing, then a restart** | the carried steps saved as data: no gap at the next start | saved empty: the next start downloads them |
| **The quiet limit on this PC's cache** (an answer with no bar for an ETF is no trade over an open stretch up to it) | equal closes counted, also those that traded: HYG 50 minutes, VIXY 60, UUP 260; and the app's own "no trade" fills, flagged live: SPY's limit 15, then 20 and 30 minutes after a 14- and a 19-minute empty answer | steps without a trade only: HYG 15, VIXY 30, UUP 210; the fills not live: SPY's stays 15, and its 19-minute empty answer stays unknown and is fetched again |
| **A 30-minute outage from 10:00 with no bar for the ETF in the answer** (25 Nov, each ETF's own history) | HYG, IEF, USO, VIXY, UUP: the 30 minutes filled with the last price, flagged live | HYG, VIXY: unknown, fetched again; IEF, USO, UUP (limits 40, 50, 210 minutes): filled, not live |
| **One NTP server (pool.ntp.org) answering with zero timestamps** (a kiss-of-death), at the shortest round trip | taken: the clock 127 years back, every tick failing (OSError 22) until the next sync | ignored ("kiss-of-death 'RATE'" in the log); the others agree: offset +202 ms |

- **The rules:** once an ETF's feed has reported in a run (any message: the stream's login counts),
  a session step the feed did not cover is a gap, before the ETF's first trade too, and when the
  stream reports in a session again the back-fill's back-off is cut short; with no feed at all (no
  Alpaca key) its steps are carried as before. A step the roll closed during a back-fill's download,
  after the download's end, that is still empty is the next attempt's gap (it was cleared with the
  gap before it: in the lifecycle harness, a minute closed while a wake's download ran, before a
  thin pair's first trade). An ETF's cache of format 4 or older is downloaded again once (format 5).
  The quiet limit is twice the longest stretch of 5-minute session steps without a trade (no traded
  volume, the close unchanged), 15 minutes at least. An NTP reply is used only from a synchronised
  server (stratum 1-15, no alarm), in server mode, answering this request, with its timestamps set,
  a round trip of 0-5 s and a correction under a day; of the replies within half a second of their
  median, the shortest round trip wins; a sync with no usable reply, or replies that disagree,
  leaves the offset in use.
- **Lifecycles** (the review's harness, with a wake whose first tick rolls the time asleep before
  the stream reports again, as the engine does): 150 short lifecycles: 124 the same as before, 26
  better, none worse; 120 long sleeps: 50 the same, 70 better, none worse; 200 mixed (24/7 and ETF):
  185 the same, 15 better, none worse. TODAY or 1H read wrong at the harness's checkpoints in 12 of
  60 long-sleep runs (14 before; the largest error 0.10 points, 30.01 before; one run reads unknown
  after an empty back-fill answer over a long open stretch, as intended) and in 17 of 120 (18
  before). Of the 12 runs flagged in the thirteenth round, the worst read is 0.36 points off (30.01
  before); two read unknown twice, after an empty back-fill answer. Not changed: four long-sleep
  runs read 3-14 points off the same way before and now (ETFs that had traded before the sleep:
  another case).
- **Tests: 264 pass.** The new ones cover each item above.
- **Mutation check: 167 of 168 changes are caught:** the thirteenth round's (rewritten where this
  round changed the code) and this round's. The one left is the twelfth round's equivalent one (the
  carry of Yahoo's 5-minute bars, done by the first roll's carry as well). Two got through at first:
  a gap's recovery on a shut step (a test now covers it), and an older one, an empty night filled on
  loading for older formats only: its test loaded formats 3 and 4, the current one until this round,
  and loads the current one too now.

**Fifteenth round (27 Nov 2026): a review of the fourteenth.** Checked on the review's reproductions
(offline: the real engine, in the wake's case its event loop on a virtual clock; only the network
stubbed), an old-format cache written by the code of two rounds ago, a stub NTP server and the review's
lifecycle harness, on the code before this round (9281ec2) and now:

| | Before | Now |
|---|---|---|
| **A wake with the Alpaca fallback's standby round due before the first tick** (asleep 9:46-11:46 with SPY trading; 9:21-11:21 before its first print; 12:01 to 10:31 the next day) | the whole time asleep covered, no gap: 110, 106 and 290 1-minute steps at the pre-sleep price (flagged live where SPY had traded); 1H +0.43%, +1.00% and +0.61% (true 0), TODAY +0.61% the next morning | gaps, back-filled: every step right, in both orders; each of the two guards alone gives the same |
| **An old ETF cache's first start with its downloads failing** (an Alpaca error for 40 s at 09:04:30; the network down for 6 minutes) | the one-time 35-day download dropped as older than the history; Friday's 42 carried steps kept, the cache saved as format 5; a week later 1W SPY +1.13%, HYG +0.73%, IEF -0.27% (true 0) | downloaded once the network is back: 42 right, 1W 0.00% |
| **The same start stopped 5 minutes in, still down, and started again** | saved as format 5 with nothing pending: never downloaded, 1W as above | the pending gap saved with the cache: downloaded at the next start, 1W 0.00% |
| **The PC clock 30 hours or 2 days behind, three NTP servers agreeing** | every reply rejected: offset 0, all times 30 hours off | +108000 s and +172800 s |
| **A sleep (or an outage) over a close, the back-fill's answer without a bar for SPY** (12:30 to 10:00 the next day; also asleep from before SPY's first print) | the night held the price from before the gap: TODAY +0.96% (true -0.04%), status live, at every read till 16:00; 9 wrong reads in each case | the night unknown: TODAY unknown; 0 wrong reads, and right after a restart as before |
| **A wake at 16:20, after the close, with a full answer** | the 1-minute grid's night at the pre-gap price (1,029 steps): TODAY +0.96% (true -0.04%) at 16:21 | right |

- **The rules:** a poll that only stood by is no successful poll; the first tick after the wall clock
  jumped (over 25 s between the tick loop's rounds, as the watchdog's) counts every feed as heard
  until the tick before it. A gap older than the history starts at its oldest step, and the gaps a
  download has yet to fill are saved with the cache. A correction over a day counts where another
  NTP server agrees (a lone reply over a day off is still ignored). After a session minute an ETF's
  feed missed, the shut steps after it are unknown until a session print or a download that covers
  the gap; the download also fills the ones the roll closed while it ran, and loading a cache no
  longer carries a close across an empty session step.
- **Lifecycles** (the review's engine-order harness, 480 long sleeps, before and now): with its empty
  back-fill answers, the runs off by a point or more went from 8 to 0 (the largest error from 14.34
  to 0.38 points), none worse, 13 better; 45 read unknown more often, 44 of them after an empty
  answer (the close it left unknown, where the stale price read an accidental 0.00 off-hours). With
  every answer complete: 477 the same, 2 better (one of them 1.95 points off before: the wake after
  the close), none worse; one reads unknown twice in the seconds between an off-hours wake and its
  back-fill. In the earlier reviews' harness: 150 short lifecycles 149 the same and 1 better, 120
  long sleeps 101 the same and 19 better, 200 mixed (24/7 and ETF) all the same; none worse, and the
  extra unknown reads come after an empty answer or in the seconds between an off-hours wake and its
  back-fill.
- **Tests: 277 pass.** The new ones cover each item above.
- **Mutation check: 188 of 189 changes are caught:** the fourteenth round's (rewritten where this round
  changed the code) and this round's. The one left is the twelfth round's equivalent one. One of
  this round's got through at first (a session print's making the price current again after a missed
  minute): a test now covers it.

**Sixteenth round (27 Nov 2026): the monthly view, and the items left open.** A fourth window, 1M
(section 3.9), and the open items of the earlier rounds. Checked on the real app (keyless, a data
folder and port of its own: the logoff), the review's reproductions (offline: the real engine, only
the network stubbed), benchmarks and tests, on the code before this round (86bcc62) and now:

| | Before | Now |
|---|---|---|
| **A logoff or a shutdown** (the messages Windows sends, to the real app's windows; then the process ended as Windows does) | no window of its own to be told: nothing saved (the cache as of its last periodic save) | 23 of 23 files saved before `WM_ENDSESSION` returned, 0.13 s after it was sent (0.94 s with every core busy) |
| **The final save** (33 instruments, 12.1 MB, 5 saves each; a closing window gets about 4.5 s) | 1.34-1.40 s; 4.12-4.74 s with every core busy | 0.43-0.53 s; 0.94-1.52 s |
| **The computed dollar after a weekend its FX legs slept through** (stale from Friday 15:00, ICE 0.3% higher at the 17:00 close) | the price 0.30% under ICE's close until Sunday 17:00: TODAY -0.30% after a wake on Saturday at 1:00 (true 0), 1W and 1M 0.30% off after any weekend wake | ICE's close, held: 0.00% |
| **A quiet ETF opening with the start after it** (the review's 5 thin ETFs, which printed every minute before; first print at 10:00 or 10:45; starts at 9:50 on a Monday and on the Friday after Thanksgiving, and at 10:30) | a hole each, 4 requests, "no exchange data" 5 times, 25-65 unknown 1-minute steps since 9:30 each (UUP's 1H chart 25 and 35 of 61 points blank) | no hole, 3 requests (one for the venue's latest trades), none unknown |
| **Refused Alpaca keys** (HTTP 401) | every start-up retry and back-fill asked again (32 failed requests from 8:00 to 12:00 New York in the review's run); the stream's and the fallback's messages could count as the ETFs' coverage | asked once; the ETFs read as without a key (Yahoo history, carried, not covered); the sources panel says why |
| **The PC clock 2 days behind** (the NTP correction +2 days) | Binance's, Hyperliquid's and Yahoo's downloads dropped the last 2 days of closed bars as still open | kept |
| **A start at 10:01:30, a quiet ETF printing at 10:01** | the 1-minute download began at 10:02:30: 10:02 unknown, 10:03-10:06 the 10:00 price (600) | from 10:00: 601 |
| **The Alpaca fallback's status while the stream is up** | blank (cleared after each round) | "standby (stream connected)" |
| **A clock helper given a time out of range** (not a number, before 1970, after 3000) | raised (OSError 22) | the nearest end |
| **The Alpaca stream refused (406: another copy holds the key's one stream), started outside a session, asleep over the open** (the review's runs: before Monday's open; Thanksgiving to Monday; a weekend) | no feed signal (the fifteenth round's standby rule): the slept session carried, not back-filled: 90 + 18, 42 and 120 + 78 steps wrong; after Thanksgiving and the weekend TODAY read HYG +0.73% and +0.6%, bonds +0.63% and +0.5% (true 0), the risk score 99 instead of 0 | back-filled: every step right, TODAY 0.00%, risk 0 |
| **The same through the real fallback loop** (the review's other harness: refused and asleep over Monday's open; over Thanksgiving to Monday) | 60 + 12 wrong steps per ETF, no download after the wake, 9 wrong reads; Friday's 42 steps wrong, TODAY HYG +0.60% and UUP +0.50% (true 0), 15 wrong reads | all right, 0 wrong reads |
| **A trading day on which nothing trades** (an unscheduled closure, IEX silent; the review's run from 9:00) | a back-fill about every 67 s: 135 by noon, with 270 Alpaca requests and 135 full cache saves | none |
| **NTP: two servers that agree 1024 weeks off, beside one or two good ones** | the clock 19.6 years back (-619,315,200 s) with one good server; with two, no offset (they disagree) | the good server's (+0.02 s); 30 hours behind with all four agreeing still corrected |

- **1M** (section 3.9): a year of daily closes per instrument, downloaded once and then extended
  about daily, gives its σ; measured against the real 30-day moves of the past year. A snapshot a
  second costs about 1 ms more.
- **The fifteenth round's three regressions** (the review of 86bcc62): a standby round of the Alpaca
  fallback counts as its feed heard again (the first tick after a wake alone guards the time asleep:
  the review's race simulation found 0 of 593 slept sessions without a gap with it alone), and NTP
  replies within a day win over any that correct by more.
- **Considered, not done:** for the quiet opening, other symbols' bars in the same answer as the
  signal: they cannot tell a thin ETF that did not trade from a liquid one whose bars the venue
  lost (the review's SPY case), so the venue's latest trade is asked instead.
- **Tests: 302 pass** (and 1 skipped on a clean copy: the replay test needs local recordings). The
  new ones cover each item above, and 1M: a 24/7 asset's 30-day
  move and σ against their oracle; an ETF across weekends and Thanksgiving (19.7 sessions); the
  computed dollar (22 weekdays); σ against known daily volatility on random walks (within 3% on
  average, 12% at worst) and a recent listing's fallback; one extraordinary day capped; the daily store
  (complete days only, a year kept, the volume ratio against an outlier month); the download (a year
  once, then the last days about daily, saved and loaded again); the wording; the API and schema
  in all four windows.
- **Mutation check: 67 of 68 changes are caught**, on this round's code only (as asked this round):
  its own (1M, items a to i, the three regressions), the earlier rounds' on lines it changed, and the
  wake guard's (it alone now closes the wake race). Nine got through at first (the stock screen's 1M
  start and σ, 1M's 2-hour steps, the daily closes loaded at the start, a 401 told apart, the dollar's
  close held across open steps, its newest close of either grid, the legs' formula while FX is open,
  a round's text kept after a rate limit): tests now catch each. The one left is equivalent now (what
  a standby round returns: every round counts). One more found a branch nothing reaches (1M's volume
  for an instrument with no 5-minute volume): removed.

---

## 11. Limits: what is real-time and what is not

| Truly real-time (≤ 0.4 s) | Real-time **proxies** | Delayed or slow (labelled) |
|---|---|---|
| Bitcoin, Ether (Binance spot) | Nasdaq 100, S&P 500, the notable stock, gold, silver, oil: **Binance perpetual index prices**, not the cash index or ETF. Weekend prices are perpetual price discovery. | **VIX level: 15 min delayed** (licence) |
| Binance perpetual index and mark prices (1 s) | Bonds outside US hours: **TMF perpetual ÷ 3** (thin, about $2M/day) | **10Y yield: FRED daily close**, 1-2 business days old |
| DXY computed from OANDA FX (~0.3 s) | VIX direction: **UVXY perpetual** | **Stablecoin supply: hourly**; total crypto cap from CoinGecko is 5-10 min old (moved live by Binance prices) |
| TLT/HYG/IEF on IEX during US hours (~0.13 s) | Stablecoin share between CoinGecko polls: moved by a 5-coin basket | Headlines: depends on the publisher (FinancialJuice ~minutes; Finnhub 15-45 min) |
| Alpaca news stream | | Open interest: polled each minute; ETF flows: not available for free (see 4.3) |

More caveats:
- **IEX is one exchange.** Its prices are real-time, but its volume is a sample.
- **Outside US hours** the HYG/IEF credit component drops out of the risk meter.
- **After a restart, the dollar needs a first tick from all six FX pairs.** Usually that takes
  seconds. In the rollover lull just after 17:00 New York it took up to 11 minutes (24 Nov). Until
  then the node shows ICE's last DXY, marked stale, and the Finnhub source lists the silent pairs
  ("no tick yet from …").
- **σ adapts to the recent regime.** After a volatile month the same % move earns a smaller σ.
- **Edges are inference.** Several assets falling while one rises will draw edges from all of
  them. The correlation weighting keeps co-moving pairs out, but it cannot prove causation.
- **Fed dates** come from the Fed's own page. An unscheduled meeting appears only once the Fed
  publishes it.
- **The notable-stock screen is a screen.** For stocks the map does not read yet, it uses hourly
  index bars, which cannot place where in an hour the variance came. On synthetic days with an
  opening burst, the node's σ ran 0.90 to 1.09 times the screen's by time of day (medians; single
  days 0.74 to 1.21, worst just before the open: section 10). A stock is picked on those numbers,
  and from the next screen on (within 5 minutes) it is ranked on the map's own. Its volume is the
  24/7 perpetual's, not the stock exchange's, and the screen's 1H volume ratio is the last whole
  clock hour's, where the node's is the rolling hour's. In 1H a move can fade between two screens,
  and the slot then says so. The reason text always quotes the node's numbers. Outside US hours a
  stock's perpetual trades thinly, and its overnight moves are perpetual price discovery (the
  map's caveat says so). Only large caps with a Binance perpetual (78 of the 138 configured) can
  be picked.
- **The one-off cap sees days, not events.** An earnings reaction that straddles the 17:00 roll
  (results at 16:05, the move running on after 17:00) is split over two days, and neither half
  may be twice any other day. It then raises σ for 20 trading days, as a volatile stretch would.
- **Trading-day σ assumes the default calendars.** An asset added with `enabled = true` gets one
  from its `hours` (US hours → NYSE days, FX hours → weekdays, otherwise every day); set
  `calendar` yourself if that is wrong for it.

---

## 12. Project layout

```
moneyflow/
  cli.py          run / read / keys / check
  engine.py       builds series from the config, runs connectors, seeds history, builds snapshots
  series.py       ring buffers, instruments, DXY, stablecoin share, stitched series
  stats.py        windows, σ estimators, volume, correlation, sparklines
  flow.py         edges, risk composite
  narrative.py    "what the flow says" rules
  fed.py          FOMC calendar parser and countdown
  news.py         RSS/Atom parsing, headline book
  server.py       REST, WebSocket, SSE, webhooks, dashboard
  clock.py        NTP-corrected clock, New York day roll, NYSE/FX/VIX calendars
  keys.py         .env loading (whitelist) and redaction
  history.py      on-disk grid cache
  latency.py      the `check` command
  client.py       a small Python client
  connectors/     binance, alpaca, finnhub, coinbase, hyperliquid, pollers (Cboe, CoinGecko,
                  DefiLlama, FRED, Fed, Forex Factory, Farside, RSS, Yahoo)
  static/         the dashboard (index.html, app.js, style.css)
  schema/         snapshot.schema.json
  default.toml    every option, documented
tests/            pytest suite + data/ (FOMC page excerpt; recorded frames stay local)
tools/            record.py, check_latency.py, measure_resources.py
examples/         client.py, webhook_receiver.py
start.ps1 / start.bat
```

## 13. License

[MIT](LICENSE). The license covers the code and documents in this repository. Market data is not
included: what the program downloads stays under each provider's terms (section 4).
