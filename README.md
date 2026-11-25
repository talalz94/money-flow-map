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
refresh is fine; the 5 minutes are there for that. Stopping is clean: the feeds stop, the history
cache is saved, the port is freed and the process exits. The log says why ("no dashboard tab or app
connected for 5 minutes: stopping"). Nothing restarts it: there is no scheduled task, startup
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
| **1H / TODAY / 1W** | The window: the last 60 minutes; since **17:00 New York** yesterday (the global trading-day roll, i.e. the US close); the last 7 days. |
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
| TODAY | **Same-clock σ**: RMS, over the previous 20 trading days (30 days for crypto), of the move from that day's 17:00 New York roll to the *same clock time*. At 10:00 New York it compares with 10:00 on previous days, so overnight gaps and intraday volatility patterns are handled for every asset class. In the first hour after the roll, the yardstick is the typical move of that first hour; otherwise right after 17:00 every past window would be shorter than one 5-minute step and σ would read 0. Fallback (fewer than 8 such days, e.g. a recent listing): daily realized variance from 5-minute returns, scaled to the elapsed part of the day. |
| 1W | RMS of the daily (roll-to-roll) moves on the trading days of the last 30 days, × √k, where k is the number of trading sessions in the window: 5 in a normal week, 4 in Thanksgiving week (z would otherwise be 11% too small), 7 for crypto. Sessions cut by the window's ends count by the share of their 24 hours inside. |

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
- **Relative volume:** volume in the window ÷ the median volume of the same window on the previous 20 trading days. For 1W it is the previous 4 weeks.
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
can be a different stock in 1H, TODAY and 1W, and it can be empty.

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
            moves of the last 30 days × √(sessions in the window). A one-off day is capped.
            Read the way the node reads it at this minute: for each of the node's window ends
            (now ± 30 min for 1H), the variance is interpolated between the whole hours either side.
    z     = r / σ
    rvol  = the window's quote volume / the median of the same on previous trading days
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
  15 min, for as long as it is picked, instead of showing "loading" for good.
- **Seen live on 25 Nov 2026 (pre-market, with the first σ estimator):**
  - TODAY: **DELL**, −9.2% since 17:00 New York on 4.1× its usual volume (screen −2.9σ, map −3.3σ),
    after a 12.4% fall in Tuesday's session (Alpaca), an arrow into bonds, the dollar and stablecoins;
  - 1H: **WMT** (−3.0σ for that pre-market hour, 6.7× volume);
  - 1W: none (best: ZS +18% but only +1.4σ, its weekly swings being large).

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
> The service measures this offset itself (SNTP) and corrects all ages.

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
| `GET /api/snapshot?tf=today` | Everything for one window (`1h`, `today`, `1w`), or `tf=all` for all three |
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
  - the move and σ in all three windows, and the typical 1σ move;
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
  - saving the history cache (every 30 min).
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
    - The limit is 45 s for Binance, 2 min for FX and 10 min for IEX while their market is open.
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
  - A **back-fill** downloads the empty steps. After a failure (DNS down after waking, no network)
    it retries after 30 s, doubling to 15 min, and at once when a feed delivers again. It stops only
    when nothing is missing, when the gap is older than the history, or when the exchange has no
    data there either. It runs in the background: it never holds up the auto-stop, and it stops
    with the app. Its state is under `/api/health` (`backfill`).
  - Empty steps in the cache (a past outage) are downloaded at the next start. A cache written
    before this fix (format version 1) can hold a sleep's flat stretch flagged live, so its whole
    range is downloaded again once.
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
  - Tested live: the bonds node stayed **live** through the REST fallback, at a data age of about 20 s.
- **Finnhub also allows one stream per key.** A second copy of this service, or another app using the same key, takes the slot, and Finnhub refuses the first with HTTP 429 (seen live on 24 Nov 2026).
  - Any websocket refused with 429 waits at least 2 minutes before retrying instead of hammering, and its status says why.
  - **Run one copy at a time;** other apps should read this service's API rather than open their own Finnhub or Alpaca streams.
- **Closing the window or Ctrl+C / Ctrl+Break:** a graceful stop that saves the history cache. Tested: exit code 0 in 1.2 s, cache written.
- **History** is saved to `data/cache/` every 30 minutes and on exit.
- **Tests:** `tests/test_reconnect.py` checks:
  - reconnection after server drops, after silence and after a `kick()`;
  - poller back-off;
  - the resume path;
  - that the sleep gap is re-downloaded, not left flat. That bug was caught here: the 1-second tick had already filled the gap with the pre-sleep price;
  - a wake with DNS failing for the first two tries: the gap ends filled with the exchange's prices,
    and no step is left carried as live (it was empty, not live, meanwhile);
  - an outage while awake, back-filled once the feed is back; a recovered feed waking the back-off
    once, not every 5 s; a gap the exchange has no data for given up, not retried forever;
  - a quiet market on a live feed staying live (the heartbeat), and a computed series not live
    while its input was down;
  - an old cache with a flat night flagged live, downloaded again; and (`tests/test_idle.py`) the
    auto-stop firing on time while a back-fill keeps failing.

**Sleep, tested live** (24 Nov 2026, US session). The running service was **frozen for 150 s**
(process suspended, which is what it experiences during sleep), then resumed:

| When | What happened |
|---|---|
| On resume | The log says `wall clock jumped 151s (sleep/hibernate?): resyncing and reconnecting`. The clock is re-synced by NTP and every stream reconnects. |
| 2 s later | Nodes honestly show **STALE**, with data ages of 126-133 s. |
| 20 s later | `history gap since 15:07:21Z refilled`. |
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
Closing the console window uses the same graceful-stop handler as Ctrl+Break, which was tested;
closing the window itself was not.

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
  - **every node, gold and the stock slot included, as the strongest outflow and the strongest inflow, in all three windows**, through the real engine (`tests/test_every_node.py`: 66 forced cases);
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
  builder. The test checks prices, the DXY formula, latency and the schema.
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
tests/            pytest suite + data/ (FOMC page excerpt, recorded frames)
tools/            record.py, check_latency.py, measure_resources.py
examples/         client.py, webhook_receiver.py
start.ps1 / start.bat
```
