> **Status: historical requirements brief.** The accepted implementation begins with
> Polygon Options Developer, not Starter. `PolygonDeveloperEngine` supersedes
> `PolygonStarterEngine`, and Polygon Options Advanced remains the quote-backed shadow
> and automated-execution upgrade. Use `OPTION_CHAIN_SCANNER_IMPLEMENTATION_GUIDE.md`
> as the implementation entry point and `OPTION_CHAIN_SCANNER_DESIGN.md` as the
> normative specification. Where this brief differs, the normative specification
> governs.

Role & Objective:
You are an elite Quantitative Software Architect specializing in high-performance option trading infrastructure, computational data pipelines, and scalable broker execution wrappers.

I need you to design and build a highly sophisticated, production-grade Option Chain Processing & Strategy Matrix Scanner system in Python. The architecture must be built cleanly using object-oriented principles (OOP). It should decouple data ingestion, strategy logic, and execution layers so that the system can seamlessly scale from its initial state to its future milestones without requiring a structural rewrite.

System Evolution Phases (Architect for this Lifecycle):
- Phase 1 (Current): Ingest Polygon.io Options Starter Plan ($29/mo) 15-minute delayed REST snapshots. Calculate IV and Gamma locally using a vectorized Newton-Raphson Black-Scholes implementation.
- Phase 2 (Imminent): Integrate a Paper Trading Simulation Engine to run live forward-testing, tracking virtual fills, account balances, and strategy performance matrices.
- Phase 3 (Future): Transition to the Polygon Advanced Plan ($199/mo) for low-latency live tick data and real-time WebSockets, feeding directly into a live automated brokerage API (e.g., Alpaca or Tradier) for algorithmic execution.

Please write a highly modular, abstract, and robust Python program that complies with the following structural layout and forward-looking engine components:

1. ABS-DATA ENGINE & MULTI-PROVIDER LAYER
   - Design an abstract BaseDataEngine class that defines standard methods for `get_spot_price`, `get_option_chain`, and `stream_market_data`.
   - Implement a `PolygonStarterEngine` that overrides these methods to pull 15-minute REST data and run the local localized Black-Scholes computation layer (IV and Gamma).
   - Ensure the architecture makes it trivial to write a future `PolygonAdvancedEngine` that plugs into Polygon's real-time WebSocket tick streams and server-side Greeks without altering the downstream strategy engines.

2. TRIPLE-STAGE PIPELINE FILTERS (Data Reduction Core)
   To prevent memory fragmentation and process degradation when handling huge streaming ticks in Phase 3, prune incoming datasets instantly using these strict logic triggers:
   - Time Horizon Filter: Classify and store contracts strictly into three buckets based on Days to Expiration (DTE): 0-DTE (exactly 0), Weeklys (1 to 14 days), and Monthlys (15 to 45 days). Drop any contract where DTE > 45.
   - Moneyness Window Filter: Calculate a dynamic ±15% price boundary using the current underlying stock spot price. Drop all option contracts whose strike price falls outside this corridor.
   - Institutional Liquidity Floor: Drop any contract displaying a current trading volume below 20 AND an outstanding Open Interest (OI) below 100.

3. STRATEGY ENGINE INTERFACE
   - Implement three decoupled strategy modules that query the clean processed master DataFrame matrix:
     * Module A (0-DTE Gamma Squeeze): Identify Near-The-Money contracts expiring today where daily Volume exceeds Open Interest by a factor of 1.5x or higher, and Gamma is > 0.05.
     * Module B (The Income Wheel Strategy): Isolate out-of-the-money Put options with DTE between 7 and 30 days. Rank and surface the top 3 highest premium-paying contracts sorted by highest IV.
     * Module C (Spread & Range Locators): Group contracts by expiration date and scan for high-volume Open Interest clusters ("OI Walls"). Output clear recommendations for Iron Condors, Butterfly Spreads, and Vertical Credit Spreads.
   - Instead of just printing text to the console, strategies must yield standardized `SignalEvent` objects containing: Timestamp, Underlyer, Strategy Name, Option Ticker(s), Action (Buy/Sell), Target Premium, Stop Loss, and Take Profit targets.

4. COUPLING DISPATCHER & SIMULATION PAPER TRADING ENGINE
   - Build a central `ExecutionManager` that listens for `SignalEvent` objects.
   - For Phase 2 reliability testing, build a comprehensive virtual `PaperExecutionEngine` nested inside this manager. It must:
     * Maintain a localized state of virtual portfolio equity, cash, margin constraints, and open positions.
     * Handle simulated orders (Market, Limit) using the incoming delayed snapshot/mid-price as the entry fill proxy.
     * Calculate and manage live trailing stop-losses, target profit takes, and option expirations automatically.
     * Log all closed trades to track overall metrics: Win Rate, Profit Factor, and Max Drawdown.
   - Ensure the `ExecutionManager` uses a factory pattern so switching from `PaperExecutionEngine` to a live live broker API implementation later only requires changing a configuration flag.

5. PRODUCTION FRAMEWORK FRAMEWORK & CONCURRENCY
   - Deliver the entire code base in production-ready format without any placeholder text or incomplete functions.
   - Utilize standard scientific libraries (`pandas`, `numpy`, `scipy`, and `polygon-api-client`).
   - Use thread-safe data queues (`queue.Queue` or `asyncio`) to pipe data from the Data Engine -> Strategy Engine -> Execution Manager to ensure smooth multithreaded operation when scaling up to real-time tick speeds.


If you design the system correctly on day one, shifting from 15-minute delayed data to live real-time tick data will require almost zero major changes to your core logic.The key is preventing your AI assistants from writing a monolithic, unorganized script. You must force the code into an Event-Driven, Object-Oriented Architecture using the Abstract Base Class (ABC) specifications we mapped out in your requirements draft.Here is the exact structural guide on how this works, why it prevents a total system rewrite, and the minor code adjustments you will eventually make.🔎 Why the Core System Stays the SameYour overall trading pipeline is built out of distinct, separate sections. By keeping them isolated, an upgrade to your data source only impacts the initial step:[1. Data Ingestion Layer] ──(Standard DataFrame)──► [2. Strategy Scanner & Math] ──► [3. Paper/Live Execution]
The Strategy Scanner doesn't care where data comes from: Your Trend Following, Wheel, or Vol > OI code only looks at a standard Pandas DataFrame containing columns like ['timestamp', 'strike', 'close', 'volume']. It does not know—or care—if those numbers came from a delayed snapshot or a real-time live fiber connection.The Calculations stay identical: Your local Black-Scholes engine will compute your Gamma and Implied Volatility using the exact same math formulas regardless of data speed.🛠️ What WILL Change (The Minor Swaps)When you eventually pay for the $199/month Advanced plan, your developer will only need to make two isolated modifications inside the Data Ingestion Layer:

1. Switching the Endpoint FunctionsOn Starter/Developer: Your script uses polling endpoints like client.get_ticker_snapshot() or client.list_aggs(), requesting data over standard web requests (REST HTTP) every few minutes.On Advanced Ticks: You will swap those lines out to initialize Polygon's persistent WebSocket stream module (client.init_websocket_stream()). Instead of your script asking for data, Polygon will actively push raw transaction blocks to your machine multiple times a second.

2. Managing Data Accumulation SpeedThe Adjustment: Delayed snapshots give you clean, pre-packaged historical bars. Live streaming tick feeds give you thousands of raw, chaotic transactions. Your Data Ingestion code will need to include a simple aggregator queue (a "buffer") that collects those raw ticks and bundles them into 1-minute blocks before passing them down to your scanner engine.



1. Advanced Options Scanners You Can Build (Developer Tier)By unlocking the Trades endpoint on the Developer tier ($79/mo), your Python system can move beyond basic charts and scan for institutional mechanics. Because your data is 15 minutes delayed, you cannot use these for microsecond scalping. Instead, you use them to identify macro money flows where institutions are positioning themselves for a move over the next few days or weeks.[Polygon Trades Endpoint] ──► [Filter 1: Trade Size] ──► [Filter 2: Exchange Flags] ──► [Signal Matrix]

🔥 Scanner A: The Institutional Block/Sweep DetectorInstitutions hide their tracks by splitting massive orders across multiple options exchanges simultaneously (called a "Sweep") or executing massive single block prints.What the Scanner Codes For: Scan the raw trades feed for single transactions where the contract size multiplied by the premium price exceeds a large dollar threshold (e.g., individual trades valued over $50,000).The Setup: If a stock sees a sudden burst of 10 different OTM Call sweeps within a 3-minute window, it indicates aggressive, informed institutional positioning.

📊 Scanner B: The Volume-to-Open-Interest (Vol > OI) SqueezerOpen Interest (OI) represents the total number of open option contracts that exist overnight.What the Scanner Codes For: Look for contracts where intraday Trading Volume is at least 3x greater than the total Open Interest.The Setup: This proves that completely new, aggressive positions are being opened at a rapid rate, rather than traders just closing out old positions.

📈 Scanner C: The Volatility Smile Distortion MapperOptions pricing models assume volatility forms a uniform curve ("smile") across strike prices.What the Scanner Codes For: Track the actual execution prices of trades across all strikes on a specific expiration.The Setup: If out-of-the-money puts or calls are trading at an implied premium significantly higher than the rest of the chain, your scanner flags an asymmetric market expectation of a crash or breakout.🔎 

2. Does this actually make money? (The Hard Truth)Yes, it is mathematically possible to make money, but most retail developers fail. Understanding why they fail is key to survival:⚠️ The Alpha Decay ProblemIf you build a simple scanner that buys calls when a big institution buys calls, you will likely lose money over time. Retail bots often buy right at the peak of the institutional rush. By the time your scanner detects it, calculates the setup, and accounts for your 15-minute data delay, the premium has already expanded. You are buying "overpriced" options.⚠️ The Market Maker AdvantageOptions are a zero-sum game. On the other side of every single trade your bot takes is an institutional Market Maker (like Citadel or Virtu). They have multi-million dollar direct-fiber connections to the exchanges and real-time computing grids. Your scanner cannot out-speed them. Your edge must come from patience, discipline, and structural strategy, not speed.

💡 3. Building a Solid Scanning & Signal Generator MechanismTo build a mechanism that successfully enters and exits trades, your code must treat options as risk-management units rather than lottery tickets.A reliable entry and exit blueprint follows this systematic logic framework:[API Scan Trigger] ──► [1. Context Filter] ──► [2. Entry Signal] ──► [3. Trailing Exit Management]

🛡️ Step 1: The Macro Context Filter (The "When NOT to Trade" Layer)Before checking options data, your code must verify the underlying stock environment.The Rule: Never let your scanner enter a trade if an unquantifiable volatility event is near. Your script should pull a financial calendar API and block entries if the stock is within 3 days of an Earnings Release or a Federal Reserve Interest Rate Announcement. This eliminates random volatility crushes (IV crush).

🛒 Step 2: The Multi-Variable Entry SignalA solid signal requires confluence. Never enter a trade based on one scanner metric alone.The Blueprint Code Rule: Trigger an entry signal only if three conditions align simultaneously:Stock Context: The underlying stock is in a confirmed trend (e.g., closing above its 50-period EMA).Option Flow: An institutional sweep (Volume > OI) occurs on an out-of-the-money contract.Pricing Integrity: The option's bid-ask spread is tight (under 5% of the total option value), ensuring you aren't paying a massive premium tax to the market makers just to get in.🚪 

Step 3: The Mechanical Exit Matrix (No Emotions)Your exit engine must run completely independent of your entries, relying on strict mathematical boundaries.Time-Based Decay Exits (Theta Management): If you are running an Income Wheel or selling Credit Spreads, your code should automatically buy back and close the position the moment it hits 50% of maximum potential profit, or when the contract reaches 21 Days to Expiration (DTE). Holding past 21 DTE exposes you to exponential directional risk for very little remaining premium reward.Underlying Technical Trailing Stops: If your trend-following strategy breaks down—for instance, if the underlying stock closes below its 20-period EMA on a 1-hour chart—your script must instantly send a market order to close out the options contract, regardless of whether the option itself is currently green or red.If you'd like, let me know:Whether you want to see the specific Python structural logic for matching Volume to Open Interest (Vol > OI) using the Developer Trades data.If you want a blueprint on how to code a 50% profit-take rule into your virtual paper trading module.I can provide the precise code boundaries to build these mechanics.AI responses may include mistakes. For financial advice, consult a professional. Learn more