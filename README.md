# BTC / BTC5M Weekly Liquidity Rhythm

Shareable static dashboard for BTC and Polymarket BTC5M weekday/hour activity.

Data snapshot generated on Studio1:
- Binance BTCUSDT spot/futures hourly public data over a 60-day lookback.
- Binance futures open interest and taker buy/sell ratio public endpoints.
- Polymarket BTC5M local CLOB websocket tape `last_trade_price` events from Studio1 rotated-orderbook-capture.

Caveats:
- Options historical activity is not included in this first cut; futures/open-interest/taker flow are used as derivatives proxies.
- Polymarket BTC5M is local tape, not an official Polymarket historical volume API.
- This is research/visualization only, not a trading signal or live-trading authorization.
