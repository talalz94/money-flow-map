"""moneyflow: a real-time, heuristic read of where money is moving across markets.

One lightweight asyncio process: public/free market-data websockets plus a few slow REST pollers,
incremental rolling statistics, a localhost JSON API (REST, WebSocket, SSE, webhooks) and an
optional dashboard. Market data only: nothing here can place an order.
"""

__version__ = "1.0.0"
SCHEMA_ID = "moneyflow.snapshot/1"
