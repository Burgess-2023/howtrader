"""
Event type string used in the trading platform.
"""

from howtrader.event import EVENT_TIMER  # noqa

EVENT_TICK = "eTick."
EVENT_TRADE = "eTrade."
EVENT_ORDER = "eOrder."
EVENT_POSITION = "ePosition."
EVENT_ORIGINAL_KLINE = "eOriginalKline"
EVENT_ORDERBOOK = "eOrderbook"
EVENT_ACCOUNT = "eAccount."
EVENT_QUOTE = "eQuote."
EVENT_CONTRACT = "eContract."
EVENT_LOG = "eLog"
EVENT_TV_SIGNAL = "eTVSignal"
EVENT_TV_LOG = "eTVLog"
EVENT_TV_STRATEGY = "eTVStrategy"
EVENT_FUNDING_RATE_LOG = "eFundingRateLog"
EVENT_FUNDING_RATE_STRATEGY = "eFundingRateStrategy"
EVENT_FUNDING_RATE_DATA = "eFundingRateData"
