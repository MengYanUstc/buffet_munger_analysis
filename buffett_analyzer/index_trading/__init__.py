# -*- coding: utf-8 -*-
"""
指数交易模块（量化交易专用）
与报告流程完全独立。
"""

from .index_collector import IndexCollector, INDEX_CONFIG, INDEX_CODES, INDEX_NAME_MAP
from .bollinger import calculate_bollinger, bollinger_signal, get_latest_bollinger
from .momentum import calculate_momentum, momentum_signal
from .trading_system import get_trade_signal, backtest_strategy, trading_universe

__all__ = [
    "IndexCollector", "INDEX_CONFIG", "INDEX_CODES", "INDEX_NAME_MAP",
    "calculate_bollinger", "bollinger_signal", "get_latest_bollinger",
    "calculate_momentum", "momentum_signal",
    "get_trade_signal", "backtest_strategy", "trading_universe",
]
