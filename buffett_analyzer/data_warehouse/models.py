"""
数据模型定义
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class FinancialReport:
    stock_code: str
    report_date: str  # YYYY-12-31
    roe: Optional[float] = None
    roic: Optional[float] = None
    revenue: Optional[float] = None
    net_profit: Optional[float] = None
    deduct_net_profit: Optional[float] = None
    parent_net_profit: Optional[float] = None
    gross_margin: Optional[float] = None
    net_margin: Optional[float] = None
    debt_ratio: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    fcf: Optional[float] = None
    capex: Optional[float] = None
    updated_at: Optional[str] = None


@dataclass
class ValuationMetric:
    stock_code: str
    trade_date: str  # YYYY-MM-DD
    close_price: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    ps_ttm: Optional[float] = None
    pe_percentile_5y: Optional[float] = None
    pb_percentile_5y: Optional[float] = None
    ps_percentile_5y: Optional[float] = None
    updated_at: Optional[str] = None
