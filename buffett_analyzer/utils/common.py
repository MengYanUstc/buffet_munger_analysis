"""
公共工具函数与常量
"""
from typing import Dict, List, Tuple, Union

# 资产负债率行业阈值: [(上限, 建议分, 描述), ...]
DEBT_RATIO_THRESHOLDS: Dict[str, List[Tuple[Union[int, float], float, str]]] = {
    "general":     [(30, 2.0, "低"), (50, 1.5, "中等"), (70, 1.0, "较高"), (float("inf"), 0.0, "过高")],
    "banking":     [(85, 2.0, "低"), (90, 1.5, "中等"), (93, 1.0, "较高"), (float("inf"), 0.0, "过高")],
    "insurance":   [(80, 2.0, "低"), (85, 1.5, "中等"), (90, 1.0, "较高"), (float("inf"), 0.0, "过高")],
    "real_estate": [(60, 2.0, "低"), (70, 1.5, "中等"), (80, 1.0, "较高"), (float("inf"), 0.0, "过高")],
    "utilities":   [(50, 2.0, "低"), (60, 1.5, "中等"), (70, 1.0, "较高"), (float("inf"), 0.0, "过高")],
}


def is_hk_stock(stock_code: str) -> bool:
    """判断是否为港股代码（5位数字且以0开头，如00700, 09633）。"""
    return (
        isinstance(stock_code, str)
        and len(stock_code) == 5
        and stock_code.isdigit()
        and stock_code.startswith("0")
    )
