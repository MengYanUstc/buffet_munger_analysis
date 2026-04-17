"""
分析器抽象基类
所有业务模块（质量分析、管理层分析、估值分析、竞争优势分析、风险分析等）
均需继承此类，以保证 CLI 与批处理流程的统一调度。
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class AnalysisReport:
    """标准化分析报告结构，便于多模块结果合并。"""

    def __init__(
        self,
        module_id: str,
        module_name: str,
        stock_code: str,
        stock_name: str = "",
        total_score: float = 0.0,
        max_score: float = 0.0,
        rating: str = "",
        dimensions: Dict[str, Any] = None,
        summary: Dict[str, Any] = None,
        risk_warnings: list = None,
        raw_data: Dict[str, Any] = None,
    ):
        self.module_id = module_id
        self.module_name = module_name
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.total_score = total_score
        self.max_score = max_score
        self.rating = rating
        self.dimensions = dimensions or {}
        self.summary = summary or {}
        self.risk_warnings = risk_warnings or []
        self.raw_data = raw_data or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "module_id": self.module_id,
            "module_name": self.module_name,
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "total_score": self.total_score,
            "max_score": self.max_score,
            "rating": self.rating,
            "dimensions": self.dimensions,
            "summary": self.summary,
            "risk_warnings": self.risk_warnings,
            "raw_data": self.raw_data,
        }


class AnalyzerBase(ABC):
    """
    分析器基类。

    子类必须实现：
      - module_id: 唯一标识符（如 "quality", "management"）
      - module_name: 人类可读名称
      - run() -> AnalysisReport: 执行分析并返回标准化报告
    """

    @property
    @abstractmethod
    def module_id(self) -> str:
        """模块唯一标识，用于 CLI 注册与缓存 key。"""
        ...

    @property
    @abstractmethod
    def module_name(self) -> str:
        """模块中文名称。"""
        ...

    @abstractmethod
    def run(self) -> AnalysisReport:
        """执行完整分析流程，返回标准化报告。"""
        ...
