"""
分析器注册表
集中管理所有分析模块，支持 CLI 动态发现与批处理调度。
"""

from typing import Dict, Type, List
from .analyzer_base import AnalyzerBase


class AnalyzerRegistry:
    """分析器注册表，支持模块的动态注册与发现。"""

    _registry: Dict[str, Type[AnalyzerBase]] = {}

    @classmethod
    def register(cls, analyzer_class: Type[AnalyzerBase]) -> Type[AnalyzerBase]:
        """装饰器/函数：将分析器类注册到全局表。"""
        cls._registry[analyzer_class.module_id] = analyzer_class
        return analyzer_class

    @classmethod
    def get(cls, module_id: str) -> Type[AnalyzerBase]:
        """根据 module_id 获取分析器类。"""
        if module_id not in cls._registry:
            raise KeyError(f"未注册的分析模块: {module_id}。已注册: {list(cls._registry.keys())}")
        return cls._registry[module_id]

    @classmethod
    def list_modules(cls) -> List[str]:
        """返回所有已注册的 module_id 列表。"""
        return list(cls._registry.keys())

    @classmethod
    def build(cls, module_id: str, *args, **kwargs) -> AnalyzerBase:
        """实例化指定模块的分析器。"""
        klass = cls.get(module_id)
        return klass(*args, **kwargs)
