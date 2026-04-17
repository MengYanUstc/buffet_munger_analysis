"""
管理层定性分析主模块
与企业质量分析模块平级，已适配 AnalyzerBase 统一接口。
"""

from typing import Dict, Any

from .data_fetcher import ManagementDataFetcher
from .plugins import CapitalAllocationPlugin, IntegrityPlugin
from ..core import AnalyzerBase, AnalysisReport
from ..quality_scoring import AiScoringEngine
from ..data_warehouse.database import Database


class ManagementAnalyzer(AnalyzerBase):
    module_id = "management"
    module_name = "管理层定性分析"

    def __init__(self, stock_code: str, industry_type: str = "general", source: str = "akshare"):
        self.stock_code = stock_code
        self.industry_type = industry_type
        self.fetcher = ManagementDataFetcher(source=source)

    def run(self) -> AnalysisReport:
        # 1. 获取数据
        data = self.fetcher.fetch_all(self.stock_code)

        # 2. 构建上下文
        context = {
            "stock_code": self.stock_code,
            "industry_type": self.industry_type,
            "roic_trend": data.get("roic_trend"),
            "dividend": data.get("dividend"),
            "pledge": data.get("pledge"),
            "mergers": data.get("mergers"),
            "violations": data.get("violations"),
            "management_holdings": data.get("management_holdings"),
        }

        # 3. 初始化引擎并运行
        plugins = [
            CapitalAllocationPlugin(),
            IntegrityPlugin(),
        ]
        db = Database()
        engine = AiScoringEngine(plugins=plugins, db=db)
        scoring_results = engine.run(context)

        # 4. 汇总输出
        cap_res = scoring_results.get("capital_allocation")
        int_res = scoring_results.get("management_integrity")

        cap_score = cap_res.score if cap_res else 0.0
        int_score = int_res.score if int_res else 0.0
        total_score = round(cap_score + int_score, 2)

        rating = self._rating(total_score)

        # 风险提示
        risks = []
        if int_res and int_res.score is not None and int_res.score <= 1.0:
            risks.append("⚠️ 管理层诚信评分极低，存在重大治理或违规风险")
        if cap_res and cap_res.score is not None and cap_res.score <= 2.0:
            risks.append("⚠️ 资本配置能力评分极低，可能存在资本浪费或并购失败")
        if data.get("pledge", {}).get("pledge_ratio") and float(data["pledge"]["pledge_ratio"]) > 50:
            risks.append("⚠️ 大股东股权质押比例较高，存在控制权不稳定风险")

        dimensions = {
            "capital_allocation": self._format_dimension(cap_res),
            "management_integrity": self._format_dimension(int_res),
        }

        summary = {
            "capital_allocation_score": cap_score,
            "management_integrity_score": int_score,
            "total_score": total_score,
            "max_score": 10.0,
            "rating": rating,
        }

        raw_data = {
            "roic_trend": data.get("roic_trend"),
            "pledge": data.get("pledge"),
            "dividend": data.get("dividend"),
            "mergers": data.get("mergers"),
            "violations": data.get("violations"),
            "management_holdings": data.get("management_holdings"),
            "dimension_scores": {k: v.to_dict() for k, v in scoring_results.items()},
            "rating_reference": {
                "9-10": "卓越（资本配置优秀+诚信无瑕疵）",
                "7.5-8.5": "优秀（资本配置良好+诚信良好）",
                "6-7": "良好（资本配置合格+有小瑕疵）",
                "5-5.5": "中等（优缺点各半）",
                "4-4.5": "较差（问题较多）",
                "<4": "差（严重问题或诚信红线）",
            },
        }

        return AnalysisReport(
            module_id=self.module_id,
            module_name=self.module_name,
            stock_code=self.stock_code,
            total_score=total_score,
            max_score=10.0,
            rating=rating,
            dimensions=dimensions,
            summary=summary,
            risk_warnings=risks,
            raw_data=raw_data,
        )

    @staticmethod
    def _format_dimension(res) -> Dict[str, Any]:
        if res is None:
            return {}
        out = {
            "score": res.score,
            "max_score": res.max_score,
            "reason": res.reason,
        }
        if res.details:
            out.update(res.details)
        return out

    @staticmethod
    def _rating(total: float) -> str:
        if total >= 9:
            return "卓越"
        elif total >= 7.5:
            return "优秀"
        elif total >= 6:
            return "良好"
        elif total >= 5:
            return "中等"
        elif total >= 4:
            return "较差"
        else:
            return "差"
