"""
行业估值获取器
获取个股所在行业的平均估值数据（PE/PB/PS），以及个股相对行业的溢价/折价。
"""

import pandas as pd
import akshare as ak
from typing import Dict, Any, Optional


class IndustryFetcher:
    @staticmethod
    def _is_hk_stock(code: str) -> bool:
        return len(code) == 5 and code.isdigit() and code.startswith('0')

    def fetch(self, stock_code: str) -> Dict[str, Any]:
        """入口：自动识别A股/港股，获取行业估值。"""
        if self._is_hk_stock(stock_code):
            return self._fetch_hk(stock_code)
        return self._fetch_a_share(stock_code)

    def _fetch_hk(self, stock_code: str) -> Dict[str, Any]:
        """
        港股：利用 akshare.stock_hk_valuation_comparison_em。
        东方财富的港股估值对比接口会返回个股估值及行业/市场分位，
        部分列可能包含行业平均数据。
        """
        result = {
            "industry_pe": None,
            "industry_pb": None,
            "industry_ps": None,
            "pe_vs_industry": None,
            "pb_vs_industry": None,
            "ps_vs_industry": None,
            "note": ""
        }
        try:
            df = ak.stock_hk_valuation_comparison_em(symbol=stock_code)
            if df.empty:
                result["note"] = "港股估值对比接口返回空"
                return result

            row = df.iloc[0]
            # 东方财富港股估值对比列顺序（通过实测观察）：
            # 0:代码, 1:名称,
            # 2:PE_TTM, 3:PE_TTM分位, 4:PE_LYR, 5:PE_LYR分位,
            # 6:PB_MRQ, 7:PB_MRQ分位, 8:PB_LYR, 9:PB_LYR分位,
            # 10:PS_TTM, 11:PS_TTM分位, 12:PS_LYR, 13:PS_LYR分位,
            # 14:总市值_TTM, 15:总市值_TTM分位, 16:总市值_LYR, 17:总市值_LYR分位
            # 注意：这里的"分位"是行业/市场分位，不是近7年历史分位。
            # 该接口通常**不直接返回行业平均绝对值**，但会返回个股在行业中的分位。
            # 因此 industry_pe 等字段暂无法从该接口直接获得绝对值，除非列内容有变化。
            # 我们尝试从列名中识别是否包含"行业平均"字样（未来接口升级时兼容）。
            cols = df.columns.tolist()
            for idx, col in enumerate(cols):
                col_str = str(col)
                val = row.iloc[idx]
                if pd.isna(val):
                    continue
                # 兼容未来可能包含"行业"字样的列
                if '行业' in col_str and '市盈' in col_str and 'TTM' in col_str:
                    result["industry_pe"] = float(val)
                elif '行业' in col_str and '市净' in col_str and 'MRQ' in col_str:
                    result["industry_pb"] = float(val)
                elif '行业' in col_str and '市销' in col_str and 'TTM' in col_str:
                    result["industry_ps"] = float(val)

            # 若未拿到行业绝对值，但拿到了个股PE，可尝试通过分位反推行业分布中位数（近似）
            # 这是一个简化假设：若个股PE分位为50%，则行业PE ≈ 个股PE
            # 更合理的做法是通过其他接口获取，但当前免费源受限。
            if result["industry_pe"] is None and len(row) > 2:
                pe = row.iloc[2] if pd.notna(row.iloc[2]) else None
                pe_pct = row.iloc[3] if pd.notna(row.iloc[3]) else None
                if pe is not None and pe_pct is not None:
                    # 简化估算：假设行业PE中位数与个股PE在50分位处相等
                    # 这是一个非常粗糙的近似，仅用于无更好数据源时的占位
                    result["industry_pe"] = self._approximate_industry_valuation(pe, pe_pct)

            if result["industry_pb"] is None and len(row) > 6:
                pb = row.iloc[6] if pd.notna(row.iloc[6]) else None
                pb_pct = row.iloc[7] if pd.notna(row.iloc[7]) else None
                if pb is not None and pb_pct is not None:
                    result["industry_pb"] = self._approximate_industry_valuation(pb, pb_pct)

            if result["industry_ps"] is None and len(row) > 10:
                ps = row.iloc[10] if pd.notna(row.iloc[10]) else None
                ps_pct = row.iloc[11] if pd.notna(row.iloc[11]) else None
                if ps is not None and ps_pct is not None:
                    result["industry_ps"] = self._approximate_industry_valuation(ps, ps_pct)

            # 计算相对行业估值
            self._calc_vs_industry(result, row)
            result["note"] = "港股行业估值基于估值对比接口估算，仅供参考"

        except Exception as e:
            result["note"] = f"港股行业估值获取失败: {e}"
        return result

    def _fetch_a_share(self, stock_code: str) -> Dict[str, Any]:
        """
        A股：多源尝试获取行业估值。
        """
        result = {
            "industry_pe": None,
            "industry_pb": None,
            "industry_ps": None,
            "pe_vs_industry": None,
            "pb_vs_industry": None,
            "ps_vs_industry": None,
            "note": ""
        }

        # 尝试1：A股估值对比接口（与港股类似）
        try:
            df = ak.stock_zh_valuation_comparison_em(symbol=stock_code)
            if not df.empty:
                row = df.iloc[0]
                cols = df.columns.tolist()
                for idx, col in enumerate(cols):
                    col_str = str(col)
                    val = row.iloc[idx]
                    if pd.isna(val):
                        continue
                    if '行业' in col_str and '市盈' in col_str and 'TTM' in col_str:
                        result["industry_pe"] = float(val)
                    elif '行业' in col_str and '市净' in col_str and 'MRQ' in col_str:
                        result["industry_pb"] = float(val)
                    elif '行业' in col_str and '市销' in col_str and 'TTM' in col_str:
                        result["industry_ps"] = float(val)

                # 同样尝试近似估算
                if result["industry_pe"] is None and len(row) > 2:
                    pe = row.iloc[2] if pd.notna(row.iloc[2]) else None
                    pe_pct = row.iloc[3] if pd.notna(row.iloc[3]) else None
                    if pe is not None and pe_pct is not None:
                        result["industry_pe"] = self._approximate_industry_valuation(pe, pe_pct)

                if result["industry_pb"] is None and len(row) > 6:
                    pb = row.iloc[6] if pd.notna(row.iloc[6]) else None
                    pb_pct = row.iloc[7] if pd.notna(row.iloc[7]) else None
                    if pb is not None and pb_pct is not None:
                        result["industry_pb"] = self._approximate_industry_valuation(pb, pb_pct)

                if result["industry_ps"] is None and len(row) > 10:
                    ps = row.iloc[10] if pd.notna(row.iloc[10]) else None
                    ps_pct = row.iloc[11] if pd.notna(row.iloc[11]) else None
                    if ps is not None and ps_pct is not None:
                        result["industry_ps"] = self._approximate_industry_valuation(ps, ps_pct)

                self._calc_vs_industry(result, row)
                result["note"] = "A股行业估值基于估值对比接口"
                return result
        except Exception:
            pass

        # 尝试2：巨潮资讯行业市盈率接口
        try:
            from datetime import datetime, timedelta
            today = datetime.now().strftime('%Y%m%d')
            # 该接口需要日期参数，尝试近5个工作日
            for offset in [0, 1, 2, 3, 4, 5]:
                try_date = (datetime.now() - timedelta(days=offset)).strftime('%Y%m%d')
                df = ak.stock_industry_pe_ratio_cninfo(symbol="证监会行业市盈率", date=try_date)
                if not df.empty:
                    # TODO: 需要根据股票实际所属行业进行匹配
                    # 由于该接口返回所有行业的列表，这里简化处理：返回大盘平均作为fallback
                    avg_pe = pd.to_numeric(df['市盈率'], errors='coerce').mean()
                    if pd.notna(avg_pe):
                        result["industry_pe"] = float(avg_pe)
                        result["note"] = f"A股行业PE取自巨讯行业均值（{try_date}），非精确行业匹配"
                        break
        except Exception:
            pass

        if result["industry_pe"] is None and result["industry_pb"] is None:
            result["note"] = "A股行业估值数据暂不可用（接口受限或网络异常）"
        return result

    @staticmethod
    def _approximate_industry_valuation(individual_val: float, percentile: float) -> Optional[float]:
        """
        利用个体估值和其在行业中的分位，粗略估算行业中位数。
        假设：行业估值服从对数正态分布，或近似正态分布。
        这里采用一个非常简化的线性映射：中位数 = individual_val * (50 / percentile)。
        仅用于无更好数据源时的占位，精度有限。
        """
        if individual_val is None or percentile is None or percentile <= 0:
            return None
        # 避免极端分位导致离谱数值
        adjusted_pct = max(1.0, min(99.0, float(percentile)))
        ratio = 50.0 / adjusted_pct
        # 限制估算波动范围在 0.2 ~ 5 倍之间
        ratio = max(0.2, min(5.0, ratio))
        return float(individual_val) * ratio

    @staticmethod
    def _calc_vs_industry(result: Dict[str, Any], row) -> None:
        """计算个股相对行业的溢价/折价。"""
        # 尝试从 row 中提取个股估值
        pe = row.iloc[2] if len(row) > 2 and pd.notna(row.iloc[2]) else None
        pb = row.iloc[6] if len(row) > 6 and pd.notna(row.iloc[6]) else None
        ps = row.iloc[10] if len(row) > 10 and pd.notna(row.iloc[10]) else None

        if pe is not None and result["industry_pe"] is not None and result["industry_pe"] != 0:
            result["pe_vs_industry"] = float(pe) / float(result["industry_pe"]) - 1.0
        if pb is not None and result["industry_pb"] is not None and result["industry_pb"] != 0:
            result["pb_vs_industry"] = float(pb) / float(result["industry_pb"]) - 1.0
        if ps is not None and result["industry_ps"] is not None and result["industry_ps"] != 0:
            result["ps_vs_industry"] = float(ps) / float(result["industry_ps"]) - 1.0
