"""
4 数据源稳定性对比测试脚本
对比 baostock / akshare / tushare / yahoo 在获取股价数据时的表现。

运行方式:
    python test_datasource_stability.py

环境要求:
    - TUSHARE_TOKEN 环境变量（或脚本内手动设置）
"""

import os
import sys
import time
import json
import datetime
import pandas as pd
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from buffett_analyzer.data_warehouse.fetchers.price_fetcher import PriceFetcher
from buffett_analyzer.data_warehouse.fetchers.tushare_fetcher import TushareFetcher
from buffett_analyzer.data_warehouse.fetchers.yahoo_fetcher import YahooFetcher
from buffett_analyzer.utils import is_hk_stock


@dataclass
class SourceResult:
    source: str
    success: bool
    elapsed_ms: float
    records: int = 0
    fields_present: Dict[str, bool] = field(default_factory=dict)
    error_msg: str = ""
    sample_close: Optional[float] = None  # 最新一条收盘价


@dataclass
class StockTestResult:
    stock_code: str
    period: str
    start_date: str
    end_date: str
    results: List[SourceResult] = field(default_factory=list)


class DataSourceStabilityTester:
    """4 数据源稳定性测试器。"""

    # 测试标的：A股 + 港股
    TEST_STOCKS = [
        "600519",  # 贵州茅台 A股
        "000001",  # 平安银行 A股
        "300750",  # 宁德时代 A股
        "00700",   # 腾讯控股 港股
        "09633",   # 农夫山泉 港股
    ]

    def __init__(self):
        self.price_fetcher = PriceFetcher()  # 内含 baostock + akshare
        self.ts_fetcher = TushareFetcher()
        self.yf_fetcher = YahooFetcher()

    def run(
        self,
        periods: List[str] = None,
        years_map: Dict[str, int] = None,
    ):
        periods = periods or ["daily", "weekly"]
        years_map = years_map or {"daily": 1, "weekly": 3}  # 周K测3年减少等待

        all_results: List[StockTestResult] = []

        for stock in self.TEST_STOCKS:
            for period in periods:
                years = years_map[period]
                end_date = datetime.date.today()
                start_date = end_date - datetime.timedelta(days=int(years * 365 + 30))

                print(f"\n{'=' * 60}")
                print(f"测试: {stock} | {period} | {start_date} ~ {end_date}")

                result = StockTestResult(
                    stock_code=stock,
                    period=period,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                )

                # 1. baostock (仅A股)
                if not is_hk_stock(stock):
                    r = self._test_baostock(stock, period, start_date, end_date)
                    result.results.append(r)
                    self._print_result(r)

                # 2. akshare
                r = self._test_akshare(stock, period, start_date, end_date)
                result.results.append(r)
                self._print_result(r)

                # 3. tushare
                r = self._test_tushare(stock, period, start_date, end_date)
                result.results.append(r)
                self._print_result(r)

                # 4. yahoo
                r = self._test_yahoo(stock, period, years)
                result.results.append(r)
                self._print_result(r)

                all_results.append(result)

        # 汇总报告
        self._print_summary(all_results)
        self._save_report(all_results)

    # ------------------------------------------------------------------
    # 各数据源测试
    # ------------------------------------------------------------------
    def _test_baostock(
        self, stock: str, period: str, start_date: datetime.date, end_date: datetime.date
    ) -> SourceResult:
        t0 = time.perf_counter()
        try:
            df = self.price_fetcher._fetch_baostock(stock, period, start_date, end_date)
            elapsed = (time.perf_counter() - t0) * 1000
            if df.empty:
                return SourceResult("baostock", False, elapsed, error_msg="返回空数据")
            return self._make_result("baostock", df, elapsed)
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            return SourceResult("baostock", False, elapsed, error_msg=str(e))

    def _test_akshare(
        self, stock: str, period: str, start_date: datetime.date, end_date: datetime.date
    ) -> SourceResult:
        t0 = time.perf_counter()
        try:
            df = self.price_fetcher._fetch_akshare(stock, period, start_date, end_date)
            elapsed = (time.perf_counter() - t0) * 1000
            if df.empty:
                return SourceResult("akshare", False, elapsed, error_msg="返回空数据")
            return self._make_result("akshare", df, elapsed)
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            return SourceResult("akshare", False, elapsed, error_msg=str(e))

    def _test_tushare(
        self, stock: str, period: str, start_date: datetime.date, end_date: datetime.date
    ) -> SourceResult:
        t0 = time.perf_counter()
        try:
            df = self.ts_fetcher._fetch(stock, period, start_date, end_date)
            elapsed = (time.perf_counter() - t0) * 1000
            if df.empty:
                return SourceResult("tushare", False, elapsed, error_msg="返回空数据")
            return self._make_result("tushare", df, elapsed)
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            return SourceResult("tushare", False, elapsed, error_msg=str(e))

    def _test_yahoo(self, stock: str, period: str, years: int) -> SourceResult:
        t0 = time.perf_counter()
        try:
            df = self.yf_fetcher._fetch(stock, period, years)
            elapsed = (time.perf_counter() - t0) * 1000
            if df.empty:
                return SourceResult("yahoo", False, elapsed, error_msg="返回空数据")
            return self._make_result("yahoo", df, elapsed)
        except Exception as e:
            elapsed = (time.perf_counter() - t0) * 1000
            return SourceResult("yahoo", False, elapsed, error_msg=str(e))

    # ------------------------------------------------------------------
    # 结果格式化
    # ------------------------------------------------------------------
    @staticmethod
    def _make_result(source: str, df: pd.DataFrame, elapsed: float) -> SourceResult:
        fields = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "amplitude",
            "change_pct",
            "turnover",
        ]
        fields_present = {
            f: f in df.columns and df[f].notna().any() for f in fields
        }
        sample_close = None
        if "close" in df.columns and not df.empty:
            sample_close = float(df["close"].iloc[-1])
        return SourceResult(
            source=source,
            success=True,
            elapsed_ms=round(elapsed, 1),
            records=len(df),
            fields_present=fields_present,
            sample_close=sample_close,
        )

    @staticmethod
    def _print_result(r: SourceResult):
        status = "[OK]" if r.success else "[FAIL]"
        print(
            f"  {status} {r.source:12s} {r.elapsed_ms:8.1f}ms  records={r.records:4d}  "
            f"close={r.sample_close}  {r.error_msg if not r.success else ''}"
        )

    # ------------------------------------------------------------------
    # 汇总与保存
    # ------------------------------------------------------------------
    def _print_summary(self, results: List[StockTestResult]):
        print(f"\n{'=' * 60}")
        print("汇总报告")
        print(f"{'=' * 60}")

        source_stats: Dict[str, dict] = {}
        for tr in results:
            for r in tr.results:
                if r.source not in source_stats:
                    source_stats[r.source] = {
                        "total": 0,
                        "success": 0,
                        "total_ms": 0.0,
                        "total_records": 0,
                    }
                source_stats[r.source]["total"] += 1
                if r.success:
                    source_stats[r.source]["success"] += 1
                    source_stats[r.source]["total_ms"] += r.elapsed_ms
                    source_stats[r.source]["total_records"] += r.records

        print(
            f"{'Source':<12} {'Tests':>6} {'Success':>8} {'Rate':>8} {'AvgMs':>10} {'AvgRec':>8}"
        )
        print("-" * 60)
        for src, st in sorted(source_stats.items()):
            total = st["total"]
            success = st["success"]
            rate = success / total * 100 if total else 0
            avg_ms = st["total_ms"] / success if success else 0
            avg_rec = st["total_records"] / success if success else 0
            print(
                f"{src:<12} {total:>6} {success:>8} {rate:>7.1f}% {avg_ms:>10.1f} {avg_rec:>8.1f}"
            )

        # 字段完整性
        print(f"\n{'=' * 60}")
        print("字段完整性（有值的字段占比）")
        print(f"{'=' * 60}")
        field_stats: Dict[str, Dict[str, int]] = {}
        for tr in results:
            for r in tr.results:
                if not r.success:
                    continue
                if r.source not in field_stats:
                    field_stats[r.source] = {}
                for f, present in r.fields_present.items():
                    field_stats[r.source][f] = field_stats[r.source].get(f, 0) + (
                        1 if present else 0
                    )

        all_fields = ["open", "high", "low", "close", "volume", "amount", "amplitude", "change_pct", "turnover"]
        print(f"{'Source':<12} " + " ".join(f"{f:>8}" for f in all_fields))
        print("-" * 90)
        for src in sorted(field_stats.keys()):
            counts = field_stats[src]
            total = source_stats[src]["success"]
            vals = []
            for f in all_fields:
                c = counts.get(f, 0)
                vals.append(f"{c}/{total}")
            print(f"{src:<12} " + " ".join(f"{v:>8}" for v in vals))

        # 一致性对比
        print(f"\n{'=' * 60}")
        print("收盘价一致性对比（同股票同周期最新一天）")
        print(f"{'=' * 60}")
        for tr in results:
            closes = {
                r.source: r.sample_close
                for r in tr.results
                if r.success and r.sample_close is not None
            }
            if len(closes) >= 2:
                vals = list(closes.values())
                max_diff = max(vals) - min(vals)
                avg = sum(vals) / len(vals)
                pct_diff = (max_diff / avg * 100) if avg else 0
                print(
                    f"{tr.stock_code} {tr.period}: {closes}  "
                    f"max_diff={max_diff:.4f} ({pct_diff:.2f}%)"
                )

    def _save_report(self, results: List[StockTestResult]):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"reports/datasource_stability_{timestamp}.json"
        os.makedirs("reports", exist_ok=True)

        data = []
        for tr in results:
            item = {
                "stock_code": tr.stock_code,
                "period": tr.period,
                "date_range": [tr.start_date, tr.end_date],
                "sources": [asdict(r) for r in tr.results],
            }
            data.append(item)

        # 处理 numpy bool 等不可序列化类型
        def _default_serializer(obj):
            if hasattr(obj, "item"):
                return obj.item()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=_default_serializer)
        print(f"\n详细报告已保存: {filename}")


if __name__ == "__main__":
    tester = DataSourceStabilityTester()
    tester.run()
