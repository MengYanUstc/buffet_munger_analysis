"""
混合数据源 PriceFetcher 验证脚本
A股: baostock -> akshare
港股: tushare -> akshare

运行:
    python test_hybrid_fetcher.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from buffett_analyzer.data_warehouse.fetchers.price_fetcher import PriceFetcher


def test_stock(pf, stock_code, name):
    print(f"\n{'='*60}")
    print(f"测试: {stock_code} ({name})")
    print(f"{'='*60}")

    # 日K
    pf.last_source = None
    df_d = pf.fetch_daily(stock_code, years=1)
    src_d = pf.last_source or "failed"
    if not df_d.empty:
        print(
            f"  日K: source={src_d:10s} records={len(df_d):4d}  "
            f"latest_close={df_d['close'].iloc[-1]:>10.2f}"
        )
    else:
        print(f"  日K: source={src_d:10s} 获取失败")

    # 周K
    pf.last_source = None
    df_w = pf.fetch_weekly(stock_code, years=3)
    src_w = pf.last_source or "failed"
    if not df_w.empty:
        print(
            f"  周K: source={src_w:10s} records={len(df_w):4d}  "
            f"latest_close={df_w['close'].iloc[-1]:>10.2f}"
        )
    else:
        print(f"  周K: source={src_w:10s} 获取失败")

    return src_d, src_w


if __name__ == "__main__":
    os.environ.setdefault(
        "TUSHARE_TOKEN",
        "37753cabf093c174adbd7f28a5d06dc3d0fe92bf131c47f1977f1142",
    )
    pf = PriceFetcher()

    results = {}

    # A股 × 2
    results["600519"] = test_stock(pf, "600519", "贵州茅台")
    results["000001"] = test_stock(pf, "000001", "平安银行")

    # 港股 × 2（tushare hk_daily 限频 2次/分钟，加间隔）
    results["00700"] = test_stock(pf, "00700", "腾讯控股")
    print("\n  [等待 35 秒，规避 tushare 港股限频...]")
    time.sleep(35)
    results["09633"] = test_stock(pf, "09633", "农夫山泉")

    # 验证结论
    print(f"\n{'='*60}")
    print("验证结论")
    print(f"{'='*60}")

    a_pass = all(
        src_d == "baostock" and src_w == "baostock"
        for code, (src_d, src_w) in results.items()
        if code in ("600519", "000001")
    )
    hk_pass = all(
        src_d == "tushare" and src_w == "tushare"
        for code, (src_d, src_w) in results.items()
        if code in ("00700", "09633")
    )

    print(f"  A股策略 (baostock -> akshare): {'PASS' if a_pass else 'FAIL'}")
    print(f"  港股策略 (tushare -> akshare):  {'PASS' if hk_pass else 'FAIL'}")

    for code, (src_d, src_w) in results.items():
        print(f"    {code}: 日K={src_d}, 周K={src_w}")
