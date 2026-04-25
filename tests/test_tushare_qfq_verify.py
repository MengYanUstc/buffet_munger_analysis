"""
Tushare 前复权自算验证脚本
对比 pro_bar 返回的前复权数据 vs daily/weekly + adj_factor 自算结果。

运行:
    python test_tushare_qfq_verify.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import tushare as ts
from buffett_analyzer.data_warehouse.fetchers.tushare_fetcher import TushareFetcher


def get_pro_bar(ts_code, freq, start_str, end_str, pro_api):
    """通过 pro_bar 获取前复权数据。"""
    df = ts.pro_bar(
        ts_code=ts_code, adj="qfq", freq=freq,
        start_date=start_str, end_date=end_str,
        api=pro_api,
    )
    if df is not None and not df.empty:
        df = df.sort_values("trade_date").reset_index(drop=True)
    return df


def get_self_qfq(ts_code, freq, start_str, end_str, pro_api):
    """通过自算获取前复权数据。"""
    fetcher = TushareFetcher()
    return fetcher._fetch_a_share_qfq(ts_code, freq, start_str, end_str, pro_api)


def compare(df_pro, df_self, stock_code, period):
    """对比两列数据，输出差异统计。"""
    if df_pro is None or df_pro.empty:
        print(f"  [{stock_code} {period}] pro_bar 无数据，跳过对比")
        return False
    if df_self is None or df_self.empty:
        print(f"  [{stock_code} {period}] 自算无数据，跳过对比")
        return False

    # 按 trade_date merge
    merged = df_pro[["trade_date", "open", "high", "low", "close"]].merge(
        df_self[["trade_date", "open", "high", "low", "close"]],
        on="trade_date",
        suffixes=("_pro", "_self"),
    )

    if merged.empty:
        print(f"  [{stock_code} {period}] 日期交集为空，跳过对比")
        return False

    print(f"\n  [{stock_code} {period}] 对比记录数: {len(merged)}")

    cols = ["open", "high", "low", "close"]
    max_diff = 0.0
    max_diff_info = ""

    for col in cols:
        diff = (merged[f"{col}_pro"] - merged[f"{col}_self"]).abs()
        max_d = diff.max()
        mean_d = diff.mean()
        max_idx = diff.idxmax()
        date = merged.loc[max_idx, "trade_date"]
        pro_val = merged.loc[max_idx, f"{col}_pro"]
        self_val = merged.loc[max_idx, f"{col}_self"]

        print(
            f"    {col:6s}: max_diff={max_d:.4f}  mean_diff={mean_d:.4f}  "
            f"worst_date={date}  pro={pro_val:.4f}  self={self_val:.4f}"
        )

        if max_d > max_diff:
            max_diff = max_d
            max_diff_info = f"{col}@{date}"

    # 判定是否一致（允许 0.01 元以内的浮点误差）
    is_match = max_diff < 0.01
    status = "[PASS] 一致" if is_match else "[FAIL] 存在差异"
    print(f"    总体判定: {status} (最大差异 {max_diff:.4f} @ {max_diff_info})")
    return is_match


def main():
    token_main = "37753cabf093c174adbd7f28a5d06dc3d0fe92bf131c47f1977f1142"
    token_fallback = "2abc8063217458c0122943ef0ce7491c27ea08cf9885b678649efb7e"

    pro_main = ts.pro_api(token=token_main)
    pro_fb = ts.pro_api(token=token_fallback)

    test_cases = [
        ("600519.SH", "D", "20250601", "20260422", "贵州茅台 日K"),
        ("600519.SH", "W", "20230601", "20260422", "贵州茅台 周K"),
        ("000001.SZ", "D", "20250601", "20260422", "平安银行 日K"),
        ("300750.SZ", "D", "20250601", "20260422", "宁德时代 日K"),
    ]

    all_pass = True
    for ts_code, freq, start, end, desc in test_cases:
        print(f"\n{'='*60}")
        print(f"测试: {desc} ({ts_code} {freq})")
        print(f"{'='*60}")

        # 主 Token
        df_pro = get_pro_bar(ts_code, freq, start, end, pro_main)
        df_self = get_self_qfq(ts_code, freq, start, end, pro_main)
        ok_main = compare(df_pro, df_self, ts_code, f"{freq}(main)")

        # 备用 Token
        df_pro_fb = get_pro_bar(ts_code, freq, start, end, pro_fb)
        df_self_fb = get_self_qfq(ts_code, freq, start, end, pro_fb)
        ok_fb = compare(df_pro_fb, df_self_fb, ts_code, f"{freq}(fallback)")

        if not ok_main or not ok_fb:
            all_pass = False

    print(f"\n{'='*60}")
    print("总结")
    print(f"{'='*60}")
    print(f"  所有测试 {'通过' if all_pass else '存在失败'}")


if __name__ == "__main__":
    main()
