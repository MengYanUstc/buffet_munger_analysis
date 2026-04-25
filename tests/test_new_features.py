"""
月度价格 + 季度财务数据 功能验证
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buffett_analyzer.data_warehouse.collector import DataCollector


def test_monthly_prices(collector, stock_code):
    print(f"\n{'='*60}")
    print(f"测试月度价格: {stock_code}")
    print(f"{'='*60}")
    result = collector.collect_monthly_prices(stock_code)
    df = result.get("monthly")
    if df is not None and not df.empty:
        print(f"  来源: {result['source']}")
        print(f"  记录数: {len(df)}")
        print(f"  最新: {df['trade_date'].iloc[-1]} close={df['close'].iloc[-1]}")
        print(f"  日期范围: {df['trade_date'].iloc[0]} ~ {df['trade_date'].iloc[-1]}")
    else:
        print(f"  失败: {result['source']}")


def test_quarterly_financials(collector, stock_code):
    print(f"\n{'='*60}")
    print(f"测试季度财务: {stock_code}")
    print(f"{'='*60}")
    result = collector.collect_quarterly_financials(stock_code)
    df = result.get("financial_reports")
    if df is not None and not df.empty:
        print(f"  来源: {result['source']}")
        print(f"  记录数: {len(df)}")
        print(f"  最新季度: {df['report_date'].iloc[-1]}")
        print(f"  日期范围: {df['report_date'].iloc[0]} ~ {df['report_date'].iloc[-1]}")
        # 显示最近4个季度的核心指标
        recent = df.tail(4)
        print(f"\n  最近4个季度:")
        for _, row in recent.iterrows():
            print(
                f"    {row['report_date']}  "
                f"ROE={row.get('roe'):>6.2f}%  "
                f"营收={row.get('revenue'):>12.2f}万  "
                f"净利={row.get('net_profit'):>12.2f}万"
            )
    else:
        print(f"  失败: {result['source']}")


if __name__ == "__main__":
    collector = DataCollector()

    # A股
    test_monthly_prices(collector, "600519")
    test_quarterly_financials(collector, "600519")

    # 港股
    test_monthly_prices(collector, "00700")

    print("\n测试完成")
