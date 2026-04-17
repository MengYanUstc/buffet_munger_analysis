# 巴菲特-芒格企业质量分析模块

本项目为 **巴菲特-芒格投资分析框架** 的第一个模块：**企业质量分析**。通过对 A 股上市公司近 5 年财务数据的定量分析，输出结构化评分，并为 AI 定性判断提供充分的数据参考。

---

## 一、评分结构（满分 20 分）

| 维度 | 满分 | 计算方式 | 数据来源 |
|------|------|----------|----------|
| ROE | 4分 | 脚本计算 | 近5年平均 ROE |
| ROE稳定性 | 2分 | 脚本输出建议分 + AI定性判断 | 近5年 ROE 波动与趋势 |
| ROIC | 6分 | 脚本计算 | 近5年平均 ROIC |
| 营收增长 | 3分 | 脚本计算 | 近5年营业总收入 CAGR |
| 利润增长 | 3分 | 脚本计算 | 近5年扣非净利润 CAGR（优先）/ 归母净利润 |
| 资产负债率 | 2分 | 脚本输出建议分 + AI定性判断 | 最新年度资产负债率 |

**全局规则**：
- 最小步长：0.5 分
- 最低得分：0 分
- 最高得分：不超过子模块满分

---

## 二、快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行分析

分析单只股票（默认一般行业）：

```bash
python main.py --code 600519
```

指定行业（影响资产负债率评分基准）：

```bash
python main.py --code 000001 --industry banking
```

保存结果到 JSON：

```bash
python main.py --code 600519 --output result_600519.json
```

---

## 三、项目结构

```
buffett_analyzer/
├── __init__.py
├── data_fetcher.py      # 数据获取（akshare）
├── scorer.py            # 评分规则与计算逻辑
└── quality_analysis.py  # 分析主控模块
main.py                  # CLI 入口
requirements.txt         # 依赖
README.md                # 说明文档
```

---

## 四、输出示例

运行后会输出如下 JSON 结构：

```json
{
  "stock_code": "600519",
  "data_years": 5,
  "roe": {
    "avg_roe": 29.5,
    "yearly_values": [..., ...],
    "score": 4.0,
    "max_score": 4.0
  },
  "roe_stability": {
    "roe_std": 1.8,
    "stability_level": "高度稳定",
    "trend_direction": "基本稳定",
    "suggested_base_score": 2.0,
    "ai_adjustment_range": "1.0 - 2.0 分"
  },
  "roic": { ... },
  "revenue_growth": { ... },
  "profit_growth": { ... },
  "debt_ratio": { ... },
  "scoring_summary": {
    "script_calculated_score": 16.0,
    "ai_qualitative_pending": 4.0,
    "current_total": 16.0,
    "max_possible_total": 20.0,
    "full_score": 20.0
  }
}
```

---

## 五、设计说明

- **ROE**：巴菲特最看重的指标，15% 为及格线，25% 以上满分。
- **ROIC**：权重最高（6分），反映真实资本回报，不受杠杆扭曲。
- **增长**：营收与利润 CAGR 反映企业成长性，优先使用扣非净利润剔除一次性损益。
- **稳定性与负债**：脚本输出完整的定量数据（标准差、趋势、行业对比），为后续 AI 判断提供依据。

---

## 六、待扩展

- [ ] 接入 baostock 数据源
- [ ] 增加更多 AI 调整因素的自动识别（如息负债占比、现金流状况）
- [ ] 构建第二个模块：估值分析
