# 企业质量分析评分规则（Module 1）

**总分：20 分**

| 维度 | 类型 | 分值 | 数据来源 |
|------|------|------|----------|
| ROE | 定量 | 4 | SQLite `financial_reports.roe` |
| ROIC | 定量 | 6 | SQLite `financial_reports.roic` |
| 营收增长 | 定量 | 3 | SQLite `financial_reports.revenue` |
| 利润增长 | 定量 | 3 | SQLite `financial_reports.net_profit` |
| ROE 稳定性 | AI_BASED | 2 | SQLite `financial_reports.roe` + LLM 微调 |
| 资产负债率 | AI_BASED | 2 | SQLite `financial_reports.debt_ratio` + LLM 微调 |

---

## 1. ROE 评分（4 分）

近5年平均 ROE（加权平均法）：

| 平均 ROE | 得分 |
|----------|------|
| >= 25% | 4.0 |
| >= 20% | 3.5 |
| >= 15% | 3.0 |
| >= 12% | 2.0 |
| >= 8% | 1.0 |
| >= 5% | 0.5 |
| < 5% | 0.0 |

**数据流**：`SQLite financial_reports -> 取 roe 列最近5年 -> np.mean() -> score_roe()`

---

## 2. ROIC 评分（6 分）

近5年平均 ROIC：

| 平均 ROIC | 得分 |
|-----------|------|
| >= 20% | 6.0 |
| >= 15% | 5.0 |
| >= 12% | 4.0 |
| >= 8% | 3.0 |
| >= 5% | 1.5 |
| < 5% | 0.0 |

**数据流**：`SQLite financial_reports -> 取 roic 列最近5年 -> np.mean() -> score_roic()`

---

## 3. 营收增长评分（3 分）

近5年营收 CAGR：

| CAGR | 得分 |
|------|------|
| >= 20% | 3.0 |
| >= 15% | 2.5 |
| >= 8% | 2.0 |
| >= 3% | 1.0 |
| < 3% | 0.0 |

**计算公式**：`CAGR = (末年营收 / 首年营收)^(1/(n-1)) - 1`，n 为数据年数

**数据流**：`SQLite financial_reports -> 取 revenue 列最近5年 -> calculate_cagr() -> score_growth()`

---

## 4. 利润增长评分（3 分）

近5年净利润 CAGR，优先 `parent_net_profit`，其次 `deduct_net_profit`：

评分阈值同营收增长。

**数据流**：`SQLite financial_reports -> 取 parent_net_profit/deduct_net_profit -> calculate_cagr() -> score_growth()`

---

## 5. ROE 稳定性（2 分）—— AI_BASED

### 5.1 定量基础分（代码计算）

基于近4年 ROE 数据的标准差 sigma 和趋势：

| sigma 范围 | 稳定性 | 基础分 |
|------------|--------|--------|
| <= 3 | 高度稳定 | 2.0 |
| <= 5 | 比较稳定 | 1.5 |
| <= 7 | 一般稳定 | 1.0 |
| <= 9 | 较不稳定 | 0.5 |
| > 9 | 很不稳定 | 0.0 |

**趋势惩罚**：
- 后2年均值 vs 前2年均值
- 明显上升：+0.5
- 温和上升 / 基本稳定：0
- 温和下降：-0.5
- 明显下降：-1.0

**penalty_score** = max(0, min(2, 基础分 + 趋势惩罚))

### 5.2 AI 微调

LLM 在 `penalty_score` 基础上做 [-0.5, +0.5] 微调（步长 0.5）。

微调依据：行业特性、周期影响、ROE 质量（是否由杠杆而非盈利驱动）等。

**数据流**：`SQLite -> analyze_roe_stability() -> AiScoringEngine -> LLM 微调`

---

## 6. 资产负债率（2 分）—— AI_BASED

### 6.1 定量基础分（代码计算）

按行业类型区分阈值：

| 行业类型 | 低负债阈值 | 中等阈值 | 较高阈值 | 过高阈值 |
|----------|-----------|----------|----------|----------|
| general（默认） | <=30% -> 2.0 | <=50% -> 1.5 | <=70% -> 1.0 | >70% -> 0.0 |
| banking | <=85% -> 2.0 | <=90% -> 1.5 | <=93% -> 1.0 | >93% -> 0.0 |
| insurance | <=80% -> 2.0 | <=85% -> 1.5 | <=90% -> 1.0 | >90% -> 0.0 |
| real_estate | <=60% -> 2.0 | <=70% -> 1.5 | <=80% -> 1.0 | >80% -> 0.0 |
| utilities | <=50% -> 2.0 | <=60% -> 1.5 | <=70% -> 1.0 | >70% -> 0.0 |

### 6.2 AI 微调

LLM 在基础分基础上做 [-0.5, +0.5] 微调。

微调依据：有息负债占比、现金流覆盖能力、行业对比等。

**数据流**：`SQLite -> analyze_debt_ratio() -> AiScoringEngine -> LLM 微调`

---

## 评级标准

| 总分 | 评级 |
|------|------|
| >= 17 | 顶级公司 |
| >= 14 | 优秀公司 |
| >= 11 | 中等 |
| >= 8 | 一般 |
| < 8 | 较差 |
