# LLM 返回 JSON 结构规范

> **用途**：定义代码期望解析的 JSON Schema，供 LLM 智能体约束输出格式。  
> **说明**：纯结构文档，不含评分标准，仅定义字段、类型和示例。

---

## 全局字段规范

| 字段名 | 类型 | 约束 |
|--------|------|------|
| `stock_code` | string | 必填，如 "600519" |
| `*.score` | number | 必填，0.5 的整数倍，范围 [0, max_score] |
| `*.max_score` | number | 必填，维度满分 |
| `*.reason` | string | 必填，30 字以内，引用具体事实 |
| `total_score` | number | 必填，各维度 score 之和 |
| `max_total` | number | 必填，模块总分 |
| `key_facts` | string[] | 必填，关键事实列表 |
| `risk_warnings` | string[] | 必填，风险警告列表 |

---

## 模块一：护城河（moat）

**总分**：25 分

### JSON Schema

```json
{
  "stock_code": "600519",
  "industry_quality": {
    "score": 4.5,
    "max_score": 5.0,
    "reason": "白酒行业CR3超60%，进入壁垒高，需求稳定"
  },
  "moat_type": {
    "score": 6.5,
    "max_score": 7.0,
    "reason": "品牌护城河极强，茅台品牌无法复制"
  },
  "moat_sustainability": {
    "score": 6.0,
    "max_score": 7.0,
    "reason": "历史超70年，多轮周期考验，趋势加强"
  },
  "pricing_power": {
    "score": 5.5,
    "max_score": 6.0,
    "reason": "近5年多次提价，销量持续增长"
  },
  "qualitative_total": 22.5,
  "qualitative_max": 25.0,
  "key_facts": [
    "白酒行业CR3约60%",
    "茅台品牌溢价显著",
    "2023年提价后销量仍增长"
  ],
  "risk_warnings": [
    "年轻化消费趋势可能削弱白酒需求"
  ]
}
```

### 字段清单

| 路径 | 类型 | 说明 |
|------|------|------|
| `stock_code` | string | 股票代码 |
| `industry_quality.score` | number | 行业质量得分，范围 [0, 5] |
| `industry_quality.max_score` | number | 固定 5.0 |
| `industry_quality.reason` | string | 评分理由 |
| `moat_type.score` | number | 护城河类型得分，范围 [0, 7] |
| `moat_type.max_score` | number | 固定 7.0 |
| `moat_type.reason` | string | 评分理由 |
| `moat_sustainability.score` | number | 可持续性得分，范围 [0, 7] |
| `moat_sustainability.max_score` | number | 固定 7.0 |
| `moat_sustainability.reason` | string | 评分理由 |
| `pricing_power.score` | number | 定价权得分，范围 [0, 6] |
| `pricing_power.max_score` | number | 固定 6.0 |
| `pricing_power.reason` | string | 评分理由 |
| `qualitative_total` | number | 四个维度得分之和 |
| `qualitative_max` | number | 固定 25.0 |
| `key_facts` | string[] | 支撑评分的关键事实 |
| `risk_warnings` | string[] | 识别的主要风险 |

---

## 模块二：商业模式（business_model）

**总分**：15 分

### JSON Schema

```json
{
  "stock_code": "600519",
  "industry_classification": "light",
  "industry_classification_desc": "轻资产模式，品牌驱动",
  "growth_stage": "mature",
  "growth_stage_desc": "成熟期，增长放缓但现金流稳定",
  "business_model_description": "公司以高端白酒为核心产品...",
  "income_stability": {
    "score": 3.5,
    "max_score": 4.0,
    "reason": "客户分散，产品单一但不可替代性强"
  },
  "business_model_quality": {
    "score": 3.5,
    "max_score": 4.0,
    "reason": "赚钱逻辑清晰，品牌壁垒高，现金流极佳"
  },
  "business_model_simplicity": {
    "score": 3.5,
    "max_score": 4.0,
    "reason": "业务简单，卖酒赚钱，外行一眼能看懂"
  },
  "growth_certainty": {
    "score": 2.5,
    "max_score": 3.0,
    "reason": "行业龙头地位稳固，提价能力支撑增长"
  },
  "total_score": 13.0,
  "max_total": 15.0,
  "rating": "优秀",
  "key_facts": [
    "预收账款模式确保收入稳定性",
    "2023年直销占比提升至45%"
  ],
  "risk_warnings": [
    "年轻人白酒消费比例下降趋势"
  ]
}
```

### 字段清单

| 路径 | 类型 | 说明 |
|------|------|------|
| `stock_code` | string | 股票代码 |
| `industry_classification` | string | 枚举：light / medium / heavy |
| `industry_classification_desc` | string | 行业分类说明 |
| `growth_stage` | string | 枚举：startup / growth / mature / decline |
| `growth_stage_desc` | string | 发展阶段说明 |
| `business_model_description` | string | 200-500字商业模式描述 |
| `income_stability.score` | number | 收入稳定性得分，范围 [0, 4] |
| `income_stability.max_score` | number | 固定 4.0 |
| `income_stability.reason` | string | 评分理由 |
| `business_model_quality.score` | number | 商业模式质量得分，范围 [0, 4] |
| `business_model_quality.max_score` | number | 固定 4.0 |
| `business_model_quality.reason` | string | 评分理由 |
| `business_model_simplicity.score` | number | 简单易懂得分，范围 [0, 4] |
| `business_model_simplicity.max_score` | number | 固定 4.0 |
| `business_model_simplicity.reason` | string | 评分理由 |
| `growth_certainty.score` | number | 增长确定性得分，范围 [0, 3] |
| `growth_certainty.max_score` | number | 固定 3.0 |
| `growth_certainty.reason` | string | 评分理由 |
| `total_score` | number | 四个维度得分 + 前置判断综合 |
| `max_total` | number | 固定 15.0 |
| `rating` | string | 综合评级：优秀/良好/中等/较差 |
| `key_facts` | string[] | 支撑评分的关键事实 |
| `risk_warnings` | string[] | 识别的主要风险 |

---

## 模块三：管理层（management）

**总分**：10 分

### JSON Schema

```json
{
  "stock_code": "600519",
  "capital_allocation": {
    "score": 3.5,
    "max_score": 4.0,
    "reason": "ROIC稳定高位，分红率超50%，无重大并购"
  },
  "business_focus": {
    "score": 2.0,
    "max_score": 2.0,
    "reason": "长期坚持白酒主业，无跨界多元化"
  },
  "management_integrity": {
    "score": 4.0,
    "max_score": 4.0,
    "reason": "无违规记录，管理层持股稳定，治理完善"
  },
  "total_score": 9.5,
  "max_total": 10.0,
  "rating": "卓越",
  "key_facts": [
    "近5年分红率稳定在50%以上",
    "无股权质押记录"
  ],
  "risk_warnings": []
}
```

### 字段清单

| 路径 | 类型 | 说明 |
|------|------|------|
| `stock_code` | string | 股票代码 |
| `capital_allocation.score` | number | 资本配置能力得分，范围 [0, 4] |
| `capital_allocation.max_score` | number | 固定 4.0 |
| `capital_allocation.reason` | string | 评分理由 |
| `business_focus.score` | number | 业务专注度得分，范围 [0, 2] |
| `business_focus.max_score` | number | 固定 2.0 |
| `business_focus.reason` | string | 评分理由 |
| `management_integrity.score` | number | 管理层诚信得分，范围 [0, 4] |
| `management_integrity.max_score` | number | 固定 4.0 |
| `management_integrity.reason` | string | 评分理由 |
| `total_score` | number | 三个维度得分之和 |
| `max_total` | number | 固定 10.0 |
| `rating` | string | 综合评级：卓越/优秀/良好/中等/较差/差 |
| `key_facts` | string[] | 支撑评分的关键事实 |
| `risk_warnings` | string[] | 识别的主要风险 |

---
