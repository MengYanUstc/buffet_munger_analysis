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
├── data_fetcher.py                  # 旧版数据获取（向后兼容）
├── scorer.py                        # 评分规则与计算逻辑
├── quality_analysis.py              # 质量分析主控模块
└── data_warehouse/                  # 新版数据收集与缓存模块
    ├── __init__.py
    ├── database.py                  # SQLite 连接与表结构
    ├── cache_manager.py             # 缓存读写与命中检查
    ├── collector.py                 # 统一数据收集入口
    ├── models.py                    # 数据模型定义
    └── fetchers/
        ├── __init__.py
        ├── akshare_fetcher.py       # A股/港股财务数据获取
        ├── baostock_fetcher.py      # A股估值数据获取
        ├── industry_fetcher.py      # 行业估值对比获取
        └── web_search_fetcher.py    # 联网搜索补缺
├── auto_commit.py                   # 自动 Git 提交脚本
main.py                              # CLI 入口
requirements.txt                     # 依赖
README.md                            # 说明文档
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

## 六、数据收集模块（data_warehouse）

`data_warehouse` 是本项目的核心数据基础设施，采用 **SQLite 本地缓存 + 多源 fallback** 架构：

### 6.1 缓存策略
- **首次请求**：从 akshare（财务）/ baostock（A股估值）/ akshare_hk（港股估值）拉取数据，写入 `data/stock_cache.db`
- **后续请求**：优先读取 SQLite，秒级响应，不再重复调用网络 API
- **自动迁移**：数据库表结构升级时自动 `ALTER TABLE`，无需手动删库

### 6.2 支持的市场
| 市场 | 财务数据 | 估值数据 | 行业估值 | 近7年历史分位 |
|------|----------|----------|----------|---------------|
| A股 | ✅ akshare | ✅ baostock | ⚠️ 接口偶发不稳定 | ✅ baostock |
| 港股 | ✅ akshare | ✅ akshare_hk | ✅ 估算值 | ⚠️ 需手动或搜索补强 |

### 6.3 CLI 用法
```bash
# 增强版收集（含行业估值 enrich + 联网搜索补缺）
python main.py --code 600519 --collect-enhanced

# 手动填补缺失的估值分位
python main.py --code 00700 --fill-valuation pe_percentile_5y=42.5 pb_percentile_5y=18.0
```

### 6.4 估值数据字段扩展
除基础的 `pe_ttm`、`pb`、`ps_ttm` 外，数据库还存储：
- `industry_pe` / `industry_pb` / `industry_ps`：行业平均估值
- `pe_vs_industry` / `pb_vs_industry` / `ps_vs_industry`：个股相对行业溢价/折价
- `data_source`：数据来源溯源（baostock / akshare_hk / manual / web_search）
- `note`：数据备注与审计信息

---

## 七、版本管理与自动提交

本项目已接入 Git 版本控制，远程仓库地址：
https://github.com/MengYanUstc/buffet_munger_analysis.git

### 自动提交脚本 `auto_commit.py`
每次修改结束后运行：
```bash
python auto_commit.py
```

脚本行为：
- 检测 `git diff --stat` 的总修改行数
- 若 **> 100 行**，自动生成 commit message 并执行 `git add → commit → push`
- 若 ≤ 100 行，提示跳过提交

也支持手动覆盖 message：
```bash
python auto_commit.py "fix: 修复港股Capex计算口径"
```

---

## 八、设计说明

- **ROE**：巴菲特最看重的指标，15% 为及格线，25% 以上满分。
- **ROIC**：权重最高（6分），反映真实资本回报，不受杠杆扭曲。
- **增长**：营收与利润 CAGR 反映企业成长性，优先使用扣非净利润剔除一次性损益。
- **稳定性与负债**：脚本输出完整的定量数据（标准差、趋势、行业对比），为后续 AI 判断提供依据。
- **数据年限**：当前财务数据默认收集近 **7 年**年报，估值历史分位也基于近 7 年计算。

---

## 九、待扩展

- [x] 接入 baostock 数据源
- [x] 构建 data_warehouse 缓存模块
- [x] 支持港股数据收集
- [x] 接入 Git 版本管理
- [ ] 增加更多 AI 调整因素的自动识别（如息负债占比、现金流状况）
- [ ] 构建第二个模块：估值分析

---

## 十、开发与协作规范

### 分支策略
- `main`：稳定分支，仅通过 PR 或审核后的提交合并
- `dev`：日常开发分支，功能完成后再合并到 `main`

### 提交信息规范
- `feat:` 新功能
- `fix:` 修复
- `refactor:` 重构
- `docs:` 文档更新
- `deps:` 依赖更新
- `auto:` 自动提交脚本产生的提交

### 自动提交测试记录
本段内容用于验证 `auto_commit.py` 的阈值触发机制。当单轮代码修改超过 100 行时，脚本应自动生成 commit message 并完成推送。
