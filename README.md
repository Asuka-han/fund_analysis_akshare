# 基金分析项目 README

本项目基于 AKShare 获取多只公募基金与市场指数的日频数据，入库到 SQLite（fund_data.db），并进行绩效分析、持有期模拟与可视化（含静态 PNG 与交互式 HTML）。本文档帮助新同事快速理解并使用本项目，优先以当前项目代码结构与实现为准。

## 目录
- [项目概览](#项目概览)
- [环境准备](#环境准备)
- [快速开始](#快速开始)
- [Docker 使用](#docker-使用)
- [扩展脚本工具](#扩展脚本工具)
  - [1. Excel数据导入工具 (import_excel_to_db.py)](#1-excel数据导入工具-importexceltodbpy)
  - [2. 数据库更新工具 (update_db.py)](#2-数据库更新工具-updatedbpy)
  - [3. 数据库分析工具 (run_analysis_from_db.py)](#3-数据库分析工具-runanalysisfromdbpy)
  - [4. Excel直接分析工具 (analysis_from_excel.py)](#4-excel直接分析工具-analysisfromexcelpy)
- [项目结构](#项目结构)
- [配置说明](#配置说明)
- [数据库结构](#数据库结构)
- [分析与可视化输出](#分析与可视化输出)
- [时间戳目录管理](#时间戳目录管理)
- [绩效指标计算公式与代码位置](#绩效指标计算公式与代码位置)
- [进阶功能与基准线管理](#进阶功能与基准线管理)
- [常见问题](#常见问题)
- [维护建议](#维护建议)
- [许可证](#许可证)
- [故障排除](#故障排除)

## 项目概览
- **核心功能**：
  - 数据获取：基金基本信息、基金净值、指数收盘价（含医药创新组合指数）。
  - 绩效分析：总收益率、年化收益率、年化波动率、最大回撤、夏普比率等。
  - 持有期模拟：N=30/60/90/180/360 天的收益分布、胜率等。
  - 可视化输出：
    - 静态 PNG：净值曲线、回撤、持有期分布、绩效对比。
    - 交互 HTML：净值曲线（含基准线）、净值+回撤、持有期分布。
- **目标用户**：金融分析师、个人投资者、量化研究者
- **解决问题**：
  - 解决手动收集基金数据效率低、易出错的问题
  - 提供统一的分析框架评估基金表现
  ## 环境准备
  ### 环境配置概述
  本项目提供两种依赖管理方式（`conda` 推荐/`pip` 备用），并配套自动化脚本完成环境创建、更新和校验，支持 Windows/macOS/Linux 全平台。

  | 配置文件/脚本 | 作用 |
  |---|---|
  | `environment.yml` | conda 环境配置文件（推荐），定义完整环境（Python 版本 + 所有依赖） |
  | `requirements.txt` | pip 依赖清单（备用），仅包含 Python 包依赖，适配 Docker/PyInstaller 部署 |
  | `scripts/create_env_conda.sh` | Unix/macOS 一键创建/更新 conda 环境脚本 |
  | `scripts/create_env_conda.ps1` | Windows PowerShell 一键创建/更新 conda 环境脚本 |
  | `scripts/check_environment.py` | 全平台环境校验脚本，验证依赖版本和安装状态 |

  ### 选项一：使用 Conda（推荐）
  - **Unix/macOS**: 运行 `bash scripts/create_env_conda.sh`
  - **Windows**: 运行 `\.\scripts\create_env_conda.ps1`
  #### 手动设置
  ```bash
  # 创建并激活 conda 环境
  conda env create -f environment.yml
  conda activate fund_analysis_env
  ```

  ### 选项二：使用 Python venv（无 conda）
  - **Windows**:
  ```bash
  # 创建虚拟环境
  python -m venv .venv
  # 激活虚拟环境
  .venv\Scripts\activate
  # 安装依赖
  pip install -r requirements.txt
  ```
  - **Unix/macOS**:
  ```bash
  # 1. 创建虚拟环境
  python3 -m venv .venv
  # 2. 激活虚拟环境
  source .venv/bin/activate
  # 3. 安装项目依赖
  pip3 install -r requirements.txt
  ```

  ### 环境校验
  运行 `python scripts/check_environment.py`，确保所有依赖包版本符合预期。

  ### Python 解释器选择
  #### 选择 Conda 环境
  1. 打开 VSCode，按下 `Ctrl+Shift+P`（Windows/Linux）或 `Cmd+Shift+P`（Mac）；
  2. 输入「Python: Select Interpreter」并回车；
  3. 在列表中找到 `fund_analysis_env`（conda 环境），格式通常为：
    - Windows: `Python 3.12.0 ('fund_analysis_env': conda)`
    - macOS/Linux: `~/miniconda3/envs/fund_analysis_env/bin/python`
  4. 选择后，VSCode 会自动关联该环境的依赖，保证代码提示/运行一致。
  #### 选择 pip 虚拟环境
  若使用 pip 的 `.venv` 虚拟环境，解释器路径通常为：
  - Windows: `.venv\Scripts\python.exe`
  - macOS/Linux: `.venv/bin/python`

  ## Docker 使用


## Docker 使用
见 [docker/Docker_Guide.md](docker/Docker_Guide.md)。


## 快速开始
1. 编辑配置：[config.py](config.py)
   - 确认基金列表 `FUND_CODES` 与指数配置 `INDICES`。
   - 如需控制是否输出交互 HTML，见下方"配置说明"。
2. 运行主流程：
```bash
python main.py
```
3. 输出位置：
   - 数据库：data/fund_data.db
   - 图表：plots/（PNG 与 HTML）
   - 报告：reports/analysis_report.md、performance_summary.xlsx



### 2. 数据库更新工具 (update_db.py)
**功能**：仅更新数据库数据，不进行分析。支持抓取指定基金和指数的历史数据，支持备份原数据库和指定日期范围。

**常用命令**：
```bash
# 更新指定基金
python scripts/update_db.py --funds 000001.OF 003095.OF

# 更新指定指数
python scripts/update_db.py --indices 000300 000905

# 更新所有配置中的基金和指数
python scripts/update_db.py --all

# 指定日期范围
python scripts/update_db.py --funds 000001.OF --start-date 2021-01-01 --end-date 2023-12-31

# 获取最近N年数据
python scripts/update_db.py --funds 000001.OF --years 5

# 强制替换已有数据（默认追加）
python scripts/update_db.py --funds 000001.OF --force-replace

# 更新前备份数据库
python scripts/update_db.py --funds 000001.OF --backup

# 指定备份文件名
python scripts/update_db.py --funds 000001.OF --backup --backup-name my_backup.db.bak
```

**输出内容与位置**：
- 数据库更新：新增基金和指数数据到 `fund_daily_data` 和 `index_daily_data` 表
- 备份文件：可选，保存在 `data/` 目录下，文件名格式：`fund_data_YYYYMMDD_HHMMSS.db.bak`
- 导入报告：`reports/data_import_report.md`
- 日志文件：`reports/logs/update_db.log`

### 3. 数据库分析工具 (run_analysis_from_db.py)
**功能**：仅从现有数据库进行分析（增强版），使用输出管理器避免文件混乱，为每个基金创建独立目录，支持多种输出格式和组织方式。

**常用命令**：
```bash
# 分析指定基金
python scripts/run_analysis_from_db.py --funds 000001.OF 510300.OF

# 分析指定持有期
python scripts/run_analysis_from_db.py --funds 000001.OF --periods 30 60 90

# 分析所有基金和指数
python scripts/run_analysis_from_db.py --all

# 按基金组织文件（每个基金独立目录）
python scripts/run_analysis_from_db.py --funds 000001.OF --organize-by-fund

# 输出交互式HTML图表
python scripts/run_analysis_from_db.py --funds 000001.OF --output-html true

# 跳过图表生成
python scripts/run_analysis_from_db.py --funds 000001.OF --no-charts

# 跳过持有期分析
python scripts/run_analysis_from_db.py --funds 000001.OF --no-holding

# 清理7天前的旧文件
python scripts/run_analysis_from_db.py --funds 000001.OF --clean-old
```

**输出内容与位置**：
- 绩效分析Excel：`reports/excel_performance/performance_analysis.xlsx`
- 持有期分析Excel：`reports/excel_holding/holding_analysis_{基金代码}.xlsx`
- 静态图表：`reports/plots/static/{基金名}/` 目录下的PNG文件
- 交互式图表：`reports/plots/interactive/{基金名}/` 目录下的HTML文件
- 分析摘要：`reports/分析摘要.md`
- 日志文件：`reports/logs/run_analysis.log`

### 4. Excel直接分析工具 (analysis_from_excel.py)
**功能**：从Excel文件直接进行分析，不写入主数据库（可选）。支持直接读取Excel中的基金数据并生成分析报告和图表。

**常用命令**：
```bash
# 基本用法：分析Excel中的基金数据
python scripts/analysis_from_excel.py --input sample/fund_sample.xlsx

# 指定Sheet名称
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --sheet daily_nav

# 指定基金代码（如果Excel中没有fund_id列）
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --fund-code 000001.OF

# 指定持有期列表
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --periods 30 60 120 240

# 将数据写入主数据库
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --write-db

# 指定输出目录
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --output-dir reports/my_analysis

# 显示详细日志
python scripts/analysis_from_excel.py --input data/fund_data.xlsx --verbose
```

**输出内容与位置**：
- 绩效摘要Excel：`reports/excel_performance/performance_summary.xlsx`
- 持有期分析Excel：`reports/excel_holding/holding_analysis_{基金代码}.xlsx`
- 静态图表：`reports/plots/static/` 目录下的PNG文件
- 交互式图表：`reports/plots/interactive/` 目录下的HTML文件
- 分析报告：`reports/excel_analysis_report.md`
- 日志文件：`reports/logs/excel_analysis.log`

## 项目结构
```
fund_analysis_project/
├── reports/                  # 分析报告、Excel文件、可视化
│   ├── db_analysis/          # 数据库分析报告
│   ├── excel_analysis/       # Excel分析报告
│   ├── import_excel_to_db/   # Excel导入数据库报告
│   ├── main/                 # 主流程分析报告
│   └── update_db/            # 数据库更新报告
├── src/                      # 源代码目录
│   ├── analysis/             # 分析模块
│   │   ├── performance.py    # 绩效分析
│   │   ├── holding_simulation.py  # 持有期模拟
│   │   └── visualization.py  # 可视化
│   ├── data_fetch/           # 数据获取模块
│   └── utils/                # 工具模块
├── scripts/                  # 脚本目录
│   ├── import_excel_to_db.py # Excel数据导入脚本
│   ├── run_analysis_from_db.py # 数据库分析脚本
│   ├── update_db.py         # 数据库更新脚本
│   └──analysis_from_excel.py # Excel直接分析脚本
├── data/                     # 数据目录
│   └── fund_data.db          # SQLite数据库文件
├── config.py                 # 配置文件
├── main.py                   # 主程序入口
├── reset_database.py         # 重置数据库脚本
├── requirements.txt          # 依赖包列表
├── environment.yml          # Conda环境配置文件
└── README.md                 # 项目说明文档
```

## 配置说明
- 添加/删除基金
  - 在 [config.py](config.py) 的 `FUND_CODES` 中维护（使用带 .OF 的显示格式，例如 "000001.OF"）。
  - 代码会自动转换为 AKShare 格式（纯数字），并按显示格式写库与出图。
- 添加/删除指数
  - 在 `INDICES` 中新增或移除映射项：
    - `akshare_code`：AKShare 查询使用的符号（如 "000300"、"HSI"）。
    - `name`：显示名称（用于中文命名和图例）。
    - `color`：推荐色（可选）。
  - 在 `BENCHMARK_IDS` 中维护参与对比的指数 ID 列表（如 `['INDEX_SSE','INDEX_HS300',...]`）。
- 控制交互 HTML 是否输出
  - 在 [config.py](config.py) 中使用以下开关：
    - `OUTPUT_HTML_NAV_CURVE = True`：控制净值曲线（交互）HTML 输出。
    - `OUTPUT_HTML_NAV_DRAWDOWN = True`：控制净值+回撤（交互）HTML 输出。
    - `OUTPUT_HTML_HOLDING_DIST = True`：控制持有期收益率分布（交互）HTML 输出。
  - 设为 `False` 即关闭对应类型 HTML 的写出，PNG 仍会输出。
 - 数据抓取年限
   - 在 [config.py](config.py) 中通过 `DEFAULT_FETCH_YEARS = 5` 控制默认抓取年限（基金与指数）。
   - 如需确保 360 天持有期分析，建议不低于 2 年；本项目默认 5 年，以提高覆盖率。

## 数据库结构
- 表 `funds`：基金基本信息（fund_id、name、type、inception_date、manager）。
- 表 `fund_daily_data`：基金日净值（fund_id、date、nav、cumulative_nav、net_assets）。
- 表 `index_daily_data`：指数日收盘价（index_id、date、close）。
- 主键与日期规范：主键去重，日期统一为 YYYY-MM-DD。

## 分析与可视化输出
- 绩效分析
  - 结果写入数据库与 Excel。
  - 生成绩效对比图：plots/绩效指标对比.png。
- 单基金图表（中文命名）：
  - 净值曲线（PNG）：`{基金名}_净值曲线.png`
  - 净值曲线（交互 HTML）：`{基金名}_净值曲线_交互.html`
  - 净值+回撤（交互 HTML）：`{基金名}_净值回撤_交互.html`
  - 回撤分析（PNG）：`{基金名}_回撤分析.png`
  - 持有期分布（PNG）：`{基金名}_持有期{N}天_收益率分布.png`（N 包含 30/60/90/180/360）
  - 持有期分布（交互 HTML）：`{基金名}_持有期{N}天_交互.html`
- 特殊规则
  - "示例基金"不会输出 PNG/HTML 图（仅用于单元测试）。
  - 若某基金在某个持有期（例如 360 天）数据不足：
    - 仍会生成占位图（PNG/HTML），并标注"数据不足，无法生成收益率分布"。

## 时间戳目录管理
本项目使用时间戳目录来组织输出文件，以避免不同运行结果之间的冲突。所有分析脚本均支持时间戳控制功能，便于区分不同时间的分析结果。

### 时间戳功能介绍
- **默认行为**：所有分析脚本默认会在输出目录下创建带时间戳的子目录，格式为 `YYYYMMDD_HHMMSS`，例如 `20260119_160506`。
- **目录结构**：时间戳目录下的文件组织结构保持一致，便于追踪每次分析的结果。

### 时间戳用法
#### 1. 启用时间戳（默认行为）
所有脚本默认启用时间戳，无需额外参数：
```bash
拥有时间戳的文件列表：
analysis_from_excel.py
import_excel_to_db.py
run_analysis_from_db.py
main.py
update_db.py
```

#### 2. 禁用时间戳
使用 `no-timestamp` 参数或修改`use_timestamp=True/False` 禁用时间戳功能，将直接输出到基础目录：
```bash  use_timestamp=True
# 示例：禁用时间戳进行数据库分析
python scripts/run_analysis_from_db.py --funds 000001.OF --no-timestamp
或将 use_timestamp=True 改为 use_timestamp=False

#### 3. 清理旧文件
使用 `--clean-old` 参数清理指定天数前的旧文件（通常为7天）：
```bash
# 运行分析并清理旧文件
python scripts/run_analysis_from_db.py --funds 000001.OF --clean-old

# 更新数据库并清理旧文件
python scripts/update_db.py --funds 000001.OF --clean-old
```

#### 4. 输出位置示例
- **启用时间戳**：`reports/db_analysis/20260119_160506/`、`reports/update_db/20260119_160812/` 等
- **禁用时间戳**：`reports/db_analysis`、`reports/update_db` 等

### 时间戳优势
- **版本控制**：每次运行的结果都有独立目录，方便对比不同时间点的分析结果
- **避免覆盖**：防止新运行结果覆盖之前的分析结果
- **便于调试**：能够追溯特定时间的分析过程和输出结果
- **自动化友好**：适合在定时任务或CI/CD环境中使用，每次运行都会产生独立的输出目录

## 绩效指标计算公式与代码位置
以下是本项目中计算的核心绩效指标及其对应的数学公式与代码实现位置：

### 1. 总收益率 (Total Return)
- **公式**: `(期末净值 - 期初净值) / 期初净值`
- **代码位置**: [src/analysis/performance.py](src/analysis/performance.py)

### 2. 年化收益率 (Annualized Return)
- **公式**: `(期末净值 / 期初净值) ^ (252 / 实际天数) - 1`
- **代码位置**: [src/analysis/performance.py](src/analysis/performance.py)

### 3. 年化波动率 (Annualized Volatility)
- **公式**: `日收益率标准差 × sqrt(252)` (其中252为年交易日数)
- **代码位置**: [src/analysis/performance.py](src/analysis/performance.py)

### 4. 最大回撤 (Maximum Drawdown)
- **公式**: `max(1 - 净值[i] / rolling_max[i])` (i为时间窗口内的每一天)
- **代码位置**: [src/analysis/performance.py](src/analysis/performance.py)

### 5. 夏普比率 (Sharpe Ratio)
- **公式**: `(年化收益率 - 无风险利率) / 年化波动率` (无风险利率设为0.02)
- **代码位置**: [src/analysis/performance.py](src/analysis/performance.py)

### 6. 持有期收益率分布 (Holding Period Return Distribution)
- **公式**: `(卖出日净值 / 买入日净值) - 1     N天后的累计收益率分布` (N为预设的持有天数)
- **代码位置**: [src/analysis/holding_simulation.py](src/analysis/holding_simulation.py)

### 7. 胜率 (Win Rate)
- **公式**: `正收益次数 / 总交易次数 × 100%`
- **代码位置**: [src/analysis/holding_simulation.py](src/analysis/holding_simulation.py)

这些指标的计算代码主要位于 [src/analysis/performance.py](src/analysis/performance.py) 和 [src/analysis/holding_simulation.py](src/analysis/holding_simulation.py) 文件中，便于审计验证和二次开发。

## 进阶功能与基准线管理
- 基准线管理（多条增删）：在 [src/analysis/visualization.py](src/analysis/visualization.py) 的 `FundVisualizer` 提供：
  - `add_baseline(chart_type, name, value, color=None, linestyle='--', linewidth=2.0)`
  - `remove_baseline(chart_type, name)`
  - `clear_baselines(chart_type=None)`
- 支持的 `chart_type`：
  - `holding_return_dist`（持有期分布纵线，支持中位数或固定值）
  - `nav_curve`（净值曲线横线）
  - `drawdown_chart`（回撤曲线横线，如 -20% 风险阈值）
- 交互图的基准线显隐：HTML 内置按钮"隐藏基准线/显示基准线"。
- 基准线配色：内置柔和调色板自动轮换（可在 `add_baseline()` 指定颜色覆盖）。

## 常见问题
- Q: 如何新增一只基金并运行？
  - A: 在 [config.py](config.py) 的 `FUND_CODES` 添加代码（如 "123456.OF"），运行 `python main.py`。
- Q: 如何新增一个指数参与对比？
  - A: 在 [config.py](config.py) 的 `INDICES` 增加映射并加入 `BENCHMARK_IDS`，确保 AKShare 能获取到该指数的历史行情。
- Q: 为什么我看不到 360 天持有期分布？
  - A: 已强制遍历 `HOLDING_PERIODS`（含 360），若数据不足也会输出占位图的 PNG 与 HTML。
- Q: 如何关闭 HTML 输出，仅保留 PNG？
  - A: 将 [config.py](config.py) 中对应的 `OUTPUT_HTML_*` 开关设为 `False`。
- Q: 如何重置数据库？
  - A: 在终端运行 `python reset_database.py reset` 将清除所有数据表并重建。
- Q: 输出文件在哪？
  - A: PNG/HTML 在 [plots](plots)；数据库在 [data/fund_data.db](data/fund_data.db)；报告与 Excel 在 [reports](reports)。
- Q: 我有Excel格式的基金数据，如何导入数据库？
  - A: 使用 `python scripts/import_excel_to_db.py -i 你的文件.xlsx`，支持多种列名映射和重复处理策略。
- Q: 如何仅更新数据库而不进行分析？
  - A: 使用`python scripts/update_db.py --funds 基金代码` 或 `--all` 更新所有基金。
- Q: 如何从数据库直接进行分析而不重新抓取数据？
  - A: 使用 `python scripts/run_analysis_from_db.py --funds 基金代码`，支持按基金组织文件和交互式图表。

## 维护建议（面向未来）
- 统一改动入口在 [config.py](config.py)：基金、指数、持有期与输出开关。
- 避免写死：新增指数时只改 `INDICES` 与 `BENCHMARK_IDS`，无需改分析逻辑。
- 可按需扩展：如需控制 PNG 命名或样式，集中改动 [src/analysis/visualization.py](src/analysis/visualization.py)。

## 许可证
本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 故障排除
如果遇到问题，请检查以下几点：
- 确保网络连接正常，因为需要从 AKShare 获取数据
- 检查 Python 版本是否符合要求
- 查看日志文件 `fund_analysis.log` 获取详细错误信息
- 确保有足够磁盘空间存储数据和图表文件
- 对于Excel导入问题，检查Excel文件格式是否符合要求（必需列：date、nav或cumulative_nav）
- 对于数据库问题，可以尝试重置数据库：`python reset_database.py reset`