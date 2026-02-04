# CHANGELOG

## 版本信息
- 当前版本：v1.2
- 发布日期：2026-01-29

## 更新内容
### Added
- 新增样式配置模块：`src/utils/excel_style_config.py`，集中定义导出 Excel 的字体、填充、对齐、数字格式与白色细边框等样式。
- 实现三个Excel输出表格生成功能：
    产品及基准收益率表格：按年、月等时间维度展示收益率对比
    周收益率曲线表格：展示周度收益率变化趋势及组合规模
    月度收益率表格：详细展示各月份收益率及胜率统计

### Changed
- 在配置文件`config.py`中
    1. 复合指数配置：`COMPOSITE_INDICES`。提供明确的复合指数代码映射，提供一个构建机制，并提供一个新增复合指数的demo。
    2. "业绩基准"配置项：`FUND_BENCHMARKS`。默认基准指数为` DEFAULT_BENCHMARK = '000300' `，该配置默认为沪深300指数，如果有配置，则才用配置的指数。   
    3. 对比指数配置：`FUND_COMPARISON_INDICES`。默认对比指数为` DEFAULT_COMPARISON_INDICES = ['000300', '000001', 'HSI'] `，该配置默认为沪深300、上证指数、恒生指数，如果有配置，则才用配置的指数。
    4. 月度对比指数配置: `FUND_MONTHLY_COMPARISON`。用于配置月度对比指数。
    5. 在指数数据分析时候，也支持带后缀代码的指数代码配置，方便后续添加新的指数。

- 在数据获取："./src/data_fetch/fund_fetcher.py"中新增开关参数控制资产规模获取。在核心方法`fetch_fund_data`中新增`include_asset_size: bool = False`参数，作为是否获取资产规模的开关：当参数为True时，触发资产规模数据的获取、合并逻辑；当参数为False时，仅返回原有净值数据，不影响原有功能。

- 在数据分析："./src/analysis/performance.py"中增加多频率计算功能，支持日、周、月、季度、年度等不同频率的绩效计算，核心函数`_resample_data`。
整体实现逻辑总结:
数据层：原始日度净值数据 → 通过`_resample_data`降频为指定频率（日 / 周 / 月 / 季度 / 年度）的序列；
计算层：基于降频后的序列，通过`_calculate_metrics_for_frequency`计算所有指标，且年化类指标（如年化收益、波动率）会根据频率适配年化因子；
接口层：`analyze_fund_performance`支持 “统一频率” 或 “收益 / 风险分开指定频率” 两种模式，默认遵循 “收益率日度、风险类周度”；
输出层：批量分析函数返回结构化 DataFrame，可通过`_rename_for_output`转为中文列名，直接对接 Excel 输出（代码中已关联 Excel 样式配置）。

- 修改完善了`main.py`，`run_analysis_from_db.py`，`analysis_from_excel.py`为实行上述功能。

### Environment
- 新增/使用依赖：`openpyxl`（用于样式化写Excel）。