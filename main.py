"""
主程序入口
协调整个基金分析项目的执行流程
"""
import sys
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

from src.utils.runtime_env import add_project_paths, get_storage_root

# 统一处理冻结/普通环境下的路径与导入
REPO_ROOT, STORAGE_ROOT = add_project_paths()

from src.data_fetch.fund_fetcher import FundDataFetcher, SAMPLE_FUND_CODES
from src.data_fetch.index_fetcher import IndexDataFetcher
from src.analysis.performance import PerformanceAnalyzer
from src.analysis.holding_simulation import HoldingSimulation
from src.analysis.visualization import FundVisualizer
from src.utils.database import fund_db
from src.utils.fund_code_manager import fund_code_manager
from src.utils.output_manager import get_output_manager
from src.utils.logger_config import LogConfig
from src.utils.logger import get_logger
from src.utils.logger import log_time
import config

# 初始化输出管理器（同时用于日志路径）
_MAIN_OUTPUT_MANAGER = get_output_manager(
    'main',
    base_dir=config.REPORTS_DIR,
    use_timestamp=config.REPORTS_USE_TIMESTAMP,
    clean_old=config.REPORTS_CLEAN_ENABLED,
    clean_days=config.REPORTS_RETENTION_DAYS,
)
_MAIN_LOG_DIR = LogConfig.resolve_log_dir('main', config.REPORTS_DIR)
LogConfig.setup_root_logger(
    _MAIN_LOG_DIR,
    level=logging.INFO,
    script_name='fund_analysis',
    base_dir=config.REPORTS_DIR,
    task_log_dir=_MAIN_OUTPUT_MANAGER.get_path('logs'),
)
logger = get_logger(__name__)


class FundAnalysisPipeline:
    """基金分析流水线"""
    
    def __init__(self):
        """初始化分析流水线"""
        # 初始化输出管理器
        self.output_manager = _MAIN_OUTPUT_MANAGER
        
        self.start_time = datetime.now()
        logger.info(f"基金分析项目启动: {self.start_time}")
        
        # 初始化各个模块
        self.fund_fetcher = FundDataFetcher()
        self.index_fetcher = IndexDataFetcher()
        self.performance_analyzer = PerformanceAnalyzer(
            risk_free_rate=config.RISK_FREE_RATE,
            trading_days=config.TRADING_DAYS
        )
        self.holding_simulator = HoldingSimulation(
            risk_free_rate=config.RISK_FREE_RATE,
            trading_days=config.TRADING_DAYS,
            annualization_days=config.ANNUALIZATION_DAYS,
        )
        # 使用输出管理器初始化可视化器
        self.visualizer = FundVisualizer(output_dir=self.output_manager.get_path('plots'), 
                                         output_manager=self.output_manager)
        
        # 设置基金代码
        self.fund_fetcher.set_fund_codes(config.FUND_CODES)
    
    def fetch_data(self):
        """步骤1: 获取数据"""
        logger.info("=" * 60)
        logger.info("步骤1: 获取基金和指数数据")
        logger.info("=" * 60)
        
        # 获取基金数据
        logger.info("开始获取基金数据")
        
        # 使用基金代码管理器转换格式
        fund_codes_for_akshare = fund_code_manager.batch_to_akshare(config.FUND_CODES)
        fund_results = self.fund_fetcher.fetch_all_funds_data(fund_codes_for_akshare)
        
        # 将基金数据保存到数据库
        for fund_code, fund_data in fund_results.items():
            # 获取基金基本信息
            fund_info = self.fund_fetcher.get_fund_info(fund_code)
            if fund_info:
                # 保存基金基本信息
                fund_db.insert_fund_info(fund_info)
            
            # 保存基金日频数据
            display_fund_code = fund_code_manager.to_display_format(fund_code)
            records_inserted = fund_db.insert_fund_daily_data(display_fund_code, fund_data)
            logger.info(f"基金 {display_fund_code} 数据入库: {records_inserted} 条")
        
        logger.info(f"基金数据获取完成: {fund_results}")
        
        # 获取指数数据
        logger.info("开始获取指数数据")
        index_results = self.index_fetcher.fetch_all_indices_data()
        
        # 将指数数据保存到数据库
        for index_code, index_data in index_results.items():
            records_inserted = fund_db.insert_index_daily_data(index_code, index_data)
            logger.info(f"指数 {index_code} 数据入库: {records_inserted} 条")
        
        logger.info(f"指数数据获取完成: {index_results}")
        
        return fund_results, index_results
    
    def analyze_performance(self):
        """步骤2: 分析绩效指标"""
        logger.info("=" * 60)
        logger.info("步骤2: 分析基金和指数绩效")
        logger.info("=" * 60)
        
        # 分析基金绩效（使用数据库格式的基金代码）
        logger.info("分析基金绩效")
        funds_performance = self.performance_analyzer.analyze_all_funds(config.FUND_CODES)

        # 将绩效结果写入数据库，便于后续查询
        if not funds_performance.empty:
            fund_db.upsert_performance_metrics(funds_performance)
        
        if not funds_performance.empty:
            logger.info(f"基金绩效分析完成，共分析 {len(funds_performance)} 只基金")
            logger.info("\n基金绩效汇总:\n%s",
                        funds_performance[['fund_id', 'total_return', 'annual_return',
                                           'max_drawdown', 'sharpe_ratio']].to_string())
        else:
            logger.warning("没有可分析的基金数据")
        
        # 分析指数绩效
        logger.info("分析指数绩效")
        indices_performance = self.performance_analyzer.analyze_all_indices()

        # 写入指数绩效
        if not indices_performance.empty:
            fund_db.upsert_performance_metrics(indices_performance)
        
        if not indices_performance.empty:
            logger.info(f"指数绩效分析完成，共分析 {len(indices_performance)} 个指数")
            logger.info("\n指数绩效汇总:\n%s",
                        indices_performance[['fund_id', 'total_return', 'annual_return',
                                             'max_drawdown', 'sharpe_ratio']].to_string())
        else:
            logger.warning("没有可分析的指数数据")
        
        # 保存绩效结果到Excel
        logger.info("保存绩效结果到 Excel")
        excel_path = self.output_manager.get_path('excel_performance', 'performance_summary.xlsx')
        success = self.performance_analyzer.save_performance_to_excel(
            funds_performance, indices_performance, str(excel_path)
        )
        
        if success:
            logger.info(f"绩效结果已保存到: {excel_path}")

        # 保存每只基金的详细绩效表格（产品及基准收益率/周收益率曲线/月度收益率）
        if config.FUND_CODES:
            for fund_id in config.FUND_CODES:
                try:
                    benchmark_id = config.get_fund_benchmark(fund_id) if hasattr(config, 'get_fund_benchmark') else config.DEFAULT_BENCHMARK
                    comparison_indices = config.get_fund_comparison_indices(fund_id) if hasattr(config, 'get_fund_comparison_indices') else None
                    detail_path = self.output_manager.get_path('excel_performance', f"{fund_id}_detailed_performance.xlsx")
                    ok = self.performance_analyzer.save_detailed_performance_to_excel(
                        fund_id,
                        output_path=str(detail_path),
                        benchmark_id=benchmark_id,
                        comparison_indices=comparison_indices
                    )
                    if ok:
                        logger.info(f"详细绩效结果已保存: {detail_path}")
                except Exception as e:
                    logger.error(f"保存基金 {fund_id} 详细绩效失败: {e}", exc_info=True)
        
        return funds_performance, indices_performance
    
    def simulate_holding_periods(self, funds_performance):
        """步骤3: 持有期收益模拟"""
        logger.info("=" * 60)
        logger.info("步骤3: 持有期收益模拟")
        logger.info("=" * 60)
        
        holding_results = {}
        
        for fund_id in config.FUND_CODES:
            logger.info(f"模拟基金持有期收益: {fund_id}")
            
            # 分析持有期收益
            analysis = self.holding_simulator.analyze_fund_holding(
                fund_id, 
                holding_periods=config.HOLDING_PERIODS
            )
            
            if analysis:
                holding_results[fund_id] = analysis
                
                # 显示汇总信息
                summary = analysis.get('summary', {})
                logger.info(f"  {fund_id} 持有期模拟完成")
                
                for holding_days, stats in summary.items():
                    logger.info(f"    {holding_days}天: "
                              f"平均收益{stats['mean_return']:.2%}, "
                              f"胜率{stats['win_rate']:.2%}")
                
                # 保存模拟结果
                report_path = self.output_manager.get_path('excel_holding', f'holding_simulation_{fund_id}.xlsx')
                self.holding_simulator.save_simulation_results(analysis, str(report_path))
        
        return holding_results
    
    def generate_visualizations(self, funds_performance, holding_results):
        """步骤4: 生成可视化图表"""
        logger.info("=" * 60)
        logger.info("步骤4: 生成可视化图表")
        logger.info("=" * 60)
        
        # 1. 生成绩效对比图
        if not funds_performance.empty:
            logger.info("生成绩效对比图")
            self.visualizer.plot_performance_comparison(funds_performance)
        
        # 2. 为每只基金生成图表
        for fund_id in config.FUND_CODES:
            logger.info(f"为基金生成图表: {fund_id}")
            
            # 获取基金数据
            df = fund_db.get_fund_daily_data(fund_id)
            
            if df.empty:
                logger.warning(f"  基金数据为空: {fund_id}")
                continue
            
            # 获取基金名称
            funds_df = fund_db.get_all_funds()
            fund_name = funds_df.loc[funds_df['fund_id'] == fund_id, 'name'].iloc[0] \
                if not funds_df.empty and fund_id in funds_df['fund_id'].values else fund_id

            # 选择净值序列
            if 'cumulative_nav' in df.columns and not df['cumulative_nav'].isna().all():
                nav_series = df['cumulative_nav'].dropna()
            else:
                nav_series = df['nav'].dropna()

            # 跳过示例基金的相关图表生成（静态+交互）
            if "示例基金" in str(fund_name):
                logger.info(f"  跳过示例金的相关图表生成...")
                continue
            
            # 生成净值曲线图
            logger.info(f"  生成净值曲线图...")
            # 使用基金配置的业绩基准
            benchmark_id = config.get_fund_benchmark(fund_id) if hasattr(config, 'get_fund_benchmark') else '000300'
            actual_benchmark_id = config.normalize_index_code(benchmark_id) if hasattr(config, 'normalize_index_code') else benchmark_id
            benchmark_df = fund_db.get_index_daily_data(actual_benchmark_id)
            benchmark_nav = benchmark_df['close'] if not benchmark_df.empty else pd.Series(dtype=float)
            if benchmark_nav.empty and hasattr(config, 'is_composite_index') and config.is_composite_index(benchmark_id):
                benchmark_nav = self._build_composite_nav_from_db(benchmark_id)
            self.visualizer.plot_nav_curve(nav_series, fund_name, benchmark_nav)
            # 交互式净值曲线（仅净值）
            if config.OUTPUT_HTML_NAV_CURVE:
                interactive_nav_fig = self.visualizer.create_interactive_nav_curve(nav_series, fund_name, benchmark_nav)
                if interactive_nav_fig:
                    html_path = self.output_manager.get_path('interactive', f"{fund_name}_净值曲线_交互.html", fund_id=fund_id)
                    interactive_nav_fig.write_html(str(html_path))
            # 交互式净值/回撤图（HTML，中文命名）
            if config.OUTPUT_HTML_NAV_DRAWDOWN:
                interactive_fig = self.visualizer.create_interactive_chart(nav_series, fund_name)
                if interactive_fig:
                    html_path = self.output_manager.get_path('interactive', f"{fund_name}_净值回撤_交互.html", fund_id=fund_id)
                    interactive_fig.write_html(str(html_path))
            
            # 生成回撤图
            logger.info(f"  生成回撤图...")
            self.visualizer.plot_drawdown_chart(nav_series, fund_name)
            
            # 生成持有期收益分布图（添加四个指数的基准线）
            logger.info(f"  生成持有期收益分布图...")

            simulation_results = {}
            if fund_id in holding_results:
                holding_analysis = holding_results[fund_id]
                simulation_results = holding_analysis.get('simulation_results', {})

            # 无论是否有模拟结果，都尝试为所有持有期生成图（空数据会生成占位图）
            for holding_days in config.HOLDING_PERIODS:
                results_df = simulation_results.get(holding_days, pd.DataFrame(columns=['holding_return']))

                # 为每个持有期获取四个基准指数的收益率序列（可能为空）
                benchmark_returns = {}
                for benchmark_id in config.BENCHMARK_IDS:
                    actual_code = config.normalize_index_code(benchmark_id) if hasattr(config, 'normalize_index_code') else benchmark_id
                    benchmark_name = config.get_benchmark_display_name(benchmark_id)
                    returns = self.holding_simulator.get_benchmark_returns(actual_code, holding_days)
                    if not returns.empty:
                        benchmark_returns[benchmark_name] = returns

                # 静态分布图（空数据将生成占位图）
                self.visualizer.plot_return_distribution(
                    results_df, holding_days, fund_name, benchmark_returns
                )
                # 交互式分布图（空数据将生成占位图）
                if config.OUTPUT_HTML_HOLDING_DIST:
                    interactive_hold_fig = self.visualizer.create_interactive_return_distribution(
                        results_df, holding_days, fund_name, benchmark_returns
                    )
                    if interactive_hold_fig:
                        html_path = self.output_manager.get_path('interactive', f"{fund_name}_持有期{holding_days}天_交互.html", fund_id=fund_id)
                        interactive_hold_fig.write_html(str(html_path))
        
            # 已在上方生成交互式净值/回撤图
        
        logger.info(f"所有图表已保存到: {self.output_manager.get_path('plots')}")

    def _build_composite_nav_from_db(self, composite_id: str) -> pd.Series:
        """
        当复合指数未入库时，尝试用成分指数在本地数据库计算复合净值序列。
        """
        if not hasattr(config, 'get_composite_components'):
            return pd.Series(dtype=float)

        components = config.get_composite_components(composite_id)
        if not components:
            return pd.Series(dtype=float)

        returns_df = None
        weights = {}

        for comp in components:
            base_code = comp.get('base_code')
            if not base_code:
                continue
            if hasattr(config, 'normalize_index_code'):
                base_code = config.normalize_index_code(base_code)
            weight = float(comp.get('weight', 0))
            if weight == 0:
                continue
            df = fund_db.get_index_daily_data(base_code)
            if df.empty or 'close' not in df.columns:
                continue
            series = df['close'].pct_change().dropna().rename(base_code)
            if series.empty:
                continue
            weights[base_code] = weight
            returns_df = series.to_frame() if returns_df is None else returns_df.join(series, how='inner')

        if returns_df is None or returns_df.empty or not weights:
            return pd.Series(dtype=float)

        weighted_returns = None
        for code, weight in weights.items():
            if code not in returns_df.columns:
                continue
            component = returns_df[code] * weight
            weighted_returns = component if weighted_returns is None else (weighted_returns + component)

        if weighted_returns is None or weighted_returns.empty:
            return pd.Series(dtype=float)

        composite_nav = (1 + weighted_returns).cumprod()
        composite_nav.name = composite_id
        return composite_nav
    
    def generate_report(self):
        """步骤5: 生成综合报告"""
        logger.info("=" * 60)
        logger.info("步骤5: 生成综合报告")
        logger.info("=" * 60)
        
        try:
            report_content = self._create_report_content()
            report_path = self.output_manager.get_path('reports', 'analysis_report.md')
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            logger.info(f"分析报告已生成: {report_path}")
            
        except Exception as e:
            logger.error(f"生成报告失败: {e}", exc_info=True)
    
    def _create_report_content(self) -> str:
        """创建报告内容"""
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        # 获取数据库统计
        with fund_db.get_connection() as conn:
            cursor = conn.cursor()
            
            # 基金数量
            cursor.execute("SELECT COUNT(*) FROM funds")
            fund_count = cursor.fetchone()[0]
            
            # 基金数据点数
            cursor.execute("SELECT COUNT(*) FROM fund_daily_data")
            fund_data_count = cursor.fetchone()[0]
            
            # 指数数量
            cursor.execute("SELECT COUNT(DISTINCT index_id) FROM index_daily_data")
            index_count = cursor.fetchone()[0]
            
            # 指数数据点数
            cursor.execute("SELECT COUNT(*) FROM index_daily_data")
            index_data_count = cursor.fetchone()[0]
        
        performance_path = self.output_manager.get_path('excel_performance', 'performance_summary.xlsx')
        holding_dir = self.output_manager.get_path('excel_holding')

        report = f"""# 基金分析项目报告

## 项目概览
- **项目启动时间**: {self.start_time}
- **项目完成时间**: {end_time}
- **运行时长**: {duration}

## 数据统计
### 基金数据
- 基金数量: {fund_count}
- 基金日频数据点数: {fund_data_count}

### 指数数据
- 指数数量: {index_count}
- 指数日频数据点数: {index_data_count}

## 分析内容
1. **数据获取**: 从AKShare接口获取基金和指数数据
2. **绩效分析**: 计算各项绩效指标（收益率、波动率、夏普比率等）
3. **持有期模拟**: 模拟不同持有期的投资收益
4. **可视化**: 生成各种图表和分析图形

## 输出文件
### 数据文件
- `data/fund_data.db`: SQLite数据库文件
- `{performance_path}`: 绩效指标汇总表
- `{holding_dir}`: 持有期模拟结果目录

### 图表文件
- `plots/`: 包含所有生成的图表
  - 净值曲线图
  - 回撤图
  - 持有期收益率分布图
  - 绩效指标对比图

### 报告文件
- `reports/analysis_report.md`: 本报告
- `{_MAIN_LOG_DIR}`: 运行日志目录

## 使用说明
### 重新运行分析
```bash
python main.py
"""

        return report
    
    def run(self):
        """运行整个分析流水线"""
        try:
            logger.info("开始基金分析项目")
            
            # 步骤1: 获取数据
            with log_time("步骤1: 获取数据", logger):
                self.fetch_data()
            
            # 步骤2: 分析绩效
            with log_time("步骤2: 分析绩效", logger):
                funds_performance, indices_performance = self.analyze_performance()
            
            # 步骤3: 持有期模拟
            with log_time("步骤3: 持有期模拟", logger):
                holding_results = self.simulate_holding_periods(funds_performance)
            
            # 步骤4: 生成可视化
            with log_time("步骤4: 生成可视化", logger):
                self.generate_visualizations(funds_performance, holding_results)
            
            # 步骤5: 生成报告
            with log_time("步骤5: 生成报告", logger):
                self.generate_report()
            
            # 完成
            end_time = datetime.now()
            duration = end_time - self.start_time
            logger.info("=" * 60)
            logger.info("基金分析项目完成")
            logger.info(f"总运行时间: {duration}")
            logger.info(f"结果保存在: {self.output_manager.get_path('base')}")
            logger.info("=" * 60)
            
            # 打印输出目录摘要
            self.output_manager.print_summary()
            
        except Exception as e:
            logger.error(f"项目运行失败: {e}", exc_info=True)
            import traceback
            traceback.print_exc()
            return False
        
        return True


def main():
    """主函数"""
    # 创建分析流水线
    pipeline = FundAnalysisPipeline()
    # 运行分析
    success = pipeline.run()

    if success:
        logger.info("基金分析项目成功完成")
        output_manager = _MAIN_OUTPUT_MANAGER
        logger.info("查看绩效结果: %s", output_manager.get_path('excel_performance', 'performance_summary.xlsx'))
        logger.info("查看持有期Excel: %s", output_manager.get_path('excel_holding'))
        logger.info("查看图表: %s/", output_manager.get_path('plots'))
        logger.info("查看报告: %s", output_manager.get_path('reports', 'analysis_report.md'))
    else:
        logger.error("基金分析项目运行失败，请查看日志目录: %s", _MAIN_LOG_DIR)
        sys.exit(1)


if __name__ == "__main__":
    main()