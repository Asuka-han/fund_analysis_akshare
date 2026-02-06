#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
analysis_from_excel.py - 从Excel文件直接进行分析

功能：
1. 从Excel读取基金数据
2. 直接调用分析函数计算指标
3. 生成报告和图表
4. 不写入主数据库（可选）

使用示例：
  测试文件在fund_analysis_project\sample\fund_sample.xlsx
  python scripts/analysis_from_excel.py --input sample/fund_sample.xlsx --fund-code 000001.OF
  python scripts/analysis_from_excel.py --input data/export.xlsx --sheet daily_nav --write-db
"""

import sys
import os
import argparse
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# 保证脚本独立运行时能找到项目内模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
for candidate in (PROJECT_ROOT, SRC_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from src.utils.runtime_env import add_project_paths

# 统一处理冻结/普通环境下的路径与导入
REPO_ROOT, STORAGE_ROOT = add_project_paths()

from src.analysis.performance import PerformanceAnalyzer
from src.analysis.holding_simulation import HoldingSimulation
from src.analysis.visualization import FundVisualizer
from src.utils.database import fund_db
from src.utils.output_manager import get_output_manager
from src.utils.logger_config import LogConfig
from src.utils.logger import get_logger, log_time
import config

# 输出管理器（用于日志与文件落盘）
OUTPUT_MANAGER = get_output_manager(
    'excel_analysis',
    base_dir=config.REPORTS_DIR,
    use_timestamp=config.REPORTS_USE_TIMESTAMP,
    clean_old=config.REPORTS_CLEAN_ENABLED,
    clean_days=config.REPORTS_RETENTION_DAYS,
)

logger = get_logger(__name__)


def summarize_holding_results(holding_results):
    """提取持有期统计用于展示和报告"""
    summary = {}
    for holding_days, results_df in holding_results.items():
        if results_df is not None and not results_df.empty:
            summary[holding_days] = {
                'count': len(results_df),
                'mean_return': results_df['holding_return'].mean(),
                'win_rate': (results_df['holding_return'] > 0).mean()
            }
    return summary


def generate_markdown_report(performance_records, holding_summaries, input_path,
                             target_paths, excel_output=None):
    """生成Markdown报告内容并写入指定路径"""

    def _format_pct(value):
        return f"{value:.2%}" if pd.notna(value) else "N/A"

    def _format_float(value, digits=4):
        return f"{value:.{digits}f}" if pd.notna(value) else "N/A"

    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    lines = [
        "# Excel数据分析报告",
        "",
        f"- 生成时间: {now_str}",
        f"- 输入文件: {input_path}",
        f"- 绩效结果文件: {excel_output or '未生成'}"
    ]

    lines.append("\n## 绩效概览")
    if performance_records is not None and len(performance_records) > 0:
        lines.append("| 基金代码 | 总收益率 | 年化收益率 | 最大回撤 | 夏普比率 |")
        lines.append("| --- | --- | --- | --- | --- |")
        for record in performance_records:
            lines.append(
                f"| {record['fund_id']} | {_format_pct(record['total_return'])} | "
                f"{_format_pct(record['annual_return'])} | {_format_pct(record['max_drawdown'])} | "
                f"{_format_float(record.get('sharpe_ratio'))} |"
            )
    else:
        lines.append("暂无绩效数据。")

    lines.append("\n## 持有期统计")
    if holding_summaries:
        for fund_id, fund_summary in holding_summaries.items():
            lines.append(f"### {fund_id}")
            if fund_summary:
                lines.append("| 持有期(天) | 样本数 | 平均收益率 | 胜率 |")
                lines.append("| --- | --- | --- | --- |")
                for holding_days in sorted(fund_summary.keys()):
                    stats = fund_summary[holding_days]
                    lines.append(
                        f"| {holding_days} | {stats['count']} | "
                        f"{_format_pct(stats['mean_return'])} | {_format_pct(stats['win_rate'])} |"
                    )
            else:
                lines.append("暂无持有期结果。")
    else:
        lines.append("暂无持有期结果。")

    content = "\n".join(lines)
    for path in target_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"Markdown报告已生成: {path}")


def read_excel_data(file_path, sheet_name=None, fund_code=None):
    """
    从Excel读取基金数据
    
    Excel格式要求：
    - 列名必须包含: date, nav (或 cumulative_nav)
    - 可选列: fund_id, daily_growth, net_assets
    - date列应为日期格式
    """
    try:
        logger.info("读取Excel文件: %s", file_path)
        
        # 读取Excel
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            # 尝试读取第一个sheet
            xl = pd.ExcelFile(file_path)
            df = xl.parse(xl.sheet_names[0])
        
        logger.info("原始数据形状: %s", df.shape)
        logger.info("列名: %s", list(df.columns))
        
        # 标准化列名（不区分大小写）
        column_mapping = {}
        for col in df.columns:
            col_lower = str(col).lower().strip()
            if col_lower in ['date', '日期', '交易日期']:
                column_mapping[col] = 'date'
            elif col_lower in ['nav', '单位净值', '净值']:
                column_mapping[col] = 'nav'
            elif col_lower in ['cumulative_nav', '累计净值', '累计净值']:
                column_mapping[col] = 'cumulative_nav'
            elif col_lower in ['fund_id', '基金代码', '代码']:
                column_mapping[col] = 'fund_id'
            elif col_lower in ['daily_growth', '日增长率', '增长率']:
                column_mapping[col] = 'daily_growth'
        
        if column_mapping:
            df = df.rename(columns=column_mapping)
            logger.info("标准化后列名: %s", list(df.columns))
        
        # 检查必要列
        if 'date' not in df.columns:
            raise ValueError("Excel中必须包含date列（或日期列）")
        
        if 'nav' not in df.columns and 'cumulative_nav' not in df.columns:
            raise ValueError("Excel中必须包含nav或cumulative_nav列")
        
        # 转换日期列
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        
        # 设置基金代码
        if 'fund_id' in df.columns:
            if df['fund_id'].isna().all():
                df['fund_id'] = fund_code or 'EXCEL_FUND'
        else:
            df['fund_id'] = fund_code or 'EXCEL_FUND'
        
        # 按日期排序
        df = df.sort_values('date')
        
        # 处理净值数据
        if 'cumulative_nav' not in df.columns and 'nav' in df.columns:
            # 如果没有累计净值，使用单位净值
            df['cumulative_nav'] = df['nav']
            logger.warning("使用单位净值作为累计净值")
        elif 'nav' not in df.columns and 'cumulative_nav' in df.columns:
            # 如果没有单位净值，使用累计净值
            df['nav'] = df['cumulative_nav']
            logger.warning("使用累计净值作为单位净值")
        
        logger.info(
            "处理后数据: %s 行，时间范围: %s 到 %s",
            len(df),
            df['date'].min(),
            df['date'].max(),
        )
        
        return df
        
    except Exception as e:
        logger.error(f"读取Excel文件失败: {e}", exc_info=True)
        raise


def analyze_excel_fund(df, fund_code, output_dir, analyzer, visualizer, periods, output_manager=None):
    """分析单个Excel基金"""
    try:
        logger.info("分析基金: %s", fund_code)
        
        # 提取净值序列
        if 'cumulative_nav' in df.columns and not df['cumulative_nav'].isna().all():
            nav_series = df.set_index('date')['cumulative_nav'].dropna()
            nav_type = 'cumulative'
        else:
            nav_series = df.set_index('date')['nav'].dropna()
            nav_type = 'nav'
        
        logger.info("净值类型: %s, 数据点数: %s", nav_type, len(nav_series))
        
        if len(nav_series) < 2:
            logger.warning("数据不足，至少需要2个数据点")
            return None
        
        # 计算绩效指标
        logger.info("计算绩效指标")
        total_return = analyzer.calculate_total_return(nav_series)
        annual_return = analyzer.calculate_annual_return(nav_series)
        annual_volatility = analyzer.calculate_annual_volatility(nav_series)
        max_drawdown, start_date, end_date = analyzer.calculate_max_drawdown(nav_series)
        sharpe_ratio = analyzer.calculate_sharpe_ratio(nav_series)
        calmar_ratio = analyzer.calculate_calmar_ratio(nav_series)
        
        performance = {
            'fund_id': fund_code,
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
            'max_drawdown': max_drawdown,
            'max_drawdown_start': start_date,
            'max_drawdown_end': end_date,
            'sharpe_ratio': sharpe_ratio,
            'calmar_ratio': calmar_ratio,
            'start_date': nav_series.index[0],
            'end_date': nav_series.index[-1],
            'periods': len(nav_series),
            'data_source': 'excel'
        }
        
        logger.info("总收益率: %.2f%%", total_return * 100)
        logger.info("年化收益率: %.2f%%", annual_return * 100)
        logger.info("最大回撤: %.2f%%", max_drawdown * 100)
        logger.info("夏普比率: %.4f", sharpe_ratio)
        
        return performance, nav_series
        
    except Exception as e:
        logger.error(f"分析基金 {fund_code} 失败: {e}", exc_info=True)
        return None, None


def _fund_daily_data_exists(fund_code: str) -> bool:
    """检查数据库中是否已有该基金的日频数据。"""
    try:
        conn = fund_db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(1) FROM fund_daily_data WHERE fund_id = ?", (fund_code,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False


def _insert_temp_fund_data(fund_code: str, nav_series: pd.Series) -> bool:
    """将Excel净值序列临时写入数据库，用于详细表格生成。"""
    try:
        fund_info = {
            'fund_id': fund_code,
            'name': f"Excel导入-{fund_code}",
            'type': 'excel_import',
            'inception_date': nav_series.index[0].strftime('%Y-%m-%d')
        }
        fund_db.insert_fund_info(fund_info)

        daily_data = pd.DataFrame({
            'date': nav_series.index,
            'nav': nav_series.values,
            'cumulative_nav': nav_series.values
        })
        daily_data['daily_growth'] = daily_data['nav'].pct_change() * 100
        fund_db.insert_fund_daily_data(fund_code, daily_data)
        return True
    except Exception as e:
        logger.error(f"临时写入基金数据失败: {e}", exc_info=True)
        return False


def _cleanup_temp_fund_data(fund_code: str) -> None:
    """清理临时写入的基金数据。"""
    try:
        conn = fund_db.get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fund_daily_data WHERE fund_id = ?", (fund_code,))
        cursor.execute("DELETE FROM funds WHERE fund_id = ?", (fund_code,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"清理临时基金数据失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='从Excel文件直接进行分析')
    
    # 输入参数
    parser.add_argument('--input', required=True, help='Excel文件路径')
    parser.add_argument('--sheet', help='工作表名称（默认：第一个工作表）')
    parser.add_argument('--fund-code', help='基金代码（如果Excel中没有fund_id列）')
    
    # 分析参数
    parser.add_argument('--periods', nargs='+', type=int, default=config.HOLDING_PERIODS,
                       help='持有期列表（默认：30 60 90 180 360）')
    
    # 输出参数
    parser.add_argument('--output-dir', default='reports/excel_analysis',
                       help='输出目录（默认：reports/excel_analysis）')
    parser.add_argument('--write-db', action='store_true',
                       help='将数据写入主数据库（默认：不写入）')
    parser.add_argument('--no-detailed-excel', action='store_true',
                       help='不生成详细绩效表格（产品及基准收益率/周收益率曲线/月度收益率）')
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细日志')
    
    args = parser.parse_args()

    # 初始化输出管理器
    output_manager = OUTPUT_MANAGER

    # 配置日志
    log_level = logging.DEBUG if args.verbose else logging.INFO
    LogConfig.setup_root_logger(
        LogConfig.resolve_log_dir('excel_analysis', config.REPORTS_DIR),
        level=log_level,
        script_name='excel_analysis',
        base_dir=config.REPORTS_DIR,
        task_log_dir=OUTPUT_MANAGER.get_path('logs'),
    )

    logger.info("开始Excel数据分析")
    logger.info("=" * 60)
    
    # 检查输入文件
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error("文件不存在: %s", args.input)
        return 1
    
    # 使用输出管理器创建输出目录
    output_dir = output_manager.get_path('base')
    excel_output_path = None
    
    try:
        # 1. 读取Excel数据
        with log_time("读取Excel数据", logger):
            df = read_excel_data(args.input, args.sheet, args.fund_code)
        
        # 2. 初始化分析器
        analyzer = PerformanceAnalyzer(
            risk_free_rate=config.RISK_FREE_RATE,
            trading_days=config.TRADING_DAYS
        )
        
        simulator = HoldingSimulation(
            risk_free_rate=config.RISK_FREE_RATE,
            trading_days=config.TRADING_DAYS,
            annualization_days=config.ANNUALIZATION_DAYS,
        )
        
        # 使用 excel_analysis 输出管理器创建可视化器（本脚本专属目录）
        visualizer = FundVisualizer(output_dir=output_dir, output_manager=OUTPUT_MANAGER)
        
        # 3. 按基金分组分析
        all_performance = []
        all_holding_results = []
        holding_summaries = {}
        
        if 'fund_id' in df.columns:
            fund_codes = df['fund_id'].unique()
            logger.info("发现 %s 只基金: %s", len(fund_codes), fund_codes)
        else:
            fund_codes = [args.fund_code or 'EXCEL_FUND']
        
        with log_time("绩效与持有期分析", logger):
            for fund_code in fund_codes:
                # 筛选该基金的数据
                fund_df = df[df['fund_id'] == fund_code].copy() if len(fund_codes) > 1 else df

                # 分析绩效
                result = analyze_excel_fund(
                    fund_df, fund_code, output_dir, analyzer, visualizer, args.periods, output_manager
                )

                if result is None:
                    continue

                performance, nav_series = result
                all_performance.append(performance)
                holding_summaries[fund_code] = {}

                # 生成详细绩效表格（使用与 main 相同的逻辑）
                export_detailed = not args.no_detailed_excel
                temp_inserted = False
                if export_detailed:
                    data_exists = _fund_daily_data_exists(fund_code)
                    if not data_exists:
                        temp_inserted = _insert_temp_fund_data(fund_code, nav_series)

                    try:
                        benchmark_id = config.get_fund_benchmark(fund_code) if hasattr(config, 'get_fund_benchmark') else config.DEFAULT_BENCHMARK
                        comparison_indices = config.get_fund_comparison_indices(fund_code) if hasattr(config, 'get_fund_comparison_indices') else None
                        detail_path = OUTPUT_MANAGER.get_path('excel_performance', f"{fund_code}_detailed_performance.xlsx")
                        ok = analyzer.save_detailed_performance_to_excel(
                            fund_code,
                            output_path=str(detail_path),
                            benchmark_id=benchmark_id,
                            comparison_indices=comparison_indices
                        )
                        if ok:
                            logger.info("详细绩效已保存: %s", detail_path.name)
                    except Exception as e:
                        logger.error(f"保存基金 {fund_code} 详细绩效失败: {e}", exc_info=True)
                    finally:
                        if temp_inserted and not args.write_db:
                            _cleanup_temp_fund_data(fund_code)

                # 分析持有期（如果数据足够）
                if len(nav_series) > max(args.periods) + 10:
                    logger.info("分析持有期收益")
                    try:
                        holding_results = simulator.simulate_multiple_periods(
                            nav_series, args.periods
                        )

                        holding_summary = summarize_holding_results(holding_results)
                        holding_summaries[fund_code] = holding_summary
                        period_label = "/".join(str(p) for p in args.periods)
                        logger.info("持有期统计（%s天）", period_label)
                        for holding_days in args.periods:
                            stats = holding_summary.get(holding_days)
                            if stats:
                                logger.info(
                                    "%s天: 平均收益%.2f%%, 胜率%.2f%%, 样本数%s",
                                    holding_days,
                                    stats['mean_return'] * 100,
                                    stats['win_rate'] * 100,
                                    stats['count'],
                                )
                            else:
                                logger.warning("%s天: 数据不足", holding_days)

                        # 使用新的批量保存方法（静态图保存到 excel_analysis 结构）
                        visualizer.save_all_holding_plots({
                            'simulation_results': holding_results
                        }, fund_code, output_manager=OUTPUT_MANAGER)

                        # 生成交互式持有期分布
                        if config.OUTPUT_HTML_HOLDING_DIST:
                            for holding_days in args.periods:
                                results_df = holding_results.get(
                                    holding_days, pd.DataFrame(columns=['holding_return'])
                                )
                                benchmark_returns = {}
                                for benchmark_id in config.BENCHMARK_IDS:
                                    if hasattr(config, 'normalize_index_code'):
                                        actual_code = config.normalize_index_code(benchmark_id)
                                    else:
                                        actual_code = benchmark_id
                                    benchmark_name = config.get_benchmark_display_name(benchmark_id)
                                    returns = simulator.get_benchmark_returns(actual_code, holding_days)
                                    if not returns.empty:
                                        benchmark_returns[benchmark_name] = returns
                                interactive_fig = visualizer.create_interactive_return_distribution(
                                    results_df, holding_days, fund_code, benchmark_returns
                                )
                                if interactive_fig:
                                    html_path = OUTPUT_MANAGER.get_path(
                                        'interactive', f"{fund_code}_持有期{holding_days}天_交互.html", fund_id=fund_code
                                    )
                                    interactive_fig.write_html(str(html_path))
                                    logger.info("交互图 %s天: %s", holding_days, html_path.name)

                        # 保存每只基金的持有期结果到 main 结构的 excel_holding
                        holding_analysis_dict = {
                            'simulation_results': holding_results,
                            'summary': holding_summary
                        }
                        holding_excel_path = OUTPUT_MANAGER.get_path('excel_holding', f'holding_analysis_{fund_code}.xlsx')
                        simulator.save_simulation_results(holding_analysis_dict, str(holding_excel_path))
                        logger.info("持有期结果已保存: %s", holding_excel_path)

                        all_holding_results.append({
                            'fund_id': fund_code,
                            'results': holding_results
                        })

                    except Exception as e:
                        logger.error(f"分析持有期失败: {e}", exc_info=True)
                        logger.error("持有期分析失败: %s", e, exc_info=True)
                else:
                    logger.warning("数据不足，无法覆盖所有持有期（需要超过 %s 个数据点）", max(args.periods) + 10)
                    for holding_days in args.periods:
                        logger.warning("%s天: 数据不足", holding_days)

                # 生成净值曲线和回撤图
                logger.info("生成图表")
                try:
                    # 净值曲线（静态）
                    visualizer.plot_nav_curve(nav_series, fund_code)

                    # 交互式净值曲线（与 main 相同）
                    if config.OUTPUT_HTML_NAV_CURVE:
                        interactive_nav = visualizer.create_interactive_nav_curve(nav_series, fund_code)
                        if interactive_nav:
                            html_path = OUTPUT_MANAGER.get_interactive_path(fund_code, 'nav_curve')
                            interactive_nav.write_html(str(html_path))
                            logger.info("交互净值曲线: %s", html_path.name)

                    # 回撤图（静态）
                    visualizer.plot_drawdown_chart(nav_series, fund_code)

                    # 交互式净值+回撤图
                    if config.OUTPUT_HTML_NAV_DRAWDOWN:
                        interactive_drawdown = visualizer.create_interactive_chart(nav_series, fund_code)
                        if interactive_drawdown:
                            html_path = OUTPUT_MANAGER.get_interactive_path(fund_code, 'nav_drawdown')
                            interactive_drawdown.write_html(str(html_path))
                            logger.info("交互净值回撤图: %s", html_path.name)

                    logger.info("图表生成完成")

                except Exception as e:
                    logger.error(f"生成图表失败: {e}", exc_info=True)
                    logger.error("图表生成失败: %s", e, exc_info=True)

                # 写入数据库（可选）
                if args.write_db:
                    logger.info("写入数据库")
                    try:
                        # 保存基金基本信息（简化）
                        fund_info = {
                            'fund_id': fund_code,
                            'name': f"Excel导入-{fund_code}",
                            'type': 'excel_import',
                            'inception_date': nav_series.index[0].strftime('%Y-%m-%d')
                        }
                        fund_db.insert_fund_info(fund_info)

                        # 保存日频数据
                        daily_data = pd.DataFrame({
                            'date': nav_series.index,
                            'nav': nav_series.values,
                            'cumulative_nav': nav_series.values
                        })
                        inserted = fund_db.insert_fund_daily_data(fund_code, daily_data)

                        logger.info("写入数据库完成: %s 条记录", inserted)

                    except Exception as e:
                        logger.error(f"写入数据库失败: {e}", exc_info=True)
                        logger.error("写入数据库失败: %s", e, exc_info=True)
        
        # 4. 保存绩效结果（与 main 一致，输出到 reports/main/.../excel/performance）
        if all_performance:
            logger.info("保存分析结果")
            
            performance_df = pd.DataFrame(all_performance)
            perf_path = OUTPUT_MANAGER.get_path('excel_performance', 'performance_summary.xlsx')
            excel_output_path = perf_path
            # 使用与 main 相同的保存方法（指数为空）
            analyzer.save_performance_to_excel(performance_df, pd.DataFrame(), str(perf_path))
            logger.info("绩效结果已保存: %s", perf_path)
            
            # 打印汇总
            logger.info("分析汇总")
            logger.info("分析基金数: %s", len(all_performance))
            logger.info("持有期分析: %s", len(all_holding_results))
            logger.info("输出目录: %s", output_dir.absolute())
        
        logger.info("=" * 60)
        logger.info("Excel数据分析完成")
        
        # 输出目录摘要
        output_manager.print_summary()

        report_targets = [
            OUTPUT_MANAGER.get_path('reports', 'excel_analysis_report.md')
        ]
        generate_markdown_report(
            all_performance,
            holding_summaries,
            str(input_path),
            report_targets,
            excel_output=str(excel_output_path) if excel_output_path else None
        )
        logger.info("Markdown报告已保存: %s", report_targets[0])
        
        return 0
        
    except Exception as e:
        logger.error(f"Excel分析失败: {e}", exc_info=True)
        logger.error("Excel分析失败: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())