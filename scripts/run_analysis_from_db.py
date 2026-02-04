#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
run_analysis_from_db.py - 仅从现有数据库进行分析（增强版）

功能：
1. 使用输出管理器避免文件混乱
2. 为每个基金创建独立目录
3. 支持多种输出格式和组织方式

使用示例：
  python scripts/run_analysis_from_db.py --funds 000001.OF --periods 30 60 90
  python scripts/run_analysis_from_db.py --all --output-html true
  python scripts/run_analysis_from_db.py --funds 000001.OF 510300.OF --organize-by-fund
"""

import sys
import os
import argparse
import logging
import io
from pathlib import Path
from datetime import datetime
import pandas as pd

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
from src.utils.output_manager import OutputManager, get_output_manager
import config

logger = logging.getLogger(__name__)

# 统一时间戳开关（本文件仅此一处）
USE_TIMESTAMP = True


def configure_logging(log_path: Path, verbose: bool = False):
    """配置日志输出到控制台和指定文件"""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    safe_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(safe_stdout),
            logging.FileHandler(str(log_path), encoding='utf-8')
        ],
        force=True
    )


def setup_output_directories(args):
    """设置输出目录"""
    # 创建输出管理器
    output_mgr = get_output_manager(
        script_type='db_analysis',
        base_dir=config.REPORTS_DIR,
        use_timestamp=USE_TIMESTAMP,
        clean_old=args.clean_old
    )
    
    # 打印目录信息
    if args.verbose:
        output_mgr.print_summary()
    
    return output_mgr


def analyze_performance(fund_ids, index_ids, output_mgr):
    """分析绩效指标"""
    analyzer = PerformanceAnalyzer(
        risk_free_rate=config.RISK_FREE_RATE,
        trading_days=config.TRADING_DAYS
    )
    
    results = {}
    
    # 分析基金
    if fund_ids:
        print(f"\n📈 分析基金绩效（{len(fund_ids)} 只）...")
        for fund_id in fund_ids:
            try:
                print(f"  分析基金: {fund_id}")
                performance = analyzer.analyze_fund_performance(fund_id)
                if performance:
                    results[fund_id] = performance
                    print(f"    ✓ 完成: 总收益率 {performance['total_return']:.2%}")
                else:
                    print(f"    ✗ 失败: 数据不足或计算错误")
            except Exception as e:
                logger.error(f"分析基金 {fund_id} 失败: {e}")
    
    # 分析指数
    if index_ids:
        print(f"\n📊 分析指数绩效（{len(index_ids)} 个）...")
        for index_id in index_ids:
            try:
                print(f"  分析指数: {index_id}")
                performance = analyzer.analyze_index_performance(index_id)
                if performance:
                    results[f"INDEX_{index_id}"] = performance
                    print(f"    ✓ 完成: 总收益率 {performance['total_return']:.2%}")
                else:
                    print(f"    ✗ 失败: 数据不足或计算错误")
            except Exception as e:
                logger.error(f"分析指数 {index_id} 失败: {e}")

    # 保存到Excel（使用输出管理器）
    if results:
        print(f"\n💾 保存绩效结果...")
        funds_df = pd.DataFrame([v for k, v in results.items() if not k.startswith('INDEX_')])
        indices_df = pd.DataFrame([v for k, v in results.items() if k.startswith('INDEX_')])
        
        # 使用输出管理器获取路径
        excel_path = output_mgr.get_path('excel_performance', 'performance_analysis.xlsx')
        
        success = analyzer.save_performance_to_excel(funds_df, indices_df, str(excel_path))
        
        if success:
            print(f"   绩效结果已保存: {excel_path}")
        else:
            print(f"   保存绩效结果失败")

        # 保存每只基金的详细绩效表格（产品及基准收益率/周收益率曲线/月度收益率）
        if fund_ids:
            for fund_id in fund_ids:
                try:
                    benchmark_id = config.get_fund_benchmark(fund_id) if hasattr(config, 'get_fund_benchmark') else config.DEFAULT_BENCHMARK
                    comparison_indices = config.get_fund_comparison_indices(fund_id) if hasattr(config, 'get_fund_comparison_indices') else None
                    detail_path = output_mgr.get_path('excel_performance', f"{fund_id}_detailed_performance.xlsx")
                    ok = analyzer.save_detailed_performance_to_excel(
                        fund_id,
                        output_path=str(detail_path),
                        benchmark_id=benchmark_id,
                        comparison_indices=comparison_indices
                    )
                    if ok:
                        print(f"   详细绩效已保存: {detail_path.name}")
                except Exception as e:
                    logger.error(f"保存基金 {fund_id} 详细绩效失败: {e}")
    
    return results


def analyze_holding_periods(fund_ids, periods, output_mgr, output_html, organize_by_fund):
    """分析持有期收益"""
    simulator = HoldingSimulation(
        risk_free_rate=config.RISK_FREE_RATE,
        trading_days=config.TRADING_DAYS
    )
    
    # 创建可视化器（使用输出管理器）
    visualizer = FundVisualizer(output_manager=output_mgr)
    
    all_results = {}
    
    if not fund_ids:
        print("没有指定基金，跳过持有期分析")
        return all_results
    
    print(f"\n📊 分析持有期收益（{len(periods)} 个持有期）...")

    for fund_id in fund_ids:
        print(f"\n🔍 分析基金持有期: {fund_id}")
        
        try:
            # 获取基金名称用于图表
            funds_df = fund_db.get_all_funds()
            fund_name = funds_df.loc[funds_df['fund_id'] == fund_id, 'name'].iloc[0] \
                if not funds_df.empty and fund_id in funds_df['fund_id'].values else fund_id
            
            # 分析持有期
            analysis = simulator.analyze_fund_holding(
                fund_id,
                holding_periods=periods,
                benchmark_ids=config.BENCHMARK_IDS
            )
            
            if not analysis:
                print(f"  ✗ 持有期分析失败: 数据不足")
                continue
            
            all_results[fund_id] = analysis
            
            # 显示汇总信息
            summary = analysis.get('summary', {})
            print(f"  ✓ 持有期分析完成:")
            for holding_days in periods:
                if holding_days in summary:
                    stats = summary[holding_days]
                    print(f"    {holding_days}天: "
                          f"平均{stats['mean_return']:.2%}, "
                          f"胜率{stats['win_rate']:.2%}")
            
            # 为这个基金创建专门的目录（如果启用）
            if organize_by_fund:
                # 提前创建基金目录
                from src.utils.output_manager import create_fund_output_dirs
                create_fund_output_dirs(fund_id, 'db_analysis')
            
            # 生成持有期分布图
            print(f"  🎨 生成持有期分布图...")
            simulation_results = analysis.get('simulation_results', {})
            
            for holding_days in periods:
                results_df = simulation_results.get(holding_days, pd.DataFrame())
                
                # 获取基准收益率
                benchmark_returns = {}
                if config.BENCHMARK_IDS:
                    for benchmark_id in config.BENCHMARK_IDS:
                        if hasattr(config, 'normalize_index_code'):
                            actual_code = config.normalize_index_code(benchmark_id)
                        else:
                            actual_code = benchmark_id
                        benchmark_name = config.get_benchmark_display_name(benchmark_id)
                        returns = simulator.get_benchmark_returns(actual_code, holding_days)
                        if not returns.empty:
                            benchmark_returns[benchmark_name] = returns
                
                # 生成静态图
                if not results_df.empty:
                    # 使用输出管理器获取路径
                    output_path = output_mgr.get_fund_plot_path(
                        fund_name, 'holding_dist', holding_days
                    )
                    
                    # 创建图形并保存
                    fig = visualizer.plot_return_distribution(
                        results_df, holding_days, fund_name, 
                        benchmark_returns, save_fig=False
                    )
                    
                    if fig:
                        fig.savefig(output_path, dpi=300, bbox_inches='tight')
                        plt.close(fig)
                        print(f"    ✓ 持有期{holding_days}天: {output_path.name}")
                
                # 生成交互图
                if output_html and not results_df.empty:
                    interactive_fig = visualizer.create_interactive_return_distribution(
                        results_df, holding_days, fund_name, benchmark_returns
                    )
                    if interactive_fig:
                        html_path = output_mgr.get_interactive_path(
                            fund_name, 'holding_dist', holding_days
                        )
                        interactive_fig.write_html(str(html_path))
                        print(f"    ✓ 交互图{holding_days}天: {html_path.name}")
            
            # 保存模拟结果到Excel（使用输出管理器）
            excel_path = output_mgr.get_path('excel_holding', f'holding_analysis_{fund_id}.xlsx')
            simulator.save_simulation_results(analysis, str(excel_path))
            print(f"  💾 持有期结果已保存: {excel_path.name}")
            
        except Exception as e:
            logger.error(f"分析基金 {fund_id} 持有期失败: {e}")
            print(f"  ✗ 持有期分析失败: {e}")

    return all_results


def generate_charts(fund_ids, output_mgr, output_html, organize_by_fund):
    """生成净值曲线和回撤图"""
    # 创建可视化器（使用输出管理器）
    visualizer = FundVisualizer(output_manager=output_mgr)

    if not fund_ids:
        return

    print(f"\n🎨 生成净值曲线和回撤图...")

    for fund_id in fund_ids:
        try:
            # 获取基金数据
            df = fund_db.get_fund_daily_data(fund_id)
            if df.empty:
                print(f"  ✗ {fund_id}: 没有数据")
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

            print(f"  📊 {fund_name}: {len(nav_series)} 个数据点")

            # 为这个基金创建专门的目录（如果启用）
            if organize_by_fund:
                from src.utils.output_manager import create_fund_output_dirs
                create_fund_output_dirs(fund_id, 'db_analysis')

            # 生成净值曲线图（使用配置的业绩基准）
            benchmark_id = config.get_fund_benchmark(fund_id) if hasattr(config, 'get_fund_benchmark') else '000300'
            actual_benchmark_id = config.normalize_index_code(benchmark_id) if hasattr(config, 'normalize_index_code') else benchmark_id
            benchmark_df = fund_db.get_index_daily_data(actual_benchmark_id)
            benchmark_nav = benchmark_df['close'] if not benchmark_df.empty else pd.Series(dtype=float)

            # 静态图
            nav_output_path = output_mgr.get_fund_plot_path(fund_name, 'nav_curve')
            fig = visualizer.plot_nav_curve(
                nav_series, fund_name, benchmark_nav, save_fig=False
            )
            if fig:
                fig.savefig(nav_output_path, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"    ✓ 净值曲线: {nav_output_path.name}")

            # 交互式净值曲线
            if output_html:
                interactive_nav = visualizer.create_interactive_nav_curve(
                    nav_series, fund_name, benchmark_nav
                )
                if interactive_nav:
                    html_path = output_mgr.get_interactive_path(fund_name, 'nav_curve')
                    interactive_nav.write_html(str(html_path))
                    print(f"    ✓ 交互净值曲线: {html_path.name}")

            # 生成回撤图
            drawdown_output_path = output_mgr.get_fund_plot_path(fund_name, 'drawdown')
            fig = visualizer.plot_drawdown_chart(
                nav_series, fund_name, save_fig=False
            )
            if fig:
                fig.savefig(drawdown_output_path, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"    ✓ 回撤图: {drawdown_output_path.name}")

            # 生成交互式净值+回撤图
            if output_html:
                interactive_drawdown = visualizer.create_interactive_chart(nav_series, fund_name)
                if interactive_drawdown:
                    html_path = output_mgr.get_interactive_path(fund_name, 'nav_drawdown')
                    interactive_drawdown.write_html(str(html_path))
                    print(f"    ✓ 交互净值回撤图: {html_path.name}")

            print(f"  ✓ {fund_name}: 图表生成完成")

        except Exception as e:
            logger.error(f"生成基金 {fund_id} 图表失败: {e}")
            print(f"  ✗ {fund_id}: 图表生成失败")


def generate_performance_comparison(fund_ids, output_mgr):
    """生成绩效对比图"""
    if not fund_ids or len(fund_ids) < 2:
        return

    try:
        print(f"\n📊 生成绩效指标对比图...")

        # 获取绩效数据
        analyzer = PerformanceAnalyzer()
        performance_data = []

        for fund_id in fund_ids:
            perf = analyzer.analyze_fund_performance(fund_id)
            if perf:
                performance_data.append(perf)

        if len(performance_data) < 2:
            print("   ⚠️  基金数量不足，跳过绩效对比")
            return

        perf_df = pd.DataFrame(performance_data)

        # 创建可视化器
        visualizer = FundVisualizer(output_manager=output_mgr)

        # 生成对比图
        output_path = output_mgr.get_path('plots', '绩效指标对比.png')
        fig = visualizer.plot_performance_comparison(perf_df, save_fig=False)

        if fig:
            fig.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close(fig)
            print(f"  ✓ 绩效对比图: {output_path.name}")

    except Exception as e:
        logger.error(f"生成绩效对比图失败: {e}")
        print(f"  ✗ 绩效对比图生成失败: {e}")


def create_summary_report(output_mgr, fund_count, index_count, holding_count):
    """创建分析摘要报告"""
    try:
        report_path = output_mgr.get_path('reports', '分析摘要.md')

        summary = f"""# 数据库分析报告

## 分析概况
- 分析时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- 输出目录: {output_mgr.dirs['base'].absolute()}

## 分析统计
- 分析基金数: {fund_count}
- 分析指数数: {index_count}
- 持有期分析基金数: {holding_count}

## 输出文件
### 绩效分析
- Excel文件: `excel/performance/performance_analysis.xlsx`
- CSV文件: `excel/performance/fund_performance.csv`

### 持有期分析
- 每个基金独立的Excel文件在 `excel/holding/` 目录

### 图表文件
- 净值曲线图: `plots/static/{{fund_name}}/净值曲线.png`
- 回撤分析图: `plots/static/{{fund_name}}/回撤分析.png`
- 持有期分布图: `plots/static/{{fund_name}}/holding/持有期N天_收益率分布.png`
- 交互式图表: `plots/interactive/{{fund_name}}/`

## 使用说明
所有输出文件已按类型和基金组织，便于查找和管理。

---
生成于: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(summary)

        print(f"📝 分析摘要已保存: {report_path}")

    except Exception as e:
        logger.error(f"创建摘要报告失败: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='仅从现有数据库进行分析（增强版）')

    # 数据选择参数
    parser.add_argument('--funds', nargs='+', help='基金代码列表（多个用空格分隔）')
    parser.add_argument('--indices', nargs='+', help='指数代码列表（多个用格分隔）')
    parser.add_argument('--all', action='store_true', help='分析所有基金和指数')

    # 分析参数
    parser.add_argument('--periods', nargs='+', type=int,
                       help='持有期列表（默认：30 60 90 180 360）')
    parser.add_argument('--no-holding', action='store_true',
                       help='跳过持有期分析')
    parser.add_argument('--no-charts', action='store_true',
                       help='跳过图表生成')
    parser.add_argument('--no-performance-chart', action='store_true',
                       help='跳绩效对比图生成')

    # 输出组织参数
    parser.add_argument('--organize-by-fund', action='store_true',
                       help='按基金组织文件（每个基金独立目录）')
    # 时间戳策略由 USE_TIMESTAMP 控制，不提供命令行开关，以保持与 main.py/analysis_from_excel.py 一致
    parser.add_argument('--clean-old', action='store_true',
                       help='清理7天前的旧文件')

    # 输出格式参数
    parser.add_argument('--output-html', type=lambda x: x.lower() == 'true',
                       help='是否输出交互式HTML图表（true/false）')
    parser.add_argument('--verbose', action='store_true',
                       help='显示详细日志')

    args = parser.parse_args()

    print("🚀 开始数据库分析（增强版）")
    print("=" * 60)

    # 检查数据库
    db_path = Path(config.DATABASE_PATH)
    if not db_path.exists():
        print(f"❌ 数据库不存在: {db_path}")
        print("   请先运行 update_db.py 或 main.py 获取数据")
        return 1

    # 设置输出管理器 - 恢复使用setup_output_directories函数
    output_mgr = setup_output_directories(args)

    # 配置日志到指定目录
    configure_logging(output_mgr.get_path('logs', 'run_analysis.log'), args.verbose)

    # 获取数据列表
    def get_all_fund_ids():
        funds_df = fund_db.get_all_funds()
        return funds_df['fund_id'].tolist() if not funds_df.empty else []

    def get_all_index_ids():
        indices_df = fund_db.get_all_indices()
        return indices_df['index_id'].tolist() if not indices_df.empty else []

    # 处理数据选择
    if args.all:
        args.funds = get_all_fund_ids()
        args.indices = get_all_index_ids()
        print(f"将分析所有基金: {len(args.funds) if args.funds else 0} 只")
        print(f"将分析所有指数: {len(args.indices) if args.indices else 0} 个")
    elif not args.funds and not args.indices:
        # 默认分析所有基金
        args.funds = get_all_fund_ids()
        print(f"将分析所有基金: {len(args.funds) if args.funds else 0} 只")

    # 处理持有期参数
    if args.periods is None:
        args.periods = config.HOLDING_PERIODS

    # 处理HTML输出
    if args.output_html is None:
        args.output_html = config.OUTPUT_HTML_NAV_CURVE  # 使用配置默认值

    # 1. 分析绩效指标
    performance_results = analyze_performance(
        args.funds, 
        args.indices, 
        output_mgr
    )

    # 2. 分析持有期收益
    holding_results = {}
    if not args.no_holding and args.funds:
        holding_results = analyze_holding_periods(
            args.funds,
            args.periods,
            output_mgr,
            args.output_html,
            args.organize_by_fund
        )

    # 3. 生成图表
    if not args.no_charts and args.funds:
        generate_charts(
            args.funds, 
            output_mgr, 
            args.output_html,
            args.organize_by_fund
        )

    # 4. 生成绩效对比图
    if not args.no_performance_chart and args.funds and len(args.funds) > 1:
        generate_performance_comparison(args.funds, output_mgr)

    # 5. 创建摘要报告
    create_summary_report(
        output_mgr,
        len(args.funds) if args.funds else 0,
        len(args.indices) if args.indices else 0,
        len(holding_results)
    )

    # 输出总结
    print("\n" + "=" * 60)
    print("✅ 数据库分析完成！")
    print(f"   输出目录: {output_mgr.dirs['base'].absolute()}")
    print(f"   分析基金: {len(args.funds) if args.funds else 0} 只")
    print(f"   分析指数: {len(args.indices) if args.indices else 0} 个")
    print(f"   持有期分析: {len(holding_results)} 只基金")

    if args.output_html:
        print(f"   交互式图表: 已生成（见 interactive/ 目录）")

    if args.organize_by_fund:
        print(f"   文件组织: 按基金分类")

    print("\n📁 查看输出:")
    output_mgr.print_summary()

    return 0


if __name__ == "__main__":
    import matplotlib.pyplot as plt
    sys.exit(main())