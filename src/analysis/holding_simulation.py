"""
持有期收益模拟模块
模拟不同持有期的投资收益
"""
import pandas as pd
import numpy as np
import warnings
import logging
from typing import Dict, List, Any

from ..utils.database import fund_db
from .performance import PerformanceAnalyzer

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

# 中文列名映射（输出友好）
SIM_DETAIL_CN_COLUMNS = {
    'buy_date': '买入日期',
    'sell_date': '卖出日期',
    'holding_days': '持有天数',
    'holding_return': '持有期收益率',
    'annual_return': '年化收益率',
    'annual_volatility': '年化波动率',
    'max_drawdown': '最大回撤',
    'sharpe_ratio': '夏普比率',
    'days_held': '实际持有天数'
}

SIM_SUMMARY_CN_COLUMNS = {
    'count': '模拟次数',
    'mean_return': '平均收益率',
    'median_return': '中位收益率',
    'std_return': '收益标准差',
    'min_return': '最小收益率',
    'max_return': '最大收益率',
    'positive_ratio': '正收益比例',
    'mean_sharpe': '平均夏普比率',
    'mean_max_dd': '平均最大回撤',
    'win_rate': '胜率',
    'holding_days': '持有天数',
    'type': '类型',
    'benchmark_id': '基准代码'
}


def _rename_simulation_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """安全地按映射重命名列"""
    if df is None or df.empty:
        return df
    rename_dict = {col: cn for col, cn in mapping.items() if col in df.columns}
    return df.rename(columns=rename_dict)


class HoldingSimulation:
    """持有期收益模拟类"""
    
    def __init__(self, risk_free_rate: float = 0.02, trading_days: int = 252):
        """
        初始化持有期模拟器
        
        Args:
            risk_free_rate: 年化无风险利率
            trading_days: 年化交易日数
        """
        self.risk_free_rate = risk_free_rate
        self.trading_days = trading_days
        self.analyzer = PerformanceAnalyzer(risk_free_rate, trading_days)

    def simulate_holding_period(self, nav_series: pd.Series, 
                                holding_days: int,
                                min_data_points: int = 10) -> pd.DataFrame:
        """
        模拟单一持有期的收益
        
        Args:
            nav_series: 净值序列
            holding_days: 持有天数
            min_data_points: 最小数据点数
            
        Returns:
            模拟结果DataFrame
        """
        # 为360天持有期调整最小数据点数要求
        required_points = holding_days + min_data_points
        if holding_days == 360:
            # 对于360天持有期，只需要比持有天数多一点的数据
            required_points = holding_days + 5
        
        if len(nav_series) < required_points:
            logger.warning(f"数据不足: 需要至少 {required_points} 个数据点，但只有 {len(nav_series)} 个数据点，持有期: {holding_days}天")
            return pd.DataFrame()
        
        results = []
        dates = nav_series.index
        
        # 遍历所有可能的买入点
        for i in range(len(nav_series) - holding_days):
            buy_date = dates[i]
            sell_date = dates[i + holding_days]
            
            # 获取持有期内的净值
            holding_nav = nav_series.iloc[i:i + holding_days + 1]
            
            # 计算持有期收益率
            holding_return = (holding_nav.iloc[-1] / holding_nav.iloc[0]) - 1
            
            # 计算持有期最大回撤
            holding_max_dd, _, _ = self.analyzer.calculate_max_drawdown(holding_nav)
            
            # 计算持有期年化收益率
            days_held = (sell_date - buy_date).days
            if days_held > 0:
                annual_return = (1 + holding_return) ** (self.trading_days / days_held) - 1
            else:
                annual_return = 0
            
            # 计算持有期年化波动率
            holding_returns = holding_nav.pct_change().dropna()
            if len(holding_returns) > 1:
                daily_vol = holding_returns.std()
                annual_vol = daily_vol * np.sqrt(self.trading_days)
                
                # 计算持有期夏普比率
                if annual_vol > 0:
                    sharpe_ratio = (annual_return - self.risk_free_rate * (holding_days / self.trading_days)) / annual_vol
                else:
                    sharpe_ratio = 0
            else:
                annual_vol = 0
                sharpe_ratio = 0
            
            # 记录结果
            results.append({
                'buy_date': buy_date,
                'sell_date': sell_date,
                'holding_days': holding_days,
                'holding_return': holding_return,
                'annual_return': annual_return,
                'annual_volatility': annual_vol,
                'max_drawdown': holding_max_dd,
                'sharpe_ratio': sharpe_ratio,
                'days_held': days_held
            })
        
        if results:
            return pd.DataFrame(results)
        else:
            return pd.DataFrame()
    
    def simulate_multiple_periods(self, nav_series: pd.Series,
                                 holding_periods: List[int] = None) -> Dict[int, pd.DataFrame]:
        """
        模拟多个持有期的收益
        
        Args:
            nav_series: 净值序列
            holding_periods: 持有天数列表，默认[30, 60, 90, 180, 360]
            
        Returns:
            各持有期模拟结果字典
        """
        if holding_periods is None:
            holding_periods = [30, 60, 90, 180, 360]
        
        results = {}
        
        for holding_days in holding_periods:
            logger.info(f"模拟持有期: {holding_days} 天")
            
            # 使用tqdm显示进度条
            sim_result = self.simulate_holding_period(nav_series, holding_days)
            
            if not sim_result.empty:
                results[holding_days] = sim_result
                logger.info(f"  完成: {len(sim_result)} 次模拟")
            else:
                logger.warning(f"  持有期 {holding_days} 天 模拟失败或无数据")
        
        return results
    
    def calculate_holding_returns(self, benchmark_id: str, holding_days: int) -> pd.Series:
        """
        计算基准指数的持有期收益率序列
        
        Args:
            benchmark_id: 基准指数ID
            holding_days: 持有天数
            
        Returns:
            持有期收益率序列
        """
        # 获取指数数据
        df = fund_db.get_index_daily_data(benchmark_id)
        
        if df.empty:
            logger.warning(f"基准指数数据为空: {benchmark_id}")
            return pd.Series()
        
        # 使用收盘价作为净值
        nav_series = df['close'].dropna()
        
        if len(nav_series) < holding_days + 10:
            logger.warning(f"基准指数数据不足: {benchmark_id}, 只有 {len(nav_series)} 个数据点")
            return pd.Series()
        
        # 计算持有期收益率
        returns = []
        dates = nav_series.index
        
        for i in range(len(nav_series) - holding_days):
            buy_nav = nav_series.iloc[i]
            sell_nav = nav_series.iloc[i + holding_days]
            holding_return = (sell_nav / buy_nav) - 1
            returns.append(holding_return)
        
        return pd.Series(returns)
    
    def analyze_fund_holding(self, fund_id: str,
                            holding_periods: List[int] = None,
                            use_cumulative: bool = True,
                            benchmark_ids: List[str] = None) -> Dict[str, Any]:
        """
        分析单只基金的持有期收益
        
        Args:
            fund_id: 基金代码
            holding_periods: 持有期列表
            use_cumulative: 是否使用累计净值
            benchmark_ids: 基准指数ID列表（使用config.BENCHMARK_IDS）
            
        Returns:
            持有期分析结果
        """
        if holding_periods is None:
            holding_periods = [30, 60, 90, 180, 360]
        
        if benchmark_ids is None:
            # 默认使用四个基准指数
            import config
            benchmark_ids = config.BENCHMARK_IDS
        
        # 获取基金数据
        df = fund_db.get_fund_daily_data(fund_id)
        
        if df.empty:
            logger.error(f"基金数据为空: {fund_id}")
            return {}
        
        # 选择净值序列
        if use_cumulative and 'cumulative_nav' in df.columns and not df['cumulative_nav'].isna().all():
            nav_series = df['cumulative_nav'].dropna()
            nav_type = 'cumulative'
        else:
            nav_series = df['nav'].dropna()
            nav_type = 'nav'
        
        if len(nav_series) < 100:  # 至少需要100个数据点
            logger.warning(f"基金数据不足: {fund_id}, 只有 {len(nav_series)} 个数据点")
            return {}
        
        logger.info(f"分析基金持有期: {fund_id}, 数据点数: {len(nav_series)}")
        
        # 模拟多个持有期
        simulation_results = self.simulate_multiple_periods(nav_series, holding_periods)
        
        # 汇总统计
        summary = {}
        for holding_days, results_df in simulation_results.items():
            if not results_df.empty:
                summary[holding_days] = {
                    'count': len(results_df),
                    'mean_return': results_df['holding_return'].mean(),
                    'median_return': results_df['holding_return'].median(),
                    'std_return': results_df['holding_return'].std(),
                    'min_return': results_df['holding_return'].min(),
                    'max_return': results_df['holding_return'].max(),
                    'positive_ratio': (results_df['holding_return'] > 0).mean(),
                    'mean_sharpe': results_df['sharpe_ratio'].mean(),
                    'mean_max_dd': results_df['max_drawdown'].mean(),
                    'win_rate': (results_df['holding_return'] > 0).mean()
                }
        
        # 计算所有基准指数的收益率序列
        benchmark_results = {}
        for benchmark_id in benchmark_ids:
            # 获取实际指数代码
            import config
            if hasattr(config, 'normalize_index_code'):
                actual_code = config.normalize_index_code(benchmark_id)
            else:
                actual_code = benchmark_id
            
            bench_results = {}
            for holding_days in holding_periods:
                bench_returns = self.calculate_holding_returns(actual_code, holding_days)
                if not bench_returns.empty:
                    # 计算基准统计量
                    bench_results[holding_days] = {
                        'count': len(bench_returns),
                        'mean_return': bench_returns.mean(),
                        'median_return': bench_returns.median(),
                        'std_return': bench_returns.std(),
                        'min_return': bench_returns.min(),
                        'max_return': bench_returns.max(),
                        'positive_ratio': (bench_returns > 0).mean(),
                        'win_rate': (bench_returns > 0).mean(),
                        'returns_series': bench_returns  # 保存完整的收益率序列
                    }
            benchmark_results[benchmark_id] = bench_results
        
        # 返回完整结果
        return {
            'fund_id': fund_id,
            'nav_type': nav_type,
            'total_days': len(nav_series),
            'start_date': nav_series.index[0],
            'end_date': nav_series.index[-1],
            'simulation_results': simulation_results,
            'summary': summary,
            'benchmark_results': benchmark_results,
            'benchmark_ids': benchmark_ids
        }
    
    def analyze_index_holding(self, index_id: str,
                            holding_periods: List[int] = None) -> Dict[str, Any]:
        """
        分析指数的持有期收益
        
        Args:
            index_id: 指数代码
            holding_periods: 持有期列表
            
        Returns:
            持有期分析结果
        """
        # 获取指数数据
        df = fund_db.get_index_daily_data(index_id)
        
        if df.empty:
            logger.error(f"指数数据为空: {index_id}")
            return {}
        
        # 使用收盘价作为净值
        nav_series = df['close'].dropna()
        
        if len(nav_series) < 100:
            logger.warning(f"指数数据不足: {index_id}, 只有 {len(nav_series)} 个数据点")
            return {}
        
        logger.info(f"分析指数持有期: {index_id}, 数据点数: {len(nav_series)}")
        
        # 模拟多个持有期
        simulation_results = self.simulate_multiple_periods(nav_series, holding_periods)
        
        # 汇总统计
        summary = {}
        for holding_days, results_df in simulation_results.items():
            if not results_df.empty:
                summary[holding_days] = {
                    'count': len(results_df),
                    'mean_return': results_df['holding_return'].mean(),
                    'median_return': results_df['holding_return'].median(),
                    'std_return': results_df['holding_return'].std(),
                    'min_return': results_df['holding_return'].min(),
                    'max_return': results_df['holding_return'].max(),
                    'positive_ratio': (results_df['holding_return'] > 0).mean(),
                    'mean_sharpe': results_df['sharpe_ratio'].mean(),
                    'mean_max_dd': results_df['max_drawdown'].mean(),
                    'win_rate': (results_df['holding_return'] > 0).mean()
                }
        
        # 返回完整结果
        return {
            'index_id': index_id,
            'total_days': len(nav_series),
            'start_date': nav_series.index[0],
            'end_date': nav_series.index[-1],
            'simulation_results': simulation_results,
            'summary': summary
        }
    
    def save_simulation_results(self, simulation_results: Dict[str, Any],
                               output_path: str = "holding_simulation_summary.xlsx"):
        """
        保存持有期模拟结果到Excel
        
        Args:
            simulation_results: 模拟结果字典
            output_path: 输出文件路径
        """
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 保存每个持有期的详细结果
                for holding_days, results_df in simulation_results.get('simulation_results', {}).items():
                    if not results_df.empty:
                        sheet_name = f"Holding_{holding_days}d"
                        # 截断sheet名称，确保不超过31个字符
                        if len(sheet_name) > 31:
                            sheet_name = sheet_name[:31]
                        output_df = _rename_simulation_columns(results_df.copy(), SIM_DETAIL_CN_COLUMNS)
                        output_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 保存基金汇总统计
                summary = simulation_results.get('summary', {})
                if summary:
                    summary_data = []
                    for holding_days, stats in summary.items():
                        stats['holding_days'] = holding_days
                        stats['type'] = 'fund'
                        summary_data.append(stats)
                    
                    summary_df = pd.DataFrame(summary_data)
                    summary_df = _rename_simulation_columns(summary_df, SIM_SUMMARY_CN_COLUMNS)
                    summary_df.to_excel(writer, sheet_name='基金汇总统计', index=False)
                
                # 保存基准汇总统计
                benchmark_results = simulation_results.get('benchmark_results', {})
                if benchmark_results:
                    for benchmark_id, bench_stats in benchmark_results.items():
                        benchmark_data = []
                        for holding_days, stats in bench_stats.items():
                            stats['holding_days'] = holding_days
                            stats['benchmark_id'] = benchmark_id
                            stats['type'] = 'benchmark'
                            # 移除可能导致写入错误的序列字段
                            stats = {k: v for k, v in stats.items() if k != 'returns_series'}
                            benchmark_data.append(stats)
                        
                        if benchmark_data:
                            benchmark_df = pd.DataFrame(benchmark_data)
                            benchmark_df = _rename_simulation_columns(benchmark_df, SIM_SUMMARY_CN_COLUMNS)
                            sheet_name = f"基准_{benchmark_id}"
                            if len(sheet_name) > 31:
                                sheet_name = sheet_name[:31]
                            benchmark_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                logger.info(f"持有期模拟结果已保存到: {output_path}")
                return True
                
        except Exception as e:
            logger.error(f"保存模拟结果失败: {e}")
            return False
    
    def get_benchmark_returns(self, index_id: str, holding_days: int) -> pd.Series:
        """
        获取基准指数的持有期收益率
        
        Args:
            index_id: 指数代码
            holding_days: 持有天数
            
        Returns:
            持有期收益率序列
        """
        # 获取指数数据
        df = fund_db.get_index_daily_data(index_id)
        
        if df.empty:
            return pd.Series()
        
        nav_series = df['close'].dropna()
        
        if len(nav_series) < holding_days + 10:
            return pd.Series()
        
        # 计算持有期收益率
        returns = []
        dates = nav_series.index
        
        for i in range(len(nav_series) - holding_days):
            buy_nav = nav_series.iloc[i]
            sell_nav = nav_series.iloc[i + holding_days]
            holding_return = (sell_nav / buy_nav) - 1
            returns.append(holding_return)
        
        return pd.Series(returns)


def main():
    """主函数，用于测试"""
    # 创建持有期模拟器
    simulator = HoldingSimulation()
    
    # 测试单只基金持有期分析（带基准）
    test_fund = "000001.OF"
    print(f"测试分析基金持有期: {test_fund}")
    
    holding_analysis = simulator.analyze_fund_holding(
        test_fund, 
        holding_periods=[30, 60, 90],
        benchmark_ids=['000300', '000905']  # 沪深300和中证500
    )
    
    if holding_analysis:
        print(f"基金: {holding_analysis['fund_id']}")
        print(f"数据时间范围: {holding_analysis['start_date']} 到 {holding_analysis['end_date']}")
        print(f"总数据点数: {holding_analysis['total_days']}")
        print(f"基准指数: {holding_analysis['benchmark_ids']}")
        
        summary = holding_analysis.get('summary', {})
        for holding_days, stats in summary.items():
            print(f"\n持有期 {holding_days} 天:")
            print(f"  模拟次数: {stats['count']}")
            print(f"  平均收益率: {stats['mean_return']:.4%}")
            print(f"  中位数收益率: {stats['median_return']:.4%}")
            print(f"  正收益比例: {stats['positive_ratio']:.2%}")
            print(f"  平均夏普比率: {stats['mean_sharpe']:.4f}")
            print(f"  平均最大回撤: {stats['mean_max_dd']:.4%}")
        
        # 打印基准对比
        benchmark_results = holding_analysis.get('benchmark_results', {})
        for benchmark_id, bench_stats in benchmark_results.items():
            print(f"\n基准指数 {benchmark_id}:")
            for holding_days, stats in bench_stats.items():
                print(f"  持有期 {holding_days} 天:")
                print(f"    平均收益率: {stats['mean_return']:.4%}")
                print(f"    正收益比例: {stats['positive_ratio']:.2%}")
        
        # 保存结果
        print(f"\n保存模拟结果...")
        success = simulator.save_simulation_results(holding_analysis, 
                                                   f"holding_simulation_{test_fund}.xlsx")
        if success:
            print("✅ 结果保存成功")
    
    # 测试指数持有期分析
    print(f"\n测试分析指数持有期: INDEX_SSE")
    index_analysis = simulator.analyze_index_holding('INDEX_SSE', holding_periods=[30, 60, 90])
    
    if index_analysis:
        print(f"指数: {index_analysis['index_id']}")
        print(f"数据时间范围: {index_analysis['start_date']} 到 {index_analysis['end_date']}")
        
        summary = index_analysis.get('summary', {})
        for holding_days, stats in summary.items():
            print(f"\n持有期 {holding_days} 天:")
            print(f"  模拟次数: {stats['count']}")
            print(f"  平均收益率: {stats['mean_return']:.4%}")
            print(f"  正收益比例: {stats['positive_ratio']:.2%}")


if __name__ == "__main__":
    main()