# 修改文件：src/analysis/performance.py
# 修改内容：增加多频率计算功能和三个Excel输出表格

"""
基金绩效分析模块
计算各种绩效指标
"""
import pandas as pd
import numpy as np
from scipy import stats
import warnings
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional, Tuple
from ..utils.database import fund_db
from ..utils.excel_style_config import (
    apply_product_sheet_style,
    apply_weekly_sheet_style,
    apply_monthly_sheet_style,
)

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)

# 导入配置
try:
    import config
    DEFAULT_BENCHMARK = getattr(config, 'DEFAULT_BENCHMARK', '000300')
    DEFAULT_COMPARISON_INDICES = getattr(
        config, 'DEFAULT_COMPARISON_INDICES', ['000300', '000001', 'HSI']
    )

    def _get_fund_benchmark_id(fund_id: str) -> str:
        if hasattr(config, 'get_fund_benchmark'):
            return config.get_fund_benchmark(fund_id)
        return DEFAULT_BENCHMARK

    def _get_fund_comparison_indices(fund_id: str) -> List[str]:
        if hasattr(config, 'get_fund_comparison_indices'):
            return config.get_fund_comparison_indices(fund_id)
        return DEFAULT_COMPARISON_INDICES

    def _get_fund_monthly_comparison(fund_id: str) -> List[str]:
        """
        获取用于月度表的对比指数列表（优先使用 config 中的专用配置）。
        返回列表，即使配置为单个字符串也会以列表形式返回。
        """
        if hasattr(config, 'get_fund_monthly_comparison'):
            try:
                val = config.get_fund_monthly_comparison(fund_id)
                if isinstance(val, (list, tuple)):
                    return list(val)
                if val:
                    return [val]
            except Exception:
                logger.debug(f"调用 config.get_fund_monthly_comparison 失败: {fund_id}")
        # 回退到通用的比较列表
        return _get_fund_comparison_indices(fund_id)

    def _normalize_index_code(index_id: str) -> str:
        if hasattr(config, 'normalize_index_code'):
            return config.normalize_index_code(index_id)
        return index_id

    def _resolve_index_id(index_id: str) -> str:
        legacy_map = {
            'INDEX_SSE': '000001',
            'INDEX_HS300': '000300',
            'INDEX_HSI': 'HSI',
            'INDEX_MED_INNOV': 'MED_INNOV'
        }
        if index_id in legacy_map:
            return legacy_map[index_id]
        return _normalize_index_code(index_id)

    def _safe_diff(left: Optional[float], right: Optional[float]) -> float:
        if left is None or right is None:
            return np.nan
        try:
            if pd.isna(left) or pd.isna(right):
                return np.nan
        except Exception:
            pass
        return left - right

except Exception as e:
    logger.warning(f"无法导入配置，使用默认设置: {e}")
    DEFAULT_BENCHMARK = '000300'
    DEFAULT_COMPARISON_INDICES = ['000300', '000001', 'HSI']

    def _get_fund_benchmark_id(fund_id: str) -> str:
        return DEFAULT_BENCHMARK

    def _get_fund_comparison_indices(fund_id: str) -> List[str]:
        return DEFAULT_COMPARISON_INDICES

    def _normalize_index_code(index_id: str) -> str:
        return index_id

    def _resolve_index_id(index_id: str) -> str:
        legacy_map = {
            'INDEX_SSE': '000001',
            'INDEX_HS300': '000300',
            'INDEX_HSI': 'HSI',
            'INDEX_MED_INNOV': 'MED_INNOV'
        }
        return legacy_map.get(index_id, index_id)

    def _safe_diff(left: Optional[float], right: Optional[float]) -> float:
        if left is None or right is None:
            return np.nan
        try:
            if pd.isna(left) or pd.isna(right):
                return np.nan
        except Exception:
            pass
        return left - right

# 英文 -> 中文列名映射，用于导出报表
CHINESE_PERFORMANCE_COLUMNS = {
    'fund_id': '标的代码',
    'frequency': '频率',
    'frequency_cn': '频率(中文)',
    'return_frequency': '收益频率',
    'risk_frequency': '风险频率',
    'total_return': '总收益率',
    'annual_return': '年化收益率',
    'annual_volatility': '年化波动率',
    'max_drawdown': '最大回撤',
    'max_drawdown_start': '回撤开始',
    'max_drawdown_end': '回撤结束',
    'sharpe_ratio': '夏普比率',
    'calmar_ratio': 'Calmar比率',
    'start_date': '开始日期',
    'end_date': '结束日期',
    'periods': '样本点数',
    'nav_type': '净值类型',
    'actual_index_id': '实际指数代码'
}

# 频率映射
FREQUENCY_MAPPING = {
    'daily': '日',
    'weekly': '周',
    'monthly': '月',
    'quarterly': '季度',
    'yearly': '年度'
}

# 频率对应的交易日数
FREQUENCY_TRADING_DAYS = {
    'daily': 252,    # 年化交易日数
    'weekly': 52,    # 年化周数
    'monthly': 12,   # 年化月数
    'quarterly': 4,  # 年化季度数
    'yearly': 1      # 年化年数
}

DEFAULT_RETURN_FREQUENCY = 'daily'
DEFAULT_RISK_FREQUENCY = 'weekly'


def _rename_for_output(df: pd.DataFrame) -> pd.DataFrame:
    """将绩效DataFrame列名替换为中文，保持底层计算不变"""
    if df is None or df.empty:
        return df
    rename_dict = {col: cn for col, cn in CHINESE_PERFORMANCE_COLUMNS.items() if col in df.columns}
    return df.rename(columns=rename_dict)


def _normalize_week_end_day(week_end_day: str) -> str:
    if not week_end_day:
        return 'FRI'
    mapping = {
        'monday': 'MON', 'mon': 'MON', '1': 'MON',
        'tuesday': 'TUE', 'tue': 'TUE', '2': 'TUE',
        'wednesday': 'WED', 'wed': 'WED', '3': 'WED',
        'thursday': 'THU', 'thu': 'THU', '4': 'THU',
        'friday': 'FRI', 'fri': 'FRI', '5': 'FRI',
        'saturday': 'SAT', 'sat': 'SAT', '6': 'SAT',
        'sunday': 'SUN', 'sun': 'SUN', '7': 'SUN'
    }
    key = str(week_end_day).strip().lower()
    return mapping.get(key, str(week_end_day).strip().upper())


def _month_code(month: int) -> str:
    months = [
        'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
        'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
    ]
    if isinstance(month, int) and 1 <= month <= 12:
        return months[month - 1]
    if isinstance(month, str):
        return month.strip().upper()[:3]
    return 'DEC'


def _normalize_month_rule(month_rule: str) -> str:
    if not month_rule:
        return 'ME'
    rule = str(month_rule).strip().upper()
    mapping = {
        'M': 'ME',
        'ME': 'ME',
        'BM': 'BME',
        'BME': 'BME',
        'Q': 'QE',
        'QE': 'QE',
        'A': 'YE',
        'Y': 'YE',
        'YE': 'YE'
    }
    return mapping.get(rule, rule)


def _get_resample_rule(
    frequency: str,
    week_end_day: str = 'FRI',
    month_end_rule: str = 'ME',
    quarter_end_month: int = 12,
    year_end_month: int = 12,
    frequency_rules: Optional[Dict[str, str]] = None
) -> str:
    if frequency_rules and frequency in frequency_rules:
        return frequency_rules[frequency]
    if frequency == 'weekly':
        return f"W-{_normalize_week_end_day(week_end_day)}"
    if frequency == 'monthly':
        return _normalize_month_rule(month_end_rule)
    if frequency == 'quarterly':
        return f"Q-{_month_code(quarter_end_month)}"
    if frequency == 'yearly':
        return f"A-{_month_code(year_end_month)}"
    return 'D'


def _resample_data(
    nav_series: pd.Series,
    frequency: str = 'daily',
    method: str = 'last',
    week_end_day: str = 'FRI',
    month_end_rule: str = 'ME',
    quarter_end_month: int = 12,
    year_end_month: int = 12,
    frequency_rules: Optional[Dict[str, str]] = None
) -> pd.Series:
    """
    将净值数据重采样到指定频率
    
    Args:
        nav_series: 净值序列（日度数据）
        frequency: 重采样频率，可选：'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
        method: 重采样方法，'last' 或 'cum_return'
        week_end_day: 周度周期结束日（如 'FRI' / 'Friday'）
        month_end_rule: 月度重采样规则（如 'M' / 'BM'）
        quarter_end_month: 季度年末月（1-12，对应 Q-XXX）
        year_end_month: 年度年末月（1-12，对应 A-XXX）
        frequency_rules: 允许外部直接传入 frequency -> rule 的映射
        
    Returns:
        重采样后的净值序列
    """
    if nav_series.empty:
        return nav_series
    
    # 确保索引是datetime类型
    nav_series.index = pd.to_datetime(nav_series.index)
    nav_series = nav_series.sort_index()
    
    if frequency == 'daily':
        return nav_series

    rule = _get_resample_rule(
        frequency=frequency,
        week_end_day=week_end_day,
        month_end_rule=month_end_rule,
        quarter_end_month=quarter_end_month,
        year_end_month=year_end_month,
        frequency_rules=frequency_rules
    )

    if method == 'cum_return':
        returns = nav_series.pct_change().dropna()
        if returns.empty:
            return pd.Series(dtype=float)
        period_returns = returns.resample(rule).apply(lambda x: (1 + x).prod() - 1).dropna()
        if period_returns.empty:
            return pd.Series(dtype=float)
        base_nav = nav_series.iloc[0]
        resampled_nav = (1 + period_returns).cumprod() * base_nav
        return resampled_nav

    # 默认使用周期末数据
    return nav_series.resample(rule).last().dropna()


def _build_product_sheet_layout(returns_df: pd.DataFrame) -> pd.DataFrame:
    """将产品及基准收益率表格转换为样式化布局（行列与样本一致）。"""
    if returns_df is None or returns_df.empty:
        return pd.DataFrame()

    period_labels = returns_df['时间区间'].tolist() if '时间区间' in returns_df.columns else []
    titles = []
    ranges = []
    for label in period_labels:
        if isinstance(label, str) and '\n' in label:
            title, date_range = label.split('\n', 1)
        else:
            title, date_range = str(label), ''
        titles.append(title)
        ranges.append(date_range)

    metric_cols = [c for c in returns_df.columns if c != '时间区间']
    rows: List[List[Any]] = []
    rows.append([None] + titles)
    rows.append([None] + ranges)

    for col in metric_cols:
        label = str(col)
        if label.endswith('收益率'):
            label = label[:-3] + '收益'
        rows.append([label] + returns_df[col].tolist())

    return pd.DataFrame(rows)


class PerformanceAnalyzer:
    """基金绩效分析类"""
    
    def __init__(
        self,
        risk_free_rate: float = 0.02,
        trading_days: int = 252,
        default_return_frequency: str = DEFAULT_RETURN_FREQUENCY,
        default_risk_frequency: str = DEFAULT_RISK_FREQUENCY,
        resample_params: Optional[Dict[str, Any]] = None
    ):
        """
        初始化绩效分析器
        
        Args:
            risk_free_rate: 年化无风险利率，默认2%
            trading_days: 年化交易日数，默认252天
            default_return_frequency: 收益类指标默认频率
            default_risk_frequency: 风险类指标默认频率
            resample_params: 重采样参数（例如 week_end_day / month_end_rule 等）
        """
        self.risk_free_rate = risk_free_rate
        self.trading_days = trading_days
        self.default_return_frequency = default_return_frequency
        self.default_risk_frequency = default_risk_frequency
        self.resample_params = resample_params or {}
        
        # 存储计算结果的缓存
        self.results_cache = {}
    
    def _get_trading_days_for_frequency(self, frequency: str) -> int:
        """
        根据频率获取年化交易日数/周期数
        
        Args:
            frequency: 频率
            
        Returns:
            年化周期数
        """
        return FREQUENCY_TRADING_DAYS.get(frequency, self.trading_days)

    def _calculate_metrics_for_frequency(
        self,
        nav_series: pd.Series,
        frequency: str,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        按指定频率计算绩效指标
        """
        resample_params = resample_params or {}
        resampled_nav = _resample_data(nav_series, frequency, **resample_params)
        if len(resampled_nav) < 2:
            return None

        total_return = self.calculate_total_return(resampled_nav)
        annual_return = self.calculate_annual_return(resampled_nav, frequency)
        annual_volatility = self.calculate_annual_volatility(resampled_nav, frequency)
        max_drawdown, start_date, end_date = self.calculate_max_drawdown(resampled_nav)
        sharpe_ratio = self.calculate_sharpe_ratio(resampled_nav, frequency)
        calmar_ratio = self.calculate_calmar_ratio(resampled_nav, frequency)

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'annual_volatility': annual_volatility,
            'max_drawdown': max_drawdown,
            'max_drawdown_start': start_date,
            'max_drawdown_end': end_date,
            'sharpe_ratio': sharpe_ratio,
            'calmar_ratio': calmar_ratio,
            'start_date': resampled_nav.index[0],
            'end_date': resampled_nav.index[-1],
            'periods': len(resampled_nav)
        }

    def _get_fund_nav_series(self, fund_id: str) -> pd.Series:
        fund_df = fund_db.get_fund_daily_data(fund_id)
        if fund_df.empty:
            return pd.Series(dtype=float)
        if 'cumulative_nav' in fund_df.columns and not fund_df['cumulative_nav'].isna().all():
            return fund_df['cumulative_nav'].dropna()
        return fund_df['nav'].dropna()

    def _build_composite_nav_from_db(self, composite_id: str) -> pd.Series:
        if not hasattr(config, 'get_composite_components'):
            return pd.Series(dtype=float)
        components = config.get_composite_components(composite_id)
        if not components:
            return pd.Series(dtype=float)

        returns_df = None
        weights: Dict[str, float] = {}

        for comp in components:
            base_code = comp.get('base_code')
            if not base_code:
                continue
            base_code = _resolve_index_id(base_code)
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

    def _get_index_nav_series(self, index_id: str) -> pd.Series:
        actual_index_id = _resolve_index_id(index_id)
        index_df = fund_db.get_index_daily_data(actual_index_id)
        if not index_df.empty and 'close' in index_df.columns:
            return index_df['close'].dropna()
        if hasattr(config, 'is_composite_index') and config.is_composite_index(actual_index_id):
            return self._build_composite_nav_from_db(actual_index_id)
        return pd.Series(dtype=float)

    @staticmethod
    def _calc_period_return(series: pd.Series, start_date: pd.Timestamp, end_date: pd.Timestamp) -> Optional[float]:
        if series is None or series.empty:
            return None
        data = series.loc[(series.index >= start_date) & (series.index <= end_date)].dropna()
        if len(data) < 2:
            return None
        return (data.iloc[-1] / data.iloc[0]) - 1
    
    def calculate_returns(self, nav_series: pd.Series, frequency: str = 'daily') -> pd.Series:
        """
        计算收益率序列
        
        Args:
            nav_series: 净值序列
            frequency: 频率，用于计算年化时使用
            
        Returns:
            收益率序列
        """
        returns = nav_series.pct_change().dropna()
        return returns
    
    def calculate_total_return(self, nav_series: pd.Series) -> float:
        """
        计算总收益率
        
        Args:
            nav_series: 净值序列
            
        Returns:
            总收益率
        """
        if len(nav_series) < 2:
            return 0.0
        
        total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1
        return total_return
    
    def calculate_annual_return(self, nav_series: pd.Series, frequency: str = 'daily') -> float:
        """
        计算年化收益率
        
        Args:
            nav_series: 净值序列
            frequency: 频率，用于计算年化
            
        Returns:
            年化收益率
        """
        if len(nav_series) < 2:
            return 0.0
        
        total_return = self.calculate_total_return(nav_series)
        days = (nav_series.index[-1] - nav_series.index[0]).days
        
        if days <= 0:
            return 0.0
        
        # 根据频率调整年化因子
        annual_factor = self._get_trading_days_for_frequency(frequency)
        
        # 计算年数（对于日度数据使用天数，其他频率使用周期数）
        if frequency == 'daily':
            years = days / 365.0
        else:
            # 对于周度、月度等，使用周期数
            periods = len(nav_series) - 1
            years = periods / annual_factor
        
        if years <= 0:
            return 0.0
        
        annual_return = (1 + total_return) ** (1 / years) - 1
        return annual_return
    
    def calculate_annual_volatility(self, nav_series: pd.Series, frequency: str = 'daily') -> float:
        """
        计算年化波动率
        
        Args:
            nav_series: 净值序列
            frequency: 频率，用于计算年化
            
        Returns:
            年化波动率
        """
        if len(nav_series) < 2:
            return 0.0
        
        returns = self.calculate_returns(nav_series)
        if len(returns) < 2:
            return 0.0
        
        # 根据频率调整年化因子
        annual_factor = self._get_trading_days_for_frequency(frequency)
        
        # 计算周期波动率
        period_volatility = returns.std()
        
        # 根据频率计算年化波动率
        if frequency == 'daily':
            annual_volatility = period_volatility * np.sqrt(annual_factor)
        elif frequency == 'weekly':
            annual_volatility = period_volatility * np.sqrt(annual_factor)
        elif frequency == 'monthly':
            annual_volatility = period_volatility * np.sqrt(annual_factor)
        else:
            # 对于季度、年度等，使用简单的年化
            annual_volatility = period_volatility * np.sqrt(annual_factor)
        
        return annual_volatility
    
    def calculate_max_drawdown(self, nav_series: pd.Series) -> Tuple[float, pd.Timestamp, pd.Timestamp]:
        """
        计算最大回撤
        
        Args:
            nav_series: 净值序列
            
        Returns:
            (最大回撤值, 回撤开始日期, 回撤结束日期)
        """
        if len(nav_series) < 2:
            return 0.0, None, None
        
        # 计算累计最大值
        cummax = nav_series.cummax()
        
        # 计算回撤
        drawdown = (nav_series - cummax) / cummax
        
        # 找到最大回撤
        max_drawdown = drawdown.min()
        end_date = drawdown.idxmin()
        
        # 找到回撤开始日期（在结束日期之前的最高点）
        before_end = nav_series[:end_date]
        if len(before_end) > 0:
            start_date = before_end.idxmax()
        else:
            start_date = nav_series.index[0]
        
        return max_drawdown, start_date, end_date
    
    def calculate_sharpe_ratio(self, nav_series: pd.Series, frequency: str = 'daily') -> float:
        """
        计算夏普比率
        
        Args:
            nav_series: 净值序列
            frequency: 频率，用于计算年化
            
        Returns:
            夏普比率
        """
        if len(nav_series) < 2:
            return 0.0
        
        annual_return = self.calculate_annual_return(nav_series, frequency)
        annual_volatility = self.calculate_annual_volatility(nav_series, frequency)
        
        if annual_volatility == 0:
            return 0.0
        
        sharpe_ratio = (annual_return - self.risk_free_rate) / annual_volatility
        return sharpe_ratio
    
    def calculate_calmar_ratio(self, nav_series: pd.Series, frequency: str = 'daily') -> float:
        """
        计算Calmar比率
        
        Args:
            nav_series: 净值序列
            frequency: 频率，用于计算年化
            
        Returns:
            Calmar比率
        """
        if len(nav_series) < 2:
            return 0.0
        
        annual_return = self.calculate_annual_return(nav_series, frequency)
        max_drawdown, _, _ = self.calculate_max_drawdown(nav_series)
        
        if max_drawdown == 0:
            return 0.0
        
        calmar_ratio = annual_return / abs(max_drawdown)
        return calmar_ratio
    
    def analyze_fund_performance(
        self,
        fund_id: str,
        frequency: Optional[str] = None,
        return_frequency: Optional[str] = None,
        risk_frequency: Optional[str] = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        分析单只基金绩效
        
        Args:
            fund_id: 基金代码
            frequency: 计算频率，可选：'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
            return_frequency: 收益类指标频率（frequency为空时生效）
            risk_frequency: 风险类指标频率（frequency为空时生效）
            resample_params: 重采样参数
            
        Returns:
            绩效指标字典
        """
        cache_key = f"fund_{fund_id}_{frequency}_{return_frequency}_{risk_frequency}_{str(resample_params)}"
        if cache_key in self.results_cache:
            return self.results_cache[cache_key]
        
        # 获取基金数据
        df = fund_db.get_fund_daily_data(fund_id)
        
        if df.empty:
            logger.error(f"基金数据为空: {fund_id}")
            return {}
        
        # 根据需求文档要求，必须使用累计净值
        # 如果累计净值列不存在或全部为空，则使用单位净值（并记录警告）
        if 'cumulative_nav' in df.columns and not df['cumulative_nav'].isna().all():
            nav_series = df['cumulative_nav'].dropna()
            nav_type = 'cumulative'
            logger.info(f"使用累计净值分析基金: {fund_id}")
        else:
            nav_series = df['nav'].dropna()
            nav_type = 'nav'
            logger.warning(f"基金 {fund_id} 缺少累计净值数据，使用单位净值替代")
        
        if len(nav_series) < 2:
            logger.warning(f"净值数据不足: {fund_id}")
            return {}
        
        if frequency:
            metrics = self._calculate_metrics_for_frequency(
                nav_series,
                frequency,
                resample_params=resample_params or self.resample_params
            )
            if not metrics:
                logger.warning(f"重采样后数据不足: {fund_id}, 频率: {frequency}")
                return {}
            logger.info(f"分析基金绩效: {fund_id}, 频率: {frequency}, 使用净值类型: {nav_type}, 数据点数: {metrics['periods']}")
            results = {
                'fund_id': fund_id,
                'frequency': frequency,
                'frequency_cn': FREQUENCY_MAPPING.get(frequency, frequency),
                'nav_type': nav_type,
                **metrics
            }
        else:
            return_freq = return_frequency or self.default_return_frequency
            risk_freq = risk_frequency or self.default_risk_frequency
            return_metrics = self._calculate_metrics_for_frequency(
                nav_series,
                return_freq,
                resample_params=resample_params or self.resample_params
            )
            risk_metrics = self._calculate_metrics_for_frequency(
                nav_series,
                risk_freq,
                resample_params=resample_params or self.resample_params
            )
            if not return_metrics or not risk_metrics:
                logger.warning(f"重采样后数据不足: {fund_id}, 频率: {return_freq}/{risk_freq}")
                return {}
            logger.info(f"分析基金绩效: {fund_id}, 默认频率(收益{return_freq}/风险{risk_freq}), 使用净值类型: {nav_type}")
            results = {
                'fund_id': fund_id,
                'frequency': 'mixed',
                'frequency_cn': '混合',
                'return_frequency': return_freq,
                'risk_frequency': risk_freq,
                'total_return': return_metrics['total_return'],
                'annual_return': return_metrics['annual_return'],
                'annual_volatility': risk_metrics['annual_volatility'],
                'max_drawdown': risk_metrics['max_drawdown'],
                'max_drawdown_start': risk_metrics['max_drawdown_start'],
                'max_drawdown_end': risk_metrics['max_drawdown_end'],
                'sharpe_ratio': risk_metrics['sharpe_ratio'],
                'calmar_ratio': risk_metrics['calmar_ratio'],
                'start_date': return_metrics['start_date'],
                'end_date': return_metrics['end_date'],
                'periods': return_metrics['periods'],
                'nav_type': nav_type
            }
        
        # 缓存结果
        self.results_cache[cache_key] = results
        
        return results
    
    def analyze_index_performance(
        self,
        index_id: str,
        frequency: Optional[str] = None,
        return_frequency: Optional[str] = None,
        risk_frequency: Optional[str] = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        分析指数绩效
        
        Args:
            index_id: 指数代码（可以是虚拟ID或实际ID）
            frequency: 计算频率，可选：'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
            return_frequency: 收益类指标频率（frequency为空时生效）
            risk_frequency: 风险类指标频率（frequency为空时生效）
            resample_params: 重采样参数
            
        Returns:
            绩效指标字典
        """
        cache_key = f"index_{index_id}_{frequency}_{return_frequency}_{risk_frequency}_{str(resample_params)}"
        if cache_key in self.results_cache:
            return self.results_cache[cache_key]
        
        # 检查是否是需要映射的虚拟ID
        actual_index_id = _resolve_index_id(index_id)
        
        # 获取指数数据
        df = fund_db.get_index_daily_data(actual_index_id)
        
        if df.empty:
            logger.error(f"指数数据为空: {actual_index_id} (原始ID: {index_id})")
            return {}
        
        # 使用收盘价作为净值
        nav_series = df['close'].dropna()
        
        if len(nav_series) < 2:
            logger.warning(f"指数数据不足: {actual_index_id}")
            return {}
        
        if frequency:
            metrics = self._calculate_metrics_for_frequency(
                nav_series,
                frequency,
                resample_params=resample_params or self.resample_params
            )
            if not metrics:
                logger.warning(f"重采样后数据不足: {index_id}, 频率: {frequency}")
                return {}
            logger.info(f"分析指数绩效: {index_id} -> {actual_index_id}, 频率: {frequency}, 数据点数: {metrics['periods']}")
            results = {
                'fund_id': index_id,
                'frequency': frequency,
                'frequency_cn': FREQUENCY_MAPPING.get(frequency, frequency),
                'actual_index_id': actual_index_id,
                **metrics
            }
        else:
            return_freq = return_frequency or self.default_return_frequency
            risk_freq = risk_frequency or self.default_risk_frequency
            return_metrics = self._calculate_metrics_for_frequency(
                nav_series,
                return_freq,
                resample_params=resample_params or self.resample_params
            )
            risk_metrics = self._calculate_metrics_for_frequency(
                nav_series,
                risk_freq,
                resample_params=resample_params or self.resample_params
            )
            if not return_metrics or not risk_metrics:
                logger.warning(f"重采样后数据不足: {index_id}, 频率: {return_freq}/{risk_freq}")
                return {}
            logger.info(f"分析指数绩效: {index_id} -> {actual_index_id}, 默认频率(收益{return_freq}/风险{risk_freq})")
            results = {
                'fund_id': index_id,
                'frequency': 'mixed',
                'frequency_cn': '混合',
                'return_frequency': return_freq,
                'risk_frequency': risk_freq,
                'total_return': return_metrics['total_return'],
                'annual_return': return_metrics['annual_return'],
                'annual_volatility': risk_metrics['annual_volatility'],
                'max_drawdown': risk_metrics['max_drawdown'],
                'max_drawdown_start': risk_metrics['max_drawdown_start'],
                'max_drawdown_end': risk_metrics['max_drawdown_end'],
                'sharpe_ratio': risk_metrics['sharpe_ratio'],
                'calmar_ratio': risk_metrics['calmar_ratio'],
                'start_date': return_metrics['start_date'],
                'end_date': return_metrics['end_date'],
                'periods': return_metrics['periods'],
                'actual_index_id': actual_index_id
            }
        
        # 缓存结果
        self.results_cache[cache_key] = results
        
        return results
    
    def analyze_all_funds(
        self,
        fund_ids: List[str] = None,
        frequency: Optional[str] = None,
        return_frequency: Optional[str] = None,
        risk_frequency: Optional[str] = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        分析所有基金绩效
        
        Args:
            fund_ids: 基金代码列表，如果为None则从数据库获取
            frequency: 计算频率
            return_frequency: 收益类指标频率（frequency为空时生效）
            risk_frequency: 风险类指标频率（frequency为空时生效）
            resample_params: 重采样参数
            
        Returns:
            绩效指标DataFrame
        """
        if fund_ids is None:
            funds_df = fund_db.get_all_funds()
            if funds_df.empty:
                logger.error("没有基金数据")
                return pd.DataFrame()
            fund_ids = funds_df['fund_id'].tolist()
        
        results = []
        
        for fund_id in fund_ids:
            logger.info(f"分析基金: {fund_id}, 频率: {frequency if frequency else '默认(收益日度/风险周度)'}")
            performance = self.analyze_fund_performance(
                fund_id,
                frequency=frequency,
                return_frequency=return_frequency,
                risk_frequency=risk_frequency,
                resample_params=resample_params
            )
            
            if performance:  # 非空字典
                results.append(performance)
        
        # 转换为DataFrame
        if results:
            df_results = pd.DataFrame(results)
            return df_results
        else:
            logger.warning("没有可分析的基金数据")
            return pd.DataFrame()
    
    def analyze_all_indices(
        self,
        frequency: Optional[str] = None,
        return_frequency: Optional[str] = None,
        risk_frequency: Optional[str] = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        分析所有指数绩效
        
        Args:
            frequency: 计算频率
            return_frequency: 收益类指标频率（frequency为空时生效）
            risk_frequency: 风险类指标频率（frequency为空时生效）
            resample_params: 重采样参数
            
        Returns:
            绩效指标DataFrame
        """
        # 使用配置中的基准指数ID
        try:
            import config
            index_ids = config.BENCHMARK_IDS
        except Exception:
            index_ids = ['000001', '000300', 'HSI', 'MED_INNOV']
        
        results = []
        
        for index_id in index_ids:
            logger.info(f"分析指数: {index_id}, 频率: {frequency if frequency else '默认(收益日度/风险周度)'}")
            performance = self.analyze_index_performance(
                index_id,
                frequency=frequency,
                return_frequency=return_frequency,
                risk_frequency=risk_frequency,
                resample_params=resample_params
            )
            
            if performance:
                results.append(performance)
        
        # 转换为DataFrame
        if results:
            df_results = pd.DataFrame(results)
            return df_results
        else:
            logger.warning("没有可分析的指数数据")
            return pd.DataFrame()
    
    def generate_fund_returns_table(
        self,
        fund_id: str,
        benchmark_id: str = None,
        comparison_indices: List[str] = None
    ) -> pd.DataFrame:
        """
        生成产品及基准收益率表格（类似Excel中的"产品及基准收益率"）
        
        Args:
            fund_id: 基金代码
            benchmark_id: 基准指数ID，如果为None则使用默认基准
            comparison_indices: 对比指数列表，如果为None则使用配置中的默认列表
            
        Returns:
            包含产品及基准收益率的DataFrame
        """
        if benchmark_id is None:
            benchmark_id = _get_fund_benchmark_id(fund_id)
        
        fund_nav = self._get_fund_nav_series(fund_id)
        if fund_nav.empty:
            logger.error(f"基金数据为空: {fund_id}")
            return pd.DataFrame()
        
        fund_start = fund_nav.index.min()
        fund_end = fund_nav.index.max()
        start_year = fund_start.year
        end_year = fund_end.year

        periods: List[Tuple[str, pd.Timestamp, pd.Timestamp]] = []

        inception_label = f"成立至今收益率\n{fund_start.strftime('%Y/%m/%d')}-{fund_end.strftime('%Y/%m/%d')}"
        periods.append((inception_label, fund_start, fund_end))

        for year in range(start_year, end_year):
            year_start = pd.Timestamp(year=year, month=1, day=1)
            year_end = pd.Timestamp(year=year, month=12, day=31)
            if year == start_year:
                year_start = fund_start
            label = f"{year}年\n{year_start.strftime('%Y/%m/%d')}-{year_end.strftime('%Y/%m/%d')}"
            periods.append((label, year_start, year_end))

        ytd_start = pd.Timestamp(year=end_year, month=1, day=1)
        ytd_label = f"年初至今收益率\n{ytd_start.strftime('%Y/%m/%d')}-{fund_end.strftime('%Y/%m/%d')}"
        periods.append((ytd_label, ytd_start, fund_end))

        month_start = fund_end.replace(day=1)
        mtd_label = f"本月收益率\n{month_start.strftime('%Y/%m/%d')}-{fund_end.strftime('%Y/%m/%d')}"
        periods.append((mtd_label, month_start, fund_end))

        index_ids: List[str] = []
        if benchmark_id:
            index_ids.append(benchmark_id)
        if comparison_indices is None:
            comparison_indices = _get_fund_comparison_indices(fund_id)
        for idx in comparison_indices:
            if idx not in index_ids:
                index_ids.append(idx)

        series_map: Dict[str, pd.Series] = {
            '组合收益率': fund_nav
        }
        display_names: Dict[str, str] = {}
        for idx in index_ids:
            display_name = config.get_benchmark_display_name(idx) if hasattr(config, 'get_benchmark_display_name') else idx
            display_names[idx] = display_name
            series_map[f"{display_name}收益率"] = self._get_index_nav_series(idx)

        rows = []
        for label, start, end in periods:
            row = {'时间区间': label}
            for name, series in series_map.items():
                row[name] = self._calc_period_return(series, start, end)
            if benchmark_id:
                benchmark_name = f"{display_names.get(benchmark_id, benchmark_id)}收益率"
                row['超额收益率'] = _safe_diff(row.get('组合收益率', np.nan), row.get(benchmark_name, np.nan))
            else:
                row['超额收益率'] = np.nan
            rows.append(row)

        return pd.DataFrame(rows)
    
    def generate_weekly_returns_curve(
        self,
        fund_id: str,
        comparison_indices: List[str] = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        生成周收益率曲线表格（类似Excel中的"周收益率曲线"）
        
        Args:
            fund_id: 基金代码
            comparison_indices: 对比指数列表，如果为None则使用配置中的默认列表
            resample_params: 重采样参数
            
        Returns:
            包含周收益率曲线的DataFrame
        """
        benchmark_id = _get_fund_benchmark_id(fund_id)
        if comparison_indices is None:
            comparison_indices = _get_fund_comparison_indices(fund_id)
        index_ids = [benchmark_id] if benchmark_id else []
        for idx in comparison_indices:
            if idx not in index_ids:
                index_ids.append(idx)
        
        fund_df = fund_db.get_fund_daily_data(fund_id)
        if fund_df.empty:
            logger.error(f"基金数据为空: {fund_id}")
            return pd.DataFrame()
        
        # 使用累计净值
        if 'cumulative_nav' in fund_df.columns and not fund_df['cumulative_nav'].isna().all():
            fund_nav = fund_df['cumulative_nav'].dropna()
        else:
            fund_nav = fund_df['nav'].dropna()
        
        # 重采样为周度数据（使用可配置的周期规则）
        fund_weekly = _resample_data(
            fund_nav,
            frequency='weekly',
            **(resample_params or self.resample_params)
        )
        
        # 计算周收益率
        fund_weekly_returns = fund_weekly.pct_change().dropna()
        
        # 创建结果数据框
        result_df = pd.DataFrame({
            '日期': fund_weekly_returns.index,
            '组合周收益率': fund_weekly_returns.values
        })
        
        # 添加对比指数的周收益率
        for index_id in index_ids:
            index_nav = self._get_index_nav_series(index_id)
            display_name = config.get_benchmark_display_name(index_id) if hasattr(config, 'get_benchmark_display_name') else index_id
            column_name = f'{display_name}周收益率'
            if not index_nav.empty:
                index_weekly = _resample_data(
                    index_nav,
                    frequency='weekly',
                    **(resample_params or self.resample_params)
                )
                index_weekly_returns = index_weekly.pct_change().dropna()

                aligned_returns = pd.DataFrame({
                    'fund': fund_weekly_returns,
                    'index': index_weekly_returns
                }).dropna()

                if not aligned_returns.empty:
                    result_df = result_df.merge(
                        aligned_returns['index'].rename(column_name),
                        left_on='日期', right_index=True, how='left'
                    )
                else:
                    result_df[column_name] = np.nan
            else:
                result_df[column_name] = np.nan

        # 组合规模（亿）
        if 'asset_size' in fund_df.columns:
            size_params = {**(resample_params or self.resample_params), 'method': 'last'}
            size_weekly = _resample_data(
                fund_df['asset_size'].dropna(),
                frequency='weekly',
                **size_params
            ).reindex(result_df['日期'])
            result_df['组合规模（亿）'] = size_weekly.values
        else:
            result_df['组合规模（亿）'] = np.nan
        
        return result_df
    
    def generate_monthly_returns_table(
        self,
        fund_id: str,
        benchmark_id: str = None,
        resample_params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        生成月度收益率表格（类似Excel中的"月度收益率"）
        
        Args:
            fund_id: 基金代码
            benchmark_id: 基准指数ID，如果为None则使用默认基准
            resample_params: 重采样参数
            
        Returns:
            包含月度收益率的DataFrame
        """
        if benchmark_id is None:
            benchmark_id = _get_fund_benchmark_id(fund_id)
        
        fund_df = fund_db.get_fund_daily_data(fund_id)
        if fund_df.empty:
            logger.error(f"基金数据为空: {fund_id}")
            return pd.DataFrame()
        
        # 使用累计净值
        if 'cumulative_nav' in fund_df.columns and not fund_df['cumulative_nav'].isna().all():
            fund_nav = fund_df['cumulative_nav'].dropna()
        else:
            fund_nav = fund_df['nav'].dropna()

        benchmark_nav = self._get_index_nav_series(benchmark_id)
        
        # 计算月度收益率
        fund_monthly = _resample_data(
            fund_nav,
            frequency='monthly',
            **(resample_params or self.resample_params)
        )
        benchmark_monthly = _resample_data(
            benchmark_nav,
            frequency='monthly',
            **(resample_params or self.resample_params)
        )
        
        fund_monthly_returns = fund_monthly.pct_change().dropna()
        if fund_monthly_returns.empty:
            logger.warning("基金月度收益率为空")
            return pd.DataFrame()

        if benchmark_id is None:
            benchmark_id = _get_fund_benchmark_id(fund_id)

        benchmark_returns = pd.Series(dtype=float)
        if benchmark_id:
            benchmark_nav = self._get_index_nav_series(benchmark_id)
            if not benchmark_nav.empty:
                benchmark_monthly = _resample_data(
                    benchmark_nav,
                    frequency='monthly',
                    **(resample_params or self.resample_params)
                )
                benchmark_returns = benchmark_monthly.pct_change().dropna()

        # 使用专门的月度对比配置（优先从 config 获取），回退到通用对比列表
        monthly_comp = _get_fund_monthly_comparison(fund_id)
        comparison_id = None
        if monthly_comp:
            for idx in monthly_comp:
                try:
                    if _normalize_index_code(idx) != _normalize_index_code(benchmark_id):
                        comparison_id = idx
                        break
                except Exception:
                    # 若 normalization 失败，则按原始值比较
                    if idx != benchmark_id:
                        comparison_id = idx
                        break
            # 若仍未选出对比，则使用配置列表的第一个
            if comparison_id is None and len(monthly_comp) > 0:
                comparison_id = monthly_comp[0]

        comparison_returns = pd.Series(dtype=float)
        comparison_display = None
        if comparison_id:
            comparison_nav = self._get_index_nav_series(comparison_id)
            if not comparison_nav.empty:
                comparison_monthly = _resample_data(
                    comparison_nav,
                    frequency='monthly',
                    **(resample_params or self.resample_params)
                )
                comparison_returns = comparison_monthly.pct_change().dropna()
                comparison_display = config.get_benchmark_display_name(comparison_id) if hasattr(config, 'get_benchmark_display_name') else comparison_id

        data = pd.DataFrame({'组合': fund_monthly_returns})
        if not benchmark_returns.empty:
            data['基准'] = benchmark_returns
        if not comparison_returns.empty:
            data['对比'] = comparison_returns

        data = data.dropna(how='all')
        if data.empty:
            logger.warning("对齐后的月度数据为空")
            return pd.DataFrame()

        data = data.sort_index()
        data['year'] = data.index.year
        data['month'] = data.index.month

        monthly_table = data.pivot_table(values='组合', index='year', columns='month', aggfunc='first')

        yearly_fund = data.groupby('year')['组合'].apply(lambda x: (1 + x).prod() - 1)
        yearly_benchmark = None
        yearly_benchmark_win = None
        if '基准' in data.columns:
            yearly_benchmark = data.groupby('year')['基准'].apply(lambda x: (1 + x).prod() - 1)
            yearly_benchmark_win = (data['组合'] > data['基准']).groupby(data['year']).mean()

        yearly_comparison = None
        yearly_comparison_win = None
        if '对比' in data.columns:
            yearly_comparison = data.groupby('year')['对比'].apply(lambda x: (1 + x).prod() - 1)
            yearly_comparison_win = (data['组合'] > data['对比']).groupby(data['year']).mean()

        result_df = pd.DataFrame(index=monthly_table.index)
        for month in range(1, 13):
            if month in monthly_table.columns:
                result_df[f'{month}月'] = monthly_table[month]

        result_df['年收益率'] = yearly_fund
        result_df['业绩基准'] = yearly_benchmark
        result_df['相对基准月胜率'] = yearly_benchmark_win
        if comparison_display:
            result_df[comparison_display] = yearly_comparison
            result_df[f'相对{comparison_display}月胜率'] = yearly_comparison_win

        inception_row = {}
        inception_row['年收益率'] = (1 + fund_monthly_returns).prod() - 1
        if not benchmark_returns.empty:
            inception_row['业绩基准'] = (1 + benchmark_returns).prod() - 1
            inception_row['相对基准月胜率'] = (fund_monthly_returns.reindex(benchmark_returns.index) > benchmark_returns).mean()
        if comparison_display and not comparison_returns.empty:
            inception_row[comparison_display] = (1 + comparison_returns).prod() - 1
            inception_row[f'相对{comparison_display}月胜率'] = (fund_monthly_returns.reindex(comparison_returns.index) > comparison_returns).mean()

        if inception_row:
            result_df.loc['成立至今'] = inception_row

        result_df.insert(0, '组合', result_df.index.astype(str))
        result_df.reset_index(drop=True, inplace=True)
        return result_df
    
    def save_performance_to_excel(self, funds_df: pd.DataFrame, indices_df: pd.DataFrame, 
                                 output_path: str = "performance_summary.xlsx"):
        """
        保存绩效分析结果到Excel
        
        Args:
            funds_df: 基金绩效DataFrame
            indices_df: 指数绩效DataFrame
            output_path: 输出文件路径
        """
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # 保存基金绩效
                if not funds_df.empty:
                    funds_output = _rename_for_output(funds_df.copy())
                    funds_output.to_excel(writer, sheet_name='基金绩效', index=False)
                
                # 保存指数绩效
                if not indices_df.empty:
                    indices_output = _rename_for_output(indices_df.copy())
                    indices_output.to_excel(writer, sheet_name='指数绩效', index=False)
                
                # 合并所有绩效
                if not funds_df.empty or not indices_df.empty:
                    all_performance = pd.concat([funds_df, indices_df], ignore_index=True)
                    all_performance = _rename_for_output(all_performance)
                    all_performance.to_excel(writer, sheet_name='全部绩效', index=False)
                
                logger.info(f"绩效分析结果已保存到: {output_path}")
                return True
                
        except Exception as e:
            logger.error(f"保存绩效结果失败: {e}")
            return False
    
    def save_detailed_performance_to_excel(self, fund_id: str, 
                                          output_path: str = "fund_detailed_performance.xlsx",
                                          benchmark_id: str = None,
                                          comparison_indices: List[str] = None):
        """
        保存详细的绩效分析结果到Excel，包含三个表格
        
        Args:
            fund_id: 基金代码
            output_path: 输出文件路径
            benchmark_id: 基准指数ID
            comparison_indices: 对比指数列表
            
        Returns:
            是否保存成功
        """
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                wrote_sheet = False
                # 1. 产品及基准收益率表格
                try:
                    yearly_returns = self.generate_fund_returns_table(
                        fund_id,
                        benchmark_id,
                        comparison_indices
                    )
                    if not yearly_returns.empty:
                        layout_df = _build_product_sheet_layout(yearly_returns)
                        layout_df.to_excel(writer, sheet_name='产品及基准收益率', index=False, header=False)
                        ws = writer.sheets.get('产品及基准收益率')
                        if ws is not None:
                            apply_product_sheet_style(ws, layout_df.shape[0], layout_df.shape[1])
                        wrote_sheet = True
                    else:
                        columns = ['时间区间', '组合收益率', '超额收益率']
                        if benchmark_id:
                            benchmark_name = config.get_benchmark_display_name(benchmark_id) if hasattr(config, 'get_benchmark_display_name') else benchmark_id
                            columns.insert(2, f"{benchmark_name}收益率")
                        if comparison_indices:
                            for idx in comparison_indices:
                                display_name = config.get_benchmark_display_name(idx) if hasattr(config, 'get_benchmark_display_name') else idx
                                col = f"{display_name}收益率"
                                if col not in columns:
                                    columns.insert(-1, col)
                        pd.DataFrame(columns=columns).to_excel(writer, sheet_name='产品及基准收益率', index=False)
                        wrote_sheet = True
                except Exception as e:
                    logger.error(f"产品及基准收益率写入失败: {e}")
                    pd.DataFrame({'提示': ['产品及基准收益率生成失败']}).to_excel(writer, sheet_name='产品及基准收益率', index=False)
                    wrote_sheet = True
                
                # 2. 周收益率曲线表格
                try:
                    weekly_returns = self.generate_weekly_returns_curve(fund_id, comparison_indices)
                    if not weekly_returns.empty:
                        weekly_returns.to_excel(writer, sheet_name='周收益率曲线', index=False)
                        ws = writer.sheets.get('周收益率曲线')
                        if ws is not None:
                            apply_weekly_sheet_style(ws, weekly_returns.shape[0] + 1, weekly_returns.shape[1])
                        wrote_sheet = True
                    else:
                        columns = ['日期', '组合周收益率']
                        benchmark_id = benchmark_id or _get_fund_benchmark_id(fund_id)
                        index_ids = [benchmark_id] if benchmark_id else []
                        if comparison_indices:
                            for idx in comparison_indices:
                                if idx not in index_ids:
                                    index_ids.append(idx)
                        for idx in index_ids:
                            display_name = config.get_benchmark_display_name(idx) if hasattr(config, 'get_benchmark_display_name') else idx
                            columns.append(f"{display_name}周收益率")
                        columns.append('组合规模（亿）')
                        pd.DataFrame(columns=columns).to_excel(writer, sheet_name='周收益率曲线', index=False)
                        wrote_sheet = True
                except Exception as e:
                    logger.error(f"周收益率曲线写入失败: {e}")
                    pd.DataFrame({'提示': ['周收益率曲线生成失败']}).to_excel(writer, sheet_name='周收益率曲线', index=False)
                    wrote_sheet = True
                
                # 3. 月度收益率表格
                try:
                    monthly_returns = self.generate_monthly_returns_table(fund_id, benchmark_id)
                    if not monthly_returns.empty:
                        monthly_returns.to_excel(writer, sheet_name='月度收益率', index=False)
                        ws = writer.sheets.get('月度收益率')
                        if ws is not None:
                            apply_monthly_sheet_style(ws, monthly_returns.shape[0] + 1, monthly_returns.shape[1])
                        wrote_sheet = True
                    else:
                        benchmark_id = benchmark_id or _get_fund_benchmark_id(fund_id)
                        comparison_indices = comparison_indices or _get_fund_comparison_indices(fund_id)
                        columns = ['组合', '1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月', '年收益率', '业绩基准', '相对基准月胜率']
                        # 使用月度专用对比配置优先决定占位列
                        monthly_comp = _get_fund_monthly_comparison(fund_id)
                        comparison_id = None
                        if monthly_comp:
                            for idx in monthly_comp:
                                try:
                                    if _normalize_index_code(idx) != _normalize_index_code(benchmark_id):
                                        comparison_id = idx
                                        break
                                except Exception:
                                    if idx != benchmark_id:
                                        comparison_id = idx
                                        break
                            if comparison_id is None and len(monthly_comp) > 0:
                                comparison_id = monthly_comp[0]
                        # 回退到通用对比列表
                        if comparison_id is None:
                            if '000300' in comparison_indices and _normalize_index_code(benchmark_id) != '000300':
                                comparison_id = '000300'
                            else:
                                for idx in comparison_indices:
                                    if idx != benchmark_id:
                                        comparison_id = idx
                                        break
                        if comparison_id:
                            display_name = config.get_benchmark_display_name(comparison_id) if hasattr(config, 'get_benchmark_display_name') else comparison_id
                            columns.append(display_name)
                            columns.append(f"相对{display_name}月胜率")
                        pd.DataFrame(columns=columns).to_excel(writer, sheet_name='月度收益率', index=False)
                        wrote_sheet = True
                except Exception as e:
                    logger.error(f"月度收益率写入失败: {e}")
                    pd.DataFrame({'提示': ['月度收益率生成失败']}).to_excel(writer, sheet_name='月度收益率', index=False)
                    wrote_sheet = True

                if not wrote_sheet:
                    pd.DataFrame({'提示': ['无可用数据']}).to_excel(writer, sheet_name='说明', index=False)
                
                logger.info(f"详细绩效分析结果已保存到: {output_path}")
                return True
                
        except Exception as e:
            logger.error(f"保存详细绩效结果失败: {e}")
            return False


def main():
    """主函数，用于测试"""
    # 创建绩效分析器
    analyzer = PerformanceAnalyzer()
    
    # 测试单只基金分析（不同频率）
    test_fund = "000001.OF"
    print(f"测试分析基金: {test_fund}")
    
    for freq in ['daily', 'weekly', 'monthly']:
        print(f"\n频率: {freq}")
        performance = analyzer.analyze_fund_performance(test_fund, frequency=freq)
        if performance:
            print(f"基金绩效指标 ({freq}):")
            for key, value in performance.items():
                if key.endswith('return') or key.endswith('ratio') or 'drawdown' in key or 'volatility' in key:
                    if isinstance(value, float):
                        print(f"  {key}: {value:.4%}" if 'return' in key else f"  {key}: {value:.4f}")
    
    # 测试指数分析
    print(f"\n测试分析指数: INDEX_SSE")
    for freq in ['daily', 'weekly']:
        index_performance = analyzer.analyze_index_performance('INDEX_SSE', frequency=freq)
        if index_performance:
            print(f"指数绩效指标 ({freq}):")
            for key, value in index_performance.items():
                if key.endswith('return') or key.endswith('ratio') or 'drawdown' in key or 'volatility' in key:
                    if isinstance(value, float):
                        print(f"  {key}: {value:.4%}" if 'return' in key else f"  {key}: {value:.4f}")
    
    # 分析所有基金
    print(f"\n分析所有基金...")
    fund_ids = ["000001.OF", "510300.OF"]  # 测试用
    funds_results = analyzer.analyze_all_funds(fund_ids, frequency='weekly')
    
    if not funds_results.empty:
        print(f"基金绩效汇总 (周度, 共 {len(funds_results)} 只):")
        print(funds_results[['fund_id', 'frequency_cn', 'total_return', 'annual_return', 'max_drawdown', 'sharpe_ratio']].to_string())
    
    # 分析所有指数
    print(f"\n分析所有指数...")
    indices_results = analyzer.analyze_all_indices(frequency='weekly')
    
    if not indices_results.empty:
        print(f"指数绩效汇总 (周度, 共 {len(indices_results)} 个):")
        print(indices_results[['fund_id', 'frequency_cn', 'total_return', 'annual_return', 'max_drawdown', 'sharpe_ratio']].to_string())
    
    # 保存到Excel
    print(f"\n保存结果到Excel...")
    success = analyzer.save_performance_to_excel(funds_results, indices_results)
    if success:
        print("✅ 结果保存成功")
    
    # 测试详细绩效表格
    print(f"\n测试生成详细绩效表格...")
    if not fund_ids:
        print("没有基金数据，跳过详细表格测试")
    else:
        test_fund = fund_ids[0]
        print(f"为基金 {test_fund} 生成详细绩效表格...")
        
        # 生成产品及基准收益率表格
        yearly_returns = analyzer.generate_fund_returns_table(test_fund)
        if not yearly_returns.empty:
            print(f"产品及基准收益率表格 (前5年):")
            print(yearly_returns.head())
        
        # 生成周收益率曲线表格
        weekly_returns = analyzer.generate_weekly_returns_curve(test_fund)
        if not weekly_returns.empty:
            print(f"\n周收益率曲线表格 (前5周):")
            print(weekly_returns.head())
        
        # 生成月度收益率表格
        monthly_returns = analyzer.generate_monthly_returns_table(test_fund)
        if not monthly_returns.empty:
            print(f"\n月度收益率表格:")
            print(monthly_returns.head())
        
        # 保存详细绩效到Excel
        print(f"\n保存详细绩效到Excel...")
        success = analyzer.save_detailed_performance_to_excel(test_fund, "detailed_performance.xlsx")
        if success:
            print("✅ 详细绩效结果保存成功")


if __name__ == "__main__":
    main()