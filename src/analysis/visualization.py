"""
数据可视化模块
生成各种图表和可视化结果
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from itertools import cycle

warnings.filterwarnings('ignore')
logger = logging.getLogger(__name__)


def _safe_savefig(fig: plt.Figure, filepath: Path) -> None:
    """Save figure safely with debug logging."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    try:
        fig.savefig(filepath, dpi=300, bbox_inches='tight')
        logger.info(f"图表已保存: {filepath}")
    except Exception as exc:  # pragma: no cover
        logger.error(f"保存图表失败 {filepath}: {exc}")


class FundVisualizer:
    """基金数据可视化类"""
    
    def __init__(self, output_dir: str = "plots", output_manager=None):
        """
        初始化可视化器
        
        Args:
            output_dir: 输出目录（备用，如果未提供output_manager）
            output_manager: 输出管理器实例
        """
        self.output_manager = output_manager
        
        if output_manager:
            self.output_dir = output_manager.get_path('plots')
        else:
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 设置matplotlib样式
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
        # 设置中文字体，优先使用容器内已安装的字体（Noto/WenQuanYi），回退到黑体/DejaVu
        try:
            plt.rcParams['font.sans-serif'] = [
                'Noto Sans CJK SC',      # fonts-noto-cjk
                'WenQuanYi Micro Hei',   # fonts-wqy-microhei
                'WenQuanYi Zen Hei',     # fonts-wqy-zenhei
                'SimHei',
                'DejaVu Sans'
            ]
            plt.rcParams['font.family'] = 'sans-serif'
            plt.rcParams['axes.unicode_minus'] = False
        except Exception as exc:  # pragma: no cover
            logger.warning(f"字体设置失败，使用默认字体: {exc}")

        # 基准线管理：按图表类型维护多条基准线
        # 结构：{chart_type: {name: {value, color, linestyle, linewidth}}}
        self._baselines: Dict[str, Dict[str, Dict[str, Any]]] = {}
        # 更友好的配色方案
        self._baseline_palette = [
            '#ef476f', '#118ab2', '#06d6a0', '#ffd166', '#8338ec', '#ff9f1c', '#4e79a7', '#f28e2c'
        ]
        self._baseline_cycle = cycle(self._baseline_palette)

    # 基准线管理API
    def add_baseline(self, chart_type: str, name: str, value: float,
                     color: Optional[str] = None, linestyle: str = '--', linewidth: float = 2.0) -> None:
        """
        为指定图表类型添加一条基准线
        Args:
            chart_type: 图表类型，例如 'holding_return_dist'、'nav_curve'、'drawdown_chart'
            name: 基准线名称（唯一键）
            value: 数值（直方图/回撤为纵线x=value；净值/回撤曲线为横线y=value）
            color: 颜色
            linestyle: 线型
            linewidth: 线宽
        """
        if chart_type not in self._baselines:
            self._baselines[chart_type] = {}
        # 如果未指定颜色，自动轮换调色板
        color_to_use = color if color else next(self._baseline_cycle)
        self._baselines[chart_type][name] = {
            'value': value,
            'color': color_to_use,
            'linestyle': linestyle,
            'linewidth': linewidth,
        }

    def remove_baseline(self, chart_type: str, name: str) -> None:
        """移除指定图表类型中的某条基准线"""
        if chart_type in self._baselines:
            self._baselines[chart_type].pop(name, None)

    def clear_baselines(self, chart_type: Optional[str] = None) -> None:
        """清空基准线：若未指定类型则清空所有"""
        if chart_type is None:
            self._baselines.clear()
        else:
            self._baselines.pop(chart_type, None)
    
    def plot_return_distribution(self, returns_data: pd.DataFrame,
                                holding_days: int,
                                fund_name: str = None,
                                benchmark_returns: Any = None,
                                save_fig: bool = True,
                                output_path: str = None) -> Optional[plt.Figure]:
        """
        绘制持有期收益率分布直方图
        
        Args:
            returns_data: 收益率数据DataFrame
            holding_days: 持有天数
            fund_name: 基金名称
            benchmark_returns: 基准收益率字典 {指数名称: 收益率序列}
            save_fig: 是否保存图片
            output_path: 自定义输出路径（覆盖默认）
        """
        placeholder_mode = False
        if returns_data is None or returns_data.empty or 'holding_return' not in returns_data.columns:
            placeholder_mode = True
            # 构造占位数据结构，避免后续访问报错
            returns = pd.Series(dtype=float)
        else:
            returns = returns_data['holding_return']
        
        # 创建图形
        fig, ax = plt.subplots(figsize=(14, 8))
        
        if not placeholder_mode and not returns.empty:
            # 计算合适的bins
            n_bins = min(50, len(returns) // 10)
            n_bins = max(n_bins, 10)
            # 绘制直方图
            ax.hist(returns, bins=n_bins, alpha=0.7, density=True,
                edgecolor='black', linewidth=0.5, label='基金收益率分布')
            # 绘制KDE曲线
            sns.kdeplot(returns, ax=ax, color='blue', linewidth=2, label='基金KDE曲线')
        else:
            ax.text(0.5, 0.5, '数据不足，无法生成收益率分布', transform=ax.transAxes,
                ha='center', va='center', fontsize=12, color='gray')
        
        # 添加统计线
        mean_return = returns.mean() if not returns.empty else np.nan
        median_return = returns.median() if not returns.empty else np.nan
        if not np.isnan(mean_return):
            ax.axvline(mean_return, color='blue', linestyle='--', linewidth=2, alpha=0.7, label=f'基金均值: {mean_return:.2%}')
        if not np.isnan(median_return):
            ax.axvline(median_return, color='blue', linestyle='-.', linewidth=2, alpha=0.7, label=f'基金中位数: {median_return:.2%}')
        ax.axvline(0, color='black', linestyle=':', linewidth=1, label='零收益线')
        
        # 基准线：来自传入的benchmark_returns或已登记的多基准线
        # 支持：
        # - dict{name: Series|float}
        # - Series（将名称设为"基准"）
        # - float/int（单值基准线）
        if (not placeholder_mode) and (benchmark_returns is not None):
            if isinstance(benchmark_returns, dict):
                for benchmark_name, bench_obj in benchmark_returns.items():
                    color_pick = next(self._baseline_cycle)
                    if isinstance(bench_obj, pd.Series) and not bench_obj.empty:
                        benchmark_median = bench_obj.median()
                        ax.axvline(benchmark_median, color=color_pick, linestyle='--',
                                   linewidth=2, alpha=0.85, label=f'{benchmark_name}中位数: {benchmark_median:.2%}')
                    elif isinstance(bench_obj, (int, float)):
                        ax.axvline(float(bench_obj), color=color_pick, linestyle='--',
                                   linewidth=2, alpha=0.85, label=f'{benchmark_name}: {float(bench_obj):.2%}')
            elif isinstance(benchmark_returns, pd.Series) and not benchmark_returns.empty:
                m = benchmark_returns.median()
                ax.axvline(m, color=next(self._baseline_cycle), linestyle='--', linewidth=2, alpha=0.85, label=f'基准中位数: {m:.2%}')
            elif isinstance(benchmark_returns, (int, float)):
                ax.axvline(float(benchmark_returns), color=next(self._baseline_cycle), linestyle='--', linewidth=2, alpha=0.85, label=f'基准: {float(benchmark_returns):.2%}')

        # 渲染登记的多基准线
        for name, conf in self._baselines.get('holding_return_dist', {}).items():
            ax.axvline(conf['value'], color=conf['color'], linestyle=conf['linestyle'],
                       linewidth=conf['linewidth'], alpha=0.9, label=f'{name}: {conf["value"]:.2%}')
        
        # 设置图形属性
        fund_name_display = fund_name if fund_name else "基金"
        title = f'{fund_name_display} - {holding_days}天持有期收益率分布'
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.set_xlabel('持有期收益率', fontsize=12)
        ax.set_ylabel('密度', fontsize=12)
        
        # 调整图例位置（右侧）
        ax.legend(fontsize=9, loc='upper left', bbox_to_anchor=(1.05, 1))
        
        ax.grid(True, alpha=0.3)
        
        # 添加统计信息文本
        if not placeholder_mode and not returns.empty and returns.std() > 0:
            stats_text = f'''
    统计信息:
    样本数: {len(returns)}
    均值: {mean_return:.2%}
    中位数: {median_return:.2%}
    标准差: {returns.std():.2%}
    最小值: {returns.min():.2%}
    最大值: {returns.max():.2%}
    正收益比例: {(returns > 0).mean():.2%}
    夏普比率: {(returns.mean() / returns.std() * np.sqrt(252/holding_days)):.2f}
    '''
            ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                fontsize=9, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        # 保存图片（中文命名）
        if save_fig:
            filepath = None
            # 如果指定了output_path，使用它
            if output_path:
                filepath = Path(output_path)
            # 如果有output_manager，使用它获取路径
            elif self.output_manager and fund_name and fund_name != '示例基金':
                filepath = self.output_manager.get_fund_plot_path(fund_name, 'holding_dist', holding_days)
            # 否则使用传统方式
            else:
                # 跳过示例基金指定图片
                if fund_name == '示例基金':
                    filepath = None
                else:
                    # 中文命名：{基金名}_持有期{N}天_收益率分布.png（无基金名则不加前缀）
                    safe_fund_name = None
                    if fund_name:
                        safe_fund_name = ''.join(c for c in fund_name if c.isalnum() or c in (' ', '-', '_'))
                    if safe_fund_name:
                        filename = f"{safe_fund_name}_持有期{holding_days}天_收益率分布.png"
                    else:
                        filename = f"持有期{holding_days}天_收益率分布.png"
                    filepath = self.output_dir / filename

            if filepath:
                _safe_savefig(fig, filepath)
        
        return fig
    
    def plot_performance_comparison(self, performance_data: pd.DataFrame,
                                   metrics: List[str] = None,
                                   save_fig: bool = True) -> Optional[plt.Figure]:
        """
        绘制绩效指标对比图
        
        Args:
            performance_data: 绩效数据DataFrame
            metrics: 要对比的指标列表
            save_fig: 是否保存图片
            
        Returns:
            matplotlib图形对象
        """
        if performance_data.empty or 'fund_id' not in performance_data.columns:
            logger.error("绩效数据为空或格式错误")
            return None
        
        if metrics is None:
            metrics = ['annual_return', 'annual_volatility', 'sharpe_ratio', 'max_drawdown']
        
        # 确保指标存在
        available_metrics = [m for m in metrics if m in performance_data.columns]
        if not available_metrics:
            logger.error("没有可用的绩效指标")
            return None
        
        n_metrics = len(available_metrics)
        
        # 创建子图
        fig, axes = plt.subplots(n_metrics, 1, figsize=(14, 4 * n_metrics))
        
        # 如果只有一个子图，将axes转换为列表
        if n_metrics == 1:
            axes = [axes]
        
        # 定义指标的中文名称和格式化方式
        metric_names = {
            'annual_return': ('年化收益率', '{:.2%}'),
            'annual_volatility': ('年化波动率', '{:.2%}'),
            'sharpe_ratio': ('夏普比率', '{:.2f}'),
            'max_drawdown': ('最大回撤', '{:.2%}'),
            'total_return': ('总收益率', '{:.2%}'),
            'calmar_ratio': ('Calmar比率', '{:.2f}')
        }
        
        # 为每个指标绘制条形图
        for idx, metric in enumerate(available_metrics):
            ax = axes[idx]
            
            # 获取数据并排序
            data = performance_data[['fund_id', metric]].copy()
            data = data.sort_values(metric, ascending=False)
            
            # 创建条形图
            bars = ax.bar(data['fund_id'], data[metric], color=plt.cm.tab20c(np.arange(len(data))))
            
            # 添加数值标签
            for bar in bars:
                height = bar.get_height()
                format_str = metric_names.get(metric, ('', '{:.2f}'))[1]
                label = format_str.format(height)
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       label, ha='center', va='bottom', fontsize=9)
            
            # 设置子图属性
            metric_display = metric_names.get(metric, (metric, ''))[0]
            ax.set_title(f'{metric_display}对比', fontsize=14, fontweight='bold')
            ax.set_xlabel('')
            ax.set_ylabel(metric_display, fontsize=12)
            ax.tick_params(axis='x', rotation=45)
            ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        
        # 保存图片（中文命名）
        if save_fig:
            filepath = self.output_dir / "绩效指标对比.png"
            _safe_savefig(fig, filepath)
        
        return fig
    
    def plot_nav_curve(self, nav_series: pd.Series,
                      fund_name: str = None,
                      benchmark_nav: pd.Series = None,
                      save_fig: bool = True,
                      output_path: str = None) -> Optional[plt.Figure]:
        """
        绘制净值曲线图
        
        Args:
            nav_series: 净值序列
            fund_name: 基金名称
            benchmark_nav: 基准净值
            save_fig: 是否保存图片
            output_path: 自定义输出路径（覆盖默认）
        """
        if nav_series.empty:
            logger.error("净值数据为空")
            return None
        
        # 创建图形
        fig, ax = plt.subplots(figsize=(14, 8))
        
        # 绘制基金净值曲线
        ax.plot(nav_series.index, nav_series.values, 
                linewidth=2, label='基金净值', color='blue')
        
        # 绘制基准净值曲线（如果有）
        if benchmark_nav is not None and not benchmark_nav.empty:
            benchmark_nav = benchmark_nav.dropna()
            # 对齐日期
            common_idx = nav_series.index.intersection(benchmark_nav.index)
            if len(common_idx) > 0:
                benchmark_aligned = benchmark_nav.reindex(common_idx).dropna()
                if not benchmark_aligned.empty and benchmark_aligned.iloc[0] != 0:
                    # 重新基准化到同一起点
                    benchmark_normalized = benchmark_aligned / benchmark_aligned.iloc[0] * nav_series.iloc[0]
                    ax.plot(benchmark_aligned.index, benchmark_normalized.values,
                           linewidth=2, label='基准指数', color='red', alpha=0.7)
        
        # 设置图形属性
        fund_name_display = fund_name if fund_name else "基金"
        title = f'{fund_name_display} - 净值曲线'
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('净值', fontsize=12)
        ax.legend(fontsize=12)
        ax.grid(True, alpha=0.3)

        # 渲染登记的多基准线（横线）
        for name, conf in self._baselines.get('nav_curve', {}).items():
            ax.axhline(conf['value'], color=conf['color'], linestyle=conf['linestyle'],
                       linewidth=conf['linewidth'], alpha=0.9, label=f'{name}: {conf["value"]:.4f}')
        
        # 格式化x轴日期
        fig.autofmt_xdate()
        
        # 添加统计信息
        total_return = (nav_series.iloc[-1] / nav_series.iloc[0]) - 1
        max_nav = nav_series.max()
        min_nav = nav_series.min()
        
        stats_text = f'''
        统计信息:
        起始净值: {nav_series.iloc[0]:.4f}
        结束净值: {nav_series.iloc[-1]:.4f}
        总收益率: {total_return:.2%}
        最高净值: {max_nav:.4f}
        最低净值: {min_nav:.4f}
        数据点数: {len(nav_series)}
        '''
        
        ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
                fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        # 保存图片
        if save_fig:
            if output_path:
                filepath = Path(output_path)
            elif self.output_manager and fund_name and fund_name != '示例基金':
                filepath = self.output_manager.get_fund_plot_path(fund_name, 'nav_curve')
            else:
                if fund_name == '示例基金':
                    return fig
                safe_fund_name = ''.join(c for c in fund_name if c.isalnum() or c in (' ', '-', '_')) if fund_name else None
                filename = f"{safe_fund_name}_净值曲线.png" if safe_fund_name else "净值曲线.png"
                filepath = self.output_dir / filename

            _safe_savefig(fig, filepath)
        
        return fig
    
    def plot_drawdown_chart(self, nav_series: pd.Series,
                           fund_name: str = None,
                           save_fig: bool = True,
                           output_path: str = None) -> Optional[plt.Figure]:
        """
        绘制回撤图
        
        Args:
            nav_series: 净值序列
            fund_name: 基金名称
            save_fig: 是否保存图片
            output_path: 自定义输出路径（覆盖默认）
            
        Returns:
            matplotlib图形对象
        """
        if nav_series.empty:
            logger.error("净值数据为空")
            return None
        
        # 计算回撤
        cummax = nav_series.cummax()
        drawdown = (nav_series - cummax) / cummax
        
        # 创建图形和子图
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), 
                                       gridspec_kw={'height_ratios': [2, 1]})
        
        # 子图1: 净值曲线和累计最大值
        ax1.plot(nav_series.index, nav_series.values, 
                linewidth=2, label='基金净值', color='blue')
        ax1.plot(nav_series.index, cummax.values, 
                linewidth=1, label='累计最大值', color='red', linestyle='--', alpha=0.7)
        
        # 填充回撤区域
        ax1.fill_between(nav_series.index, nav_series.values, cummax.values,
                        where=(nav_series < cummax), color='red', alpha=0.3, label='回撤区域')
        
        # 标记最大回撤
        max_dd_idx = drawdown.idxmin()
        max_dd_value = drawdown.min()
        
        if pd.notna(max_dd_idx):
            ax1.scatter(max_dd_idx, nav_series.loc[max_dd_idx], 
                       color='darkred', s=100, zorder=5, label=f'最大回撤: {max_dd_value:.2%}')
        
        # 设置子图1属性
        fund_name_display = fund_name if fund_name else "基金"
        ax1.set_title(f'{fund_name_display} - 净值与回撤', fontsize=16, fontweight='bold')
        ax1.set_ylabel('净值', fontsize=12)
        ax1.legend(fontsize=10)
        ax1.grid(True, alpha=0.3)
        
        # 子图2: 回撤曲线
        ax2.fill_between(drawdown.index, drawdown.values, 0,
                        where=(drawdown < 0), color='red', alpha=0.5)
        ax2.plot(drawdown.index, drawdown.values, 
                linewidth=1, color='darkred')
        
        # 标记最大回撤
        if pd.notna(max_dd_idx):
            ax2.scatter(max_dd_idx, max_dd_value, 
                       color='darkred', s=100, zorder=5)
            ax2.text(max_dd_idx, max_dd_value * 1.1, 
                    f'最大回撤: {max_dd_value:.2%}', 
                    fontsize=10, ha='center', va='bottom')
        
        # 设置子图2属性
        ax2.set_xlabel('日期', fontsize=12)
        ax2.set_ylabel('回撤', fontsize=12)
        ax2.grid(True, alpha=0.3)

        # 渲染登记的多基准线（横线，通常为回撤阈值如-20%等）
        for name, conf in self._baselines.get('drawdown_chart', {}).items():
            ax2.axhline(conf['value'], color=conf['color'], linestyle=conf['linestyle'],
                        linewidth=conf['linewidth'], alpha=0.9, label=f'{name}: {conf["value"]:.2%}')
        ax2.legend(fontsize=10, loc='upper left')
        
        # 格式化x轴日期
        fig.autofmt_xdate()
        
        plt.tight_layout()
        
        # 保存图片
        if save_fig:
            if output_path:
                filepath = Path(output_path)
            elif self.output_manager and fund_name and fund_name != '示例基金':
                filepath = self.output_manager.get_fund_plot_path(fund_name, 'drawdown_chart')
            else:
                if fund_name == '示例基金':
                    return fig
                safe_fund_name = ''.join(c for c in fund_name if c.isalnum() or c in (' ', '-', '_')) if fund_name else None
                filename = f"{safe_fund_name}_回撤分析.png" if safe_fund_name else "回撤分析.png"
                filepath = self.output_dir / filename

            _safe_savefig(fig, filepath)
        
        return fig
    
    def create_interactive_chart(self, nav_series: pd.Series,
                                fund_name: str = None) -> Optional[go.Figure]:
        """
        创建交互式净值+回撤图，支持基准线显隐（Plotly）
        """
        if nav_series.empty:
            return None

        cummax = nav_series.cummax()
        drawdown = (nav_series - cummax) / cummax

        fig = make_subplots(rows=2, cols=1,
                           subplot_titles=('净值曲线', '回撤曲线'),
                           vertical_spacing=0.15)

        # 主体曲线
        fig.add_trace(
            go.Scatter(x=nav_series.index, y=nav_series.values,
                      mode='lines', name='净值',
                      line=dict(color='#1f77b4', width=2),
                      hovertemplate='日期: %{x}<br>净值: %{y:.4f}<extra></extra>'),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(x=nav_series.index, y=cummax.values,
                      mode='lines', name='累计最大值',
                      line=dict(color='#ff7f0e', width=1, dash='dash'),
                      hovertemplate='日期: %{x}<br>累计最大值: %{y:.4f}<extra></extra>'),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(x=drawdown.index, y=drawdown.values,
                      mode='lines', name='回撤',
                      line=dict(color='#d62728', width=2),
                      fill='tozeroy',
                      fillcolor='rgba(214,39,40,0.25)',
                      hovertemplate='日期: %{x}<br>回撤: %{y:.2%}<extra></extra>'),
            row=2, col=1
        )

        baseline_trace_indices = []

        # 基准线：净值水平线
        for name, conf in self._baselines.get('nav_curve', {}).items():
            line_color = conf['color']
            idx_start = len(fig.data)
            fig.add_trace(
                go.Scatter(
                    x=nav_series.index,
                    y=np.full(len(nav_series), conf['value']),
                    mode='lines',
                    name=f'基准线-净值:{name}',
                    line=dict(color=line_color, dash=conf['linestyle'], width=conf['linewidth']),
                    hovertemplate=f'{name}: {conf["value"]:.4f}<extra></extra>',
                ),
                row=1, col=1
            )
            baseline_trace_indices.append(idx_start)

        # 基准线：回撤水平线
        for name, conf in self._baselines.get('drawdown_chart', {}).items():
            line_color = conf['color']
            idx_start = len(fig.data)
            fig.add_trace(
                go.Scatter(
                    x=drawdown.index,
                    y=np.full(len(drawdown), conf['value']),
                    mode='lines',
                    name=f'基准线-回撤:{name}',
                    line=dict(color=line_color, dash=conf['linestyle'], width=conf['linewidth']),
                    hovertemplate=f'{name}: {conf["value"]:.2%}<extra></extra>',
                ),
                row=2, col=1
            )
            baseline_trace_indices.append(idx_start)

        # 基准线显隐按钮
        total_traces = len(fig.data)
        visible_all = [True] * total_traces
        visible_hide_baselines = [i not in baseline_trace_indices for i in range(total_traces)]

        fig.update_layout(
            updatemenus=[
                dict(
                    type='buttons',
                    direction='right',
                    buttons=[
                        dict(label='隐藏基准线', method='update', args=[{'visible': visible_hide_baselines}]),
                        dict(label='显示基准线', method='update', args=[{'visible': visible_all}]),
                    ],
                    x=0.0, y=1.15, showactive=False
                )
            ]
        )

        fund_name_display = fund_name if fund_name else "基金"
        fig.update_layout(
            title=f'{fund_name_display} - 净值与回撤分析（交互）',
            height=800,
            hovermode='x unified',
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0)
        )

        fig.update_xaxes(title_text="日期", row=2, col=1)
        fig.update_yaxes(title_text="净值", row=1, col=1)
        fig.update_yaxes(title_text="回撤", row=2, col=1)

        return fig

    def create_interactive_return_distribution(self, returns_data: pd.DataFrame,
                                               holding_days: int,
                                               fund_name: str = None,
                                               benchmark_returns: Any = None) -> Optional[go.Figure]:
        """交互式持有期收益率分布，支持基准线显隐"""
        if returns_data is None or returns_data.empty or 'holding_return' not in returns_data.columns:
            returns = pd.Series(dtype=float)
        else:
            returns = returns_data['holding_return'].dropna()

        n_bins = min(50, len(returns) // 10)
        n_bins = max(n_bins, 10)
        if len(returns) > 0:
            counts, bins = np.histogram(returns, bins=n_bins, density=True)
            centers = 0.5 * (bins[:-1] + bins[1:])
            max_density = counts.max() if len(counts) else 1
        else:
            counts, centers, max_density = np.array([]), np.array([]), 1

        fig = go.Figure()

        if len(returns) > 0:
            fig.add_trace(go.Histogram(
                x=returns,
                histnorm='probability density',
                nbinsx=n_bins,
                name='收益率分布',
                marker_color='#4e79a7',
                opacity=0.65
            ))
            fig.add_trace(go.Scatter(
                x=centers, y=counts,
                mode='lines', name='密度曲线',
                line=dict(color='#f28e2c', width=2)
            ))
        else:
            fig.add_annotation(
                x=0.5, y=0.5, xref='paper', yref='paper',
                text='数据不足，无法生成收益率分布', showarrow=False, font=dict(color='gray')
            )

        baseline_indices = []

        # benchmark baselines
        def _add_vline(value: float, label: str, color_val: str):
            idx = len(fig.data)
            fig.add_trace(go.Scatter(
                x=[value, value], y=[0, max_density * 1.1],
                mode='lines', name=label,
                line=dict(color=color_val, dash='dash', width=2.5),
                hovertemplate=f'{label}: {value:.2%}<extra></extra>'
            ))
            baseline_indices.append(idx)

        if len(returns) > 0 and benchmark_returns is not None:
            if isinstance(benchmark_returns, dict):
                for benchmark_name, bench_obj in benchmark_returns.items():
                    color_pick = next(self._baseline_cycle)
                    if isinstance(bench_obj, pd.Series) and not bench_obj.empty:
                        _add_vline(bench_obj.median(), f'{benchmark_name}中位数', color_pick)
                    elif isinstance(bench_obj, (int, float)):
                        _add_vline(float(bench_obj), benchmark_name, color_pick)
            elif isinstance(benchmark_returns, pd.Series) and not benchmark_returns.empty:
                _add_vline(benchmark_returns.median(), '基准中位数', next(self._baseline_cycle))
            elif isinstance(benchmark_returns, (int, float)):
                _add_vline(float(benchmark_returns), '基准', next(self._baseline_cycle))

        # registered baselines
        for name, conf in self._baselines.get('holding_return_dist', {}).items():
            _add_vline(conf['value'], f'基准线:{name}', conf['color'])

        total_traces = len(fig.data)
        visible_all = [True] * total_traces
        visible_hide_baselines = [i not in baseline_indices for i in range(total_traces)]

        fig.update_layout(
            title=f"{fund_name if fund_name else '基金'} - {holding_days}天持有期收益率分布（交互）",
            bargap=0.05,
            hovermode='x unified',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0),
            updatemenus=[
                dict(
                    type='buttons',
                    direction='right',
                    buttons=[
                        dict(label='隐藏基准线', method='update', args=[{'visible': visible_hide_baselines}]),
                        dict(label='显示基准线', method='update', args=[{'visible': visible_all}]),
                    ],
                    x=0.0, y=1.12, showactive=False
                )
            ],
            xaxis_title='持有期收益率',
            yaxis_title='密度'
        )

        return fig

    def create_interactive_nav_curve(self, nav_series: pd.Series,
                                     fund_name: str = None,
                                     benchmark_nav: pd.Series = None) -> Optional[go.Figure]:
        """交互式净值曲线，含基准线显隐与基准净值对比"""
        if nav_series.empty:
            return None

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=nav_series.index, y=nav_series.values,
            mode='lines', name='基金净值',
            line=dict(color='#1f77b4', width=2),
            hovertemplate='日期: %{x}<br>净值: %{y:.4f}<extra></extra>'
        ))

        if benchmark_nav is not None and not benchmark_nav.empty:
            benchmark_nav = benchmark_nav.dropna()
            common_idx = nav_series.index.intersection(benchmark_nav.index)
            if len(common_idx) > 0:
                benchmark_aligned = benchmark_nav.reindex(common_idx).dropna()
                if not benchmark_aligned.empty and benchmark_aligned.iloc[0] != 0:
                    benchmark_normalized = benchmark_aligned / benchmark_aligned.iloc[0] * nav_series.iloc[0]
                    fig.add_trace(go.Scatter(
                        x=benchmark_aligned.index, y=benchmark_normalized.values,
                        mode='lines', name='基准净值',
                        line=dict(color='#ff7f0e', width=2, dash='dash'),
                        hovertemplate='日期: %{x}<br>基准净值: %{y:.4f}<extra></extra>'
                    ))

        baseline_indices = []
        for name, conf in self._baselines.get('nav_curve', {}).items():
            idx = len(fig.data)
            fig.add_trace(go.Scatter(
                x=nav_series.index,
                y=np.full(len(nav_series), conf['value']),
                mode='lines',
                name=f'基准线:{name}',
                line=dict(color=conf['color'], dash=conf['linestyle'], width=conf['linewidth']),
                hovertemplate=f'{name}: {conf["value"]:.4f}<extra></extra>'
            ))
            baseline_indices.append(idx)

        total_traces = len(fig.data)
        visible_all = [True] * total_traces
        visible_hide_baselines = [i not in baseline_indices for i in range(total_traces)]

        fund_name_display = fund_name if fund_name else '基金'
        fig.update_layout(
            title=f"{fund_name_display} - 净值曲线（交互）",
            hovermode='x unified',
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='left', x=0),
            updatemenus=[
                dict(
                    type='buttons',
                    direction='right',
                    buttons=[
                        dict(label='隐藏基准线', method='update', args=[{'visible': visible_hide_baselines}]),
                        dict(label='显示基准线', method='update', args=[{'visible': visible_all}]),
                    ],
                    x=0.0, y=1.12, showactive=False
                )
            ],
            xaxis_title='日期',
            yaxis_title='净值'
        )

        return fig
    
    def save_all_holding_plots(self, holding_analysis: Dict[str, Any],
                              fund_name: str = None,
                              benchmark_id: str = None,
                              output_manager=None):
        """
        保存所有持有期分析图表（增强版）
        
        Args:
            holding_analysis: 持有期分析结果
            fund_name: 基金名称
            benchmark_id: 基准指数ID
            output_manager: 输出管理器（覆盖实例属性）
        """
        # 确定使用哪个输出管理器
        use_manager = output_manager or self.output_manager
        
        if not holding_analysis:
            logger.error("持有期分析结果为空")
            return
        
        simulation_results = holding_analysis.get('simulation_results', {})
        
        if not simulation_results:
            logger.warning("没有模拟结果数据")
            return
        
        # 获取基准收益率（如果有）
        benchmark_returns = {}
        if benchmark_id:
            from ..utils.database import fund_db
            from .holding_simulation import HoldingSimulation
            
            simulator = HoldingSimulation()
            
            for holding_days in simulation_results.keys():
                returns = simulator.get_benchmark_returns(benchmark_id, holding_days)
                if not returns.empty:
                    benchmark_returns[holding_days] = returns
        
        # 为每个持有期生成图表
        for holding_days, results_df in simulation_results.items():
            if not results_df.empty:
                # 获取基准收益率
                benchmark_series = benchmark_returns.get(holding_days)
                
                # 确定输出路径
                if use_manager and fund_name and fund_name != '示例基金':
                    output_path = use_manager.get_fund_plot_path(
                        fund_name, 'holding_dist', holding_days
                    )
                    save_fig = True
                else:
                    output_path = None
                    save_fig = (fund_name != '示例基金')
                
                # 生成图表
                fig = self.plot_return_distribution(
                    results_df, holding_days, fund_name, 
                    benchmark_series, save_fig=save_fig, output_path=output_path
                )
                
                if fig and save_fig and output_path:
                    # 如果plot_return_distribution没有保存（因为需要自定义路径）
                    plt.savefig(output_path, dpi=300, bbox_inches='tight')
                    logger.info(f"持有期分布图已保存: {output_path}")
                
                if fig:
                    plt.close(fig)  # 关闭图形以释放内存
        
        logger.info(f"所有持有期图表已保存")


def main():
    """主函数，用于测试"""
    # 创建可视化器
    visualizer = FundVisualizer()
    
    # 生成示例数据
    dates = pd.date_range('2020-01-01', '2023-12-31', freq='B')
    np.random.seed(42)
    
    # 创建示例净值序列
    returns = np.random.normal(0.0005, 0.015, len(dates))
    nav_series = pd.Series((1 + returns).cumprod(), index=dates)
    nav_series.name = '示例基金'
    
    # 创建示例基准净值
    benchmark_returns = np.random.normal(0.0003, 0.012, len(dates))
    benchmark_nav = pd.Series((1 + benchmark_returns).cumprod(), index=dates)
    
    # 测试净值曲线图（示例基金不保存图片）
    print("测试净值曲线图...")
    fig1 = visualizer.plot_nav_curve(nav_series, '示例基金', benchmark_nav)
    if fig1:
        plt.show(block=False)
    
    # 测试回撤图（示例基金不保存图片）
    print("测试回撤图...")
    fig2 = visualizer.plot_drawdown_chart(nav_series, '示例基金')
    if fig2:
        plt.show(block=False)
    
    # 创建示例持有期收益率数据
    np.random.seed(123)
    holding_returns = np.random.normal(0.02, 0.08, 1000)
    returns_df = pd.DataFrame({'holding_return': holding_returns})
    
    # 测试收益率分布图（示例基金不保存图片）
    print("测试收益率分布图...")
    fig3 = visualizer.plot_return_distribution(returns_df, 30, '示例基金')
    if fig3:
        plt.show(block=False)
    
    # 测试交互式图表
    print("测试交互式图表...")
    interactive_fig = visualizer.create_interactive_chart(nav_series, '示例基金')
    if interactive_fig:
        interactive_fig.show()
    
    plt.show()


if __name__ == "__main__":
    main()