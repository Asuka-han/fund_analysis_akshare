#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
output_manager.py - 统一管理输出路径，避免文件混乱

功能：
1. 为每个脚本创建独立的输出目录
2. 按类型（图片/Excel/HTML/日志）组织文件
3. 支持时间戳目录和清理功能
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Union
import shutil
import logging

logger = logging.getLogger(__name__)


class OutputManager:
    """输出管理器，统一管理各脚本的输出路径"""
    
    # 预定义的脚本类型
    SCRIPT_TYPES = {
        'main': '主程序',
        'update_db': '数据库更新',
        'db_analysis': '数据库分析',
        'excel_analysis': 'Excel分析',
        'calculate_only': '快速计算'
    }
    
    # 输出类型
    OUTPUT_TYPES = {
        'plots': '静态图表',
        'interactive': '交互图表',
        'excel': 'Excel文件',
        'logs': '日志文件',
        'reports': '报告文件',
        'backups': '备份文件'
    }
    
    def __init__(self, script_type: str = 'main', 
                 base_dir: str = '.',
                 use_timestamp: bool = False,
                 clean_old: bool = False):
        """
        初始化输出管理器
        
        Args:
            script_type: 脚本类型（main/update_db/db_analysis/excel_analysis/calculate_only）
            base_dir: 基础目录
            use_timestamp: 是否使用时间戳子目录
            clean_old: 是否清理旧文件
        """
        self.script_type = script_type
        self.base_dir = Path(base_dir)
        
        # 创建时间戳（如果需要）
        self.timestamp = None
        if use_timestamp:
            self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 构建目录结构
        self.dirs = self._build_directory_structure()
        
        # 清理旧文件（如果需要）
        if clean_old and not use_timestamp:
            self.clean_old_files(days=7)
    
    def _build_directory_structure(self) -> Dict[str, Path]:
        """构建目录结构"""
        dirs = {}
        
        # 基础脚本目录
        if self.timestamp:
            script_base = self.base_dir / self.script_type / self.timestamp
        else:
            script_base = self.base_dir / self.script_type
        
        # 创建各种输出目录
        dirs['base'] = script_base
        
        # 图表目录
        dirs['plots'] = script_base / 'plots'
        dirs['plots_static'] = dirs['plots'] / 'static'
        dirs['plots_interactive'] = dirs['plots'] / 'interactive'
        
        # 数据目录
        dirs['data'] = script_base / 'data'
        dirs['excel'] = script_base / 'excel'
        dirs['excel_performance'] = dirs['excel'] / 'performance'
        dirs['excel_holding'] = dirs['excel'] / 'holding'
        
        # 报告目录
        dirs['reports'] = script_base / 'reports'
        
        # 日志目录
        dirs['logs'] = script_base / 'logs'
        
        # 备份目录
        dirs['backups'] = script_base / 'backups'
        
        # 确保所有目录都存在
        for dir_path in dirs.values():
            dir_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"📁 输出目录结构已创建: {script_base}")
        return dirs
    
    def get_path(self, output_type: str, filename: str = None, 
                subdir: str = None, fund_id: str = None) -> Path:
        """
        获取输出文件路径
        
        Args:
            output_type: 输出类型（plots/excel/reports/logs/backups）
            filename: 文件名（可选）
            subdir: 子目录（可选）
            fund_id: 基金ID（用于创建基金专用目录）
            
        Returns:
            完整文件路径
        """
        # 确定基础目录
        if output_type == 'plots':
            base_dir = self.dirs['plots_static']
        elif output_type == 'interactive':
            base_dir = self.dirs['plots_interactive']
        elif output_type in ['excel', 'excel_performance', 'excel_holding']:
            base_dir = self.dirs[output_type]
        elif output_type in self.dirs:
            base_dir = self.dirs[output_type]
        else:
            base_dir = self.dirs['base'] / output_type
            base_dir.mkdir(parents=True, exist_ok=True)
        
        # 如果指定了基金ID，创建基金专用目录
        if fund_id:
            fund_dir = base_dir / self._sanitize_filename(fund_id)
            fund_dir.mkdir(exist_ok=True)
            base_dir = fund_dir
            
            # 对于持有期图表，创建持有期子目录
            if 'holding' in str(filename) or '持有期' in str(filename):
                holding_dir = base_dir / 'holding'
                holding_dir.mkdir(exist_ok=True)
                base_dir = holding_dir
        
        # 如果指定了子目录
        if subdir:
            subdir_path = base_dir / subdir
            subdir_path.mkdir(parents=True, exist_ok=True)
            base_dir = subdir_path
        
        # 返回路径
        if filename:
            return base_dir / self._sanitize_filename(filename)
        else:
            return base_dir
    
    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除特殊字符"""
        # 移除或替换特殊字符
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # 限制长度
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:195] + ext
        
        return filename
    
    def get_fund_plot_path(self, fund_name: str, plot_type: str, 
                          holding_days: int = None) -> Path:
        """
        获取基金图表路径（智能命名）
        
        Args:
            fund_name: 基金名称
            plot_type: 图表类型（nav_curve/drawdown/holding_dist）
            holding_days: 持有天数（仅holding_dist需要）
            
        Returns:
            图表文件路径
        """
        # 清理基金名称
        safe_name = self._sanitize_filename(fund_name)
        
        # 根据图表类型生成文件名
        if plot_type == 'nav_curve':
            filename = f"{safe_name}_净值曲线.png"
        elif plot_type == 'drawdown':
            filename = f"{safe_name}_回撤分析.png"
        elif plot_type == 'holding_dist':
            if holding_days:
                filename = f"{safe_name}_持有期{holding_days}天_收益率分布.png"
            else:
                filename = f"{safe_name}_持有期收益率分布.png"
        elif plot_type == 'performance_comparison':
            filename = "绩效指标对比.png"
        else:
            filename = f"{safe_name}_{plot_type}.png"
        
        return self.get_path('plots', filename, fund_id=safe_name)
    
    def get_interactive_path(self, fund_name: str, plot_type: str,
                            holding_days: int = None) -> Path:
        """获取交互式图表路径"""
        safe_name = self._sanitize_filename(fund_name)
        
        if plot_type == 'nav_curve':
            filename = f"{safe_name}_净值曲线_交互.html"
        elif plot_type == 'nav_drawdown':
            filename = f"{safe_name}_净值回撤_交互.html"
        elif plot_type == 'holding_dist':
            if holding_days:
                filename = f"{safe_name}_持有期{holding_days}天_交互.html"
            else:
                filename = f"{safe_name}_持有期收益率分布_交互.html"
        else:
            filename = f"{safe_name}_{plot_type}_交互.html"
        
        return self.get_path('interactive', filename, fund_id=safe_name)
    
    def clean_old_files(self, days: int = 7):
        """清理指定天数前的旧文件"""
        import time
        cutoff_time = time.time() - (days * 24 * 60 * 60)
        
        for dir_type, dir_path in self.dirs.items():
            if dir_type == 'base':
                continue
                
            if dir_path.exists():
                for item in dir_path.rglob('*'):
                    if item.is_file():
                        if item.stat().st_mtime < cutoff_time:
                            try:
                                item.unlink()
                                logger.debug(f"清理旧文件: {item}")
                            except Exception as e:
                                logger.warning(f"清理文件失败 {item}: {e}")
    
    def get_summary_info(self) -> Dict[str, str]:
        """获取输出目录摘要信息"""
        summary = {
            'script_type': self.SCRIPT_TYPES.get(self.script_type, self.script_type),
            'base_dir': str(self.dirs['base'].absolute()),
            'timestamp': self.timestamp or '无'
        }
        
        # 统计目录大小
        for name, path in self.dirs.items():
            if path.exists():
                try:
                    file_count = sum(1 for _ in path.rglob('*') if _.is_file())
                    summary[f'{name}_files'] = str(file_count)
                except:
                    summary[f'{name}_files'] = '无法统计'
        
        return summary
    
    def print_summary(self):
        """打印输出目录摘要"""
        lines = []
        lines.append("\n📁 输出目录结构摘要")
        lines.append("=" * 60)

        summary = self.get_summary_info()
        for key, value in summary.items():
            lines.append(f"{key:20}: {value}")

        lines.append("\n📂 目录结构:")
        for name, path in sorted(self.dirs.items()):
            if path.exists():
                relative_path = path.relative_to(self.base_dir)
                lines.append(f"  {name:20}: {relative_path}")

        lines.append("=" * 60)

        for line in lines:
            logger.info(line)


# 全局输出管理器实例（按需创建）
_output_manager_cache = {}

def get_output_manager(script_type: str = 'main', **kwargs) -> OutputManager:
    """获取或创建输出管理器（单例模式）"""
    if script_type not in _output_manager_cache:
        _output_manager_cache[script_type] = OutputManager(script_type, **kwargs)
    
    return _output_manager_cache[script_type]


def create_fund_output_dirs(fund_id: str, script_type: str = 'main'):
    """为特定基金创建输出目录（提前创建）"""
    output_mgr = get_output_manager(script_type)
    
    # 创建基金相关的目录
    dirs_to_create = [
        output_mgr.get_path('plots', fund_id=fund_id),
        output_mgr.get_path('interactive', fund_id=fund_id),
        output_mgr.get_path('plots', subdir='holding', fund_id=fund_id),
    ]
    
    for dir_path in dirs_to_create:
        dir_path.mkdir(parents=True, exist_ok=True)
    
    return dirs_to_create