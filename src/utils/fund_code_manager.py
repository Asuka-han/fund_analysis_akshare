"""
基金代码格式管理器
处理不同格式的基金代码转换
"""
import re
from typing import List, Union


class FundCodeManager:
    """基金代码格式管理器"""
    
    @staticmethod
    def to_akshare_format(fund_code: str) -> str:
        """
        转换为AKShare接口格式（纯数字）
        
        Args:
            fund_code: 基金代码，可以是 '000001' 或 '000001.OF'
            
        Returns:
            纯数字格式的基金代码
        """
        if not fund_code:
            return ''
        
        # 移除所有非数字字符
        return re.sub(r'[^\d]', '', fund_code)
    
    @staticmethod
    def to_display_format(fund_code: str) -> str:
        """
        转换为显示格式（带.OF后缀）
        
        Args:
            fund_code: 基金代码，可以是 '000001' 或 '000001.OF'
            
        Returns:
            带.OF后缀的基金代码
        """
        if not fund_code:
            return ''
        
        # 如果已经是带后缀的格式，直接返回
        if fund_code.endswith('.OF'):
            return fund_code
        
        # 否则添加后缀
        return f"{fund_code}.OF"
    
    @staticmethod
    def to_database_format(fund_code: str) -> str:
        """
        转换为数据库格式（统一使用带后缀的格式）
        
        Args:
            fund_code: 基金代码，可以是 '000001' 或 '000001.OF'
            
        Returns:
            数据库格式的基金代码（带.OF后缀）
        """
        return FundCodeManager.to_display_format(fund_code)
    
    @staticmethod
    def is_akshare_format(fund_code: str) -> bool:
        """
        检查是否为AKShare格式（纯数字）
        
        Args:
            fund_code: 基金代码
            
        Returns:
            如果是纯数字格式返回True，否则返回False
        """
        if not fund_code:
            return False
        return bool(re.fullmatch(r'\d+', fund_code))
    
    @staticmethod
    def is_display_format(fund_code: str) -> bool:
        """
        检查是否为显示格式（带.OF后缀）
        
        Args:
            fund_code: 基金代码
            
        Returns:
            如果是带.OF后缀的格式返回True，否则返回False
        """
        if not fund_code:
            return False
        return fund_code.endswith('.OF')
    
    @staticmethod
    def get_pure_code(fund_code: str) -> str:
        """
        获取纯数字代码（无论输入什么格式）
        
        Args:
            fund_code: 基金代码，可以是 '000001' 或 '000001.OF'
            
        Returns:
            纯数字格式的基金代码
        """
        return FundCodeManager.to_akshare_format(fund_code)
    
    @staticmethod
    def batch_to_akshare(fund_codes: List[str]) -> List[str]:
        """
        批量转换为AKShare格式
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            纯数字格式的基金代码列表
        """
        return [FundCodeManager.to_akshare_format(code) for code in fund_codes]
    
    @staticmethod
    def batch_to_display(fund_codes: List[str]) -> List[str]:
        """
        批量转换为显示格式
        
        Args:
            fund_codes: 基金代码列表
            
        Returns:
            带.OF后缀的基金代码列表
        """
        return [FundCodeManager.to_display_format(code) for code in fund_codes]
    
    @staticmethod
    def get_code_pair(fund_code: str) -> dict:
        """
        获取基金代码的多种格式
        
        Args:
            fund_code: 基金代码
            
        Returns:
            包含多种格式的字典
        """
        pure_code = FundCodeManager.to_akshare_format(fund_code)
        display_code = FundCodeManager.to_display_format(fund_code)
        
        return {
            'original': fund_code,
            'akshare': pure_code,
            'display': display_code,
            'database': display_code
        }
    
    @staticmethod
    def normalize_code(fund_code: str, target_format: str = 'display') -> str:
        """
        标准化基金代码格式
        
        Args:
            fund_code: 基金代码
            target_format: 目标格式，可选 'akshare'、'display' 或 'database'
            
        Returns:
            标准化后的基金代码
        """
        if target_format == 'akshare':
            return FundCodeManager.to_akshare_format(fund_code)
        elif target_format in ['display', 'database']:
            return FundCodeManager.to_display_format(fund_code)
        else:
            raise ValueError(f"不支持的格式: {target_format}")


# 创建全局实例以便使用 - 这是关键！必须要有这行代码
fund_code_manager = FundCodeManager()