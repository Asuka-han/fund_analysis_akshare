#!/usr/bin/env python
# scripts/check_environment.py
# 环境检查脚本，验证所有依赖是否安装正确

import sys
from pathlib import Path
import logging
import pkg_resources

# 保证脚本独立运行时能找到项目内模块
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"
for candidate in (PROJECT_ROOT, SRC_DIR):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from src.utils.logger_config import LogConfig
from src.utils.logger import get_logger
import config

logger = get_logger(__name__)

REQUIRED_PACKAGES = {
    # 核心依赖
    'pandas': '2.0.0',
    'numpy': '1.24.0',
    'scipy': '1.11.0',  # 从1.10.0升级到1.11.0
    'sqlalchemy': '2.0.0',  # 新增：数据库ORM
    'openpyxl': '3.1.0',
    'akshare': '1.12.42',  # 从1.12.0升级到1.12.42
    # 数据分析
    'statsmodels': '0.14.0',  # 新增：统计模型
    # 可视化
    'matplotlib': '3.7.0',
    'seaborn': '0.12.0',
    'plotly': '5.17.0',  # 从5.18.0调整为5.17.0
    'jinja2': '3.1.0',  # 新增：HTML模板渲染
    # 开发工具（可选，如需检查则保留）
    'python-dotenv': '1.0.0',  # 新增：环境变量管理
    'pytest': '7.4.0',
    'black': '23.9.0',
    'flake8': '6.0.0',
}

def check_package(package_name, min_version):
    """检查包是否安装且版本满足要求"""
    try:
        # 获取已安装版本
        installed_version = pkg_resources.get_distribution(package_name).version
        
        # 版本比较
        installed = pkg_resources.parse_version(installed_version)
        required = pkg_resources.parse_version(min_version)
        
        if installed >= required:
            return True, installed_version, None
        else:
            return False, installed_version, f"需要版本 >= {min_version}"
            
    except pkg_resources.DistributionNotFound:
        return False, None, "未安装"
    except Exception as e:
        return False, None, f"检查错误: {str(e)}"

def main():
    LogConfig.setup_root_logger(
        LogConfig.resolve_log_dir('check_environment', config.REPORTS_DIR),
        level=logging.INFO,
        script_name='check_environment'
    )

    logger.info("检查基金分析项目环境依赖")
    logger.info("=" * 50)
    
    all_passed = True
    results = []
    
    for package, min_version in REQUIRED_PACKAGES.items():
        success, version, message = check_package(package, min_version)
        
        status = "OK" if success else "FAIL"
        results.append((package, status, version, message))
        
        if not success:
            all_passed = False
    
    # 打印结果
    logger.info("%s", f"{'包名称':<15} {'状态':<5} {'版本':<12} {'信息':<20}")
    logger.info("-" * 60)
    
    for package, status, version, message in results:
        version_str = version if version else "N/A"
        message_str = message if message else "OK"
        logger.info("%s", f"{package:<15} {status:<5} {version_str:<12} {message_str:<20}")
    
    logger.info("=" * 50)
    
    # 检查Python版本
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    logger.info("Python版本: %s", python_version)
    
    if all_passed:
        logger.info("所有依赖检查通过，环境配置正确")
        logger.info("运行项目: python main.py")
        return 0
    else:
        logger.warning("部分依赖检查失败，请参考以下建议")
        logger.info("解决方案:")
        logger.info("1. 使用 conda 环境: conda activate fund_analysis_env")
        logger.info("2. 使用 pip 安装缺失包: pip install -r requirements.txt")
        logger.info("3. 或运行环境设置脚本:")
        logger.info("   Unix/macOS: bash scripts/create_env_conda.sh")
        logger.info("   Windows PowerShell: powershell scripts/create_env_conda.ps1")
        return 1

if __name__ == "__main__":
    sys.exit(main())