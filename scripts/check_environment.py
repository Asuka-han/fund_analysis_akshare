#!/usr/bin/env python
# scripts/check_environment.py
# 环境检查脚本，验证所有依赖是否安装正确

import sys
import pkg_resources

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
    print("🔍 检查基金分析项目环境依赖")
    print("=" * 50)
    
    all_passed = True
    results = []
    
    for package, min_version in REQUIRED_PACKAGES.items():
        success, version, message = check_package(package, min_version)
        
        status = "✅" if success else "❌"
        results.append((package, status, version, message))
        
        if not success:
            all_passed = False
    
    # 打印结果
    print(f"{'包名称':<15} {'状态':<5} {'版本':<12} {'信息':<20}")
    print("-" * 60)
    
    for package, status, version, message in results:
        version_str = version if version else "N/A"
        message_str = message if message else "OK"
        print(f"{package:<15} {status:<5} {version_str:<12} {message_str:<20}")
    
    print("=" * 50)
    
    # 检查Python版本
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    print(f"Python版本: {python_version}")
    
    if all_passed:
        print("\n🎉 所有依赖检查通过！环境配置正确。")
        print("\n运行项目:")
        print("  python main.py")
        return 0
    else:
        print("\n⚠️  部分依赖检查失败。请参考以下建议:")
        print("\n解决方案:")
        print("1. 使用conda环境:")
        print("   conda activate fund_analysis_env")
        print("2. 使用pip安装缺失包:")
        print("   pip install -r requirements.txt")
        print("3. 或运行环境设置脚本:")
        print("   # Unix/macOS:")
        print("   bash scripts/create_env_conda.sh")
        print("   # Windows PowerShell:")
        print("   powershell scripts/create_env_conda.ps1")
        return 1

if __name__ == "__main__":
    sys.exit(main())