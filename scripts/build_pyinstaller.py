#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
build_pyinstaller.py - 使用PyInstaller构建基金分析工具的一键脚本

此脚本提供了一个Python版本的PyInstaller构建工具，类似于PowerShell脚本，
但可以直接在Python环境中运行，并包含修复PyInstaller conda metadata bug的措施。
"""

import subprocess
import sys
import os
import shutil
from pathlib import Path


def _supports_multiprocess_flag() -> bool:
    """Check if current pyinstaller supports --multiprocess."""
    try:
        help_text = subprocess.check_output(["pyinstaller", "--help"], text=True, errors="ignore")
        return "--multiprocess" in help_text
    except Exception:
        return False


def build_with_pyinstaller():
    """使用PyInstaller构建所有脚本"""
    # 设置环境变量以解决PyInstaller conda metadata bug (KeyError: 'depends')
    os.environ['PYINSTALLER_IGNORE_CONDA'] = '1'

    # 定义要构建的脚本
    scripts = [
        {"name": "fund_main", "entry": "main.py"},
        {"name": "excel_analysis", "entry": "scripts/analysis_from_excel.py"},
        {"name": "import_excel", "entry": "scripts/import_excel_to_db.py"},
        {"name": "analysis_from_db", "entry": "scripts/run_analysis_from_db.py"},
        {"name": "update_db", "entry": "scripts/update_db.py"}
    ]

    # 公共参数
    # 计算平台专用的 add-data 分隔符（Windows 使用 ;，POSIX 使用 :）
    data_sep = ';' if os.name == 'nt' else ':'

    common_args = [
        "--onefile",
        "--clean",
        "--noconfirm",
        "--workpath", "build/pyinstaller",
        "--distpath", "dist",
        "--runtime-hook", "scripts/pyi_runtime_paths.py",
        f"--paths=src",
        f"--paths=.",
        "--add-data", f"src{data_sep}src",
        "--collect-all", "matplotlib",
        "--collect-all", "plotly",
        "--collect-all", "pandas",
        "--collect-all", "py_mini_racer",
        "--collect-submodules", "akshare",
        "--collect-data", "akshare",
        "--hidden-import", "config",
        "--hidden-import", "seaborn",
        "--hidden-import", "pytz",
        "--hidden-import", "dateutil.tz",
        "--copy-metadata", "pandas",
        "--copy-metadata", "plotly",
        "--copy-metadata", "matplotlib"
    ]

    if _supports_multiprocess_flag():
        common_args.insert(3, "--multiprocess")
    else:
        print("当前 PyInstaller 不支持 --multiprocess，已跳过该参数")

    # 确保输出目录存在
    Path("build/pyinstaller").mkdir(parents=True, exist_ok=True)
    Path("dist").mkdir(parents=True, exist_ok=True)

    # 构建每个脚本
    for script in scripts:
        print(f"正在构建 {script['name']} 从 {script['entry']}...")
        
        cmd = ["pyinstaller"] + common_args + ["--name", script['name'], script['entry']]
        
        try:
            result = subprocess.run(cmd, check=True)
            print(f"成功构建 {script['name']}")
        except subprocess.CalledProcessError as e:
            print(f"构建 {script['name']} 失败: {e}")
            return False
    
    print("构建完成。可执行文件位于 dist/ 目录中。")
    return True


def clean_build_artifacts():
    """清理构建产生的中间文件"""
    dirs_to_clean = ["build", "dist"]
    files_to_clean = list(Path('.').glob('*.spec'))
    
    for d in dirs_to_clean:
        path = Path(d)
        if path.exists() and path.is_dir():
            shutil.rmtree(path)
            print(f"已删除目录: {path}")
    
    for f in files_to_clean:
        try:
            f.unlink()
            print(f"已删除文件: {f}")
        except OSError as e:
            print(f"无法删除文件 {f}: {e}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='使用PyInstaller构建基金分析工具')
    parser.add_argument('--clean', action='store_true', help='清理之前的构建产物')
    parser.add_argument('--no-console', action='store_true', help='构建无控制台窗口的应用（Windows）')
    
    args = parser.parse_args()
    
    if args.clean:
        print("正在清理构建产物...")
        clean_build_artifacts()
    
    # 如果指定了no-console选项，我们需要修改构建过程
    if args.no_console:
        print("注意: 无控制台模式目前不被此脚本直接支持，请编辑脚本添加--noconsole参数")
        # 这里可以进一步扩展以支持no-console参数
    
    build_with_pyinstaller()


if __name__ == "__main__":
    main()