#!/bin/bash
# scripts/create_env_conda.sh
# 自动创建conda环境的脚本（Unix/macOS）

set -e  # 遇到错误立即退出

echo "🔧 基金分析项目环境设置脚本 (Unix/macOS)"
echo "========================================"

# 检查conda是否可用
if ! command -v conda &> /dev/null; then
    echo "❌ 未找到conda命令。"
    echo ""
    echo "请先安装Anaconda或Miniconda:"
    echo "  - Anaconda: https://www.anaconda.com/download"
    echo "  - Miniconda: https://docs.conda.io/en/latest/miniconda.html"
    echo ""
    echo "或者使用pip方式安装（非conda环境）："
    echo "  python -m venv .venv"
    echo "  source .venv/bin/activate  # 在Unix/macOS上"
    echo "  pip install -r requirements.txt"
    echo ""
    exit 1
fi

ENV_NAME="fund_analysis_env"
ENV_FILE="../environment.yml"  # 脚本在scripts目录，环境文件在项目根目录

echo "📦 检查环境 '$ENV_NAME'..."

# 检查环境是否已存在
if conda env list | grep -q "^$ENV_NAME "; then
    echo "🔄 环境 '$ENV_NAME' 已存在，正在更新..."
    conda env update -f "$ENV_FILE" -n "$ENV_NAME"
    echo "✅ 环境 '$ENV_NAME' 更新完成！"
else
    echo "🆕 创建新环境 '$ENV_NAME'..."
    conda env create -f "$ENV_FILE"
    echo "✅ 环境 '$ENV_NAME' 创建完成！"
fi

echo ""
echo "🎉 环境设置完成！"
echo ""
echo "使用以下命令激活环境："
echo "  conda activate $ENV_NAME"
echo ""
echo "验证安装："
echo "  python -c \"import pandas; print(f'pandas版本: {pandas.__version__}')\""
echo "  python -c \"import akshare; print('akshare导入成功')\""
echo ""
echo "运行项目："
echo "  python main.py"