<#
.SYNOPSIS
创建基金分析项目的conda环境（适配Windows）
.DESCRIPTION
自动检测conda、检查环境文件路径、创建/更新指定的conda环境
#>

# 定义核心变量
$ENV_NAME = "fund_analysis_env"
# 相对于脚本所在目录（scripts）的环境文件路径
$ENV_FILE_REL = "..\environment.yml"  

# 美化输出的分隔符和样式
Write-Host "`n🔧 基金分析项目环境设置脚本 (Windows)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# 1. 检查conda是否安装并可用
try {
    $condaVersion = conda --version 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "conda未找到，请先安装Anaconda/Miniconda并配置环境变量"
    }
    Write-Host "✅ 找到conda: $condaVersion" -ForegroundColor Green
}
catch {
    Write-Host "❌ 错误: $_" -ForegroundColor Red
    exit 1
}

# 2. 关键修复：基于脚本所在目录定位environment.yml（不再依赖工作目录）
# $PSScriptRoot = 脚本所在的绝对路径（即scripts文件夹）
$ENV_FILE_ABS = Join-Path -Path $PSScriptRoot -ChildPath $ENV_FILE_REL
try {
    # 验证文件是否存在，并转换为绝对路径
    $ENV_FILE_ABS = Resolve-Path -Path $ENV_FILE_ABS -ErrorAction Stop
    Write-Host "📄 找到环境配置文件: $ENV_FILE_ABS" -ForegroundColor Green
}
catch [System.Management.Automation.ItemNotFoundException] {
    Write-Host "❌ 错误: 未找到environment.yml文件！" -ForegroundColor Red
    Write-Host "   请确认文件位置：$ENV_FILE_ABS" -ForegroundColor Yellow
    Write-Host "   （文件应放在项目根目录：$((Get-Item $PSScriptRoot).Parent.FullName)）" -ForegroundColor Yellow
    exit 1
}

# 3. 检查并处理conda环境
try {
    # 检查环境是否已存在
    $envExists = conda info --envs | Select-String -Pattern "^$ENV_NAME\s+"
    if ($envExists) {
        Write-Host "🔄 环境 '$ENV_NAME' 已存在，开始更新依赖..." -ForegroundColor Yellow
        conda env update --name $ENV_NAME --file $ENV_FILE_ABS --prune
    }
    else {
        Write-Host "🆕 创建新环境 '$ENV_NAME'..." -ForegroundColor Yellow
        conda env create --name $ENV_NAME --file $ENV_FILE_ABS
    }

    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ 环境 '$ENV_NAME' 处理完成！" -ForegroundColor Green
    }
    else {
        throw "conda环境创建/更新失败，请检查environment.yml语法"
    }
}
catch {
    Write-Host "❌ 错误: $_" -ForegroundColor Red
    exit 1
}

# 4. 输出后续操作指引
Write-Host "`n🎉 环境设置完成！`n" -ForegroundColor Green
Write-Host "使用以下命令激活环境：" -ForegroundColor Cyan
Write-Host "  conda activate $ENV_NAME" -ForegroundColor White
Write-Host "`n验证安装：" -ForegroundColor Cyan
Write-Host "  python -c `"import pandas; print(f'pandas版本: {pandas.__version__}')`"" -ForegroundColor White
Write-Host "  python -c `"import akshare; print('akshare导入成功')`"" -ForegroundColor White
Write-Host "`n运行项目：" -ForegroundColor Cyan
Write-Host "  python main.py`n" -ForegroundColor White