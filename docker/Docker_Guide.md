# Docker 快速上手指南

适用于希望在容器中运行本项目的同事，覆盖镜像构建、运行、常见任务和故障排除。默认将输出挂载到宿主机的 [data/](../data) 与 [reports/](../reports)，保持与本地运行一致的目录结构。

## 目录
- [前置条件](#前置条件)
- [目录与入口](#目录与入口)
- [快速开始](#快速开始)
- [Docker 实现各功能的具体方法](#docker-实现各功能的具体方法)
  - [1. 主流程运行](#1-主流程运行)
  - [2. 更新数据库](#2-更新数据库)
  - [3. 数据库分析](#3-数据库分析)
  - [4. Excel导入](#4-excel导入)
  - [5. Excel直接分析](#5-excel直接分析)
- [实用 Docker 操作示例](#实用-docker-操作示例)
  - [场景1：首次使用项目](#场景1首次使用项目)
  - [场景2：定期更新数据并分析](#场景2定期更新数据并分析)
  - [场景3：导入外部Excel数据](#场景3导入外部excel数据)
- [Docker 配置定制](#docker-配置定制)
  - [1. 修改挂载目录](#1-修改挂载目录)
  - [2. 自定义镜像标签](#2-自定义镜像标签)
  - [3. 使用不同的配置文件](#3-使用不同的配置文件)
- [常见 Docker 问题解决](#常见-docker-问题解决)
  - [问题1：权限错误](#问题1权限错误)
  - [问题2：磁盘空间不足](#问题2磁盘空间不足)
  - [问题3：网络超时](#问题3网络超时)
  - [问题4：查看容器内文件](#问题4查看容器内文件)
- [Docker 自动化部署示例](#docker-自动化部署示例)
  - [1. 定时任务（Cron）](#1-定时任务cron)
  - [2. 使用 Docker Compose 持久化运行](#2-使用-docker-compose-持久化运行)
  - [3. 备份和恢复](#3-备份和恢复)
- [快速参考卡片](#快速参考卡片)

## 前置条件
- 已安装 Docker 与 Docker Compose（Docker Desktop 即可）。
- 可选但强烈建议：安装 make(切换到项目目录运行sudo apt update && sudo apt install make -y)，方便使用项目内置的 [Makefile](../Makefile)。

## 目录与入口
- 镜像构建文件：[docker/Dockerfile](../docker/Dockerfile)
- Compose 配置：[docker/docker-compose.yml](../docker/docker-compose.yml)
- 入口脚本：[docker/docker-entrypoint.sh](../docker/docker-entrypoint.sh)
- 运行包装脚本：[docker/build.sh](../docker/build.sh)、[docker/run.sh](../docker/run.sh)
- 容器任务脚本：[docker/scripts/init-db.sh](../docker/scripts/init-db.sh)、[docker/scripts/update-cron.sh](../docker/scripts/update-cron.sh)

入口脚本支持的命令（CMD）：
- `main`（默认）: 执行 [python main.py](../main.py)
- `update-db`: 执行 [python scripts/update_db.py](../scripts/update_db.py)
- `run-analysis`: 执行 [python scripts/run_analysis_from_db.py](../scripts/run_analysis_from_db.py)
- `analysis-from-excel`: 执行 [python scripts/analysis_from_excel.py](../scripts/analysis_from_excel.py)
- `import-excel`: 执行 [python scripts/import_excel_to_db.py](../scripts/import_excel_to_db.py)
- 其他：传入任何自定义 shell/命令

## 快速开始
1) 构建镜像（需要几分钟）：
   ```bash
   make docker-build           # 或运行: bash [docker/build.sh](../docker/build.sh)
   ```

2) 启动后台服务（不推荐跑一次性任务）：
  ```bash
  make docker-up              # 后台服务；一次性任务请用 make docker-main
  ```
  说明：后台服务会以容器方式启动并执行一次主程序后退出；无需长驻任务时请使用 `make docker-main`（compose run）按需运行。

3) 查看日志：
   ```bash
   make docker-logs
   ```

4) 停止并移除容器：
   ```bash
   make docker-down
   ```

## Docker 实现各功能的具体方法

### 1. 主流程运行（相当于 [python main.py](../main.py)）
```bash
# 方法1：使用 make 命令（最简单）
make docker-main

# 方法2：使用 docker compose
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm fund-analysis main

# 方法3：直接使用 docker run
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/reports:/app/reports" \
  fund-analysis:latest
```

### 2. 更新数据库（相当于 [python scripts/update_db.py](../scripts/update_db.py)）
```bash
# 更新指定基金
make docker-update-db ARGS="--funds 000001.OF 003095.OF --backup"

# 更新所有基金和指数
make docker-update-db ARGS="--all"

# 更新最近5年数据
make docker-update-db ARGS="--years 5"
```

### 3. 数据库分析（相当于 [python scripts/run_analysis_from_db.py](../scripts/run_analysis_from_db.py)）

```bash
# 分析指定基金
make docker-analysis ARGS="--funds 000001.OF --periods 30 60 90 "

# 分析所有基金
make docker-analysis ARGS="--all"

# 启用HTML输出（默认已经启用）
make docker-analysis ARGS="--funds 000001.OF --output-html true"
```

### 4. Excel导入（相当于 [python scripts/import_excel_to_db.py](../scripts/import_excel_to_db.py)）
```bash
# 导入Excel数据到数据库
make docker-run CMD="import-excel" ARGS="--input sample/fund_sample.xlsx"

# 导入并覆盖已有数据
make docker-run CMD="import-excel" ARGS="--input sample/fund_sample.xlsx --duplicate-action replace"
```

### 5. Excel直接分析（相当于 [python scripts/analysis_from_excel.py](../scripts/analysis_from_excel.py)）
```bash
# 从Excel直接分析
make docker-run CMD="analysis-from-excel" ARGS="--input sample/fund_sample.xlsx"

# 分析并写入数据库
make docker-run CMD="analysis-from-excel" ARGS="--input sample/fund_sample.xlsx --write-db"
```

## 实用 Docker 操作示例

### 场景1：首次使用项目
```bash
# 1. 构建Docker镜像（一次构建，长期使用）
make docker-build

# 2. 运行完整分析流程
make docker-main

# 3. 查看结果
ls reports/
```

### 场景2：定期更新数据并分析
```bash
# 周一：更新数据库
make docker-update-db ARGS="--all --backup"

# 周二：从更新后的数据库进行分析
make docker-analysis ARGS="--all --output-html true"

# 查看生成的报告
open reports/db_analysis/最新时间戳目录/analysis_report.md
```

### 场景3：导入外部Excel数据
```bash
# 1. 将Excel文件放在项目根目录下（如 my_funds.xlsx）
# 2. 导入到数据库
make docker-run CMD="import-excel" ARGS="--input my_funds.xlsx"

# 3. 分析导入的基金
make docker-analysis ARGS="--funds 从Excel导入的基金代码"
```

## Docker 配置定制

### 1. 修改挂载目录
编辑 [docker/docker-compose.yml](../docker/docker-compose.yml)：

```yaml
volumes:
  - ./my_data:/app/data      # 自定义数据目录
  - ./my_reports:/app/reports # 自定义报告目录
  - ./config.py:/app/config.py # 挂载配置文件（可选）
```

### 2. 自定义镜像标签
```bash
# 构建自定义镜像
docker build -t mycompany/fund-analysis:v1.0 -f [docker/Dockerfile](../docker/Dockerfile) .

# 使用自定义镜像运行
IMAGE_NAME=mycompany/fund-analysis:v1.0 make docker-main
```

### 3. 使用不同的配置文件
```bash
# 方法1：挂载自定义配置
docker run --rm \
  -v "$(pwd)/my_config.py:/app/config.py" \
  -v "$(pwd)/data:/app/data" \
  fund-analysis:latest

# 方法2：在容器内修改配置
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm fund-analysis \
  sh -c "echo 'FUND_CODES = [\"000001.OF\", \"003095.OF\"]' > /app/config_custom.py && python main.py --config config_custom.py"
```

## 常见 Docker 问题解决

### 问题1：权限错误
```bash
# 在 Linux/Mac 上
sudo make docker-build

# 或在 docker-compose.yml 中添加用户映射
# user: "${UID}:${GID}"
```

### 问题2：磁盘空间不足
```bash
# 清理Docker无用数据
docker system prune -af
docker volume prune -f
```

### 问题3：网络超时（AKShare 获取数据失败）
```bash
# 增加超时时间
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm \
  -e "AKSHARE_TIMEOUT=30" \
  fund-analysis update-db --funds 000001.OF
```

### 问题4：查看容器内文件
```bash
# 进入容器shell
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm fund-analysis bash

# 查看容器内目录结构
ls -la /app/
```

## Docker 自动化部署示例

### 1. 定时任务（Cron）
```bash
# 编辑 crontab
crontab -e

# 每天凌晨2点更新数据并分析
0 2 * * * cd /path/to/fund_analysis_project && make docker-update-db ARGS="--all" && make docker-analysis ARGS="--all"
```

### 2. 使用 Docker Compose 持久化运行
```bash
# 启动后台服务（适合长时间运行）
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) up -d

# 查看服务状态
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) ps

# 停止服务
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) down
```

### 3. 备份和恢复
```bash
# 备份数据库
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm fund-analysis \
  cp /app/data/fund_data.db /app/data/fund_data_$(date +%Y%m%d).db.bak

# 从备份恢复
docker compose -f [docker/docker-compose.yml](../docker/docker-compose.yml) run --rm fund-analysis \
  cp /app/data/fund_data_20240101.db.bak /app/data/fund_data.db
```

## 快速参考卡片

| 功能 | Docker 命令 | 等效 Python 命令 |
|------|-------------|------------------|
| 完整分析 | `make docker-main` | [python main.py](../main.py) |
| 更新数据 | `make docker-update-db` | [python scripts/update_db.py](../scripts/update_db.py) |
| 数据库分析 | `make docker-analysis` | [python scripts/run_analysis_from_db.py](../scripts/run_analysis_from_db.py) |
| Excel导入 | `make docker-run CMD="import-excel"` | [python scripts/import_excel_to_db.py](../scripts/import_excel_to_db.py) |
| Excel分析 | `make docker-run CMD="analysis-from-excel"` | [python scripts/analysis_from_excel.py](../scripts/analysis_from_excel.py) |
| 重置数据库 | `make docker-run CMD="python reset_database.py reset"` | [python reset_database.py reset](../reset_database.py) |

这样你就可以完全通过 Docker 容器来实现项目的所有功能，无需在本地安装 Python 环境或任何依赖包。