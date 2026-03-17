# Data Migrator

云原生数据库迁移引擎。通过 Helm Umbrella Chart 统一编排，自动完成微服务数据库的初始化与版本升级。支持 9 种异构数据库，内置 SQL 幂等预检、故障熔断与断点续传能力。

## 核心特性

- **伞形 Chart 顶层触发** — Umbrella Chart 顶层声明 Hook Job，一次性完成所有微服务迁移后再部署业务 Pod
- **SQL 幂等预检** — 执行前自动解析 DDL 类型，查询元数据判断是否已生效，已生效则跳过
- **故障熔断** — DDL 无法回滚，遇异常立即阻断发布流水线（`exit 1`），保护现场
- **断点续传** — 失败后人工修复业务库，重新触发部署即可从断点恢复
- **Checksum 审计** — 记录每个脚本的哈希值，跨环境一致性校验（不一致时输出警告，不阻断）

## 支持的数据库

| 类型 | 支持列表 |
|------|----------|
| 开源数据库 | MariaDB, MySQL, TiDB |
| 信创数据库 | DM8 (达梦), KDB9 (人大金仓), GoldenDB |
| 云/分布式数据库 | OceanBase, TDSQL, TXSQL |

## 架构概览

采用"统一镜像构建、伞形 Chart 顶层 Hook 触发、中心化审计"的三段式设计：

```
┌─────────────────────────────────────────────────────────────┐
│  CI/CD 构建阶段                                               │
│  收集各微服务 migrations/ 目录 + 引擎代码 → 统一 Docker 镜像     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
            ┌─────────────────────────────┐
            │  helm upgrade (Umbrella)     │
            └──────────────┬──────────────┘
                           │
                           ▼
            ┌─────────────────────────────┐
            │  顶层 pre-install/upgrade    │
            │  Hook Job (Migration)        │
            │  依次为每个微服务执行迁移      │
            └──────────────┬──────────────┘
                           │
                           ▼
        ┌─────────────────────────────────────┐
        │      deploy 管控库 (状态 + 审计)      │
        │  schema_migration_task              │
        │  schema_migration_history           │
        └──────────────┬──────────────────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      [业务库 A]    [业务库 B]    [业务库 C]
```

伞形 Chart 在顶层声明 `pre-install/pre-upgrade` Hook，统一拉起一个 Migration Job。该 Job 依次为所有微服务执行迁移，全部完成后 Helm 再部署各子 Chart 的业务 Pod。

## 迁移脚本目录规范

各微服务在代码仓库中维护 `migrations/` 目录：

```
<service-repo>/migrations/
├── mariadb/
│   ├── 1.0.0/
│   │   ├── init.sql              # 该版本的完整数据库快照（建表+建索引+初始数据）
│   │   ├── 01-add-column.sql     # 增量脚本，按编号顺序执行
│   │   ├── 02-create-index.sql
│   │   └── 03-data-clean.py      # Python 数据清洗脚本
│   └── 1.1.0/
│       ├── init.sql
│       └── 01-alter-table.sql
├── dm8/
│   └── ...
└── kdb9/
    └── ...
```

**目录规范：**
- 数据库类型目录名必须小写（`mariadb`、`dm8`、`kdb9`、`mysql` 等）
- 升级脚本编号 `01` ~ `99`，按编号顺序执行
- 仅支持 `.sql` 和 `.py` 两种格式（`.json` 支持，但不建议继续使用）

**`init.sql` 定位：** 是该版本的完整数据库快照，而非增量脚本。`V_base` 版本目录中的编号增量脚本不会被执行，因为 `init.sql` 已经是该版本的终态。

**Python 脚本：** 以子进程方式执行，通过环境变量注入数据库连接信息。

## 迁移工作流

### 核心状态机

引擎启动后执行严格的单向状态机流转：

```
[节点 0] Helm Hook 拉起 Pod，通过 CLI args 接收全部配置
    │
    ▼
[节点 1] 任务注册
    │  INSERT/UPDATE task 记录，status='running'
    ▼
[节点 2] 校验历史与断点计算
    │  扫描版本目录，查询执行流水表
    │  Checksum 不一致 → 警告日志（不阻断）
    ▼
[节点 3] 循环执行脚本
    │  .sql → sqlparse 解析 + 幂等预检 + 执行
    │  .py  → 传入连接池动态执行
    │  失败 → 熔断，status='fail', exit 1
    ▼
[节点 4] 全部成功 → status='success', exit 0
```

### 脚本筛选规则

**首次安装：**
1. 从 `TARGET_VERSION` 逆序回溯，找到最近的包含 `init.sql` 的版本目录（记为 `V_base`）
2. 执行 `V_base/init.sql`（完整快照）
3. 按序执行 `V_base` **之后**所有版本的编号增量脚本（`V_base` 自身的增量脚本不执行）

**版本升级：**
1. 跳过所有版本的 `init.sql`
2. 从当前记录版本之后开始，按序执行各版本的编号增量脚本

### SQL 幂等预检

对每个 `.sql` 文件：
1. `sqlparse.split()` 拆分为独立语句
2. 过滤注释和空语句
3. 对每条语句：解析 DDL 类型 → 查询元数据判断是否已生效 → 已生效跳过，未生效执行
4. 执行失败 → 记录异常，主任务置为 `fail`，`exit 1`

## 数据库设计

`deploy` 管控库中采用"任务主表 + 历史流水表"双表设计：

### `schema_migration_task` — 任务管控

```sql
CREATE TABLE IF NOT EXISTS `schema_migration_task` (
  `id`              BIGINT(20) NOT NULL AUTO_INCREMENT,
  `service_name`    VARCHAR(50) NOT NULL,
  `target_version`  VARCHAR(50) NOT NULL,
  `current_script`  VARCHAR(255) NOT NULL DEFAULT '',
  `status`          VARCHAR(20) NOT NULL,       -- running / success / fail
  `error_msg`       TEXT,
  `create_time`     DATETIME DEFAULT CURRENT_TIMESTAMP,
  `update_time`     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_service_version` (`service_name`, `target_version`)
);
```

### `schema_migration_history` — 单步执行流水

```sql
CREATE TABLE IF NOT EXISTS `schema_migration_history` (
  `id`                BIGINT(20) NOT NULL AUTO_INCREMENT,
  `service_name`      VARCHAR(50) NOT NULL,
  `target_version`    VARCHAR(50) NOT NULL,
  `script_name`       VARCHAR(255) NOT NULL,    -- 如 1.0.0/01-add-column.sql
  `checksum`          VARCHAR(64) NOT NULL,
  `status`            VARCHAR(20) NOT NULL,     -- success / fail
  `execution_time_ms` INT DEFAULT 0,
  `create_time`       DATETIME DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_service_script` (`service_name`, `script_name`)
);
```

## 使用方式

### 子命令

| 子命令 | 说明 |
|--------|------|
| `migrate` | 执行数据库迁移（初始化 + 增量升级） |
| `collect` | 从 Git 仓库下载和收集各微服务的 migrations 脚本文件 |
| `check` | 检测和验证迁移脚本的目录结构与 SQL 正确性 |

### 通用参数

| 参数 | 说明 |
|------|------|
| `--config` | 配置文件路径（包含数据库连接、目标版本等） |
| `--service` | 微服务列表，指定本次操作的微服务范围 |

### 迁移执行（通过 Umbrella Chart 顶层 Hook 自动触发）

```bash
python data-migrator.py migrate \
  --config /app/config.yaml \
  --service service-a service-b service-c
```

### 收集迁移脚本

```bash
python data-migrator.py collect \
  --config /app/config.yaml \
  --service service-a service-b service-c
```

### 检测和验证

```bash
python data-migrator.py check \
  --config /app/config.yaml \
  --service service-a service-b
```

### 存量环境基线补录

对已有数据库但未纳入引擎管控的存量环境，通过独立的 baseline 工具脚本执行一次性基线初始化，将 `<= target_version` 的所有脚本标记为已成功执行，仅写入 `deploy` 管控库，不操作业务库。详见 baseline 工具的独立文档。

## Helm 集成

Hook Job 声明在 Umbrella Chart 的顶层，而非各子 Chart 内部。Helm 执行 `install/upgrade` 时先拉起此 Job 完成所有微服务的迁移，成功后再部署各子 Chart 的业务 Pod。

Hook Job 模板详见 Helm Chart 模板文件。

## 统一镜像构建

CI/CD 将各微服务 migrations 目录与引擎代码打包为统一镜像：

```
/app/
├── engine/                     # 迁移引擎代码
│   ├── data-migrator.py        # 入口
│   ├── sql_parser.py
│   ├── db_drivers/
│   └── requirements.txt
└── migrations/                 # 全量微服务迁移脚本
    ├── service-a/
    │   ├── mariadb/
    │   └── dm8/
    ├── service-b/
    └── service-c/
```

## CI 校验

即 `check` 子命令，校验配置详见 `config-template.yaml`。

## 技术限制

- **DDL 不可回滚** — 多数数据库 DDL 触发隐式提交，失败后需人工修复业务库，引擎通过熔断锁定保护现场
- **SQL 幂等预检局限** — 基于 sqlparse 解析，极其复杂的非标准 SQL 可能识别失败，此类语句将直接执行
- **凭证可见性** — args 中的密码经 kubelet 展开后明文出现在 Pod Spec 中，建议通过 RBAC 限制读取权限

## 依赖

- Python >= 3.10
- 主要依赖：PyYAML, requests, tenacity, sqlparse, GitPython, dbutils
- 内部依赖：proton-rds-sdk-py

## License

See [LICENSE](LICENSE).