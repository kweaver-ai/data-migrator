# Data Migrator

云原生数据库迁移引擎。微服务在代码仓库中维护 `migrations/` 目录，CI 流水线通过本工具完成脚本收集、语法校验、执行校验，生产部署时由 Helm Hook 自动触发迁移。

## 支持的数据库

| 类型 | 支持列表 |
|------|----------|
| 开源数据库 | MariaDB, MySQL, TiDB |
| 信创数据库 | DM8 (达梦), KDB9 (人大金仓), GoldenDB |
| 云/分布式数据库 | OceanBase, TDSQL, TXSQL |

## CI 工作流

```
collect  →  lint  →  check  →  (merge)  →  migrate
 拉脚本    静态校验   DB校验              生产部署
（无DB）   （无DB）  （测试DB）          （生产DB）
```

- **`collect`** — 从 Git 仓库拉取各微服务 `migrations/` 到本地 `repos/`
- **`lint`** — 校验目录结构合规性 + SQL 语法正确性，无 DB 依赖，适合早期快速失败
- **`check`** — 在测试 DB 上执行 SQL，对比多 DB 类型 schema 一致性
- **`migrate`** — 生产部署，由 Helm `pre-install/pre-upgrade` Hook 自动触发

---

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

**规范要点：**
- 数据库类型目录名必须小写（`mariadb`、`dm8`、`kdb9` 等）
- 版本目录名为 semver 格式（`1.0.0`、`1.1.0`）
- 升级脚本编号 `01` ~ `99`，按编号顺序执行
- 支持 `.sql` 和 `.py` 两种格式（`.json` 支持但不建议继续使用）

**`init.sql` 定位：** 是该版本的完整数据库快照，而非增量脚本。包含 `init.sql` 的版本目录，其编号增量脚本不会被执行——`init.sql` 已是该版本的终态。

**Python 脚本：** 以子进程方式执行，通过环境变量注入数据库连接信息（`DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PASSWD`、`DB_TYPE`）。

---

## 使用方式

### 通用参数

所有子命令均支持以下参数：

| 参数 | 必填 | 说明 |
|------|------|------|
| `--config` | 是 | YAML 配置文件路径，参见 `config-template.yaml` |
| `--service` | 否 | 指定本次操作的服务名称，空格分隔；默认处理配置中全部服务 |
| `--log-level` | 否 | `DEBUG` / `INFO` / `WARNING` / `ERROR`，默认 `INFO` |

### collect — 拉取迁移脚本

```bash
MY_PAT=<github_pat> python data-migrator.py collect \
  --config config.yaml \
  --service bkn-backend vega-backend
```

`MY_PAT` 仅在 `repos/<service>` 目录不存在时才需要（用于克隆私有仓库）。目录已存在则跳过克隆。

### lint — 静态校验（无需 DB）

```bash
python data-migrator.py lint \
  --config config.yaml \
  --service bkn-backend vega-backend
```

校验内容：
- 目录结构：db 类型目录名、版本号格式、文件命名规范
- `init.sql`：`USE` 语句存在性、建表语法、表名/索引名命名规范、主键存在性
- 升级脚本：仅允许合法的 DDL / DML 语句类型

### check — 执行校验（需要测试 DB）

```bash
CHECK_RDS_CONFIG=/path/to/check_rds_config.yaml \
  python data-migrator.py check \
  --config config.yaml \
  --service bkn-backend vega-backend
```

`check_rds_config.yaml` 指定各数据库类型的测试实例连接信息（默认路径 `server/check/rds/check_rds_config.yaml`）：

```yaml
mariadb:
  primary:
    host: "127.0.0.1"
    port: 3330
    user: "root"
    password: "xxx"
    charset: "utf8mb4"
    autocommit: true
  secondary:
    host: "127.0.0.1"
    port: 3331
    user: "root"
    password: "xxx"
    charset: "utf8mb4"
    autocommit: true
dm8:
  primary:
    host: "127.0.0.1"
    port: 5237
    user: "SYSDBA"
    password: "xxx"
    autocommit: true
  secondary:
    host: "127.0.0.1"
    port: 5238
    user: "SYSDBA"
    password: "xxx"
    autocommit: true
```

> 建议将此文件放在项目根目录（已加入 `.gitignore`），通过 `CHECK_RDS_CONFIG` 指向，避免意外提交凭证。

### migrate — 执行迁移（生产部署）

通常由 Helm Hook 自动触发，无需手动执行。本地调试时：

```bash
python data-migrator.py migrate \
  --config /app/config.yaml \
  --service service-a service-b service-c
```

### 本地开发环境变量

推荐在项目根创建 `.env` 文件（已加入 `.gitignore`）：

```bash
export MY_PAT=github_pat_xxxx
export CHECK_RDS_CONFIG=/path/to/check_rds_config.yaml
```

| 环境变量 | 用于子命令 | 说明 |
|----------|-----------|------|
| `MY_PAT` | `collect` | GitHub PAT，拉取私有仓库时使用 |
| `CHECK_RDS_CONFIG` | `check` | 测试 DB 连接配置文件路径 |

---

## 技术限制

- **DDL 不可回滚** — 多数数据库 DDL 触发隐式提交，失败后需人工修复业务库，引擎通过熔断锁定保护现场
- **SQL 幂等预检局限** — 基于 sqlparse 解析，极其复杂的非标准 SQL 可能识别失败，此类语句将直接执行
- **凭证可见性** — Helm values 中的密码经 kubelet 展开后明文出现在 Pod Spec 中，建议通过 RBAC 限制读取权限

---

## 平台参考

> 以下内容面向平台/运维同学，微服务开发者通常无需关心。

### 迁移工作流

#### 脚本筛选规则

**首次安装：**
1. 从 `TARGET_VERSION` 逆序回溯，找到最近包含 `init.sql` 的版本目录（记为 `V_base`）
2. 执行 `V_base/init.sql`（完整快照）
3. 按序执行 `V_base` **之后**所有版本的编号增量脚本（`V_base` 自身的增量脚本不执行）

**版本升级：**
1. 跳过所有版本的 `init.sql`
2. 从当前记录版本之后开始，按序执行各版本的编号增量脚本

#### 核心状态机

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

#### SQL 幂等预检

对每个 `.sql` 文件：
1. `sqlparse.split()` 拆分为独立语句
2. 过滤注释和空语句
3. 对每条语句：解析 DDL 类型 → 查询元数据判断是否已生效 → 已生效跳过，未生效执行
4. 执行失败 → 记录异常，主任务置为 `fail`，`exit 1`

### Helm 集成与统一镜像构建

Hook Job 声明在 Umbrella Chart 的顶层，而非各子 Chart 内部。Helm 执行 `install/upgrade` 时先拉起此 Job 完成所有微服务的迁移，成功后再部署各子 Chart 的业务 Pod。

CI/CD 将各微服务 migrations 目录与引擎代码打包为统一镜像：

```
/app/
├── engine/                     # 迁移引擎代码
│   ├── data-migrator.py
│   └── ...
└── migrations/                 # 全量微服务迁移脚本
    ├── service-a/
    │   ├── mariadb/
    │   └── dm8/
    └── service-b/
```

### 管控库表结构

`deploy` 管控库采用"任务主表 + 历史流水表"双表设计：

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

### 存量环境基线补录

对已有数据库但未纳入引擎管控的存量环境，通过独立的 baseline 工具脚本执行一次性基线初始化，将 `<= target_version` 的所有脚本标记为已成功执行，仅写入 `deploy` 管控库，不操作业务库。详见 baseline 工具的独立文档。

---

## 依赖

- Python >= 3.10
- 主要依赖：PyYAML, requests, tenacity, sqlparse, GitPython, dbutils
- 内部依赖：proton-rds-sdk-py

## License

See [LICENSE](LICENSE).
