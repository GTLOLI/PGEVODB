# PGEVODB

PGEVODB 是一个专注于 PostgreSQL 14+ 的数据库迁移管理工具。
它使用 Python 编写，遵循 “一个目录一个迁移” 的约束，能够可靠地执行升级与回退，同时跟踪状态与校验和，防止脚本被静默修改。

## 目录结构

```
.
├── migrate.yaml          # 全局配置（必需）
├── migrations/           # 迁移目录根（可自定义）
│   └── 2025-11-12T10-15-30__add_email_to_users/
│       ├── up.sql        # 升级脚本（必需）
│       ├── down.sql      # 回退脚本（必需，可为空）
│       ├── verify.sql    # 可选校验脚本
│       └── meta.yaml     # 可选元信息
└── src/pgmigrate         # CLI 与执行逻辑
```

迁移目录按照名称（时间戳 + 描述）排序并依次执行。`meta.yaml` 支持配置 `timeout_sec`、`tags`、`reversible`、`requires` 等信息。

## 配置文件 migrate.yaml 示例

```yaml
profiles:
  dev:
    dsn: "postgresql://dev_user:dev_pass@127.0.0.1:5432/devdb"
    schema: "public"
    app_env: "dev"
    confirm_prod: false
  prod:
    dsn: "postgresql://prod_user:***@prod.db.internal:5432/proddb"
    schema: "public"
    app_env: "prod"
    confirm_prod: true

default_profile: dev

global:
  migrations_dir: "./migrations"
  log_dir: "./.migrate-logs"
  lock_key: 732161886195405824
  timeout_sec: 600
  allow_tags: ["expand", "datafix"]
  interactive: true
```

## CLI 用法

在项目根目录执行（或通过 `python -m pgmigrate.cli` 调用）：

```
python -m pgmigrate.cli [全局参数] <命令> [命令参数]
```

常用命令：

| 命令 | 说明 |
| --- | --- |
| `migrate status` | 展示当前迁移状态（已应用、待执行、失败列表）。 |
| `migrate plan [--to <id>]` | 按顺序列出即将执行的迁移。 |
| `migrate up [--to <id>] [--non-interactive]` | 执行待定迁移到指定版本或最新。 |
| `migrate down --to <id> [--non-interactive]` | 逆序回退至目标版本（包含）。 |
| `migrate verify [--latest|--id <id>]` | 独立运行 `verify.sql` 做审计。 |
| `migrate repair --accept-checksum <id>` | 在人工确认后更新校验和。 |

全局参数说明：

| 参数 | 作用 |
| --- | --- |
| `--env <profile>` | 选择配置档（默认 `default_profile`）。 |
| `--dsn <dsn>` | 临时覆盖连接串。 |
| `--config <path>` | 指定配置文件。 |
| `--log-dir <path>` / `--migrations-dir <path>` | 覆盖默认目录。 |
| `--timeout-sec <seconds>` | 全局超时覆盖。 |
| `--non-interactive` | 关闭所有交互提示（CI 场景）。 |
| `--confirm-prod` | 在 `confirm_prod=true` 的环境下以参数确认执行。 |

## 关键特性

* **状态表**：自动维护 `schema_migrations` 表，记录校验和、执行耗时、日志引用及验证结果。
* **校验和保护**：检测到脚本修改时阻断执行，需显式 `repair` 才能继续。
* **全局锁**：使用 `pg_try_advisory_lock` 防止多进程并发迁移。
* **日志与验证**：每次执行生成独立日志文件，支持可选的 `verify.sql` 校验。
* **安全回退**：对不可逆迁移在回退时硬性阻断。

## 开发与调试

1. 准备 Python 环境并安装依赖（需要 `psycopg` 与 `PyYAML`）。
2. 在 `migrations/` 下创建迁移目录并填充 `up.sql`/`down.sql`。
3. 配置 `migrate.yaml`，运行 `python -m pgmigrate.cli status` 检查连接。
4. 在 CI 中可使用 `--non-interactive --confirm-prod` 实现无人值守执行。

欢迎根据实际需求扩展校验逻辑与 Hook 机制。
