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
| `migrate verify <id> ` | 独立运行 `verify.sql` 做审计。 |
| `migrate repair --accept-checksum <id>` | 在人工确认后更新校验和。 |
| `migrate retry --id <id> [--accept-checksum] [--force] [--non-interactive]` | 重置失败迁移并按顺序重新执行到该版本。 |
| `migrate reset-failed --id <id> [--delete] [--non-interactive]` | 仅调整 `schema_migrations` 状态（可删除或置为 reverted）。 |

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

### 失败处理命令

* `migrate retry`：
  * 会确认本地是否存在对应迁移目录，并读取数据库中的状态。
  * 若迁移已应用则直接返回；若仍标记为 `running`，除非显式 `--force`，否则阻止执行。
  * 检测到校验和不一致时，需要显式添加 `--accept-checksum`（会自动调用 repair 逻辑）。
  * 重置状态为 `reverted` 并清空 `applied_at/applied_by/execution_ms/verify_ok` 后，按顺序重新执行直到包含目标迁移。
  * 默认要求交互确认，可通过 `--non-interactive` 配合 `--confirm-prod` 在 CI 中运行。

* `migrate reset-failed`：
  * 仅修改 `schema_migrations`，不会执行任何 SQL 迁移。
  * 默认把状态重置为 `reverted` 并清空执行相关字段，同时保留 `checksum`/`log_ref` 以便审计。
  * 使用 `--delete` 时，会直接删除对应记录，使其看起来像从未执行过。
  * 同样支持交互确认控制，适合在处理失败记录或准备手动恢复时使用。

### 不能在事务块内执行的命令（注意）

PGEVODB 默认在执行 `up.sql` 时使用事务（`autocommit = false`）。因此请**不要**在 `up.sql` 中放置任何可能 **在事务块内报错** 的 PostgreSQL 命令，例如（常见、非穷尽）：

- `CREATE INDEX CONCURRENTLY ...` / `DROP INDEX CONCURRENTLY ...`
- `REFRESH MATERIALIZED VIEW CONCURRENTLY ...`
- `REINDEX` / `REINDEX TABLE` / `REINDEX DATABASE`
- `CREATE DATABASE ...` / `DROP DATABASE ...`
- `CREATE TABLESPACE ...` / `DROP TABLESPACE ...`
- `VACUUM` / `VACUUM FULL`（请参照所用 PostgreSQL 版本文档）
- `CLUSTER ...`
- `ALTER SYSTEM ...`
- 以及其它带 `CONCURRENTLY` 的并发变种或某些维护/管理命令

**处理建议：**
- 对于上面这些命令，请通过外部脚本在非事务上下文执行（例如用 `psql -c "CREATE INDEX CONCURRENTLY ..."`），或在 `meta.yaml` 的 `pre_hooks` / `post_hooks` 中调用外部脚本，或由运维在维护窗口手动运行。  
- 使用 `verify.sql` 做事后校验（PGEVODB 支持 `verify.sql` 且会为其设置 `statement_timeout`）。  
- 本清单为常见命令示例，**并非穷尽**；实际以你所使用 PostgreSQL 版本的官方文档为准。

示例：在 `meta.yaml` 中使用 pre-hook 启动并发索引脚本：
```yaml
timeout_sec: 7200
online_safe: true
pre_hooks:
  - "./scripts/create-concurrent-index.sh"
```


## 开发与调试

1. 准备 Python 环境并安装依赖（需要 `psycopg` 与 `PyYAML`）。
2. 在 `migrations/` 下创建迁移目录并填充 `up.sql`/`down.sql`。
3. 配置 `migrate.yaml`，运行 `python -m pgmigrate.cli status` 检查连接。
4. 在 CI 中可使用 `--non-interactive --confirm-prod` 实现无人值守执行。

欢迎根据实际需求扩展校验逻辑与 Hook 机制。
