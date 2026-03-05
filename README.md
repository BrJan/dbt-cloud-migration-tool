# dbt-cloud-migrate

A CLI tool and MCP server that audits dbt Core projects and generates actionable guidance for migrating to dbt Cloud.

## What it does

`dbt-cloud-migrate` scans your dbt project and reports issues across three areas:

- **Profiles migration** — analyzes `profiles.yml`, maps adapter types to dbt Cloud connection types, flags hardcoded credentials, and generates recommended `DBT_ENV_SECRET_` environment variable mappings
- **Project structure** — checks model layer organization (staging/intermediate/marts), naming conventions (`stg_`, `int_`, `fct_`, `dim_`), source YAML definitions, documentation coverage, primary key test coverage, and `.gitignore` settings
- **Deprecated syntax** — detects renamed `dbt_project.yml` keys, legacy `tests:` YAML keys, deprecated `dbt_utils` macros, `env_var()` calls without defaults, hardcoded `target.name` references, hardcoded 3-part database references, and unpinned packages

Each issue includes a severity level (`ERROR`, `WARNING`, `INFO`) and a concrete fix recommendation.

## Installation

Requires Python 3.10+.

```bash
git clone <repo>
cd dbt-refactoring-tool
python3 -m venv .venv
.venv/bin/pip install -e .
```

## CLI Usage

### Run all checks

```bash
dbt-cloud-migrate check /path/to/your/dbt/project
```

Run from your project root:

```bash
cd ~/projects/my_dbt_project
dbt-cloud-migrate check .
```

### Output formats

```bash
# Rich terminal output (default)
dbt-cloud-migrate check . --output rich

# JSON — useful for CI or piping to other tools
dbt-cloud-migrate check . --output json

# Compact summary table
dbt-cloud-migrate check . --output summary
```

### Run specific checks only

```bash
dbt-cloud-migrate check . --only profiles
dbt-cloud-migrate check . --only deprecations
dbt-cloud-migrate check . --only profiles,deprecations
```

Available check names: `profiles`, `structure`, `deprecations`

### Profiles-only command

```bash
dbt-cloud-migrate profiles /path/to/project
```

### Auto-fix deprecated syntax

Fix renamed `dbt_project.yml` keys and `tests:` → `data_tests:` in schema YAML files:

```bash
# Preview changes without modifying files
dbt-cloud-migrate fix . --dry-run

# Apply fixes
dbt-cloud-migrate fix .
```

The `--fix` flag on the `check` command also applies these fixes inline:

```bash
dbt-cloud-migrate check . --fix
```

### Version

```bash
dbt-cloud-migrate version
```

## MCP Server

`dbt-cloud-migrate` ships an MCP server that exposes its checks as tools, allowing Claude (Desktop, Code, or API) to audit and fix dbt projects during a conversation.

### Start the server

```bash
# stdio transport (default, for Claude Desktop / Claude Code)
dbt-cloud-migrate-mcp

# SSE transport (for remote clients)
dbt-cloud-migrate-mcp --port 8000
```

### Configure in Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dbt-cloud-migrate": {
      "command": "dbt-cloud-migrate-mcp"
    }
  }
}
```

### Configure in Claude Code

Add to `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "dbt-cloud-migrate": {
      "type": "stdio",
      "command": "dbt-cloud-migrate-mcp"
    }
  }
}
```

### Available MCP tools

| Tool | Description |
|------|-------------|
| `check_project` | Run all checks and return a full JSON report |
| `check_profiles` | Analyze `profiles.yml` and generate Cloud connection guidance |
| `check_structure` | Audit model organization, naming, sources, docs, and tests |
| `check_deprecations` | Scan for deprecated syntax and configuration |
| `fix_deprecations` | Auto-fix deprecated keys and `tests:` → `data_tests:`. Supports `dry_run: true` |

## Checks reference

### Profiles migration

- Adapter → dbt Cloud connection type mapping (Snowflake, BigQuery, Databricks, Redshift, Postgres, and more)
- Hardcoded sensitive fields (`password`, `token`, `private_key`, etc.) that should use `env_var()`
- Recommended `DBT_ENV_SECRET_` variable names per adapter
- Multiple targets → separate dbt Cloud Environments guidance
- `profiles.yml` committed to the project directory (security risk)

### Project structure

- `profiles.yml` inside the project repo
- Missing or incomplete `.gitignore` (`target/`, `dbt_packages/`, `profiles.yml`, `logs/`)
- Missing `dbt_project.yml` or required keys (`name`, `version`, `profile`)
- SQL models in the root `models/` directory (not organized into layers)
- Missing standard layer folders (`staging/`, `intermediate/`, `marts/`)
- Naming convention violations (`stg_` in staging, `int_` in intermediate, `fct_`/`dim_` in marts)
- No source YAML files in `models/staging/`
- Models with no schema YAML entry or empty `description`
- Primary key columns (`id`, `*_id`, `*_key`, `*_pk`) missing `unique` + `not_null` tests

### Deprecated syntax

- Renamed `dbt_project.yml` keys: `source-paths` → `model-paths`, `data-paths` → `seed-paths`, `modules-path` → `packages-install-path`
- `config-version: 2` no longer required (dbt 1.5+)
- Legacy `tests:` key in schema YAML (should be `data_tests:` in dbt 1.8+)
- `version: 2` in schema YAML files (no longer required in dbt 1.5+)
- Unpinned Hub packages (no `version`) and unpinned Git packages (no `revision`)
- Deprecated `dbt_utils` macros moved to dbt core (`dbt_utils.surrogate_key`, `dbt_utils.current_timestamp`, type macros, date macros)
- `env_var()` calls without a default value (will fail in Cloud if variable is unset)
- `{{ target.name }}` usage (recommend environment variable approach in Cloud)
- Hardcoded 3-part `database.schema.table` references in SQL (should use `ref()` or `source()`)

## Exit codes

- `0` — all checks passed
- `1` — one or more ERROR or WARNING issues found

This makes `dbt-cloud-migrate check` suitable for use in CI pipelines.
