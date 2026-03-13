# dbt-cloud-migrate

A CLI tool and MCP server that audits dbt Core projects and generates actionable guidance for migrating to dbt Cloud. Designed to work alongside the [official dbt MCP server](https://github.com/dbt-labs/dbt-mcp).

## How it fits with the dbt MCP server

| Tool | Role |
|------|------|
| **dbt MCP server** (`uvx dbt-mcp`) | Runs dbt commands (`compile`, `build`, `test`, `show`), queries the Semantic Layer, Discovery API, and Admin API |
| **dbt-cloud-migrate** (this tool) | Audits your project for migration blockers: profiles config, structure issues, deprecated syntax |

The typical workflow is:
1. Run `check_project` or `check_deprecations` to find migration issues
2. Run `fix_deprecations` to auto-fix safe changes
3. Use the dbt MCP server's `compile` or `build` tools to confirm the project still works

## What it checks

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

## MCP Server Setup

### Prerequisites

Install `uv` for the dbt MCP server:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Claude Code — add both servers

```bash
# Official dbt MCP server
claude mcp add dbt -s user -- uvx dbt-mcp

# Migration audit server (this tool)
claude mcp add dbt-cloud-migrate -s user -- dbt-cloud-migrate-mcp
```

Set your project path for the dbt MCP server — edit `~/.claude.json` and add env vars:

```json
{
  "mcpServers": {
    "dbt": {
      "type": "stdio",
      "command": "uvx",
      "args": ["dbt-mcp"],
      "env": {
        "DBT_PROJECT_DIR": "/path/to/your/dbt/project",
        "DBT_PATH": "/path/to/dbt"
      }
    },
    "dbt-cloud-migrate": {
      "type": "stdio",
      "command": "dbt-cloud-migrate-mcp"
    }
  }
}
```

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dbt": {
      "command": "uvx",
      "args": ["dbt-mcp"],
      "env": {
        "DBT_PROJECT_DIR": "/path/to/your/dbt/project",
        "DBT_PATH": "/path/to/dbt"
      }
    },
    "dbt-cloud-migrate": {
      "command": "dbt-cloud-migrate-mcp"
    }
  }
}
```

### Available MCP tools

**dbt-cloud-migrate tools** (this server):

| Tool | Description |
|------|-------------|
| `check_project` | Run all migration checks and return a full JSON report |
| `check_profiles` | Analyze `profiles.yml` and generate Cloud connection guidance |
| `check_structure` | Audit model organization, naming, sources, docs, and tests |
| `check_deprecations` | Scan for deprecated syntax and configuration |
| `fix_deprecations` | Auto-fix deprecated keys and `tests:` → `data_tests:`. Supports `dry_run: true` |

**dbt MCP server tools** (complement these with):

| Tool | Description |
|------|-------------|
| `dbt_compile` | Compile models to validate SQL after fixes |
| `dbt_build` | Run and test models end-to-end |
| `dbt_show` | Preview model output |
| `list_metrics` | Query the Semantic Layer |

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

After fixing, validate with the dbt MCP server or CLI:

```bash
dbt compile
```

### Version

```bash
dbt-cloud-migrate version
```

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
