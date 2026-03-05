"""
Analyze profiles.yml and generate dbt Cloud connection migration guidance.
"""

import re
import yaml
from pathlib import Path

from ..models import CheckResult, Issue, Severity

CHECK_NAME = "profiles_migration"

# Mapping from dbt adapter type → dbt Cloud connection type
ADAPTER_TO_CLOUD_CONNECTION = {
    "snowflake": "Snowflake",
    "bigquery": "BigQuery",
    "databricks": "Databricks",
    "spark": "Apache Spark (via Databricks connection in dbt Cloud)",
    "redshift": "Redshift",
    "postgres": "Postgres",
    "duckdb": "DuckDB — NOT supported in dbt Cloud; consider a supported warehouse",
    "trino": "Starburst (Enterprise tier only)",
    "athena": "Athena — not natively available; evaluate Redshift Spectrum or Starburst",
    "synapse": "Azure Synapse Analytics (Fabric)",
    "fabric": "Microsoft Fabric",
    "clickhouse": "ClickHouse — not natively supported in dbt Cloud",
}

# Adapter-specific fields that should use env_var() in Cloud
SENSITIVE_FIELDS = {
    "snowflake": ["password", "private_key", "private_key_passphrase", "token"],
    "bigquery": ["keyfile", "keyfile_json", "token", "refresh_token", "client_secret"],
    "databricks": ["token", "http_password"],
    "redshift": ["password"],
    "postgres": ["password"],
    "spark": ["token", "http_password"],
}

# Recommended DBT_ENV_SECRET_ variable names per adapter
CLOUD_ENV_VARS = {
    "snowflake": {
        "account": "DBT_ENV_SECRET_SNOWFLAKE_ACCOUNT",
        "user": "DBT_ENV_SECRET_SNOWFLAKE_USER",
        "password": "DBT_ENV_SECRET_SNOWFLAKE_PASSWORD",
        "role": "DBT_ENV_SECRET_SNOWFLAKE_ROLE",
        "warehouse": "DBT_ENV_SECRET_SNOWFLAKE_WAREHOUSE",
        "database": "DBT_ENV_SECRET_SNOWFLAKE_DATABASE",
        "schema": "DBT_SNOWFLAKE_SCHEMA",
    },
    "bigquery": {
        "project": "DBT_ENV_SECRET_GCP_PROJECT",
        "dataset": "DBT_BIGQUERY_DATASET",
        "keyfile": "DBT_ENV_SECRET_GCP_KEYFILE",
    },
    "databricks": {
        "host": "DBT_ENV_SECRET_DATABRICKS_HOST",
        "http_path": "DBT_ENV_SECRET_DATABRICKS_HTTP_PATH",
        "token": "DBT_ENV_SECRET_DATABRICKS_TOKEN",
        "catalog": "DBT_DATABRICKS_CATALOG",
        "schema": "DBT_DATABRICKS_SCHEMA",
    },
    "redshift": {
        "host": "DBT_ENV_SECRET_REDSHIFT_HOST",
        "user": "DBT_ENV_SECRET_REDSHIFT_USER",
        "password": "DBT_ENV_SECRET_REDSHIFT_PASSWORD",
        "dbname": "DBT_REDSHIFT_DBNAME",
        "schema": "DBT_REDSHIFT_SCHEMA",
    },
    "postgres": {
        "host": "DBT_ENV_SECRET_POSTGRES_HOST",
        "user": "DBT_ENV_SECRET_POSTGRES_USER",
        "password": "DBT_ENV_SECRET_POSTGRES_PASSWORD",
        "dbname": "DBT_POSTGRES_DBNAME",
        "schema": "DBT_POSTGRES_SCHEMA",
    },
}

ENV_VAR_RE = re.compile(r"env_var\(['\"]([^'\"]+)['\"]")


def _find_profiles_file(project_path: Path) -> Path | None:
    """Look for profiles.yml in project dir, then ~/.dbt/."""
    candidates = [
        project_path / "profiles.yml",
        Path.home() / ".dbt" / "profiles.yml",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def _extract_env_var_name(value) -> str | None:
    """If a value is an env_var() call, return the variable name."""
    if not isinstance(value, str):
        return None
    m = ENV_VAR_RE.search(value)
    return m.group(1) if m else None


def run(project_path: Path) -> CheckResult:
    result = CheckResult(
        name="Profiles Migration",
        description=(
            "Analyzes profiles.yml and generates dbt Cloud connection migration guidance, "
            "environment variable mapping, and security recommendations"
        ),
    )

    profiles_file = _find_profiles_file(project_path)
    if not profiles_file:
        result.issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.INFO,
                message=(
                    "No profiles.yml found in project directory or ~/.dbt/. "
                    "In dbt Cloud, profiles.yml is not needed — connections are configured in the UI."
                ),
            )
        )
        return result

    try:
        with open(profiles_file) as f:
            profiles_data = yaml.safe_load(f)
    except Exception as e:
        result.issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message=f"Could not parse profiles.yml: {e}",
                file=str(profiles_file),
            )
        )
        return result

    if not isinstance(profiles_data, dict):
        return result

    for profile_name, profile in profiles_data.items():
        if not isinstance(profile, dict):
            continue

        outputs = profile.get("outputs", {})
        if not isinstance(outputs, dict):
            continue

        for target_name, target_config in outputs.items():
            if not isinstance(target_config, dict):
                continue

            adapter = target_config.get("type", "unknown").lower()
            cloud_connection = ADAPTER_TO_CLOUD_CONNECTION.get(
                adapter, f"'{adapter}' — check if this adapter is supported in dbt Cloud"
            )

            result.issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.INFO,
                    message=(
                        f"Profile '{profile_name}' / target '{target_name}' (adapter: {adapter}) "
                        f"→ dbt Cloud connection type: {cloud_connection}"
                    ),
                    file=str(profiles_file),
                    fix=(
                        f"In dbt Cloud: go to Account Settings > Connections > New Connection, "
                        f"select '{cloud_connection}' and enter your credentials."
                    ),
                )
            )

            # Check for hardcoded sensitive values (not using env_var)
            sensitive = SENSITIVE_FIELDS.get(adapter, [])
            for field in sensitive:
                val = target_config.get(field)
                if val and not _extract_env_var_name(str(val)):
                    result.issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.ERROR,
                            message=(
                                f"Profile '{profile_name}/{target_name}': field '{field}' "
                                f"appears to be hardcoded (not using env_var())"
                            ),
                            file=str(profiles_file),
                            fix=(
                                f"Replace with: {field}: \"{{{{ env_var('"
                                f"DBT_ENV_SECRET_{field.upper()}') }}}}\""
                            ),
                        )
                    )

            # Generate env var mapping guidance
            env_var_map = CLOUD_ENV_VARS.get(adapter, {})
            if env_var_map:
                mapping_lines = [
                    f"  {field}: ${{{{ env_var('{var}') }}}}"
                    for field, var in env_var_map.items()
                    if field in target_config
                ]
                if mapping_lines:
                    result.issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.INFO,
                            message=(
                                f"Recommended environment variable mapping for "
                                f"'{profile_name}/{target_name}' ({adapter}):\n"
                                + "\n".join(mapping_lines)
                            ),
                            file=str(profiles_file),
                            fix=(
                                "Set these as dbt Cloud environment variables under:\n"
                                "  Deploy > Environments > [your environment] > Environment Variables\n"
                                "Use 'DBT_ENV_SECRET_' prefix for sensitive values (they will be masked in logs)."
                            ),
                        )
                    )

            # Warn about multiple targets (they become separate Cloud environments)
        if len(outputs) > 1:
            result.issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.INFO,
                    message=(
                        f"Profile '{profile_name}' has {len(outputs)} targets: "
                        f"{', '.join(outputs.keys())}. "
                        "In dbt Cloud, each target becomes a separate Environment."
                    ),
                    file=str(profiles_file),
                    fix=(
                        "Create one dbt Cloud Environment per target (e.g., dev, staging, prod) "
                        "and configure the appropriate connection and environment variables for each."
                    ),
                )
            )

    # If profiles.yml is inside the project directory, flag it
    if profiles_file.parent == project_path:
        result.issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message="profiles.yml is inside the project directory and may be committed to git",
                file="profiles.yml",
                fix=(
                    "Add 'profiles.yml' to .gitignore immediately. "
                    "In dbt Cloud, profiles.yml is not needed — delete it from the repo."
                ),
            )
        )

    return result
