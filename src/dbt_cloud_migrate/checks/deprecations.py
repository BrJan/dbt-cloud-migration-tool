"""
Check for deprecated dbt syntax and configuration that needs updating
before or after migrating to dbt Cloud.
"""

import re
import yaml
from pathlib import Path
from typing import Iterator

from ..models import CheckResult, Issue, Severity

CHECK_NAME = "deprecated_syntax"

# dbt_project.yml keys renamed in dbt 1.0
RENAMED_PROJECT_KEYS = {
    "source-paths": ("model-paths", "dbt 1.0"),
    "data-paths": ("seed-paths", "dbt 1.0"),
    "modules-path": ("packages-install-path", "dbt 1.0"),
}

# dbt_utils macros renamed/removed
DEPRECATED_MACROS = {
    "dbt_utils.surrogate_key": (
        "dbt_utils.generate_surrogate_key",
        "Renamed in dbt-utils 0.8.0",
    ),
    "dbt_utils.current_timestamp": (
        "dbt.current_timestamp",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.current_timestamp_in_utc": (
        "dbt.current_timestamp_in_utc",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.last_day": (
        "dbt.last_day",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.date_trunc": (
        "dbt.date_trunc",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.dateadd": (
        "dbt.dateadd",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.datediff": (
        "dbt.datediff",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.safe_cast": (
        "dbt.safe_cast",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.hash": (
        "dbt.hash",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_string": (
        "dbt.type_string",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_float": (
        "dbt.type_float",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_numeric": (
        "dbt.type_numeric",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_int": (
        "dbt.type_int",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_bigint": (
        "dbt.type_bigint",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_timestamp": (
        "dbt.type_timestamp",
        "Moved to dbt core in dbt 1.2",
    ),
    "dbt_utils.type_boolean": (
        "dbt.type_boolean",
        "Moved to dbt core in dbt 1.2",
    ),
}

# Regex: env_var() without a default (second argument)
ENV_VAR_NO_DEFAULT_RE = re.compile(
    r"""env_var\(\s*['"][^'"]+['"]\s*\)""",
    re.MULTILINE,
)

# Regex: detect {{ target.name }} usage (warn to use env var approach in Cloud)
TARGET_NAME_RE = re.compile(r"\{\{[-\s]*target\.name[-\s]*\}\}")

# Detect hardcoded database refs in FROM/JOIN (not using ref() or source())
HARDCODED_DB_RE = re.compile(
    r"""\bfrom\s+`?[a-zA-Z0-9_]+`?\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+""",
    re.IGNORECASE,
)


def _iter_yaml_files(project_path: Path) -> Iterator[Path]:
    for ext in ("*.yml", "*.yaml"):
        yield from project_path.rglob(ext)


def _iter_sql_files(project_path: Path) -> Iterator[Path]:
    yield from project_path.rglob("*.sql")


def _check_dbt_project_yml(project_path: Path) -> list[Issue]:
    issues: list[Issue] = []
    dbt_project_file = project_path / "dbt_project.yml"
    if not dbt_project_file.exists():
        return issues

    try:
        with open(dbt_project_file) as f:
            content = f.read()
        data = yaml.safe_load(content)
    except Exception as e:
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message=f"Could not parse dbt_project.yml: {e}",
                file=str(dbt_project_file.relative_to(project_path)),
            )
        )
        return issues

    if not isinstance(data, dict):
        return issues

    for old_key, (new_key, since) in RENAMED_PROJECT_KEYS.items():
        if old_key in data:
            lines = content.splitlines()
            line_num = next(
                (i + 1 for i, l in enumerate(lines) if l.strip().startswith(old_key)),
                None,
            )
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.ERROR,
                    message=f"Deprecated key '{old_key}' found in dbt_project.yml (renamed to '{new_key}' in {since})",
                    file="dbt_project.yml",
                    line=line_num,
                    fix=f"Rename '{old_key}:' to '{new_key}:' in dbt_project.yml",
                )
            )

    # Warn if config-version is present (it's being phased out)
    if "config-version" in data:
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.WARNING,
                message="'config-version: 2' is no longer required in dbt_project.yml (dbt 1.5+)",
                file="dbt_project.yml",
                fix="Remove the 'config-version: 2' line from dbt_project.yml",
            )
        )

    return issues


def _check_schema_yaml_files(project_path: Path) -> list[Issue]:
    """Check for old 'tests:' key (should be 'data_tests:' in dbt 1.8+)."""
    issues: list[Issue] = []

    skip_dirs = {project_path / d for d in ("target", "dbt_packages", ".git", "node_modules")}

    for yaml_file in _iter_yaml_files(project_path):
        if any(yaml_file.is_relative_to(d) for d in skip_dirs):
            continue

        try:
            with open(yaml_file) as f:
                content = f.read()
            data = yaml.safe_load(content)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        rel_path = str(yaml_file.relative_to(project_path))
        lines = content.splitlines()

        # Check for old 'tests:' key at model/source level and column level
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped == "tests:" or stripped.startswith("tests:"):
                # Make sure it's not inside a config block or a column constraint
                # Simple heuristic: if indented and previous context is a model/column
                issues.append(
                    Issue(
                        check=CHECK_NAME,
                        severity=Severity.WARNING,
                        message=f"Found legacy 'tests:' key — prefer 'data_tests:' (introduced in dbt 1.8)",
                        file=rel_path,
                        line=i,
                        fix="Rename 'tests:' to 'data_tests:' in this YAML file",
                    )
                )

        # Check for 'version: 2' at top level of schema files (no longer needed)
        if isinstance(data, dict) and data.get("version") == 2:
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.INFO,
                    message="'version: 2' at the top of schema YAML files is no longer required (dbt 1.5+)",
                    file=rel_path,
                    fix="Remove the 'version: 2' line or update to 'version: 1' (current default)",
                )
            )

    return issues


def _check_packages_yml(project_path: Path) -> list[Issue]:
    issues: list[Issue] = []
    packages_file = project_path / "packages.yml"
    if not packages_file.exists():
        return issues

    try:
        with open(packages_file) as f:
            data = yaml.safe_load(f)
    except Exception as e:
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message=f"Could not parse packages.yml: {e}",
                file="packages.yml",
            )
        )
        return issues

    packages = (data or {}).get("packages", [])
    for pkg in packages:
        if isinstance(pkg, dict):
            if "git" in pkg and "revision" not in pkg:
                issues.append(
                    Issue(
                        check=CHECK_NAME,
                        severity=Severity.ERROR,
                        message=f"Git package '{pkg['git']}' has no 'revision' pinned — unpredictable in dbt Cloud CI",
                        file="packages.yml",
                        fix="Add 'revision: <tag_or_sha>' to pin this package to a specific version",
                    )
                )
            if "package" in pkg and "version" not in pkg:
                issues.append(
                    Issue(
                        check=CHECK_NAME,
                        severity=Severity.WARNING,
                        message=f"Hub package '{pkg['package']}' has no version pinned",
                        file="packages.yml",
                        fix="Add 'version: \">=x.y.z,<x.z.0\"' to prevent unexpected upgrades",
                    )
                )

    return issues


def _check_sql_files(project_path: Path) -> list[Issue]:
    issues: list[Issue] = []
    skip_dirs = {project_path / d for d in ("target", "dbt_packages", ".git")}

    for sql_file in _iter_sql_files(project_path):
        if any(sql_file.is_relative_to(d) for d in skip_dirs):
            continue

        try:
            content = sql_file.read_text()
        except Exception:
            continue

        rel_path = str(sql_file.relative_to(project_path))
        lines = content.splitlines()

        # Check for deprecated macros
        for macro, (replacement, note) in DEPRECATED_MACROS.items():
            for i, line in enumerate(lines, 1):
                if macro in line:
                    issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.WARNING,
                            message=f"Deprecated macro '{macro}' found ({note})",
                            file=rel_path,
                            line=i,
                            fix=f"Replace '{macro}' with '{replacement}'",
                        )
                    )

        # env_var() without default — will fail in Cloud if var not set
        for match in ENV_VAR_NO_DEFAULT_RE.finditer(content):
            line_num = content[: match.start()].count("\n") + 1
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message=f"env_var() call without a default value: {match.group().strip()}",
                    file=rel_path,
                    line=line_num,
                    fix="Add a default: env_var('VAR_NAME', 'default_value') to prevent job failures when the variable is unset",
                )
            )

        # {{ target.name }} usage — warn to use env vars in Cloud
        for match in TARGET_NAME_RE.finditer(content):
            line_num = content[: match.start()].count("\n") + 1
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.INFO,
                    message="Direct use of {{ target.name }} detected",
                    file=rel_path,
                    line=line_num,
                    fix="In dbt Cloud, set custom target names via environment variables (DBT_TARGET_NAME) rather than relying on {{ target.name }} branching logic",
                )
            )

        # Hardcoded 3-part database references
        for match in HARDCODED_DB_RE.finditer(content):
            line_num = content[: match.start()].count("\n") + 1
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message=f"Possible hardcoded database reference: '{match.group().strip()}'",
                    file=rel_path,
                    line=line_num,
                    fix="Use {{ ref('model_name') }} or {{ source('source', 'table') }} instead of hardcoded database.schema.table references",
                )
            )

    return issues


def run(project_path: Path) -> CheckResult:
    result = CheckResult(
        name="Deprecated Syntax",
        description="Detects deprecated dbt config keys, renamed YAML fields, and outdated macro usage",
    )

    result.issues.extend(_check_dbt_project_yml(project_path))
    result.issues.extend(_check_schema_yaml_files(project_path))
    result.issues.extend(_check_packages_yml(project_path))
    result.issues.extend(_check_sql_files(project_path))

    return result
