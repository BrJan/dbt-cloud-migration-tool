"""
Check project structure against dbt Cloud best practices:
- Model organization (staging / intermediate / marts layers)
- Naming conventions
- Source definitions
- Documentation coverage
- Test coverage on primary keys
"""

import re
import yaml
from pathlib import Path

from ..models import CheckResult, Issue, Severity

CHECK_NAME = "project_structure"

# Expected model layer prefixes and their canonical folder names
LAYER_PREFIXES = {
    "stg_": "staging",
    "int_": "intermediate",
    "fct_": "marts",
    "dim_": "marts",
    "rpt_": "marts",
    "agg_": "marts",
}

EXPECTED_TOP_LEVEL_DIRS = {"staging", "intermediate", "marts"}

# Columns that should have unique + not_null tests
PK_COLUMN_PATTERNS = re.compile(
    r"^(id|.*_id|.*_key|.*_pk|surrogate_key|primary_key)$", re.IGNORECASE
)

REF_OR_SOURCE_RE = re.compile(r"\{\{\s*(ref|source)\s*\(")
FROM_CLAUSE_RE = re.compile(r"\bfrom\b", re.IGNORECASE)


def _check_profiles_in_project(project_path: Path) -> list[Issue]:
    """Warn if profiles.yml lives inside the project repo (security risk)."""
    issues: list[Issue] = []
    profiles_in_project = project_path / "profiles.yml"
    if profiles_in_project.exists():
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message="profiles.yml found inside the project directory — this is a security risk (credentials may be committed to git)",
                file="profiles.yml",
                fix=(
                    "Move profiles.yml to ~/.dbt/profiles.yml and add 'profiles.yml' to .gitignore. "
                    "In dbt Cloud, profiles.yml is not needed at all — connections are managed in the UI."
                ),
            )
        )
    return issues


def _check_gitignore(project_path: Path) -> list[Issue]:
    issues: list[Issue] = []
    gitignore = project_path / ".gitignore"
    if not gitignore.exists():
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.WARNING,
                message="No .gitignore found in project root",
                fix=(
                    "Create a .gitignore that includes: target/, dbt_packages/, logs/, "
                    "profiles.yml, .env, *.pyc"
                ),
            )
        )
        return issues

    content = gitignore.read_text()
    must_ignore = {
        "target/": "Compiled artifacts should not be committed",
        "dbt_packages/": "Installed packages should not be committed",
        "profiles.yml": "profiles.yml may contain credentials",
        "logs/": "Log files should not be committed",
    }
    for pattern, reason in must_ignore.items():
        if pattern not in content:
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message=f"'{pattern}' not in .gitignore — {reason}",
                    file=".gitignore",
                    fix=f"Add '{pattern}' to .gitignore",
                )
            )
    return issues


def _check_model_organization(project_path: Path) -> list[Issue]:
    issues: list[Issue] = []
    models_dir = project_path / "models"
    if not models_dir.exists():
        return issues

    # Check for SQL files in the root models/ directory (no subdirectory)
    root_sql_files = list(models_dir.glob("*.sql"))
    if root_sql_files:
        for f in root_sql_files:
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message=f"Model '{f.name}' is in the root models/ directory (not organized into a layer)",
                    file=str(f.relative_to(project_path)),
                    fix="Move this model into a subdirectory: staging/, intermediate/, or marts/",
                )
            )

    # Check that at least one of staging/intermediate/marts folders exists
    top_level_subdirs = {d.name for d in models_dir.iterdir() if d.is_dir()}
    if not top_level_subdirs.intersection(EXPECTED_TOP_LEVEL_DIRS):
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.INFO,
                message=(
                    f"No standard layer folders found in models/ (found: {sorted(top_level_subdirs) or 'none'}). "
                    "dbt best practice uses staging/, intermediate/, and marts/ layers."
                ),
                fix=(
                    "Organize models into:\n"
                    "  models/staging/   — raw source cleaning (stg_ prefix)\n"
                    "  models/intermediate/ — business logic (int_ prefix)\n"
                    "  models/marts/     — final analytics tables (fct_, dim_ prefixes)"
                ),
            )
        )

    return issues


def _check_model_naming(project_path: Path) -> list[Issue]:
    """Check that model filenames follow layer prefix conventions."""
    issues: list[Issue] = []
    models_dir = project_path / "models"
    if not models_dir.exists():
        return issues

    layer_dirs = {
        "staging": "stg_",
        "intermediate": "int_",
        "marts": ("fct_", "dim_", "rpt_", "agg_"),
    }

    for layer, expected_prefix in layer_dirs.items():
        layer_path = models_dir / layer
        if not layer_path.exists():
            continue
        for sql_file in layer_path.rglob("*.sql"):
            name = sql_file.stem
            if isinstance(expected_prefix, str):
                if not name.startswith(expected_prefix):
                    issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.INFO,
                            message=f"Model '{name}' in {layer}/ does not use the '{expected_prefix}' prefix convention",
                            file=str(sql_file.relative_to(project_path)),
                            fix=f"Rename to '{expected_prefix}{name}.sql' to follow naming conventions",
                        )
                    )
            else:
                if not any(name.startswith(p) for p in expected_prefix):
                    issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.INFO,
                            message=f"Model '{name}' in {layer}/ does not use a standard prefix ({', '.join(expected_prefix)})",
                            file=str(sql_file.relative_to(project_path)),
                            fix=f"Rename to use one of: {', '.join(p + name for p in expected_prefix[:2])}",
                        )
                    )

    return issues


def _check_sources_defined(project_path: Path) -> list[Issue]:
    """Ensure raw tables referenced in staging models use source() not direct refs."""
    issues: list[Issue] = []
    staging_dir = project_path / "models" / "staging"
    if not staging_dir.exists():
        return issues

    # Check if any sources.yml / _sources.yml files exist under staging
    source_yamls = list(staging_dir.rglob("*source*.yml")) + list(
        staging_dir.rglob("*source*.yaml")
    )
    if not source_yamls:
        # Also check project root models dir
        root_source_yamls = list((project_path / "models").rglob("*source*.yml"))
        if not root_source_yamls:
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message="No source YAML files found in models/staging/ — raw tables should be declared as sources",
                    fix=(
                        "Create models/staging/_sources.yml with:\n"
                        "  version: 2\n"
                        "  sources:\n"
                        "    - name: raw\n"
                        "      tables:\n"
                        "        - name: your_table\n"
                        "Then reference with {{ source('raw', 'your_table') }}"
                    ),
                )
            )

    return issues


def _collect_schema_data(project_path: Path) -> list[tuple[Path, dict]]:
    """Parse all schema YAML files under models/ and return (path, data) pairs."""
    results = []
    models_dir = project_path / "models"
    if not models_dir.exists():
        return results
    skip_dirs = {project_path / d for d in ("target", "dbt_packages")}
    for yml_file in models_dir.rglob("*.yml"):
        if any(yml_file.is_relative_to(d) for d in skip_dirs):
            continue
        try:
            with open(yml_file) as f:
                data = yaml.safe_load(f)
            if isinstance(data, dict):
                results.append((yml_file, data))
        except Exception:
            continue
    return results


def _check_documentation_coverage(project_path: Path) -> list[Issue]:
    """Check that models have description fields in their schema YAML."""
    issues: list[Issue] = []
    models_dir = project_path / "models"
    if not models_dir.exists():
        return issues

    # Collect all SQL model names
    all_models = {f.stem for f in models_dir.rglob("*.sql")}
    documented_models: set[str] = set()

    for yml_file, data in _collect_schema_data(project_path):
        for model in data.get("models", []):
            name = model.get("name", "")
            desc = model.get("description", "").strip()
            if name:
                documented_models.add(name)
                if not desc:
                    issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.INFO,
                            message=f"Model '{name}' has no description in schema YAML",
                            file=str(yml_file.relative_to(project_path)),
                            fix=f"Add 'description: ...' to the '{name}' entry in this YAML file",
                        )
                    )

    undocumented = all_models - documented_models
    if undocumented and len(undocumented) <= 20:
        for model_name in sorted(undocumented):
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.INFO,
                    message=f"Model '{model_name}' has no schema YAML entry (no documentation or tests defined)",
                    fix=f"Add a schema YAML entry for '{model_name}' with description and column tests",
                )
            )
    elif len(undocumented) > 20:
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.WARNING,
                message=f"{len(undocumented)} models have no schema YAML entries (no documentation or tests)",
                fix="Add schema YAML files for all models with descriptions and column-level tests",
            )
        )

    return issues


def _check_primary_key_tests(project_path: Path) -> list[Issue]:
    """Check that columns matching PK patterns have unique + not_null tests."""
    issues: list[Issue] = []

    for yml_file, data in _collect_schema_data(project_path):
        rel_path = str(yml_file.relative_to(project_path))
        for model in data.get("models", []):
            model_name = model.get("name", "?")
            for col in model.get("columns", []):
                col_name = col.get("name", "")
                if not PK_COLUMN_PATTERNS.match(col_name):
                    continue
                # Check for unique + not_null under either 'tests' or 'data_tests'
                tests = col.get("data_tests", col.get("tests", []))
                test_names = []
                for t in tests:
                    if isinstance(t, str):
                        test_names.append(t)
                    elif isinstance(t, dict):
                        test_names.extend(t.keys())
                missing = []
                if "unique" not in test_names:
                    missing.append("unique")
                if "not_null" not in test_names:
                    missing.append("not_null")
                if missing:
                    issues.append(
                        Issue(
                            check=CHECK_NAME,
                            severity=Severity.WARNING,
                            message=(
                                f"Column '{col_name}' in model '{model_name}' looks like a primary key "
                                f"but is missing tests: {', '.join(missing)}"
                            ),
                            file=rel_path,
                            fix=(
                                f"Add to column '{col_name}':\n"
                                f"  data_tests:\n"
                                + "\n".join(f"    - {t}" for t in missing)
                            ),
                        )
                    )

    return issues


def _check_dbt_project_yml(project_path: Path) -> list[Issue]:
    """Check dbt_project.yml for Cloud-relevant settings."""
    issues: list[Issue] = []
    dbt_project_file = project_path / "dbt_project.yml"
    if not dbt_project_file.exists():
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message="No dbt_project.yml found — this is required for a valid dbt project",
            )
        )
        return issues

    try:
        with open(dbt_project_file) as f:
            data = yaml.safe_load(f)
    except Exception as e:
        issues.append(
            Issue(
                check=CHECK_NAME,
                severity=Severity.ERROR,
                message=f"Could not parse dbt_project.yml: {e}",
                file="dbt_project.yml",
            )
        )
        return issues

    if not isinstance(data, dict):
        return issues

    required_keys = ["name", "version", "profile"]
    for key in required_keys:
        if key not in data:
            issues.append(
                Issue(
                    check=CHECK_NAME,
                    severity=Severity.WARNING,
                    message=f"dbt_project.yml is missing required key: '{key}'",
                    file="dbt_project.yml",
                    fix=f"Add '{key}:' to dbt_project.yml",
                )
            )

    return issues


def run(project_path: Path) -> CheckResult:
    result = CheckResult(
        name="Project Structure",
        description="Audits model organization, naming conventions, source definitions, docs, and test coverage",
    )

    result.issues.extend(_check_profiles_in_project(project_path))
    result.issues.extend(_check_gitignore(project_path))
    result.issues.extend(_check_dbt_project_yml(project_path))
    result.issues.extend(_check_model_organization(project_path))
    result.issues.extend(_check_model_naming(project_path))
    result.issues.extend(_check_sources_defined(project_path))
    result.issues.extend(_check_documentation_coverage(project_path))
    result.issues.extend(_check_primary_key_tests(project_path))

    return result
