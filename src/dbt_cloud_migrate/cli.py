"""
dbt-cloud-migrate CLI

Audits a dbt Core project and generates actionable guidance for migrating to dbt Cloud.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
import yaml
from rich.console import Console

from .checks import ALL_CHECKS
from .models import Report
from . import reporter

app = typer.Typer(
    name="dbt-cloud-migrate",
    help=(
        "Analyze a dbt Core project and get actionable guidance for migrating to dbt Cloud.\n\n"
        "Run 'dbt-cloud-migrate check .' from your project root to get started."
    ),
    add_completion=False,
)
console = Console()


def _resolve_project_path(project_path: str) -> Path:
    path = Path(project_path).expanduser().resolve()
    if not path.exists():
        console.print(f"[red]Error:[/red] Path does not exist: {path}")
        raise typer.Exit(1)
    if not path.is_dir():
        console.print(f"[red]Error:[/red] Path is not a directory: {path}")
        raise typer.Exit(1)
    return path


@app.command()
def check(
    project_path: Annotated[
        str,
        typer.Argument(help="Path to the dbt project root directory"),
    ] = ".",
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: 'rich' (default), 'json', or 'summary'"),
    ] = "rich",
    only: Annotated[
        Optional[str],
        typer.Option(
            "--only",
            help="Run only specific checks (comma-separated): profiles, structure, deprecations",
        ),
    ] = None,
    fix: Annotated[
        bool,
        typer.Option("--fix", help="Auto-fix safe issues (renames deprecated dbt_project.yml keys)"),
    ] = False,
) -> None:
    """
    Run all migration checks against a dbt Core project.

    Examples:

      dbt-cloud-migrate check .

      dbt-cloud-migrate check ~/projects/my_dbt_project --output json

      dbt-cloud-migrate check . --only profiles,deprecations

      dbt-cloud-migrate check . --fix
    """
    path = _resolve_project_path(project_path)

    # Filter checks if --only specified
    checks_to_run = ALL_CHECKS
    if only:
        names = {n.strip().lower() for n in only.split(",")}
        checks_to_run = [c for c in ALL_CHECKS if c.__name__.split(".")[-1] in names]
        if not checks_to_run:
            console.print(f"[red]No matching checks for --only '{only}'[/red]")
            console.print("Available checks: profiles, structure, deprecations")
            raise typer.Exit(1)

    report = Report(project_path=str(path))

    if output == "rich":
        console.print(f"[dim]Scanning project: {path}[/dim]")

    for check_module in checks_to_run:
        result = check_module.run(path)
        report.results.append(result)

    if fix:
        _apply_fixes(path, report)

    if output == "json":
        reporter.print_json(report)
    elif output == "summary":
        reporter.print_summary_table(report)
    else:
        reporter.print_report(report)

    if not report.passed:
        raise typer.Exit(1)


@app.command()
def profiles(
    project_path: Annotated[
        str,
        typer.Argument(help="Path to the dbt project root directory"),
    ] = ".",
) -> None:
    """
    Analyze profiles.yml and generate dbt Cloud connection migration guidance.
    """
    from .checks import profiles as profiles_check

    path = _resolve_project_path(project_path)
    report = Report(project_path=str(path))
    result = profiles_check.run(path)
    report.results.append(result)
    reporter.print_report(report)


@app.command()
def fix(
    project_path: Annotated[
        str,
        typer.Argument(help="Path to the dbt project root directory"),
    ] = ".",
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be changed without modifying files"),
    ] = False,
) -> None:
    """
    Auto-fix safe, mechanical issues in the project.

    Currently fixes:
      - Deprecated dbt_project.yml keys (source-paths → model-paths, data-paths → seed-paths)
      - 'tests:' → 'data_tests:' in schema YAML files
    """
    from .checks import deprecations

    path = _resolve_project_path(project_path)
    result = deprecations.run(path)
    report = Report(project_path=str(path), results=[result])

    _apply_fixes(path, report, dry_run=dry_run)


def _apply_fixes(path: Path, report: Report, dry_run: bool = False) -> None:
    """Apply mechanical, safe auto-fixes."""
    from .checks.deprecations import RENAMED_PROJECT_KEYS

    fixed_count = 0

    # Fix 1: Rename deprecated keys in dbt_project.yml
    dbt_project_file = path / "dbt_project.yml"
    if dbt_project_file.exists():
        content = dbt_project_file.read_text()
        original = content
        for old_key, (new_key, _) in RENAMED_PROJECT_KEYS.items():
            content = content.replace(f"{old_key}:", f"{new_key}:")
        if content != original:
            if dry_run:
                console.print(f"[dim][dry-run][/dim] Would update dbt_project.yml: rename deprecated keys")
            else:
                dbt_project_file.write_text(content)
                console.print("[green]Fixed[/green] dbt_project.yml: renamed deprecated config keys")
                fixed_count += 1

    # Fix 2: Rename 'tests:' → 'data_tests:' in schema YAML files
    skip_dirs = {path / d for d in ("target", "dbt_packages", ".git")}
    for yml_file in path.rglob("*.yml"):
        if any(yml_file.is_relative_to(d) for d in skip_dirs):
            continue
        try:
            content = yml_file.read_text()
        except Exception:
            continue
        if "  tests:" not in content and "\ttests:" not in content:
            continue
        # Only replace indented 'tests:' (not top-level, not inside config blocks)
        import re
        new_content = re.sub(
            r"^(\s+)tests:(\s*$|\s+#)",
            r"\1data_tests:\2",
            content,
            flags=re.MULTILINE,
        )
        if new_content != content:
            rel = yml_file.relative_to(path)
            if dry_run:
                console.print(f"[dim][dry-run][/dim] Would rename 'tests:' → 'data_tests:' in {rel}")
            else:
                yml_file.write_text(new_content)
                console.print(f"[green]Fixed[/green] {rel}: renamed 'tests:' → 'data_tests:'")
                fixed_count += 1

    if not dry_run:
        if fixed_count:
            console.print(f"\n[green]{fixed_count} file(s) updated.[/green]")
        else:
            console.print("[dim]No auto-fixable issues found.[/dim]")
    else:
        console.print("[dim]Dry run complete — no files were modified.[/dim]")


@app.command()
def version() -> None:
    """Show the dbt-cloud-migrate version."""
    from . import __version__
    console.print(f"dbt-cloud-migrate {__version__}")


if __name__ == "__main__":
    app()
