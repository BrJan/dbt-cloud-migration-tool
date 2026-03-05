"""
Rich-based terminal output for dbt Cloud migration reports.
"""

import json
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.text import Text
from rich.rule import Rule

from .models import Report, CheckResult, Issue, Severity

console = Console()

SEVERITY_STYLES = {
    Severity.ERROR: ("red", "[ERROR]"),
    Severity.WARNING: ("yellow", "[WARN] "),
    Severity.INFO: ("blue", "[INFO] "),
}


def _severity_text(severity: Severity) -> Text:
    style, label = SEVERITY_STYLES[severity]
    return Text(label, style=f"bold {style}")


def print_issue(issue: Issue, prefix: str = "  ") -> None:
    sev_text = _severity_text(issue.severity)
    location = ""
    if issue.file:
        location = f"[dim]{issue.file}"
        if issue.line:
            location += f":{issue.line}"
        location += "[/dim] "

    console.print(f"{prefix}", sev_text, location + issue.message)
    if issue.fix:
        fix_lines = issue.fix.split("\n")
        console.print(f"{prefix}         [dim green]Fix: {fix_lines[0]}[/dim green]")
        for line in fix_lines[1:]:
            console.print(f"{prefix}              [dim green]{line}[/dim green]")


def print_check_result(result: CheckResult) -> None:
    status = "[bold green]PASS[/bold green]" if result.passed else "[bold red]FAIL[/bold red]"
    counts = ""
    if result.error_count:
        counts += f"[red]{result.error_count} error{'s' if result.error_count != 1 else ''}[/red]"
    if result.warning_count:
        if counts:
            counts += ", "
        counts += f"[yellow]{result.warning_count} warning{'s' if result.warning_count != 1 else ''}[/yellow]"
    info_count = sum(1 for i in result.issues if i.severity == Severity.INFO)
    if info_count:
        if counts:
            counts += ", "
        counts += f"[blue]{info_count} info[/blue]"

    header = f"{status}  [bold]{result.name}[/bold]"
    if counts:
        header += f"  ({counts})"
    console.print(f"\n{header}")
    console.print(f"  [dim]{result.description}[/dim]")

    if not result.issues:
        console.print("  [dim green]No issues found.[/dim green]")
        return

    for issue in result.issues:
        print_issue(issue)


def print_report(report: Report) -> None:
    console.print()
    console.rule(f"[bold]dbt Cloud Migration Report[/bold]")
    console.print(f"[dim]Project: {report.project_path}[/dim]")
    console.print()

    for result in report.results:
        print_check_result(result)

    console.print()
    console.rule("[bold]Summary[/bold]")

    if report.passed:
        console.print(
            Panel(
                f"[bold green]All checks passed![/bold green]  "
                f"[yellow]{report.total_warnings} warning(s)[/yellow]  "
                f"[blue]{sum(1 for r in report.results for i in r.issues if i.severity == Severity.INFO)} info[/blue]",
                box=box.ROUNDED,
            )
        )
    else:
        console.print(
            Panel(
                f"[bold red]{report.total_errors} error(s)[/bold red]  "
                f"[yellow]{report.total_warnings} warning(s)[/yellow]  "
                f"[dim]must fix errors before migrating to dbt Cloud[/dim]",
                box=box.ROUNDED,
            )
        )


def print_json(report: Report) -> None:
    print(json.dumps(report.to_dict(), indent=2))


def print_summary_table(report: Report) -> None:
    table = Table(title="Check Results", box=box.SIMPLE_HEAVY)
    table.add_column("Check", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("Errors", justify="right", style="red")
    table.add_column("Warnings", justify="right", style="yellow")
    table.add_column("Info", justify="right", style="blue")

    for result in report.results:
        status = "[green]PASS[/green]" if result.passed else "[red]FAIL[/red]"
        info_count = sum(1 for i in result.issues if i.severity == Severity.INFO)
        table.add_row(
            result.name,
            status,
            str(result.error_count) if result.error_count else "-",
            str(result.warning_count) if result.warning_count else "-",
            str(info_count) if info_count else "-",
        )

    console.print(table)
