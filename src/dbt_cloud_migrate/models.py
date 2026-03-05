from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional


class Severity(str, Enum):
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Issue:
    check: str
    severity: Severity
    message: str
    file: Optional[str] = None
    line: Optional[int] = None
    fix: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "check": self.check,
            "severity": self.severity.value,
            "message": self.message,
            "file": self.file,
            "line": self.line,
            "fix": self.fix,
        }


@dataclass
class CheckResult:
    name: str
    description: str
    issues: list[Issue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.WARNING)

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "issues": [i.to_dict() for i in self.issues],
        }


@dataclass
class Report:
    project_path: str
    results: list[CheckResult] = field(default_factory=list)

    @property
    def total_errors(self) -> int:
        return sum(r.error_count for r in self.results)

    @property
    def total_warnings(self) -> int:
        return sum(r.warning_count for r in self.results)

    @property
    def passed(self) -> bool:
        return self.total_errors == 0

    def to_dict(self) -> dict:
        return {
            "project_path": self.project_path,
            "passed": self.passed,
            "total_errors": self.total_errors,
            "total_warnings": self.total_warnings,
            "results": [r.to_dict() for r in self.results],
        }
