"""
dbt Cloud Migration MCP Server

Exposes the migration checks as MCP tools so Claude (Desktop, Code, or API)
can call them directly while helping users refactor their dbt projects.

Usage:
  dbt-cloud-migrate-mcp          # stdio transport (for Claude Desktop)
  dbt-cloud-migrate-mcp --port 8000  # SSE transport (for remote clients)

Configure in Claude Desktop's config (~/Library/Application Support/Claude/claude_desktop_config.json):

  {
    "mcpServers": {
      "dbt-cloud-migrate": {
        "command": "dbt-cloud-migrate-mcp"
      }
    }
  }

Configure in Claude Code (~/.claude/settings.json):

  {
    "mcpServers": {
      "dbt-cloud-migrate": {
        "type": "stdio",
        "command": "dbt-cloud-migrate-mcp"
      }
    }
  }
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import mcp.server.stdio
import mcp.types as types
from mcp.server import Server

from .checks import ALL_CHECKS
from .checks import deprecations as deprecations_check
from .checks import profiles as profiles_check
from .checks import structure as structure_check
from .models import Report

server = Server("dbt-cloud-migrate")


def _run_checks(project_path: str, check_modules) -> dict:
    path = Path(project_path).expanduser().resolve()
    if not path.exists() or not path.is_dir():
        return {"error": f"Project path does not exist or is not a directory: {project_path}"}

    report = Report(project_path=str(path))
    for module in check_modules:
        result = module.run(path)
        report.results.append(result)

    return report.to_dict()


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="check_project",
            description=(
                "Run all dbt Core → dbt Cloud migration checks against a project directory. "
                "Returns a full JSON report with errors, warnings, and actionable fix guidance "
                "for: profiles.yml migration, project structure, and deprecated syntax."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the dbt project root directory",
                    }
                },
                "required": ["project_path"],
            },
        ),
        types.Tool(
            name="check_profiles",
            description=(
                "Analyze a dbt project's profiles.yml and generate dbt Cloud connection migration guidance. "
                "Detects hardcoded credentials, maps adapter types to Cloud connection types, "
                "and suggests DBT_ENV_SECRET_ variable naming conventions."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the dbt project root directory",
                    }
                },
                "required": ["project_path"],
            },
        ),
        types.Tool(
            name="check_structure",
            description=(
                "Audit a dbt project's folder structure and organization against dbt Cloud best practices. "
                "Checks: model layer organization (staging/intermediate/marts), naming conventions "
                "(stg_, int_, fct_, dim_), source YAML definitions, documentation coverage, "
                "primary key test coverage, and gitignore settings."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the dbt project root directory",
                    }
                },
                "required": ["project_path"],
            },
        ),
        types.Tool(
            name="check_deprecations",
            description=(
                "Scan a dbt project for deprecated syntax and configuration that must be updated "
                "for dbt Cloud compatibility. Detects: renamed dbt_project.yml keys (source-paths, "
                "data-paths), legacy YAML test keys (tests: → data_tests:), deprecated dbt_utils macros, "
                "env_var() calls without defaults, hardcoded target.name references, and unpinned packages."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the dbt project root directory",
                    }
                },
                "required": ["project_path"],
            },
        ),
        types.Tool(
            name="fix_deprecations",
            description=(
                "Auto-fix safe, mechanical deprecation issues in a dbt project. "
                "Fixes: deprecated dbt_project.yml keys and 'tests:' → 'data_tests:' in YAML files. "
                "Use dry_run=true to preview changes without modifying files."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "project_path": {
                        "type": "string",
                        "description": "Absolute or relative path to the dbt project root directory",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": "If true, show what would change without modifying files",
                        "default": False,
                    },
                },
                "required": ["project_path"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
    project_path = arguments.get("project_path", ".")

    if name == "check_project":
        result = _run_checks(project_path, ALL_CHECKS)

    elif name == "check_profiles":
        result = _run_checks(project_path, [profiles_check])

    elif name == "check_structure":
        result = _run_checks(project_path, [structure_check])

    elif name == "check_deprecations":
        result = _run_checks(project_path, [deprecations_check])

    elif name == "fix_deprecations":
        dry_run = arguments.get("dry_run", False)
        path = Path(project_path).expanduser().resolve()
        if not path.exists() or not path.is_dir():
            result = {"error": f"Project path does not exist: {project_path}"}
        else:
            from .checks.deprecations import RENAMED_PROJECT_KEYS
            import re

            changes = []

            # Fix dbt_project.yml deprecated keys
            dbt_project_file = path / "dbt_project.yml"
            if dbt_project_file.exists():
                content = dbt_project_file.read_text()
                original = content
                for old_key, (new_key, _) in RENAMED_PROJECT_KEYS.items():
                    content = content.replace(f"{old_key}:", f"{new_key}:")
                if content != original:
                    changes.append({"file": "dbt_project.yml", "action": "renamed deprecated config keys"})
                    if not dry_run:
                        dbt_project_file.write_text(content)

            # Fix tests: → data_tests: in YAML files
            skip_dirs = {path / d for d in ("target", "dbt_packages", ".git")}
            for yml_file in path.rglob("*.yml"):
                if any(yml_file.is_relative_to(d) for d in skip_dirs):
                    continue
                try:
                    content = yml_file.read_text()
                except Exception:
                    continue
                new_content = re.sub(
                    r"^(\s+)tests:(\s*$|\s+#)",
                    r"\1data_tests:\2",
                    content,
                    flags=re.MULTILINE,
                )
                if new_content != content:
                    rel = str(yml_file.relative_to(path))
                    changes.append({"file": rel, "action": "renamed 'tests:' to 'data_tests:'"})
                    if not dry_run:
                        yml_file.write_text(new_content)

            result = {
                "dry_run": dry_run,
                "changes": changes,
                "files_changed": len(changes),
                "message": (
                    f"{'Would fix' if dry_run else 'Fixed'} {len(changes)} file(s)"
                ),
            }
    else:
        result = {"error": f"Unknown tool: {name}"}

    return [types.TextContent(type="text", text=json.dumps(result, indent=2))]


def main() -> None:
    import asyncio
    import argparse

    parser = argparse.ArgumentParser(description="dbt Cloud Migration MCP Server")
    parser.add_argument("--port", type=int, default=None, help="Port for SSE transport (omit for stdio)")
    args = parser.parse_args()

    if args.port:
        # SSE transport
        import mcp.server.sse as sse_transport
        from starlette.applications import Starlette
        from starlette.routing import Route
        import uvicorn

        async def handle_sse(request):
            async with sse_transport.SseServerTransport("/messages") as (read, write):
                await server.run(read, write, server.create_initialization_options())

        starlette_app = Starlette(routes=[Route("/sse", endpoint=handle_sse)])
        uvicorn.run(starlette_app, host="0.0.0.0", port=args.port)
    else:
        # stdio transport (default, for Claude Desktop / Claude Code)
        asyncio.run(mcp.server.stdio.stdio_server(server))


if __name__ == "__main__":
    main()
