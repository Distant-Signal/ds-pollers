#!/usr/bin/env python3
"""Fail if a crate's normal dependency closure holds a forbidden crate.

  uv run scripts/check-crate-deps.py [--tree-file CRATE=FILE ...]

Ingest architecture spec §5.1: `ds-store` is the shared data-access crate
that every ingest service links, so it must never pull in a web framework,
an HTTP client, Redis or the api itself. Its NORMAL dependency closure
(build and dev dependencies do not count) must not contain axum*, tower*,
hyper*, redis, reqwest, oauth2, openidconnect or api. The same holds for
`ds-migrate`, the migrator binary (plan 1B.1), which links only ds-store.

The closure comes from

  cargo tree --locked -p ds-store -e normal --target all --all-features
      --prefix none --format {p}

and not from `cargo metadata`: metadata's resolve unifies features across
the whole workspace (the api turns on `common`'s `redis` and `http`
features), so it would list crates `ds-store` itself never builds. `cargo
tree -p` resolves features as `cargo build -p ds-store` does.
`--all-features` and `--target all` make the check cover every optional
and platform-specific dependency the crate could ever build.

Each finding is a GitHub Actions `::error::` line naming the crate and the
command that shows the path to it (`cargo tree -i`). Exit status: 0 clean,
1 a forbidden dependency, 2 cargo failed. `--tree-file` reads a saved
`cargo tree` output instead of running cargo (the unit tests use it).
Stdlib only; needs cargo on PATH.
"""

import argparse
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# crate -> the patterns (full matches on package names) its normal
# dependency closure must not contain.
# No web framework, HTTP client, Redis or api (spec §5.1).
NO_SERVER_STACK = (
    r"axum(-.+)?",
    r"tower(-.+)?",
    r"hyper(-.+)?",
    "redis",
    "reqwest",
    "oauth2",
    "openidconnect",
    "api",
)
FORBIDDEN: Mapping[str, Sequence[str]] = {
    "ds-store": NO_SERVER_STACK,
    "ds-migrate": NO_SERVER_STACK,
}


class CargoError(Exception):
    """cargo failed; its stderr has already been shown."""


def cargo_tree(crate: str) -> str:
    """Return `cargo tree`'s package list for CRATE's normal closure."""
    result = subprocess.run(  # noqa: S603  # fixed cargo subcommand
        [  # noqa: S607  # cargo from PATH
            "cargo",
            "tree",
            "--locked",
            "-p",
            crate,
            "-e",
            "normal",
            "--target",
            "all",
            "--all-features",
            "--prefix",
            "none",
            "--format",
            "{p}",
        ],
        check=False,
        stdout=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    if result.returncode != 0:
        raise CargoError(crate)
    return result.stdout


def package_names(tree: str) -> set[str]:
    """Return the package names in a `cargo tree --prefix none` output.

    Each line is `<name> v<version> [(<source>)] [(*)]`; blank lines
    separate roots.
    """
    return {words[0] for line in tree.splitlines() if (words := line.split())}


def forbidden_in(names: Iterable[str], patterns: Sequence[str]) -> list[str]:
    """Return the names matching any pattern, sorted."""
    compiled = [re.compile(pattern) for pattern in patterns]
    return sorted(name for name in names if any(p.fullmatch(name) for p in compiled))


def check(crate: str, tree: str) -> list[str]:
    """Return the `::error::` lines for CRATE's tree (empty when clean)."""
    if crate not in package_names(tree):
        return [
            (
                f"::error::{crate} is not in its own cargo tree output; "
                "is the crate name right?"
            )
        ]
    return [
        f"::error::{crate}'s normal dependency closure contains {name}, "
        "which it must never depend on (ingest architecture spec §5.1). "
        f"Path: cargo tree -p {crate} -e normal --target all --all-features "
        f"-i {name}"
        for name in forbidden_in(package_names(tree), FORBIDDEN[crate])
    ]


def parse_tree_files(values: Sequence[str]) -> dict[str, Path]:
    """Parse `CRATE=FILE` arguments."""
    files: dict[str, Path] = {}
    for value in values:
        crate, sep, path = value.partition("=")
        if not sep or crate not in FORBIDDEN:
            msg = f"--tree-file wants CRATE=FILE with CRATE one of {sorted(FORBIDDEN)}"
            raise argparse.ArgumentTypeError(msg)
        files[crate] = Path(path)
    return files


def main(argv: Sequence[str] | None = None) -> int:
    """Check every crate in FORBIDDEN; 0 clean, 1 findings, 2 cargo failed."""
    parser = argparse.ArgumentParser(
        description="Check crates' normal dependency closures for forbidden crates."
    )
    parser.add_argument(
        "--tree-file",
        action="append",
        default=[],
        metavar="CRATE=FILE",
        help="read CRATE's `cargo tree` output from FILE instead of running cargo",
    )
    args = parser.parse_args(argv)
    try:
        tree_files = parse_tree_files(args.tree_file)
    except argparse.ArgumentTypeError as err:
        parser.error(str(err))

    findings: list[str] = []
    for crate in FORBIDDEN:
        try:
            tree = (
                tree_files[crate].read_text(encoding="utf-8")
                if crate in tree_files
                else cargo_tree(crate)
            )
        except CargoError:
            sys.stdout.write(f"::error::cargo tree failed for {crate}\n")
            return 2
        crate_findings = check(crate, tree)
        findings.extend(crate_findings)
        if not crate_findings:
            count = len(package_names(tree))
            sys.stdout.write(
                f"{crate}: {count} packages in the normal closure, none forbidden\n"
            )
    for line in findings:
        sys.stdout.write(f"{line}\n")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
