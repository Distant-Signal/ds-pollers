"""Tests for scripts/check-crate-deps.py, against saved `cargo tree` outputs.

  uv run python -m unittest discover -s scripts/tests

The fixtures in fixtures/crate-deps/ are `cargo tree --prefix none
--format {p}` outputs: a clean ds-store closure, one holding every
forbidden crate (through common's HTTP features and the api), and one
whose root is the wrong crate; and ds-migrate closures, clean and not,
so `main` never runs cargo here.
"""

import importlib.util
import io
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import ModuleType

SCRIPT = Path(__file__).resolve().parent.parent / "check-crate-deps.py"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "crate-deps"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_crate_deps", SCRIPT)
    if spec is None or spec.loader is None:
        msg = f"cannot load {SCRIPT}"
        raise ImportError(msg)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_crate_deps"] = module
    spec.loader.exec_module(module)
    return module


deps = _load()


def run(fixture: str, crate: str = "ds-store") -> tuple[int, str]:
    """Run main on a fixture as CRATE's tree (the others clean); (status, stdout)."""
    clean = {
        "ds-store": FIXTURES / "ds-store-clean.txt",
        "ds-migrate": FIXTURES / "ds-migrate-clean.txt",
    }
    trees = {**clean, crate: FIXTURES / fixture}
    out = io.StringIO()
    with redirect_stdout(out):
        status = deps.main(
            [
                arg
                for name, path in trees.items()
                for arg in ("--tree-file", f"{name}={path}")
            ]
        )
    return status, out.getvalue()


class PackageNamesTest(unittest.TestCase):
    """Parsing `cargo tree --prefix none` output."""

    def test_names_ignore_versions_sources_repeats_and_blank_lines(self) -> None:
        """Each line's first word, deduplicated; `(*)` repeats count once."""
        lines = ["ds-store v0.1.0 (/repo)", "hyper v1.11.1", "hyper v1.11.1 (*)"]
        tree = "\n".join([*lines, "", "api v0.1.0", ""])
        self.assertEqual(deps.package_names(tree), {"ds-store", "hyper", "api"})


class ForbiddenTest(unittest.TestCase):
    """The ds-store patterns."""

    def test_families_match_their_prefixed_crates_only(self) -> None:
        """axum*, tower*, hyper* match `-` suffixed crates, not look-alikes."""
        names = [
            "axum",
            "axum-core",
            "tower",
            "tower-http",
            "tower-service",
            "hyper",
            "hyper-util",
            "towerish",
            "hyperloop",
            "apis",
            "api",
            "redis",
            "redis-test",
        ]
        self.assertEqual(
            deps.forbidden_in(names, deps.FORBIDDEN["ds-store"]),
            [
                "api",
                "axum",
                "axum-core",
                "hyper",
                "hyper-util",
                "redis",
                "tower",
                "tower-http",
                "tower-service",
            ],
        )


class MainTest(unittest.TestCase):
    """The CLI over the fixture trees."""

    def test_a_clean_closure_passes(self) -> None:
        """No forbidden crate: exit 0, with a summary line."""
        status, out = run("ds-store-clean.txt")
        self.assertEqual(status, 0, out)
        self.assertIn("ds-store: 14 packages in the normal closure", out)
        self.assertIn("ds-migrate: 9 packages in the normal closure", out)
        self.assertNotIn("::error::", out)

    def test_ds_migrate_is_checked_too(self) -> None:
        """The migrator has the same rules as ds-store (plan 1B.1)."""
        self.assertEqual(deps.FORBIDDEN["ds-migrate"], deps.FORBIDDEN["ds-store"])
        status, out = run("ds-migrate-forbidden.txt", crate="ds-migrate")
        self.assertEqual(status, 1)
        reported = sorted(
            line.split(" contains ")[1].split(",")[0]
            for line in out.splitlines()
            if line.startswith("::error::ds-migrate's")
        )
        self.assertEqual(reported, ["api", "axum"])
        self.assertIn("ds-store: 14 packages in the normal closure", out)

    def test_every_forbidden_crate_is_reported_once(self) -> None:
        """Exit 1, one `::error::` per forbidden crate, with the -i hint."""
        status, out = run("ds-store-forbidden.txt")
        self.assertEqual(status, 1)
        reported = sorted(
            line.split(" contains ")[1].split(",")[0]
            for line in out.splitlines()
            if line.startswith("::error::")
        )
        self.assertEqual(
            reported,
            [
                "api",
                "axum",
                "axum-core",
                "hyper",
                "hyper-tls",
                "hyper-util",
                "oauth2",
                "openidconnect",
                "redis",
                "reqwest",
                "tower",
                "tower-service",
            ],
        )
        self.assertIn("-i reqwest", out)

    def test_a_tree_for_another_crate_is_an_error(self) -> None:
        """A tree whose root is not ds-store fails instead of passing vacuously."""
        status, out = run("wrong-root.txt")
        self.assertEqual(status, 1)
        self.assertIn("ds-store is not in its own cargo tree output", out)

    def test_a_bad_tree_file_argument_is_a_usage_error(self) -> None:
        """An unknown crate name exits through argparse (status 2)."""
        with (
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as raised,
        ):
            deps.main(["--tree-file", "api=whatever.txt"])
        self.assertEqual(raised.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
