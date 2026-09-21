# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Contract: the error catalog stays complete as raise sites evolve (U7).

Walks every in-scope raise site under ``dq/`` (excluding tests and
notebooks) and requires docs/error-catalog.adoc to carry a section for each
module, naming the exception types that module raises, with the stated
raise-site total matching the AST count.
"""

import ast
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPOSITORY_ROOT / "dq"
CATALOG_PATH = REPOSITORY_ROOT / "docs" / "error-catalog.adoc"
IN_SCOPE = {
    "ConfigurationError",
    "ValidationError",
    "RepositoryError",
    "IdentifierError",
    "ValueError",
    "DataFrameNotFoundError",
}


def raise_sites():
    """Map module path -> in-scope exception names raised there.

    Returns ``(sites, total)`` where ``total`` counts raise nodes across all
    modules (a node may raise any of the in-scope exception names).
    """
    sites = {}
    total = 0
    for path in sorted(PACKAGE_ROOT.rglob("*.py")):
        parts = path.relative_to(REPOSITORY_ROOT).parts
        if "tests" in parts or "notebooks" in parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)):
                continue
            func = node.exc.func
            if isinstance(func, ast.Name) and func.id in IN_SCOPE:
                module = path.relative_to(REPOSITORY_ROOT).as_posix()
                sites.setdefault(module, set()).add(func.id)
                total += 1
    return sites, total


def catalog_sections(catalog_text):
    """Split the catalog into {module path: section text} by == headers."""
    sections = {}
    current = None
    for line in catalog_text.splitlines():
        if line.startswith("== "):
            current = line[3:].strip()
            sections[current] = []
        elif current is not None:
            sections[current].append(line)
    return {key: "\n".join(lines) for key, lines in sections.items()}


def test_every_module_with_raise_sites_has_a_catalog_section():
    sites, _total = raise_sites()
    assert sites, "raise-site inventory must not be empty"
    sections = catalog_sections(CATALOG_PATH.read_text(encoding="utf-8"))
    for module in sites:
        assert (
            f"== {module}" in sections or module in sections
        ), f"error catalog is missing a section for {module}"


def test_catalog_sections_name_their_exception_types():
    sections = catalog_sections(CATALOG_PATH.read_text(encoding="utf-8"))
    sites, _total = raise_sites()
    for module, types in sorted(sites.items()):
        assert module in sections, f"no catalog section for {module}"
        for exception_type in sorted(types):
            assert (
                exception_type in sections[module]
            ), f"{module} section must name {exception_type}"


def test_catalog_states_the_current_raise_site_total():
    sites, total = raise_sites()
    catalog = CATALOG_PATH.read_text(encoding="utf-8")
    assert (
        f"{total} raise sites" in catalog
    ), f"catalog intro must state the current total of {total} raise sites"
