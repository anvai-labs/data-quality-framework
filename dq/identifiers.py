# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Validated identifier value objects shared by every catalog and SQL boundary.

TD-ARCH-2/U4: table, column, and catalog identifiers arriving from
configuration are parsed once into immutable value objects and rejected
before any Spark call is made. Identifier parts allow only
``[A-Za-z_][A-Za-z0-9_]*``, are bounded to 128 characters, and table names
are limited to three dot-separated parts. Rendering for SQL uses backtick
quoting; because the charset is validated first, quoting is defence in
depth rather than the primary control.
"""

from __future__ import annotations

from dataclasses import dataclass
import re

from dq.exceptions import ConfigurationError

_PART_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAX_IDENTIFIER_PARTS = 3
MAX_PART_LENGTH = 128


class IdentifierError(ConfigurationError):
    """Raised when an identifier is malformed for its boundary."""


def _validated_part(value: str, label: str) -> str:
    if not 1 <= len(value) <= MAX_PART_LENGTH or _PART_PATTERN.match(value) is None:
        raise IdentifierError(
            f"{label} part {value!r} must be 1-{MAX_PART_LENGTH} characters of "
            "letters, digits, and underscores, starting with a letter or "
            "underscore"
        )
    return value


def _validated_parts(value: object, label: str, max_parts: int) -> tuple[str, ...]:
    if type(value) is not str:
        raise IdentifierError(f"{label} must be a string, not {type(value).__name__}")
    parts = tuple(value.split("."))
    if not 1 <= len(parts) <= max_parts:
        raise IdentifierError(
            f"{label} must be one to {max_parts} dot-separated parts, not "
            f"{len(parts)}"
        )
    return tuple(_validated_part(part, label) for part in parts)


@dataclass(frozen=True, slots=True)
class TableName:
    """A validated one-to-three part table identifier."""

    parts: tuple[str, ...]

    @classmethod
    def parse(
        cls,
        value: object,
        *,
        label: str = "table name",
        max_parts: int = MAX_IDENTIFIER_PARTS,
    ) -> TableName:
        return cls(_validated_parts(value, label, max_parts))

    def __str__(self) -> str:
        return ".".join(self.parts)

    @property
    def quoted(self) -> str:
        return ".".join(f"`{part}`" for part in self.parts)

    @property
    def quoted_pg(self) -> str:
        return ".".join(f'"{part}"' for part in self.parts)


@dataclass(frozen=True, slots=True)
class ColumnName:
    """A validated single-part column or catalog identifier."""

    value: str

    @classmethod
    def parse(cls, value: object, *, label: str = "column name") -> ColumnName:
        return cls(_validated_parts(value, label, 1)[0])

    def __str__(self) -> str:
        return self.value

    @property
    def quoted(self) -> str:
        return f"`{self.value}`"

    @property
    def quoted_pg(self) -> str:
        return f'"{self.value}"'


def qualified_table_name(
    table_reference: object,
    database: object = None,
    catalog: object = None,
    *,
    label: str = "table name",
    max_parts: int = MAX_IDENTIFIER_PARTS,
) -> str:
    """Validate a table reference plus optional components into a dotted name.

    Catalog and database components are prepended when present. The combined
    reference is validated as one bounded identifier, so malformed or
    injected components fail before any Spark call.
    """
    parts = []
    for component in (catalog, database):
        if component and str(component).strip():
            parts.append(str(component).strip())
    if type(table_reference) is str and table_reference.strip():
        parts.append(table_reference.strip())
    combined = ".".join(parts)
    table = TableName.parse(combined, label=label, max_parts=max_parts)
    return str(table)
