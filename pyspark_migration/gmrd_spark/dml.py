"""Translate Ab Initio DML record types into Spark schemas.

Only the subset of DML used by the GMRD record files is supported:
``type <name> = record <type>("<delim>") <field>; ... end;``
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql.types import DecimalType, StringType, StructField, StructType

REPO_ROOT = Path(__file__).resolve().parents[2]

_TYPE_RE = re.compile(r"type\s+(\w+)\s*=\s*record(.*?)end\s*;", re.S)
_FIELD_RE = re.compile(r"^\s*(string|decimal|integer|date)\s*\(([^)]*)\)\s+(\w+)\s*;", re.M)
_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)

DECIMAL_AMOUNT = DecimalType(28, 8)


@dataclass(frozen=True)
class DmlField:
    name: str
    dml_type: str

    @property
    def spark_type(self):
        return DECIMAL_AMOUNT if self.dml_type == "decimal" else StringType()


def parse_dml(path: str | Path) -> dict[str, list[DmlField]]:
    text = _COMMENT_RE.sub("", Path(path).read_text())
    types: dict[str, list[DmlField]] = {}
    for name, body in _TYPE_RE.findall(text):
        types[name] = [DmlField(f, t) for t, _delim, f in _FIELD_RE.findall(body)]
    return types


def load_record(ref: str) -> list[DmlField]:
    """Resolve an Ab Initio reference like ``dml/trade_record.dml::trade_record_t``."""
    rel, type_name = ref.split("::")
    return parse_dml(REPO_ROOT / rel)[type_name]


def raw_schema(ref: str) -> StructType:
    """Schema for reading a delimited extract: every field as string, as on disk."""
    return StructType([StructField(f.name, StringType(), True) for f in load_record(ref)])


def typed_schema(ref: str) -> StructType:
    return StructType([StructField(f.name, f.spark_type, True) for f in load_record(ref)])


def field_names(ref: str) -> list[str]:
    return [f.name for f in load_record(ref)]
