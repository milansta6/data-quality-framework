"""Load and validate data quality rules from YAML.

A *suite* is one YAML file. It contains:

* ``suite``     - a name for the rule set (for example ``customer_domain``)
* ``defaults``  - optional values applied to every rule (owner, severity, ...)
* ``tables``    - optional table sources (path / catalog table) used by the CLI
* ``output``    - optional output settings (path, format, sample size)
* ``scoring``   - optional severity weights and rating thresholds
* ``rules``     - the list of rules

Every rule has the common fields ``id``, ``table``, ``type``, ``description``,
``severity``, ``active``, ``owner``, ``dimension`` and ``threshold``. Every other
key is a rule-specific parameter (for example ``column`` or ``values``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Union

import yaml

SEVERITIES = ("warning", "error")
DIMENSIONS = ("completeness", "validity", "uniqueness", "consistency", "timeliness")
COMMON_RULE_KEYS = {
    "id",
    "table",
    "type",
    "description",
    "severity",
    "active",
    "owner",
    "dimension",
    "threshold",
}
DEFAULT_WEIGHTS = {"error": 2.0, "warning": 1.0}
DEFAULT_RATINGS = {"good": 95.0, "attention": 80.0}

_MISSING = object()


class ConfigError(ValueError):
    """Raised when a rule suite is not valid."""


@dataclass(frozen=True)
class Rule:
    """One data quality rule, as defined in YAML."""

    id: str
    table: str
    type: str
    description: str = ""
    severity: str = "error"
    active: bool = True
    owner: str = "unassigned"
    dimension: str = "validity"
    threshold: float = 1.0
    params: dict[str, Any] = field(default_factory=dict)

    def param(self, name: str, default: Any = None) -> Any:
        """Return an optional rule parameter."""
        return self.params.get(name, default)

    def require(self, name: str) -> Any:
        """Return a mandatory rule parameter or raise :class:`ConfigError`."""
        value = self.params.get(name, _MISSING)
        if value is _MISSING or value is None:
            raise ConfigError(f"Rule '{self.id}' ({self.type}) needs parameter '{name}'.")
        return value

    def columns(self) -> list[str]:
        """Return ``column`` / ``columns`` as a list (used by multi-column rules)."""
        cols = self.params.get("columns", self.params.get("column"))
        if cols is None:
            raise ConfigError(f"Rule '{self.id}' ({self.type}) needs 'column' or 'columns'.")
        return [cols] if isinstance(cols, str) else list(cols)


@dataclass(frozen=True)
class TableSource:
    """Where to read a table from when running from the CLI."""

    name: str
    path: str | None = None
    table: str | None = None
    format: str = "parquet"
    options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OutputConfig:
    """Where and how to store results."""

    path: str | None = "dq_output"
    database: str | None = None
    format: str = "parquet"
    sample_size: int = 20
    report: bool = True


@dataclass(frozen=True)
class ScoringConfig:
    """How rule pass rates are combined into quality indices."""

    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    ratings: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_RATINGS))


@dataclass(frozen=True)
class Suite:
    """A validated set of rules plus run settings."""

    name: str
    rules: list[Rule]
    tables: dict[str, TableSource] = field(default_factory=dict)
    output: OutputConfig = field(default_factory=OutputConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    @property
    def active_rules(self) -> list[Rule]:
        return [r for r in self.rules if r.active]

    @property
    def inactive_rules(self) -> list[Rule]:
        return [r for r in self.rules if not r.active]

    @property
    def table_names(self) -> list[str]:
        """All tables referenced by active rules (including reference/target tables)."""
        names: list[str] = []
        for rule in self.active_rules:
            for name in (
                rule.table,
                rule.param("reference_table"),
                rule.param("target_table"),
            ):
                if name and name not in names:
                    names.append(name)
            for name in rule.param("depends_on", []) or []:
                if name not in names:
                    names.append(name)
        return names


SuiteSource = Union[str, Path, dict]


def load_suite(source: SuiteSource) -> Suite:
    """Load a suite from a YAML file path, a YAML string or an already parsed dict."""
    if isinstance(source, dict):
        raw = source
    else:
        text = str(source)
        path = Path(text)
        is_file = "\n" not in text and len(text) < 1024 and path.suffix in {".yml", ".yaml"}
        if is_file:
            if not path.exists():
                raise ConfigError(f"Suite file not found: {path}")
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        else:
            raw = yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ConfigError("A suite must be a YAML mapping with at least a 'rules' list.")
    return _parse_suite(raw)


def _parse_suite(raw: dict) -> Suite:
    name = str(raw.get("suite", "default_suite"))
    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ConfigError("'defaults' must be a mapping.")

    raw_rules = raw.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ConfigError("A suite needs a non-empty 'rules' list.")

    rules = [_parse_rule(item, defaults, index) for index, item in enumerate(raw_rules)]

    seen: set[str] = set()
    for rule in rules:
        if rule.id in seen:
            raise ConfigError(f"Duplicate rule id '{rule.id}'.")
        seen.add(rule.id)

    return Suite(
        name=name,
        rules=rules,
        tables=_parse_tables(raw.get("tables") or {}),
        output=_parse_output(raw.get("output") or {}),
        scoring=_parse_scoring(raw.get("scoring") or {}),
    )


def _parse_rule(item: Any, defaults: dict, index: int) -> Rule:
    # Imported here to avoid a circular import (rules.py imports this module).
    from dq.rules import get_rule_type

    if not isinstance(item, dict):
        raise ConfigError(f"Rule #{index + 1} must be a mapping.")
    data = {**defaults, **item}

    for key in ("id", "table", "type"):
        if not data.get(key):
            raise ConfigError(f"Rule #{index + 1} is missing '{key}'.")

    rule_id = str(data["id"])
    rule_type_name = str(data["type"])
    try:
        rule_type = get_rule_type(rule_type_name)
    except KeyError as exc:
        raise ConfigError(f"Rule '{rule_id}': {exc.args[0]}") from None

    severity = str(data.get("severity", "error")).lower()
    if severity not in SEVERITIES:
        raise ConfigError(f"Rule '{rule_id}': severity must be one of {SEVERITIES}.")

    dimension = str(data.get("dimension", rule_type.default_dimension)).lower()
    if dimension not in DIMENSIONS:
        raise ConfigError(f"Rule '{rule_id}': dimension must be one of {DIMENSIONS}.")

    try:
        threshold = float(data.get("threshold", 1.0))
    except (TypeError, ValueError):
        raise ConfigError(f"Rule '{rule_id}': threshold must be a number.") from None
    if not 0.0 <= threshold <= 1.0:
        raise ConfigError(f"Rule '{rule_id}': threshold must be between 0 and 1.")

    active = data.get("active", True)
    if not isinstance(active, bool):
        raise ConfigError(f"Rule '{rule_id}': active must be true or false.")

    params = {k: v for k, v in data.items() if k not in COMMON_RULE_KEYS}
    rule = Rule(
        id=rule_id,
        table=str(data["table"]),
        type=rule_type_name,
        description=str(data.get("description", "")),
        severity=severity,
        active=active,
        owner=str(data.get("owner", "unassigned")),
        dimension=dimension,
        threshold=threshold,
        params=params,
    )
    rule_type.validate(rule)
    return rule


def _parse_tables(raw: Any) -> dict[str, TableSource]:
    if not isinstance(raw, dict):
        raise ConfigError("'tables' must be a mapping of table name -> source.")
    tables = {}
    for name, spec in raw.items():
        if isinstance(spec, str):
            spec = {"path": spec}
        if not isinstance(spec, dict) or not (spec.get("path") or spec.get("table")):
            raise ConfigError(f"Table '{name}' needs a 'path' or a 'table'.")
        tables[name] = TableSource(
            name=name,
            path=spec.get("path"),
            table=spec.get("table"),
            format=spec.get("format", "parquet"),
            options={k: str(v) for k, v in (spec.get("options") or {}).items()},
        )
    return tables


def _parse_output(raw: Any) -> OutputConfig:
    if not isinstance(raw, dict):
        raise ConfigError("'output' must be a mapping.")
    allowed = set(OutputConfig.__dataclass_fields__)
    unknown = set(raw) - allowed
    if unknown:
        raise ConfigError(f"Unknown output setting(s): {sorted(unknown)}")
    return OutputConfig(**raw)


def _parse_scoring(raw: Any) -> ScoringConfig:
    if not isinstance(raw, dict):
        raise ConfigError("'scoring' must be a mapping.")
    weights = {**DEFAULT_WEIGHTS, **(raw.get("weights") or {})}
    ratings = {**DEFAULT_RATINGS, **(raw.get("ratings") or {})}
    return ScoringConfig(
        weights={k: float(v) for k, v in weights.items()},
        ratings={k: float(v) for k, v in ratings.items()},
    )
