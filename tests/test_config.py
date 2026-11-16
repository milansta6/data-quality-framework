from pathlib import Path

import pytest

from dq.config import ConfigError, load_suite

EXAMPLE = Path(__file__).parents[1] / "examples" / "demo_suite.yml"


def rule(**overrides):
    base = {"id": "R1", "table": "t", "type": "not_null", "column": "a"}
    return {**base, **overrides}


def test_example_suite_loads():
    suite = load_suite(EXAMPLE)
    assert suite.name == "retail_demo"
    assert len(suite.rules) == 19
    assert len(suite.active_rules) == 18
    assert [r.id for r in suite.inactive_rules] == ["REC_003"]
    assert set(suite.table_names) == {"customers", "accounts", "transactions", "transactions_raw"}
    assert suite.output.sample_size == 10


def test_example_suite_uses_every_rule_type():
    from dq.rules import available_rule_types

    used = {r.type for r in load_suite(EXAMPLE).rules}
    assert used == set(available_rule_types())


def test_defaults_are_applied_and_can_be_overridden():
    suite = load_suite(
        {
            "defaults": {"owner": "team-a", "severity": "warning"},
            "rules": [rule(), rule(id="R2", owner="team-b", severity="error")],
        }
    )
    r1, r2 = suite.rules
    assert (r1.owner, r1.severity) == ("team-a", "warning")
    assert (r2.owner, r2.severity) == ("team-b", "error")


def test_rule_specific_keys_become_params():
    suite = load_suite({"rules": [rule(type="range", column="x", min=0, max=10, filter="y > 1")]})
    r = suite.rules[0]
    assert r.params == {"column": "x", "min": 0, "max": 10, "filter": "y > 1"}
    assert r.param("missing", "dflt") == "dflt"


def test_dimension_is_inferred_from_rule_type():
    suite = load_suite(
        {
            "rules": [
                rule(id="a"),
                rule(id="b", type="unique"),
                rule(id="c", type="freshness", max_age_hours=1),
                rule(id="d", type="referential_integrity", reference_table="r"),
                rule(id="e", type="regex", pattern=".*"),
                rule(id="f", type="regex", pattern=".*", dimension="consistency"),
            ]
        }
    )
    dims = [r.dimension for r in suite.rules]
    assert dims == [
        "completeness",
        "uniqueness",
        "timeliness",
        "consistency",
        "validity",
        "consistency",
    ]


def test_load_from_yaml_string():
    suite = load_suite(
        """
suite: inline
rules:
  - {id: X1, table: t, type: unique, columns: [a, b]}
"""
    )
    assert suite.name == "inline"
    assert suite.rules[0].columns() == ["a", "b"]


def test_tables_section():
    suite = load_suite(
        {
            "rules": [rule()],
            "tables": {
                "t": {"path": "/data/t", "format": "csv", "options": {"header": True}},
                "u": "/data/u",
                "v": {"table": "catalog.schema.v"},
            },
        }
    )
    assert suite.tables["t"].options == {"header": "True"}
    assert suite.tables["u"].path == "/data/u"
    assert suite.tables["v"].table == "catalog.schema.v"


@pytest.mark.parametrize(
    "bad_rule, message",
    [
        (rule(type="does_not_exist"), "Unknown rule type"),
        (rule(severity="critical"), "severity"),
        (rule(dimension="accuracy"), "dimension"),
        (rule(threshold=1.5), "threshold"),
        (rule(threshold="abc"), "threshold"),
        (rule(active="yes"), "active"),
        ({"id": "R1", "type": "not_null", "column": "a"}, "missing 'table'"),
        (rule(column=None), "column"),
        (rule(type="accepted_values"), "values"),
        (rule(type="range"), "min, max"),
        (rule(type="custom_sql"), "sql"),
        (rule(type="row_count_reconciliation"), "target_table"),
        (rule(type="freshness"), "max_age_hours"),
    ],
)
def test_invalid_rules_are_rejected(bad_rule, message):
    with pytest.raises(ConfigError, match=message):
        load_suite({"rules": [bad_rule]})


def test_duplicate_rule_ids_are_rejected():
    with pytest.raises(ConfigError, match="Duplicate rule id"):
        load_suite({"rules": [rule(), rule()]})


@pytest.mark.parametrize(
    "raw",
    [
        {},
        {"rules": []},
        {"rules": "nope"},
        "just a string",
        {"rules": [rule()], "output": {"x": 1}},
    ],
)
def test_invalid_suites_are_rejected(raw):
    with pytest.raises(ConfigError):
        load_suite(raw)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_suite(tmp_path / "nope.yml")
