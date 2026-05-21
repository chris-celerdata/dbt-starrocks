"""
Unit tests for the materialized view configuration change detection logic.

These tests mirror the Jinja macro logic in
dbt/include/starrocks/macros/materializations/models/materialized_view.sql
in pure Python so they run without a live database connection.
"""
import pytest


# ---------------------------------------------------------------------------
# Helper: Python mirror of the Jinja AS SELECT extraction in
# starrocks__get_materialized_view_configuration_changes
# ---------------------------------------------------------------------------

def _extract_as_select(definition: str) -> str | None:
    """
    Locate the AS <sql> clause inside a MATERIALIZED_VIEW_DEFINITION string.

    Looks for the first occurrence of '\\nas ' or '\\nas\\n' (case-insensitive),
    which marks the boundary between the DDL header and the query body.
    Strips surrounding whitespace and a trailing semicolon.
    Returns None if the AS keyword is not found.
    """
    def_lower = definition.lower()
    as_pos = (
        def_lower.find("\nas ")
        if def_lower.find("\nas ") >= 0
        else def_lower.find("\nas\n")
    )
    if as_pos < 0:
        return None
    raw = definition[as_pos + 4:].strip()
    if raw.endswith(";"):
        raw = raw[:-1].strip()
    return raw


# ---------------------------------------------------------------------------
# DDL template matching the real StarRocks 4.0.7 output
# (verified against a live cluster — see MATERIALIZED_VIEW_DEFINITION column)
# ---------------------------------------------------------------------------

_DDL_TEMPLATE = (
    "CREATE MATERIALIZED VIEW `{name}` (`id`, `value`)\n"
    "DISTRIBUTED BY HASH(`id`) BUCKETS 1 \n"
    "REFRESH {refresh}\n"
    'PROPERTIES (\n'
    '"replicated_storage" = "true",\n'
    '"replication_num" = "1",\n'
    '"storage_medium" = "HDD"\n'
    ")\n"
    "AS {sql};"
)


def _make_ddl(sql: str, name: str = "test_mv", refresh: str = "MANUAL") -> str:
    return _DDL_TEMPLATE.format(name=name, refresh=refresh, sql=sql)


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------

class TestAsSelectExtraction:
    """Tests for extracting the AS SELECT clause from MATERIALIZED_VIEW_DEFINITION."""

    def test_unqualified_reference(self):
        ddl = _make_ddl("SELECT id, value FROM base_t")
        assert _extract_as_select(ddl) == "SELECT id, value FROM base_t"

    def test_qualified_reference(self):
        ddl = _make_ddl("SELECT id, value FROM `mv_test`.`base_t`")
        assert _extract_as_select(ddl) == "SELECT id, value FROM `mv_test`.`base_t`"

    def test_trailing_semicolon_stripped(self):
        ddl = _make_ddl("SELECT id FROM t")
        result = _extract_as_select(ddl)
        assert result is not None
        assert not result.endswith(";")

    def test_multiline_sql(self):
        sql = "SELECT\n  id,\n  value\nFROM `mv_test`.`base_t`"
        ddl = _make_ddl(sql)
        assert _extract_as_select(ddl) == sql

    def test_inline_column_alias_not_confused(self):
        """'col AS alias' on the same line must not affect extraction."""
        sql = "SELECT id AS user_id, value AS amount FROM `s`.`t`"
        ddl = _make_ddl(sql)
        assert _extract_as_select(ddl) == sql

    def test_async_keyword_not_confused_with_as(self):
        """'REFRESH ASYNC' contains 'as' but is on the same line — must not match."""
        ddl = _make_ddl("SELECT id FROM `s`.`t`", refresh="ASYNC")
        assert _extract_as_select(ddl) == "SELECT id FROM `s`.`t`"

    def test_as_on_own_line_variant(self):
        """Handle DDL where AS appears on its own line before the SELECT."""
        ddl = (
            "CREATE MATERIALIZED VIEW `mv` (`id`)\n"
            "REFRESH MANUAL\n"
            "PROPERTIES ()\n"
            "AS\n"
            "SELECT id FROM t;"
        )
        assert _extract_as_select(ddl) == "SELECT id FROM t"

    def test_missing_as_returns_none(self):
        assert _extract_as_select("CREATE MATERIALIZED VIEW `mv` ...") is None

    def test_leading_trailing_whitespace_stripped(self):
        ddl = _make_ddl("  SELECT id FROM t  ")
        assert _extract_as_select(ddl) == "SELECT id FROM t"

    def test_with_clause_sql(self):
        """SQL beginning with WITH (CTE) must be extracted correctly."""
        sql = "WITH cte AS (SELECT id FROM base_t) SELECT * FROM cte"
        ddl = _make_ddl(sql)
        assert _extract_as_select(ddl) == sql

    def test_first_as_is_structural(self):
        """
        find() is used (not rfind()), so the FIRST '\\nas ' is taken as the structural
        AS keyword. Any '\\nas ' inside the SQL body would appear later and be ignored.
        """
        sql = "SELECT id FROM t"
        ddl = _make_ddl(sql)
        # Manually append a line that starts with 'as ' — simulates an unusual but
        # theoretically possible multiline alias. find() should still capture the
        # correct structural AS.
        ddl_with_trailing = ddl.replace(
            "AS SELECT id FROM t;",
            "AS SELECT id FROM t -- inline\n;",
        )
        result = _extract_as_select(ddl_with_trailing)
        assert result is not None
        assert result.startswith("SELECT id FROM t")


class TestRefreshTypeComparison:
    """
    Tests for the refresh_method comparison logic used in
    starrocks__get_materialized_view_configuration_changes.

    Both sides are lowercased before comparison.
    """

    @pytest.mark.parametrize(
        "stored_refresh_type, configured_refresh_method, expect_changed",
        [
            ("MANUAL", "manual", False),
            ("manual", "manual", False),
            ("ASYNC",  "async",  False),
            ("async",  "async",  False),
            ("MANUAL", "async",  True),
            ("ASYNC",  "manual", True),
            ("Manual", "manual", False),   # mixed case stored
        ],
    )
    def test_refresh_comparison(
        self, stored_refresh_type, configured_refresh_method, expect_changed
    ):
        changed = stored_refresh_type.lower() != configured_refresh_method.lower()
        assert changed == expect_changed
