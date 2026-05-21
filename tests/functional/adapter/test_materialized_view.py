"""
Functional tests for the materialized view materialization.

Tests are designed to run against a live StarRocks cluster (localhost:9030).

Covers:
- Option D: config change detection prevents unnecessary drop+recreate (SSU-1881 fix)
- Option F: self-reactivation when IS_ACTIVE='false' caused by upstream view DDL
"""
import pytest

from dbt.tests.util import (
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from dbt.adapters.contracts.relation import RelationType


# ---------------------------------------------------------------------------
# Seed
# ---------------------------------------------------------------------------

MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()

# ---------------------------------------------------------------------------
# Model SQL
# ---------------------------------------------------------------------------

MY_MV_SQL = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual'
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

MY_MV_SQL_CHANGED = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual'
) }}
select id, value * 2 as value from {{ ref('my_seed') }}
""".lstrip()

MY_MV_ASYNC_SQL = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='async'
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

# Models for the Option F (self-reactivation) test.
# my_mv_on_view depends on my_base_view, which is a regular view.
# When dbt recreates the view in the same run, StarRocks deactivates the MV;
# Option F reactivates it before returning from get_materialized_view_configuration_changes.

MY_BASE_VIEW_SQL = """
{{ config(materialized='view') }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

MY_MV_ON_VIEW_SQL = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual'
) }}
select id, value from {{ ref('my_base_view') }}
""".lstrip()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _is_active(project, mv_name: str) -> str:
    """Return IS_ACTIVE string ('true' or 'false') for the named MV."""
    schema = project.test_schema
    result = project.run_sql(
        f"select is_active from information_schema.materialized_views"
        f" where table_schema = '{schema}' and table_name = '{mv_name}'",
        fetch="one",
    )
    assert result is not None, f"MV '{mv_name}' not found in information_schema"
    return result[0]


def _refresh_type(project, mv_name: str) -> str:
    """Return REFRESH_TYPE (upper-cased) for the named MV."""
    schema = project.test_schema
    result = project.run_sql(
        f"select refresh_type from information_schema.materialized_views"
        f" where table_schema = '{schema}' and table_name = '{mv_name}'",
        fetch="one",
    )
    assert result is not None, f"MV '{mv_name}' not found in information_schema"
    return result[0].upper()


# ---------------------------------------------------------------------------
# Test: Option D — config change detection
# ---------------------------------------------------------------------------

class TestMaterializedViewConfigChangeDetection:
    """
    Tests that starrocks__get_materialized_view_configuration_changes correctly
    distinguishes between unchanged config (no-op refresh) and changed config
    (drop+recreate).

    This is the core fix for SSU-1881: previously the empty macro stub returned
    "" which dbt treated as "changes present", causing every MV to be dropped
    and recreated on every run.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {"my_mv.sql": MY_MV_SQL}

    @pytest.fixture(scope="class")
    def my_mv(self, project):
        return project.adapter.Relation.create(
            identifier="my_mv",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.MaterializedView,
        )

    @pytest.fixture(autouse=True)
    def setup(self, project, my_mv):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        initial_model = get_model_file(project, my_mv)
        yield
        set_model_file(project, my_mv, initial_model)
        project.run_sql(f"drop database if exists {project.test_schema}")

    def test_unchanged_config_is_noop(self, project, my_mv):
        """
        A second dbt run with no model changes must NOT drop+recreate the MV.
        get_materialized_view_configuration_changes should return none, dbt
        should take the REFRESH path (our no-op), and log 'Applying REFRESH to:'.
        """
        _, logs = run_dbt_and_capture(["--debug", "run"])

        # The ALTER path is taken only when configuration changes are detected.
        assert f"Applying ALTER to: {my_mv}" not in logs, (
            "MV was unnecessarily altered on an unchanged config run"
        )
        # The REFRESH path (our no-op) should be taken instead.
        assert f"Applying REFRESH to: {my_mv}" in logs

    def test_sql_change_triggers_replace(self, project, my_mv):
        """
        Changing the model SQL must trigger get_alter_materialized_view_as_sql
        (logged as 'Applying ALTER to:'), which internally drops and recreates
        the MV.
        """
        set_model_file(project, my_mv, MY_MV_SQL_CHANGED)

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be altered after SQL change"
        )

    def test_refresh_method_change_uses_alter(self, project, my_mv):
        """
        Changing refresh_method from 'manual' to 'async' must be detected as a
        configuration change and trigger an in-place ALTER REFRESH (not a swap).
        The MV must remain active throughout — no drop+recreate occurs.
        """
        set_model_file(project, my_mv, MY_MV_ASYNC_SQL)

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be altered after refresh_method change"
        )
        # After in-place ALTER REFRESH the stored type should be ASYNC.
        assert _refresh_type(project, "my_mv") == "ASYNC"
        # MV must stay active — an in-place ALTER does not deactivate it.
        assert _is_active(project, "my_mv") == "true"


# ---------------------------------------------------------------------------
# Test: Option F — self-reactivation
# ---------------------------------------------------------------------------

class TestMaterializedViewSelfReactivation:
    """
    Tests that starrocks__get_materialized_view_configuration_changes reactivates
    an MV that was set IS_ACTIVE='false' by upstream view DDL in the same run.

    This is Option F from the design doc (ssu-1881-mv-invalidation.md).

    Setup: my_seed → my_base_view (view) → my_mv_on_view (materialized_view).
    When dbt re-runs both models, it recreates my_base_view (DROP+CREATE), which
    StarRocks marks my_mv_on_view as IS_ACTIVE='false'. The MV materialization
    then calls ALTER MATERIALIZED VIEW ... ACTIVE to heal it.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_base_view.sql": MY_BASE_VIEW_SQL,
            "my_mv_on_view.sql": MY_MV_ON_VIEW_SQL,
        }

    @pytest.fixture(autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield
        project.run_sql(f"drop database if exists {project.test_schema}")

    def test_mv_active_after_initial_run(self, project):
        """Sanity check: the MV should be active immediately after creation."""
        assert _is_active(project, "my_mv_on_view") == "true"

    def test_mv_stays_active_after_view_ddl_in_same_run(self, project):
        """
        A second full dbt run recreates my_base_view (which deactivates the MV),
        then processes my_mv_on_view — Option F should reactivate it so the MV
        is active at the end of the run.
        """
        # Second run: view is recreated (deactivates MV), MV is then processed
        # (Option F reactivates it, config unchanged → no-op refresh).
        run_dbt(["run"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "MV should be active after Option F reactivation in the same run"
        )

    def test_mv_reactivated_when_deactivated_before_mv_only_run(self, project):
        """
        When the MV is deactivated outside of dbt (simulated by manually
        dropping and recreating the upstream view) and then only the MV model
        is run (not the view), Option F reactivates it.
        """
        schema = project.test_schema

        # Simulate external view recreation that deactivates the MV.
        view_def = project.run_sql(
            f"select view_definition from information_schema.views"
            f" where table_schema = '{schema}' and table_name = 'my_base_view'",
            fetch="one",
        )
        assert view_def is not None
        project.run_sql(f"drop view if exists `{schema}`.`my_base_view`")
        project.run_sql(f"create view `{schema}`.`my_base_view` as {view_def[0]}")

        # Confirm the MV is now inactive.
        assert _is_active(project, "my_mv_on_view") == "false", (
            "MV should be inactive after the view was externally dropped+recreated"
        )

        # Run only the MV model — Option F should reactivate it.
        run_dbt(["run", "--models", "my_mv_on_view"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "MV should be active after Option F reactivation (MV-only run)"
        )
