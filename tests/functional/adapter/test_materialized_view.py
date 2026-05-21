import pytest

from dbt.tests.util import (
    get_model_file,
    run_dbt,
    run_dbt_and_capture,
    set_model_file,
)
from dbt.adapters.contracts.relation import RelationType

MY_SEED = """
id,value
1,100
2,200
3,300
""".strip()

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

# Structural-config changes (distribution / buckets). These can't be altered in
# place, so they must force a rebuild.

MY_MV_DIST_CHANGED = """
{{ config(
    materialized='materialized_view',
    distributed_by=['value'],
    refresh_method='manual'
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

MY_MV_BUCKETS_CHANGED = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    buckets=3,
    refresh_method='manual'
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

MY_MV_ORDER_BY = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual',
    order_by=['id']
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

# A user-set property. Only keys the model specifies are compared; StarRocks'
# injected defaults (replication_num, storage_medium, ...) must be ignored.
MY_MV_WITH_PROPERTY = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual',
    properties={"session.insert_timeout": "3600"}
) }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

# Models for reactivation tests

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

# Models for skip DDL when view unchanged

MY_VIEW_SQL = """
{{ config(materialized='view') }}
select id, value from {{ ref('my_seed') }}
""".lstrip()

# A genuine SQL change that does NOT alter column types, so the dependent
# passthrough MV stays schema-compatible and can be reactivated. (Changing a
# column's type, e.g. value * 10, would make StarRocks reject reactivation.)
MY_VIEW_SQL_CHANGED = """
{{ config(materialized='view') }}
select id, value from {{ ref('my_seed') }} where id >= 1
""".lstrip()

MY_MV_ON_MY_VIEW_SQL = """
{{ config(
    materialized='materialized_view',
    distributed_by=['id'],
    refresh_method='manual'
) }}
select id, value from {{ ref('my_view') }}
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


def _mv_definition(project, mv_name: str) -> str:
    """Return the stored MATERIALIZED_VIEW_DEFINITION (lower-cased) for the MV."""
    schema = project.test_schema
    result = project.run_sql(
        f"select materialized_view_definition from information_schema.materialized_views"
        f" where table_schema = '{schema}' and table_name = '{mv_name}'",
        fetch="one",
    )
    assert result is not None, f"MV '{mv_name}' not found in information_schema"
    return result[0].lower()


def _server_version(project) -> tuple:
    """Return the running StarRocks version as a (major, minor, patch) tuple."""
    raw = project.run_sql("select current_version()", fetch="one")[0]
    first = raw.split('-')[0].split(' ')[0]
    parts = first.split('.')
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return tuple(int(p) for p in parts)
    return (999, 999, 999)


def _skip_if_before(project, version: tuple, reason: str) -> None:
    """Skip the current test when the server is older than `version`.

    Verbatim definition storage — which the SQL-comparison logic depends on —
    only exists from 4.0.2 (materialized views, #64318) and 4.0.6 (views, #68040).
    Below those versions StarRocks canonicalizes the stored SQL, so the no-op /
    skip optimizations are intentionally disabled and these assertions don't hold.
    """
    if _server_version(project) < version:
        pytest.skip(reason)


class TestMaterializedViewConfigChangeDetection:
    """
    Tests that starrocks__get_materialized_view_configuration_changes correctly
    distinguishes between unchanged config (no-op refresh) and changed config
    (drop+recreate).

    Previously an empty macro stub returned "", which dbt treated as "changes
    present", causing every MV to be dropped and recreated on every run.
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
        project.run_sql(f"drop database if exists {project.test_schema} force")

    def test_unchanged_config_is_noop(self, project, my_mv):
        """
        A second dbt run with no model changes must NOT drop+recreate the MV.
        get_materialized_view_configuration_changes should return none, dbt
        should take the REFRESH path (our no-op), and log 'Applying REFRESH to:'.

        Only valid on >= 4.0.2: earlier versions canonicalize the stored MV
        definition, so the SQL comparison can't detect "unchanged" and the
        materialization intentionally rebuilds every run.
        """
        _skip_if_before(project, (4, 0, 2),
                        "MV no-op detection requires verbatim MV storage (>= 4.0.2)")

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

    def test_distribution_change_triggers_rebuild(self, project, my_mv):
        """
        Changing distributed_by can't be altered in place, so it must trigger a
        rebuild ('Applying ALTER to:') and the new distribution must be applied.
        """
        _skip_if_before(project, (4, 0, 2),
                        "structural change detection requires verbatim MV storage (>= 4.0.2)")

        set_model_file(project, my_mv, MY_MV_DIST_CHANGED)

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be rebuilt after distributed_by change"
        )
        assert "hash(`value`)" in _mv_definition(project, "my_mv"), (
            "rebuilt MV should use the new distribution key"
        )

    def test_buckets_change_triggers_rebuild(self, project, my_mv):
        """
        Adding/changing buckets can't be altered in place, so it must trigger a
        rebuild and the new bucket count must be applied.
        """
        _skip_if_before(project, (4, 0, 2),
                        "structural change detection requires verbatim MV storage (>= 4.0.2)")

        set_model_file(project, my_mv, MY_MV_BUCKETS_CHANGED)

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be rebuilt after buckets change"
        )
        assert "buckets 3" in _mv_definition(project, "my_mv"), (
            "rebuilt MV should use the new bucket count"
        )

    def test_order_by_change_triggers_rebuild(self, project, my_mv):
        """
        ORDER BY can't be altered in place, so adding/changing it must trigger a
        rebuild and the new sort key must be applied.
        """
        _skip_if_before(project, (4, 0, 2),
                        "structural change detection requires verbatim MV storage (>= 4.0.2)")

        set_model_file(project, my_mv, MY_MV_ORDER_BY)

        _, logs = run_dbt_and_capture(["--debug", "run"])

        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be rebuilt after order_by was set"
        )
        assert "order by (id)" in _mv_definition(project, "my_mv"), (
            "rebuilt MV should carry the new ORDER BY"
        )

    def test_property_change_triggers_rebuild(self, project, my_mv):
        """
        Setting/changing a user property triggers a rebuild and is applied. A
        subsequent unchanged run must NOT rebuild — StarRocks injects default
        properties on every MV, and only the user-set keys should be compared.
        """
        _skip_if_before(project, (4, 0, 2),
                        "property change detection requires verbatim MV storage (>= 4.0.2)")

        set_model_file(project, my_mv, MY_MV_WITH_PROPERTY)

        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert f"Applying ALTER to: {my_mv}" in logs, (
            "Expected MV to be rebuilt after a property was set"
        )
        assert 'insert_timeout" = "3600"' in _mv_definition(project, "my_mv"), (
            "rebuilt MV should carry the user-set property"
        )

        # Re-run with no change: the user property still matches, and the
        # StarRocks-injected defaults must not be mistaken for a change.
        _, logs2 = run_dbt_and_capture(["--debug", "run"])
        assert f"Applying ALTER to: {my_mv}" not in logs2, (
            "unchanged property run must not rebuild (injected defaults ignored)"
        )


class TestMaterializedViewSelfReactivation:
    """
    Tests that starrocks__get_materialized_view_configuration_changes reactivates
    an MV that was set IS_ACTIVE='false' by upstream view DDL in the same run.

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
        project.run_sql(f"drop database if exists {project.test_schema} force")

    def test_mv_active_after_initial_run(self, project):
        """Sanity check: the MV should be active immediately after creation."""
        assert _is_active(project, "my_mv_on_view") == "true"

    def test_mv_stays_active_after_view_ddl_in_same_run(self, project):
        """
        A second full dbt run recreates my_base_view (which deactivates the MV),
        then processes my_mv_on_view — the materialization should reactivate it so
        the MV is active at the end of the run.
        """
        # Second run: view is recreated (deactivates MV), MV is then processed
        # (reactivated, config unchanged → no-op refresh).
        run_dbt(["run"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "MV should be reactivated and active at the end of the same run"
        )

    def test_mv_reactivated_when_deactivated_before_mv_only_run(self, project):
        """
        When the MV is deactivated outside of dbt (simulated by manually
        dropping and recreating the upstream view) and then only the MV model
        is run (not the view), the materialization reactivates it.
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

        # Run only the MV model — the materialization should reactivate it.
        run_dbt(["run", "--models", "my_mv_on_view"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "MV should be reactivated and active after an MV-only run"
        )


class TestViewSkipWhenUnchanged:
    """
    Tests the StarRocks view materialization's skip-when-unchanged behavior.

    On >= 4.0.6 StarRocks stores the original view SQL verbatim, so the
    materialization compares the stored definition against the compiled SQL and
    issues no DDL when they match. Because StarRocks deactivates dependent MVs
    whenever a base view is recreated, skipping the unchanged view keeps those
    MVs active without relying on reactivation.

    Setup: my_seed -> my_view (view) -> my_mv_on_view (materialized_view).
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"my_seed.csv": MY_SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_view.sql": MY_VIEW_SQL,
            "my_mv_on_view.sql": MY_MV_ON_MY_VIEW_SQL,
        }

    @pytest.fixture(scope="class")
    def my_view(self, project):
        return project.adapter.Relation.create(
            identifier="my_view",
            schema=project.test_schema,
            database=project.database,
            type=RelationType.View,
        )

    @pytest.fixture(autouse=True)
    def setup(self, project, my_view):
        run_dbt(["seed"])
        run_dbt(["run", "--full-refresh"])
        initial_model = get_model_file(project, my_view)
        yield
        set_model_file(project, my_view, initial_model)
        project.run_sql(f"drop database if exists {project.test_schema} force")

    def test_unchanged_view_is_skipped(self, project, my_view):
        """An unchanged view must issue no DDL — logged as 'skip <relation>'."""
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_view"])

        assert f"skip {my_view}" in logs, (
            "an unchanged view should be skipped (no DDL issued)"
        )

    def test_unchanged_view_run_keeps_dependent_mv_active(self, project):
        """
        Running ONLY the (unchanged) view must not deactivate its dependent MV.
        Because the MV model is not run here, MV self-reactivation cannot mask a
        view rebuild — so the MV staying active proves the view was skipped.
        """
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

        run_dbt(["run", "--select", "my_view"])

        assert _is_active(project, "my_mv_on_view") == "true", (
            "dependent MV must stay active when the unchanged view is skipped"
        )

    def test_sql_change_rebuilds_view(self, project, my_view):
        """
        A genuine SQL change must rebuild the view (not skip it). Rebuilding the
        view deactivates the dependent MV; running the MV afterwards reactivates
        it so it ends up active again.
        """
        _skip_if_before(project, (4, 0, 6),
                        "view skip requires verbatim view storage (>= 4.0.6)")

        set_model_file(project, my_view, MY_VIEW_SQL_CHANGED)

        # Rebuild just the view: the SQL changed, so it must not be skipped.
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "my_view"])
        assert f"skip {my_view}" not in logs, (
            "a changed view must be rebuilt, not skipped"
        )
        stored = project.run_sql(
            f"select view_definition from information_schema.views"
            f" where table_schema = '{project.test_schema}' and table_name = 'my_view'",
            fetch="one",
        )[0]
        assert "id >= 1" in stored, "rebuilt view should reflect the new SQL"

        # Rebuilding the base view deactivates the dependent MV.
        assert _is_active(project, "my_mv_on_view") == "false", (
            "rebuilding the base view should deactivate the dependent MV"
        )

        # Running the MV reactivates it.
        run_dbt(["run", "--select", "my_mv_on_view"])
        assert _is_active(project, "my_mv_on_view") == "true", (
            "dependent MV should be reactivated after it is run"
        )
