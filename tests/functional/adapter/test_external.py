import pytest
from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.util import (
    check_relation_types,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
)

seed_base_csv = """
id,name,some_date
1,Alice,2023-01-01
2,Bob,2023-01-02
3,Charlie,2023-01-03
4,David,2023-01-04
5,Eve,2023-01-05
6,Frank,2023-01-06
7,Grace,2023-01-07
8,Henry,2023-01-08
9,Iris,2023-01-09
10,Jack,2023-01-10
""".lstrip()

external_catalog_table_sql = """
{{
    config(
        materialized = 'table',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
        partition_by = ['some_date'],
    )
}}
select * from {{ ref('base') }}
""".lstrip()

class TestExternalCatalogTable:
    """Test basic table materialization in external catalog"""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "base.csv": seed_base_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "external_table.sql": external_catalog_table_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        """Create external database with location"""
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
        """)
        yield

        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_test_db.external_table")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    def test_external_catalog_table(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1

        results = run_dbt()
        assert len(results) == 1

        relation = relation_from_name(project.adapter, "external_table")
        result = project.run_sql(
            f"select count(*) as num_rows from iceberg_catalog.dbt_test_db.{relation.identifier}",
            fetch="one"
        )
        assert result[0] == 10

        # Verify it's actually a table
        expected = {
            "base": "table",
            "external_table": "table",
        }
        check_relation_types(project.adapter, expected)


seed_partition_1_csv = """
id,partition_key
1,1
2,1
3,1
4,1
5,1
""".lstrip()

seed_partition_1_replaced_plus_2_csv = """
id,partition_key
6,1
7,1
8,1
9,1
10,1
11,2
12,2
13,2
14,2
15,2
""".lstrip()

external_catalog_incremental_sql = """
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'dynamic_overwrite',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
        partition_by = ['partition_key'],
    )
}}
select id, partition_key from {{ ref(var('seed_name')) }}
""".lstrip()


class TestExternalCatalogIncremental:
    """Test incremental materialization in an external catalog.

    A second run has to find the table it created on the first one. Relations in
    an external catalog were invisible to the relation cache, so every run after
    the first re-emitted CREATE TABLE and failed with "table already exists".
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "partition_1.csv": seed_partition_1_csv,
            "partition_1_replaced_plus_2.csv": seed_partition_1_replaced_plus_2_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_external.sql": external_catalog_incremental_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        # A default so the model parses before the first --vars override.
        return {"vars": {"seed_name": "partition_1"}}

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
        """)
        yield

        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_test_db.incremental_external")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    @staticmethod
    def _ids_by_partition(project):
        rows = project.run_sql(
            "select partition_key, id"
            " from iceberg_catalog.dbt_test_db.incremental_external"
            " order by partition_key, id",
            fetch="all",
        )
        ids_by_partition = {}
        for partition_key, id_ in rows:
            ids_by_partition.setdefault(partition_key, []).append(id_)
        return ids_by_partition

    def test_incremental_dynamic_overwrite(self, project):
        assert len(run_dbt(["seed"])) == 2

        # First run creates the Iceberg table.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {1: [1, 2, 3, 4, 5]}

        # Second run must reach the insert instead of re-creating the table.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_replaced_plus_2"])) == 1

        # Partition 1 is replaced in place rather than appended to, and the
        # partition the query newly produced is created.
        assert self._ids_by_partition(project) == {
            1: [6, 7, 8, 9, 10],
            2: [11, 12, 13, 14, 15],
        }

        # Dynamic overwrite leaves a partition the query no longer produces alone.
        assert len(run_dbt(["run", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {
            1: [1, 2, 3, 4, 5],
            2: [11, 12, 13, 14, 15],
        }

        # External catalogs have no ALTER TABLE ... SWAP WITH, so --full-refresh
        # drops and recreates: only what the query produces survives.
        assert len(run_dbt(["run", "--full-refresh", "--vars", "seed_name: partition_1"])) == 1
        assert self._ids_by_partition(project) == {1: [1, 2, 3, 4, 5]}


external_catalog_incremental_custom_schema_sql = """
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        catalog = 'iceberg_catalog',
        schema = 'custom',
    )
}}
select 1 as id
""".lstrip()


class TestExternalCatalogIncrementalCustomSchema:
    """An incremental model in an external catalog builds in its resolved schema.

    With no `database` config, the model has to land in the schema dbt resolved for
    it (`<target schema>_custom` here), as a table model does. It used to land in
    the profile's schema while dbt reported the resolved one.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_custom_schema.sql": external_catalog_incremental_custom_schema_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        # Both schemas exist, so building in the wrong one succeeds rather than
        # failing on a missing database.
        schemas = [project.test_schema, f"{project.test_schema}_custom"]
        for schema in schemas:
            project.run_sql(f"""
                CREATE DATABASE IF NOT EXISTS iceberg_catalog.{schema}
                PROPERTIES ("location" = "s3://warehouse/{schema}")
            """)
        yield

        for schema in schemas:
            project.run_sql(
                f"DROP TABLE IF EXISTS iceberg_catalog.{schema}.incremental_custom_schema"
            )
            project.run_sql(f"DROP DATABASE IF EXISTS iceberg_catalog.{schema} FORCE")

    @staticmethod
    def _tables(project, schema):
        rows = project.run_sql(f"SHOW TABLES FROM iceberg_catalog.{schema}", fetch="all")
        return [row[0] for row in rows]

    def test_incremental_builds_in_resolved_schema(self, project):
        # The second run takes the incremental path against the table the first created.
        for _ in range(2):
            assert len(run_dbt(["run"])) == 1

        assert self._tables(project, f"{project.test_schema}_custom") == [
            "incremental_custom_schema"
        ]
        assert self._tables(project, project.test_schema) == []


external_catalog_view_sql = """
{{
    config(
        materialized = 'view',
        catalog = 'iceberg_catalog',
        database = 'dbt_test_db',
    )
}}
select 1 as id
""".lstrip()


class TestExternalCatalogView:
    """A view configured with an external catalog must be created there.

    The view materialization used to ignore `catalog` and silently build in
    default_catalog (#127).
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "external_view.sql": external_catalog_view_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_test_db
            PROPERTIES ("location" = "s3://warehouse/dbt_test_db")
        """)
        yield

        project.run_sql("DROP VIEW IF EXISTS iceberg_catalog.dbt_test_db.external_view")
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_test_db FORCE")

    def test_external_catalog_view(self, project):
        results = run_dbt()
        assert len(results) == 1

        result = project.run_sql(
            "select id from iceberg_catalog.dbt_test_db.external_view",
            fetch="one",
        )
        assert result[0] == 1

        internal = project.run_sql(
            "select count(*) from information_schema.views"
            f" where table_schema = '{project.test_schema}'"
            " and table_name = 'external_view'",
            fetch="one",
        )
        assert internal[0] == 0

        # A second run finds the existing view and, since it is unchanged, skips it.
        _, logs = run_dbt_and_capture(["--debug", "run"])
        assert "skip `iceberg_catalog`.`dbt_test_db`.`external_view`" in logs


external_ref_table_sql = """
{{ config(materialized='table', catalog='iceberg_catalog', database='dbt_ref_db') }}
select 1 as id
""".lstrip()

external_ref_default_db_sql = """
{{ config(materialized='table', catalog='iceberg_catalog') }}
select 2 as id
""".lstrip()

internal_from_external_sql = """
{{ config(materialized='table') }}
select id from {{ ref('ext_table') }}
union all
select id from {{ ref('ext_default_db') }}
""".lstrip()

external_view_from_external_sql = """
{{ config(materialized='view', catalog='iceberg_catalog', database='dbt_ref_db') }}
select id from {{ ref('ext_table') }}
""".lstrip()


class TestExternalCatalogRef:
    """ref() to a model in an external catalog resolves to where it was built.

    ref() used to render `<target schema>`.`<name>`, dropping both the catalog
    and the `database` config, so it only resolved when the session was already
    in that catalog and no `database` was set.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ext_table.sql": external_ref_table_sql,
            "ext_default_db.sql": external_ref_default_db_sql,
            "internal_from_ext.sql": internal_from_external_sql,
            "ext_view_from_ext.sql": external_view_from_external_sql,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup_external_catalog(self, project):
        # dbt_ref_db is created up front. The target schema in the external
        # catalog is left for dbt to create, as it does for any model's schema.
        project.run_sql("""
            CREATE DATABASE IF NOT EXISTS iceberg_catalog.dbt_ref_db
            PROPERTIES ("location" = "s3://warehouse/dbt_ref_db")
        """)
        yield

        project.run_sql("DROP VIEW IF EXISTS iceberg_catalog.dbt_ref_db.ext_view_from_ext")
        project.run_sql("DROP TABLE IF EXISTS iceberg_catalog.dbt_ref_db.ext_table FORCE")
        project.run_sql(
            f"DROP TABLE IF EXISTS iceberg_catalog.{project.test_schema}.ext_default_db FORCE"
        )
        project.run_sql("DROP DATABASE IF EXISTS iceberg_catalog.dbt_ref_db FORCE")
        project.run_sql(f"DROP DATABASE IF EXISTS iceberg_catalog.{project.test_schema} FORCE")

    def test_ref_to_external_model(self, project):
        results = run_dbt(["run"])
        assert len(results) == 4

        rows = project.run_sql(
            f"select id from {project.test_schema}.internal_from_ext order by id",
            fetch="all",
        )
        assert [row[0] for row in rows] == [1, 2]

        rows = project.run_sql(
            "select id from iceberg_catalog.dbt_ref_db.ext_view_from_ext", fetch="all"
        )
        assert [row[0] for row in rows] == [1]

        # A second run takes the existing-relation paths against what the first built.
        assert len(run_dbt(["run"])) == 4
