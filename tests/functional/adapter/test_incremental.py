import pytest

from abc import ABC, abstractmethod

from dbt.tests.adapter.basic.test_incremental import BaseIncremental
from dbt.tests.util import run_dbt, run_dbt_and_capture, check_relations_equal, relation_from_name

partition_1_base_csv = """
id,partition_key
1,1
2,1
3,1
4,1
5,1
""".lstrip()

partition_1_added_csv = """
id,partition_key
6,1
7,1
8,1
9,1
10,1
""".lstrip()

partition_2_base_csv = """
id,partition_key
11,2
12,2
13,2
14,2
15,2
""".lstrip()

full_partition_1_csv = partition_1_base_csv + """
6,1
7,1
8,1
9,1
10,1
""".lstrip()

full_partition_2_csv = full_partition_1_csv + """
11,2
12,2
13,2
14,2
15,2
""".lstrip()

dynamic_partition_2_csv = partition_1_added_csv + """
11,2
12,2
13,2
14,2
15,2
""".lstrip()

class TestBaseIncrementalStrategyModel(ABC, BaseIncremental):

    @staticmethod
    @abstractmethod
    def _get_strategy():
        raise NotImplementedError

    @abstractmethod
    def _specific_assertions(self, project):
        raise NotImplementedError

    @staticmethod
    def _seeds():
        return {
            "partition_1_base.csv": partition_1_base_csv,
            "partition_1_added.csv": partition_1_added_csv,
            "partition_2_base.csv": partition_2_base_csv,
            "full_partition_1.csv": full_partition_1_csv,
            "full_partition_2.csv": full_partition_2_csv,
            "dynamic_partition_2.csv": dynamic_partition_2_csv,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "name": "test_incremental",
            "models": {
                "+materialized": "incremental",
                "+partition_type": "Expr",
                "+partition_by": ["partition_key"],
            } | self._get_strategy()
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return self._seeds()

    def _common_assertions(self, project):
        _seeds_row_counts = [
            ("partition_1_base", 5),
            ("partition_1_added", 5),
            ("partition_2_base", 5),
            ("full_partition_1", 10),
            ("full_partition_2", 15),
            ("dynamic_partition_2", 10),
        ]

        # seed command
        results = run_dbt(["seed"])
        assert len(results) == len(_seeds_row_counts)

        # Make sure seeds are properly setup
        for pname, pcount in _seeds_row_counts:
            relation = relation_from_name(project.adapter, f"{pname}")
            result = project.run_sql(f"select count(*) as num_rows from {relation}", fetch="one")
            assert result[0] == pcount

        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_base"])) == 1
        check_relations_equal(project.adapter, ["partition_1_base", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_1_added"])) == 1

    def _doc_tests(self):
        # get catalog from docs generate
        catalog = run_dbt(["docs", "generate"])
        assert len(catalog.nodes) == len(self._seeds()) + 1

    def test_incremental(self, project):
        self._common_assertions(project=project)
        self._specific_assertions(project=project)
        self._doc_tests()


class TestInvalidStrategyIncrementalModel(TestBaseIncrementalStrategyModel):


    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "foobar"}

    def _specific_assertions(self, project):
        pass

    def test_incremental(self, project):
        try:
            run_dbt(["run"])
        except AssertionError as ae:
            assert str(ae) == "dbt exit state did not match expected"


class TestOmittedStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["full_partition_1", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_2_base"])) == 1
        check_relations_equal(project.adapter, ["full_partition_2", "incremental"])


class TestDefaultStrategyIncrementalModel(TestOmittedStrategyIncrementalModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "default"}


class TestDefaultStrategyDeprecationNotice(TestBaseIncrementalStrategyModel):
    """Omitting incremental_strategy runs 'default' and logs that it will be deprecated."""

    @staticmethod
    def _get_strategy():
        return {}

    def _specific_assertions(self, project):
        pass

    def test_incremental(self, project):
        run_dbt(["seed"])
        # A notice, not a warning: it must not fail a run under --warn-error.
        _, logs = run_dbt_and_capture(
            ["--warn-error", "run", "--vars", "seed_name: partition_1_base"]
        )
        assert "'default' is used. It will be deprecated in a future release" in logs


class TestInsertOverwriteStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "insert_overwrite"}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["partition_1_added", "incremental"])

        assert len(run_dbt(["run", "--vars", "seed_name: partition_2_base"])) == 1
        check_relations_equal(project.adapter, ["partition_2_base", "incremental"])


class TestDynamicOverwriteStrategyIncrementalModel(TestBaseIncrementalStrategyModel):

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "dynamic_overwrite"}

    def _specific_assertions(self, project):
        check_relations_equal(project.adapter, ["partition_1_added", "incremental"])

        results = run_dbt(["run", "--vars", "seed_name: partition_2_base"])
        assert len(results) == 1

        check_relations_equal(project.adapter, ["dynamic_partition_2", "incremental"])


class TestAppendStrategyIncrementalModel(TestOmittedStrategyIncrementalModel):
    """'append' is the explicit name for what 'default' does without a unique_key."""

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "append"}


merge_base_csv = """
id,partition_key,value
1,1,10
2,1,20
3,1,30
""".lstrip()

merge_updated_csv = """
id,partition_key,value
2,1,200
3,1,300
4,1,40
""".lstrip()

merge_model_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['id', 'partition_key'],
    )
}}
select * from {{ ref(var('seed_name', 'merge_base')) }}
""".lstrip()


class TestMergeStrategyIncrementalModel:
    """'merge' upserts on unique_key: overlapping keys update in place, new keys insert.

    StarRocks has no MERGE statement; the upsert comes from unique_key becoming the
    PRIMARY KEY of the target table.
    """

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "merge_base.csv": merge_base_csv,
            "merge_updated.csv": merge_updated_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": merge_model_sql}

    def test_merge_upserts_on_unique_key(self, project):
        assert len(run_dbt(["seed"])) == 2

        assert len(run_dbt(["run", "--vars", "seed_name: merge_base"])) == 1
        relation = relation_from_name(project.adapter, "merge_model")
        assert project.run_sql(
            f"select count(*) from {relation}", fetch="one"
        )[0] == 3

        assert len(run_dbt(["run", "--vars", "seed_name: merge_updated"])) == 1

        # 4 rows, not 6: ids 2 and 3 were updated in place rather than appended.
        assert project.run_sql(
            f"select count(*) from {relation}", fetch="one"
        )[0] == 4

        result = project.run_sql(
            f"select id, value from {relation} order by id", fetch="all"
        )
        assert result == [(1, 10), (2, 200), (3, 300), (4, 40)]


class TestMergeStrategyWithPrimaryKeysConfig(TestMergeStrategyIncrementalModel):
    """'merge' also accepts a PRIMARY KEY declared through table_type and keys."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": merge_model_sql.replace(
            "unique_key=['id', 'partition_key'],",
            "table_type='PRIMARY', keys=['id', 'partition_key'], distributed_by=['id'],",
        )}


class TestMergeStrategyRequiresPrimaryKey(TestBaseIncrementalStrategyModel):
    """'merge' on a DUPLICATE KEY table would append, so it must fail instead."""

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "merge"}

    def _specific_assertions(self, project):
        pass

    def test_incremental(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--vars", "seed_name: partition_1_base"], expect_pass=False)
        assert "requires a PRIMARY KEY table" in results[0].message


class TestAppendStrategyRejectsUniqueKey(TestBaseIncrementalStrategyModel):
    """unique_key makes the table PRIMARY KEY, where an INSERT upserts rather than appends."""

    @staticmethod
    def _get_strategy():
        return {"+incremental_strategy": "append", "+unique_key": ["id", "partition_key"]}

    def _specific_assertions(self, project):
        pass

    def test_incremental(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run", "--vars", "seed_name: partition_1_base"], expect_pass=False)
        assert "requires a DUPLICATE KEY table" in results[0].message


strategy_switch_model_sql = """
{%- set strategy = var('strategy') -%}
{{
    config(
        materialized='incremental',
        incremental_strategy=strategy,
        unique_key=(['id', 'partition_key'] if strategy == 'merge' else none),
    )
}}
select * from {{ ref('merge_base') }}
""".lstrip()


class TestStrategyChecksExistingTableType:
    """The existing table's key type is checked too: a table built as DUPLICATE KEY stays
    that way until a full refresh, whatever the model's config now says."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"merge_base.csv": merge_base_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"switch_model.sql": strategy_switch_model_sql}

    def test_merge_into_duplicate_key_table_fails(self, project):
        run_dbt(["seed"])
        run_dbt(["run", "--vars", "strategy: append"])

        results = run_dbt(["run", "--vars", "strategy: merge"], expect_pass=False)
        assert "is a DUP_KEYS table" in results[0].message

        # A full refresh rebuilds it as PRIMARY KEY, after which merge runs.
        run_dbt(["run", "--vars", "strategy: merge", "--full-refresh"])
        run_dbt(["run", "--vars", "strategy: merge"])


class TestMergeRejectsIncrementalPredicates(TestMergeStrategyIncrementalModel):
    """'merge' matches on the PRIMARY KEY alone, so extra match conditions are refused rather
    than silently ignored."""

    @pytest.fixture(scope="class")
    def models(self):
        return {"merge_model.sql": merge_model_sql.replace(
            "unique_key=['id', 'partition_key'],",
            "unique_key=['id', 'partition_key'], incremental_predicates=['value > 0'],",
        )}

    def test_merge_upserts_on_unique_key(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert "does not support 'incremental_predicates'" in results[0].message


class TestMergeAcceptsExistingUniqueKeyTable(TestMergeStrategyIncrementalModel):
    """dbt cannot build a UNIQUE KEY table (StarRocks CTAS rejects them), but 'merge' accepts one
    created outside dbt, since an INSERT replaces its rows by key too."""

    def test_merge_upserts_on_unique_key(self, project):
        assert len(run_dbt(["seed"])) == 2
        project.run_sql(f"""
            create table {project.test_schema}.merge_model (id bigint, partition_key bigint, value bigint)
            UNIQUE KEY(id, partition_key) DISTRIBUTED BY HASH(id)
        """)
        project.run_sql(f"insert into {project.test_schema}.merge_model values (1, 1, 10), (2, 1, 20)")

        assert len(run_dbt(["run", "--vars", "seed_name: merge_updated"])) == 1
        relation = relation_from_name(project.adapter, "merge_model")
        result = project.run_sql(f"select id, value from {relation} order by id", fetch="all")
        assert result == [(1, 10), (2, 200), (3, 300), (4, 40)]


delete_insert_base_csv = """
id,day,value
1,1,10
2,1,20
3,1,30
4,2,40
""".lstrip()

delete_insert_update_csv = """
id,day,value
2,1,200
5,1,50
""".lstrip()

delete_insert_model_template = """
{{{{ config(materialized='incremental', incremental_strategy='delete+insert', {config}) }}}}
select id, day, value from {{{{ ref(var('seed_name', 'delete_insert_base')) }}}}
""".lstrip()


class TestDeleteInsertStrategyIncrementalModel:
    """'delete+insert' deletes target rows whose unique_key appears in the new data, then
    inserts the new data. With explicit PRIMARY KEY `keys`, unique_key can be coarser than
    the key: here every row of a day is replaced, including rows the new data omits."""

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "delete_insert_base.csv": delete_insert_base_csv,
            "delete_insert_update.csv": delete_insert_update_csv,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            # unique_key becomes the PRIMARY KEY, as under 'merge'.
            "by_unique_key.sql": delete_insert_model_template.format(
                config="unique_key=['id', 'day']"),
            # The table keeps (id, day) as its PRIMARY KEY; rows are replaced per day.
            "by_day.sql": delete_insert_model_template.format(
                config="table_type='PRIMARY', keys=['id', 'day'], unique_key='day'"),
            # Predicates narrow the delete: only rows of day 1 with value < 25 go.
            "by_day_with_predicate.sql": delete_insert_model_template.format(
                config="table_type='PRIMARY', keys=['id', 'day'], unique_key='day', "
                       "incremental_predicates=['value < 25']"),
        }

    def _rows(self, project, name):
        relation = relation_from_name(project.adapter, name)
        return project.run_sql(f"select id, value from {relation} order by id", fetch="all")

    def test_delete_insert(self, project):
        assert len(run_dbt(["seed"])) == 2
        assert len(run_dbt(["run"])) == 3
        assert len(run_dbt(["run", "--vars", "seed_name: delete_insert_update"])) == 3

        assert self._rows(project, "by_unique_key") == [(1, 10), (2, 200), (3, 30), (4, 40), (5, 50)]
        # Ids 1 and 3 are gone: day 1 was replaced wholesale.
        assert self._rows(project, "by_day") == [(2, 200), (4, 40), (5, 50)]
        # Id 3 (value 30) survives the predicate.
        assert self._rows(project, "by_day_with_predicate") == [(2, 200), (3, 30), (4, 40), (5, 50)]


class TestDeleteInsertStrategyValidation:

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"delete_insert_base.csv": delete_insert_base_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "no_unique_key.sql": delete_insert_model_template.format(config="keys=['id']"),
            "duplicate_keys.sql": delete_insert_model_template.format(
                config="keys=['id'], unique_key='day'"),
        }

    def test_validation(self, project):
        run_dbt(["seed"])
        results = {r.node.name: r.message for r in run_dbt(["run"], expect_pass=False)}
        assert "requires the 'unique_key' configuration" in results["no_unique_key"]
        assert "requires a PRIMARY KEY table" in results["duplicate_keys"]
