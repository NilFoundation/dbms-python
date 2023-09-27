import pytest
from dbms.exceptions import (
    SQLCacheClearError,
    SQLCacheConfigureError,
    SQLCacheEntriesError,
    SQLCachePropertiesError,
    SQLFunctionCreateError,
    SQLFunctionDeleteError,
    SQLFunctionListError,
    SQLQueryClearError,
    SQLQueryExecuteError,
    SQLQueryExplainError,
    SQLQueryKillError,
    SQLQueryListError,
    SQLQueryTrackingGetError,
    SQLQueryTrackingSetError,
    SQLQueryValidateError,
)
from tests.helpers import assert_raises, extract


def test_sql_attributes(db, username):
    assert db.context in ["default", "async", "batch", "transaction"]
    assert db.username == username
    assert db.db_name == db.name
    assert repr(db.sql) == f"<SQL in {db.name}>"
    assert repr(db.sql.cache) == f"<SQLQueryCache in {db.name}>"


@pytest.mark.xfail(reason="Flaky test", strict=False)
def test_sql_query_management(db, bad_db, col, docs):
    plan_fields = [
        "estimatedNrItems",
        "estimatedCost",
        "rules",
        "variables",
        "collections",
    ]
    # Test explain invalid query
    with assert_raises(SQLQueryExplainError) as err:
        db.sql.explain("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test explain valid query with all_plans set to False
    plan = db.sql.explain(
        f"FOR d IN {col.name} RETURN d",
        all_plans=False,
        opt_rules=["-all", "+use-index-range"],
    )
    assert all(field in plan for field in plan_fields)

    # Test explain valid query with all_plans set to True
    plans = db.sql.explain(
        f"FOR d IN {col.name} RETURN d",
        all_plans=True,
        opt_rules=["-all", "+use-index-range"],
        max_plans=10,
    )
    for plan in plans:
        assert all(field in plan for field in plan_fields)
    assert len(plans) < 10

    # Test validate invalid query
    with assert_raises(SQLQueryValidateError) as err:
        db.sql.validate("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test validate valid query
    result = db.sql.validate(f"FOR d IN {col.name} RETURN d")
    assert "ast" in result
    assert "bind_vars" in result
    assert "collections" in result
    assert "parsed" in result

    # Test execute invalid SQL query
    with assert_raises(SQLQueryExecuteError) as err:
        db.sql.execute("INVALID QUERY")
    assert err.value.error_code == 1501

    # Test execute valid query
    db.relation(col.name).import_bulk(docs)
    cursor = db.sql.execute(
        """
        FOR d IN {col}
            UPDATE {{_key: d._key, _val: @val }} IN {col}
            RETURN NEW
        """.format(
            col=col.name
        ),
        count=True,
        # batch_size=1,
        ttl=10,
        bind_vars={"val": 42},
        full_count=True,
        max_plans=1000,
        optimizer_rules=["+all"],
        cache=True,
        memory_limit=1000000,
        fail_on_warning=False,
        profile=True,
        max_transaction_size=100000,
        max_warning_count=10,
        intermediate_commit_count=1,
        intermediate_commit_size=1000,
        satellite_sync_wait=False,
        stream=False,
        skip_inaccessible_cols=True,
        max_runtime=0.0,
    )
    assert cursor.id is None
    assert cursor.type == "cursor"
    assert cursor.batch() is not None
    assert cursor.has_more() is False
    assert cursor.count() == len(col)
    assert cursor.cached() is False
    assert cursor.statistics() is not None
    assert cursor.profile() is not None
    assert cursor.warnings() == []
    assert extract("_key", cursor) == extract("_key", docs)
    assert cursor.close(ignore_missing=True) is None

    # Test get tracking properties with bad database
    with assert_raises(SQLQueryTrackingGetError) as err:
        bad_db.sql.tracking()
    assert err.value.error_code in {11, 1228}

    # Test get tracking properties
    tracking = db.sql.tracking()
    assert isinstance(tracking["enabled"], bool)
    assert isinstance(tracking["max_query_string_length"], int)
    assert isinstance(tracking["max_slow_queries"], int)
    assert isinstance(tracking["slow_query_threshold"], int)
    assert isinstance(tracking["track_bind_vars"], bool)
    assert isinstance(tracking["track_slow_queries"], bool)

    # Test set tracking properties with bad database
    with assert_raises(SQLQueryTrackingSetError) as err:
        bad_db.sql.set_tracking(enabled=not tracking["enabled"])
    assert err.value.error_code in {11, 1228}
    assert db.sql.tracking()["enabled"] == tracking["enabled"]

    # Test set tracking properties
    new_tracking = db.sql.set_tracking(
        enabled=not tracking["enabled"],
        max_query_string_length=4000,
        max_slow_queries=60,
        slow_query_threshold=15,
        track_bind_vars=not tracking["track_bind_vars"],
        track_slow_queries=not tracking["track_slow_queries"],
    )
    assert new_tracking["enabled"] != tracking["enabled"]
    assert new_tracking["max_query_string_length"] == 4000
    assert new_tracking["max_slow_queries"] == 60
    assert new_tracking["slow_query_threshold"] == 15
    assert new_tracking["track_bind_vars"] != tracking["track_bind_vars"]
    assert new_tracking["track_slow_queries"] != tracking["track_slow_queries"]

    # Make sure to revert the properties
    new_tracking = db.sql.set_tracking(
        enabled=True, track_bind_vars=True, track_slow_queries=True
    )
    assert new_tracking["enabled"] is True
    assert new_tracking["track_bind_vars"] is True
    assert new_tracking["track_slow_queries"] is True

    # Kick off some long lasting queries in the background
    db.begin_async_execution().sql.execute("RETURN SLEEP(100)")
    db.begin_async_execution().sql.execute("RETURN SLEEP(50)")

    # Test list queries
    queries = db.sql.queries()
    for query in queries:
        assert "id" in query
        assert "query" in query
        assert "started" in query
        assert "state" in query
        assert "bind_vars" in query
        assert "runtime" in query
    assert len(queries) == 2

    # Test list queries with bad database
    with assert_raises(SQLQueryListError) as err:
        bad_db.sql.queries()
    assert err.value.error_code in {11, 1228}

    # Test kill queries
    query_id_1, query_id_2 = extract("id", queries)
    assert db.sql.kill(query_id_1) is True

    while len(queries) > 1:
        queries = db.sql.queries()
    assert query_id_1 not in extract("id", queries)

    assert db.sql.kill(query_id_2) is True
    while len(queries) > 0:
        queries = db.sql.queries()
    assert query_id_2 not in extract("id", queries)

    # Test kill missing queries
    with assert_raises(SQLQueryKillError) as err:
        db.sql.kill(query_id_1)
    assert err.value.error_code == 1591
    with assert_raises(SQLQueryKillError) as err:
        db.sql.kill(query_id_2)
    assert err.value.error_code == 1591

    # Test list slow queries
    assert db.sql.slow_queries() == []

    # Test list slow queries with bad database
    with assert_raises(SQLQueryListError) as err:
        bad_db.sql.slow_queries()
    assert err.value.error_code in {11, 1228}

    # Test clear slow queries
    assert db.sql.clear_slow_queries() is True

    # Test clear slow queries with bad database
    with assert_raises(SQLQueryClearError) as err:
        bad_db.sql.clear_slow_queries()
    assert err.value.error_code in {11, 1228}


def test_sql_function_management(db, bad_db):
    fn_group = "functions::temperature"
    fn_name_1 = "functions::temperature::celsius_to_fahrenheit"
    fn_body_1 = "function (celsius) { return celsius * 1.8 + 32; }"
    fn_name_2 = "functions::temperature::fahrenheit_to_celsius"
    fn_body_2 = "function (fahrenheit) { return (fahrenheit - 32) / 1.8; }"
    bad_fn_name = "functions::temperature::should_not_exist"
    bad_fn_body = "function (celsius) { invalid syntax }"

    # Test list SQL functions
    assert db.sql.functions() == []

    # Test list SQL functions with bad database
    with assert_raises(SQLFunctionListError) as err:
        bad_db.sql.functions()
    assert err.value.error_code in {11, 1228}

    # Test create invalid SQL function
    with assert_raises(SQLFunctionCreateError) as err:
        db.sql.create_function(bad_fn_name, bad_fn_body)
    assert err.value.error_code == 1581

    # Test create SQL function one
    assert db.sql.create_function(fn_name_1, fn_body_1) == {"is_new": True}
    functions = db.sql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert "is_deterministic" in functions[0]

    # Test create SQL function one again (idempotency)
    assert db.sql.create_function(fn_name_1, fn_body_1) == {"is_new": False}
    functions = db.sql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert "is_deterministic" in functions[0]

    # Test create SQL function two
    assert db.sql.create_function(fn_name_2, fn_body_2) == {"is_new": True}
    functions = sorted(db.sql.functions(), key=lambda x: x["name"])
    assert len(functions) == 2
    assert functions[0]["name"] == fn_name_1
    assert functions[0]["code"] == fn_body_1
    assert functions[1]["name"] == fn_name_2
    assert functions[1]["code"] == fn_body_2
    assert "is_deterministic" in functions[0]
    assert "is_deterministic" in functions[1]

    # Test delete SQL function one
    assert db.sql.delete_function(fn_name_1) == {"deleted": 1}
    functions = db.sql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_2
    assert functions[0]["code"] == fn_body_2

    # Test delete missing SQL function
    with assert_raises(SQLFunctionDeleteError) as err:
        db.sql.delete_function(fn_name_1)
    assert err.value.error_code == 1582
    assert db.sql.delete_function(fn_name_1, ignore_missing=True) is False
    functions = db.sql.functions()
    assert len(functions) == 1
    assert functions[0]["name"] == fn_name_2
    assert functions[0]["code"] == fn_body_2

    # Test delete SQL function group
    assert db.sql.delete_function(fn_group, group=True) == {"deleted": 1}
    assert db.sql.functions() == []


def test_sql_cache_management(db, bad_db):
    # Test get SQL cache properties
    properties = db.sql.cache.properties()
    assert "mode" in properties
    assert "max_results" in properties
    assert "max_results_size" in properties
    assert "max_entry_size" in properties
    assert "include_system" in properties

    # Test get SQL cache properties with bad database
    with assert_raises(SQLCachePropertiesError):
        bad_db.sql.cache.properties()

    # Test get SQL cache configure properties
    properties = db.sql.cache.configure(
        mode="on",
        max_results=100,
        max_results_size=10000,
        max_entry_size=10000,
        include_system=True,
    )
    assert properties["mode"] == "on"
    assert properties["max_results"] == 100
    assert properties["max_results_size"] == 10000
    assert properties["max_entry_size"] == 10000
    assert properties["include_system"] is True

    properties = db.sql.cache.properties()
    assert properties["mode"] == "on"
    assert properties["max_results"] == 100
    assert properties["max_results_size"] == 10000
    assert properties["max_entry_size"] == 10000
    assert properties["include_system"] is True

    # Test get SQL cache configure properties with bad database
    with assert_raises(SQLCacheConfigureError):
        bad_db.sql.cache.configure(mode="on")

    # Test get SQL cache entries
    result = db.sql.cache.entries()
    assert isinstance(result, list)

    # Test get SQL cache entries with bad database
    with assert_raises(SQLCacheEntriesError) as err:
        bad_db.sql.cache.entries()
    assert err.value.error_code in {11, 1228}

    # Test get SQL cache clear
    result = db.sql.cache.clear()
    assert isinstance(result, bool)

    # Test get SQL cache clear with bad database
    with assert_raises(SQLCacheClearError) as err:
        bad_db.sql.cache.clear()
    assert err.value.error_code in {11, 1228}
