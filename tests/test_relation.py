from dbms.relation import StandardRelation
from dbms.exceptions import (
    RelationChecksumError,
    RelationConfigureError,
    RelationCreateError,
    RelationDeleteError,
    RelationListError,
    RelationLoadError,
    RelationPropertiesError,
    RelationRecalculateCountError,
    RelationRenameError,
    RelationRevisionError,
    RelationStatisticsError,
    RelationTruncateError,
    RelationUnloadError,
)
from tests.helpers import assert_raises, extract, generate_col_name


def test_relation_attributes(db, col, username):
    assert col.context in ["default", "async", "batch", "transaction"]
    assert col.username == username
    assert col.db_name == db.name
    assert col.name.startswith("test_relation")
    assert repr(col) == f"<StandardRelation {col.name}>"


def test_relation_misc_methods(col, bad_col, cluster):
    # Test get properties
    properties = col.properties()
    assert properties["name"] == col.name
    assert properties["system"] is False

    # Test get properties with bad relation
    with assert_raises(RelationPropertiesError) as err:
        bad_col.properties()
    assert err.value.error_code in {11, 1228}

    # Test configure properties
    prev_sync = properties["sync"]
    properties = col.configure(sync=not prev_sync, schema={})
    assert properties["name"] == col.name
    assert properties["system"] is False
    assert properties["sync"] is not prev_sync

    # Test configure properties with bad relation
    with assert_raises(RelationConfigureError) as err:
        bad_col.configure(sync=True)
    assert err.value.error_code in {11, 1228}

    # Test get statistics
    stats = col.statistics()
    assert isinstance(stats, dict)
    assert "indexes" in stats

    # Test get statistics with bad relation
    with assert_raises(RelationStatisticsError) as err:
        bad_col.statistics()
    assert err.value.error_code in {11, 1228}

    # Test get revision
    assert isinstance(col.revision(), str)

    # Test get revision with bad relation
    with assert_raises(RelationRevisionError) as err:
        bad_col.revision()
    assert err.value.error_code in {11, 1228}

    # Test load into memory
    assert col.load() is True

    # Test load with bad relation
    with assert_raises(RelationLoadError) as err:
        bad_col.load()
    assert err.value.error_code in {11, 1228}

    # Test unload from memory
    assert col.unload() is True

    # Test unload with bad relation
    with assert_raises(RelationUnloadError) as err:
        bad_col.unload()
    assert err.value.error_code in {11, 1228}

    if cluster:
        col.insert({})
    else:
        # Test checksum with empty relation
        assert int(col.checksum(with_rev=True, with_data=False)) == 0
        assert int(col.checksum(with_rev=True, with_data=True)) == 0
        assert int(col.checksum(with_rev=False, with_data=False)) == 0
        assert int(col.checksum(with_rev=False, with_data=True)) == 0

        # Test checksum with non-empty relation
        col.insert({})
        assert int(col.checksum(with_rev=True, with_data=False)) > 0
        assert int(col.checksum(with_rev=True, with_data=True)) > 0
        assert int(col.checksum(with_rev=False, with_data=False)) > 0
        assert int(col.checksum(with_rev=False, with_data=True)) > 0

        # Test checksum with bad relation
        with assert_raises(RelationChecksumError) as err:
            bad_col.checksum()
        assert err.value.error_code in {11, 1228}

    # Test preconditions
    assert len(col) == 1

    # Test truncate relation
    assert col.truncate() is True
    assert len(col) == 0

    # Test truncate with bad relation
    with assert_raises(RelationTruncateError) as err:
        bad_col.truncate()
    assert err.value.error_code in {11, 1228}

    # Test recalculate count
    assert col.recalculate_count() is True

    # Test recalculate count with bad relation
    with assert_raises(RelationRecalculateCountError) as err:
        bad_col.recalculate_count()
    assert err.value.error_code in {11, 1228}


def test_relation_management(db, bad_db, cluster):
    # Test create relation
    col_name = generate_col_name()
    assert db.has_relation(col_name) is False

    schema = {
        "rule": {
            "type": "object",
            "properties": {
                "test_attr": {"type": "string"},
            },
            "required": ["test_attr"],
        },
        "level": "moderate",
        "message": "Schema Validation Failed.",
        "type": "json",
    }

    col = db.create_relation(
        name=col_name,
        sync=True,
        system=False,
        key_generator="traditional",
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=["test_attr"],
        replication_factor=1,
        shard_like="",
        sync_replication=False,
        enforce_replication_factor=False,
        sharding_strategy="community-compat",
        smart_join_attribute="test",
        write_concern=1,
        schema=schema,
    )
    assert db.has_relation(col_name) is True

    properties = col.properties()
    assert "key_options" in properties
    assert properties["schema"] == schema
    assert properties["name"] == col_name
    assert properties["sync"] is True
    assert properties["system"] is False

    # Test create duplicate relation
    with assert_raises(RelationCreateError) as err:
        db.create_relation(col_name)
    assert err.value.error_code == 1207

    # Test list relations
    assert all(
        entry["name"].startswith("test_relation") or entry["name"].startswith("_")
        for entry in db.relations()
    )

    # Test list relations with bad database
    with assert_raises(RelationListError) as err:
        bad_db.relations()
    assert err.value.error_code in {11, 1228}

    # Test has relation with bad database
    with assert_raises(RelationListError) as err:
        bad_db.has_relation(col_name)
    assert err.value.error_code in {11, 1228}

    # Test get relation object
    test_col = db.relation(col.name)
    assert isinstance(test_col, StandardRelation)
    assert test_col.name == col.name

    test_col = db[col.name]
    assert isinstance(test_col, StandardRelation)
    assert test_col.name == col.name

    # Test delete relation
    assert db.delete_relation(col_name, system=False) is True
    assert col_name not in extract("name", db.relations())

    # Test drop missing relation
    with assert_raises(RelationDeleteError) as err:
        db.delete_relation(col_name)
    assert err.value.error_code == 1203
    assert db.delete_relation(col_name, ignore_missing=True) is False

    if not cluster:
        # Test rename relation
        new_name = generate_col_name()
        col = db.create_relation(new_name)
        assert col.rename(new_name) is True
        assert col.name == new_name
        assert repr(col) == f"<StandardRelation {new_name}>"

        # Try again (the operation should be idempotent)
        assert col.rename(new_name) is True
        assert col.name == new_name
        assert repr(col) == f"<StandardRelation {new_name}>"

        # Test rename with bad relation
        with assert_raises(RelationRenameError) as err:
            bad_db.relation(new_name).rename(new_name)
        assert err.value.error_code in {11, 1228}
