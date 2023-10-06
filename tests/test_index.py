import pytest
from dbms.exceptions import (
    IndexCreateError,
    IndexDeleteError,
    IndexListError,
    IndexLoadError,
)
from tests.helpers import assert_raises, extract


def test_list_indexes(icol, bad_col):
    indexes = icol.indexes()
    assert isinstance(indexes, list)
    assert len(indexes) > 0
    assert "id" in indexes[0]
    assert "type" in indexes[0]
    assert "fields" in indexes[0]
    assert "selectivity" in indexes[0]
    assert "sparse" in indexes[0]
    assert "unique" in indexes[0]

    with assert_raises(IndexListError) as err:
        bad_col.indexes()
    assert err.value.error_code in {11, 1228}


def test_add_hash_index(icol):
    icol = icol

    fields = ["attr1", "attr2"]
    result = icol.add_hash_index(
        fields=fields,
        unique=True,
        sparse=True,
        deduplicate=True,
        name="hash_index",
        in_background=False,
    )

    expected_index = {
        "sparse": True,
        "type": "hash",
        "fields": ["attr1", "attr2"],
        "unique": True,
        "deduplicate": True,
        "name": "hash_index",
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result["id"] in extract("id", icol.indexes())

    # Clean up the index
    icol.delete_index(result["id"])


def test_add_skiplist_index(icol):
    fields = ["attr1", "attr2"]
    result = icol.add_skiplist_index(
        fields=fields,
        unique=True,
        sparse=True,
        deduplicate=True,
        name="skiplist_index",
        in_background=False,
    )

    expected_index = {
        "sparse": True,
        "type": "skiplist",
        "fields": ["attr1", "attr2"],
        "unique": True,
        "deduplicate": True,
        "name": "skiplist_index",
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result["id"] in extract("id", icol.indexes())

    # Clean up the index
    icol.delete_index(result["id"])


@pytest.mark.skip(reason="FIXME: Is not adapted for DBMS")
def test_add_persistent_index(icol):
    # Test add persistent index with two attributes
    result = icol.add_persistent_index(
        fields=["attr1", "attr2"],
        unique=True,
        sparse=True,
        name="persistent_index",
        in_background=True,
    )
    expected_index = {
        "sparse": True,
        "type": "persistent",
        "fields": ["attr1", "attr2"],
        "unique": True,
        "name": "persistent_index",
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result["id"] in extract("id", icol.indexes())

    # Clean up the index
    icol.delete_index(result["id"])


def test_add_ttl_index(icol):
    # Test add persistent index with two attributes
    result = icol.add_ttl_index(
        fields=["attr1"], expiry_time=1000, name="ttl_index", in_background=True
    )
    expected_index = {
        "type": "ttl",
        "fields": ["attr1"],
        "expiry_time": 1000,
        "name": "ttl_index",
    }
    for key, value in expected_index.items():
        assert result[key] == value

    assert result["id"] in extract("id", icol.indexes())

    # Clean up the index
    icol.delete_index(result["id"])


def test_delete_index(icol, bad_col):
    old_indexes = set(extract("id", icol.indexes()))
    icol.add_hash_index(["attr3", "attr4"], unique=True)
    icol.add_skiplist_index(["attr3", "attr4"], unique=True)

    new_indexes = set(extract("id", icol.indexes()))
    assert new_indexes.issuperset(old_indexes)

    indexes_to_delete = new_indexes - old_indexes
    for index_id in indexes_to_delete:
        assert icol.delete_index(index_id) is True

    new_indexes = set(extract("id", icol.indexes()))
    assert new_indexes == old_indexes

    # Test delete missing indexes
    for index_id in indexes_to_delete:
        assert icol.delete_index(index_id, ignore_missing=True) is False
    for index_id in indexes_to_delete:
        with assert_raises(IndexDeleteError) as err:
            icol.delete_index(index_id, ignore_missing=False)
        assert err.value.error_code == 1212

    # Test delete indexes with bad relation
    for index_id in indexes_to_delete:
        with assert_raises(IndexDeleteError) as err:
            bad_col.delete_index(index_id, ignore_missing=False)
        assert err.value.error_code in {11, 1228}


def test_load_indexes(icol, bad_col):
    # Test load indexes
    assert icol.load_indexes() is True

    # Test load indexes with bad relation
    with assert_raises(IndexLoadError) as err:
        bad_col.load_indexes()
    assert err.value.error_code in {11, 1228}
