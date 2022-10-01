from dbms.exceptions import (
    ViewCreateError,
    ViewDeleteError,
    ViewGetError,
    ViewListError,
    ViewRenameError,
    ViewReplaceError,
    ViewUpdateError,
)
from tests.helpers import assert_raises, generate_view_name


def test_view_management(db, bad_db, col, cluster):
    # Test create view
    view_name = generate_view_name()
    bad_view_name = generate_view_name()
    view_type = "dbmssearch"

    result = db.create_view(
        view_name,
        view_type,
        {"consolidationIntervalMsec": 50000, "links": {col.name: {}}},
    )
    assert "id" in result
    assert result["name"] == view_name
    assert result["type"] == view_type
    assert result["consolidation_interval_msec"] == 50000
    assert col.name in result["links"]

    view_id = result["id"]

    # Test create duplicate view
    with assert_raises(ViewCreateError) as err:
        db.create_view(view_name, view_type, {"consolidationIntervalMsec": 50000})
    assert err.value.error_code == 1207

    # Test list views
    result = db.views()
    assert len(result) == 1
    view = result[0]
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type

    # Test list views with bad database
    with assert_raises(ViewListError) as err:
        bad_db.views()
    assert err.value.error_code in {11, 1228}

    # Test get view
    view = db.view(view_name)
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidation_interval_msec"] == 50000

    # Test get missing view
    with assert_raises(ViewGetError) as err:
        db.view(bad_view_name)
    assert err.value.error_code == 1203

    # Test update view
    view = db.update_view(view_name, {"consolidationIntervalMsec": 70000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidation_interval_msec"] == 70000

    # Test update view with bad database
    with assert_raises(ViewUpdateError) as err:
        bad_db.update_view(view_name, {"consolidationIntervalMsec": 80000})
    assert err.value.error_code in {11, 1228}

    # Test replace view
    view = db.replace_view(view_name, {"consolidationIntervalMsec": 40000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == view_type
    assert view["consolidation_interval_msec"] == 40000

    # Test replace view with bad database
    with assert_raises(ViewReplaceError) as err:
        bad_db.replace_view(view_name, {"consolidationIntervalMsec": 7000})
    assert err.value.error_code in {11, 1228}

    if cluster:
        new_view_name = view_name
    else:
        # Test rename view
        new_view_name = generate_view_name()
        assert db.rename_view(view_name, new_view_name) is True
        result = db.views()
        assert len(result) == 1
        view = result[0]
        assert view["id"] == view_id
        assert view["name"] == new_view_name

        # Test rename missing view
        with assert_raises(ViewRenameError) as err:
            db.rename_view(bad_view_name, view_name)
        assert err.value.error_code == 1203

    # Test delete view
    assert db.delete_view(new_view_name) is True
    assert len(db.views()) == 0

    # Test delete missing view
    with assert_raises(ViewDeleteError) as err:
        db.delete_view(new_view_name)
    assert err.value.error_code == 1203

    # Test delete missing view with ignore_missing set to True
    assert db.delete_view(view_name, ignore_missing=True) is False


def test_dbmssearch_view_management(db, bad_db, cluster):
    # Test create dbmssearch view
    view_name = generate_view_name()
    result = db.create_dbmssearch_view(
        view_name, {"consolidationIntervalMsec": 50000}
    )
    assert "id" in result
    assert result["name"] == view_name
    assert result["type"].lower() == "dbmssearch"
    assert result["consolidation_interval_msec"] == 50000
    view_id = result["id"]

    # Test create duplicate dbmssearch view
    with assert_raises(ViewCreateError) as err:
        db.create_dbmssearch_view(view_name, {"consolidationIntervalMsec": 50000})
    assert err.value.error_code == 1207

    result = db.views()
    if not cluster:
        assert len(result) == 1
        view = result[0]
        assert view["id"] == view_id
        assert view["name"] == view_name
        assert view["type"] == "dbmssearch"

    # Test update dbmssearch view
    view = db.update_dbmssearch_view(view_name, {"consolidationIntervalMsec": 70000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"].lower() == "dbmssearch"
    assert view["consolidation_interval_msec"] == 70000

    # Test update dbmssearch view with bad database
    with assert_raises(ViewUpdateError) as err:
        bad_db.update_dbmssearch_view(view_name, {"consolidationIntervalMsec": 70000})
    assert err.value.error_code in {11, 1228}

    # Test replace dbmssearch view
    view = db.replace_dbmssearch_view(view_name, {"consolidationIntervalMsec": 40000})
    assert view["id"] == view_id
    assert view["name"] == view_name
    assert view["type"] == "dbmssearch"
    assert view["consolidation_interval_msec"] == 40000

    # Test replace dbmssearch with bad database
    with assert_raises(ViewReplaceError) as err:
        bad_db.replace_dbmssearch_view(
            view_name, {"consolidationIntervalMsec": 70000}
        )
    assert err.value.error_code in {11, 1228}

    # Test delete dbmssearch view
    assert db.delete_view(view_name, ignore_missing=False) is True
