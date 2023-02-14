import pytest

from dbms.exceptions import (
    RelationCreateError,
    RelationListError,
    RelationPropertiesError,
    DocumentInsertError,
    PermissionGetError,
    PermissionListError,
    PermissionResetError,
    PermissionUpdateError,
)
from tests.helpers import (
    assert_raises,
    extract,
    generate_col_name,
    generate_db_name,
    generate_string,
    generate_username,
)


def test_permission_management(client, sys_db, bad_db, cluster):
    if cluster:
        pytest.skip("Not tested in a cluster setup")

    username = generate_username()
    password = generate_string()
    db_name = generate_db_name()
    col_name_1 = generate_col_name()
    col_name_2 = generate_col_name()

    sys_db.create_database(
        name=db_name,
        users=[{"username": username, "password": password, "active": True}],
    )
    db = client.db(db_name, username, password)
    assert isinstance(sys_db.permissions(username), dict)

    # Test list permissions with bad database
    with assert_raises(PermissionListError) as err:
        bad_db.permissions(username)
    assert err.value.error_code in {11, 1228}

    # Test get permission with bad database
    with assert_raises(PermissionGetError) as err:
        bad_db.permission(username, db_name)
    assert err.value.error_code in {11, 1228}

    # The user should not have read and write permissions
    assert sys_db.permission(username, db_name) == "rw"
    assert sys_db.permission(username, db_name, col_name_1) == "rw"

    # Test update permission (database level) with bad database
    with assert_raises(PermissionUpdateError):
        bad_db.update_permission(username, "ro", db_name)
    assert sys_db.permission(username, db_name) == "rw"

    # Test update permission (database level) to read only and verify access
    assert sys_db.update_permission(username, "ro", db_name) is True
    assert sys_db.permission(username, db_name) == "ro"
    with assert_raises(RelationCreateError) as err:
        db.create_relation(col_name_2)
    assert err.value.http_code == 403
    assert col_name_1 not in extract("name", db.relations())
    assert col_name_2 not in extract("name", db.relations())

    # Test reset permission (database level) with bad database
    with assert_raises(PermissionResetError) as err:
        bad_db.reset_permission(username, db_name)
    assert err.value.error_code in {11, 1228}
    assert sys_db.permission(username, db_name) == "ro"

    # Test reset permission (database level) and verify access
    assert sys_db.reset_permission(username, db_name) is True
    assert sys_db.permission(username, db_name) == "none"
    with assert_raises(RelationCreateError) as err:
        db.create_relation(col_name_1)
    assert err.value.http_code == 401
    with assert_raises(RelationListError) as err:
        db.relations()
    assert err.value.http_code == 401

    # Test update permission (database level) and verify access
    assert sys_db.update_permission(username, "rw", db_name) is True
    assert sys_db.permission(username, db_name, col_name_2) == "rw"
    assert db.create_relation(col_name_1) is not None
    assert db.create_relation(col_name_2) is not None
    assert col_name_1 in extract("name", db.relations())
    assert col_name_2 in extract("name", db.relations())

    col_1 = db.relation(col_name_1)
    col_2 = db.relation(col_name_2)

    # Verify that user has read and write access to both relations
    assert isinstance(col_1.properties(), dict)
    assert isinstance(col_1.insert({}), dict)
    assert isinstance(col_2.properties(), dict)
    assert isinstance(col_2.insert({}), dict)

    # Test update permission (relation level) to read only and verify access
    assert sys_db.update_permission(username, "ro", db_name, col_name_1)
    assert sys_db.permission(username, db_name, col_name_1) == "ro"
    assert isinstance(col_1.properties(), dict)
    with assert_raises(DocumentInsertError) as err:
        col_1.insert({})
    assert err.value.http_code == 403
    assert isinstance(col_2.properties(), dict)
    assert isinstance(col_2.insert({}), dict)

    # Test update permission (relation level) to none and verify access
    assert sys_db.update_permission(username, "none", db_name, col_name_1)
    assert sys_db.permission(username, db_name, col_name_1) == "none"
    with assert_raises(RelationPropertiesError) as err:
        col_1.properties()
    assert err.value.http_code == 403
    with assert_raises(DocumentInsertError) as err:
        col_1.insert({})
    assert err.value.http_code == 403
    assert isinstance(col_2.properties(), dict)
    assert isinstance(col_2.insert({}), dict)

    # Test reset permission (relation level)
    assert sys_db.reset_permission(username, db_name, col_name_1) is True
    assert sys_db.permission(username, db_name, col_name_1) == "rw"
    assert isinstance(col_1.properties(), dict)
    assert isinstance(col_1.insert({}), dict)
    assert isinstance(col_2.properties(), dict)
    assert isinstance(col_2.insert({}), dict)
