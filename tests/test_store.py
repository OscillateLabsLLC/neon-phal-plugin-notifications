from datetime import datetime, timedelta, timezone

from neon_data_models.enum import NotificationScope, NotificationState

from neon_phal_plugin_notifications.store import (InMemoryNotificationStore,
                                                  JsonNotificationStore,
                                                  StoredNotification)
from tests.conftest import NODE, OTHER_NODE, OTHER_SKILL, SKILL, USER, \
    make_notification

NOW = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


def stored(state=NotificationState.ACTIVE, created=NOW, **overrides):
    return StoredNotification(make_notification(**overrides), state=state,
                              created_at=created, updated_at=created)


def test_round_trip_dict():
    record = stored(state=NotificationState.SNOOZED)
    record.renotify_at = NOW + timedelta(minutes=5)
    record.dismissed_by_clients = {"a", "b"}
    loaded = StoredNotification.from_dict(record.to_dict())
    assert loaded.notification == record.notification
    assert loaded.state == NotificationState.SNOOZED
    assert loaded.renotify_at == record.renotify_at
    assert loaded.dismissed_by_clients == {"a", "b"}
    assert loaded.created_at == NOW and loaded.updated_at == NOW


def test_addressed_to():
    client = stored()
    user = stored(scope=NotificationScope.USER, target=USER)
    global_ = stored(scope=NotificationScope.GLOBAL, target=None)
    assert client.addressed_to(None, None)
    assert client.addressed_to(NODE, None) and not client.addressed_to(OTHER_NODE, None)
    assert not client.addressed_to(None, USER)
    assert user.addressed_to(None, USER) and not user.addressed_to(NODE, None)
    assert global_.addressed_to(OTHER_NODE, None) and global_.addressed_to(None, "x")


def test_state_for_client():
    record = stored()
    record.dismissed_by_clients.add(NODE)
    assert record.state_for_client(NODE) == NotificationState.DISMISSED
    assert record.state_for_client(OTHER_NODE) == NotificationState.ACTIVE
    assert record.state_for_client(None) == NotificationState.ACTIVE


def test_list_filters():
    store = InMemoryNotificationStore()
    a = stored(created=NOW)
    b = stored(skill_id=OTHER_SKILL, created=NOW + timedelta(seconds=1),
               scope=NotificationScope.GLOBAL, target=None)
    c = stored(state=NotificationState.DISMISSED, created=NOW + timedelta(seconds=2),
               scope=NotificationScope.USER, target=USER)
    for record in (a, b, c):
        store.upsert(record)
    ids = lambda records: [r.notification_id for r in records]
    assert ids(store.list()) == [a.notification_id, b.notification_id, c.notification_id]
    assert ids(store.list(skill_id=SKILL)) == [a.notification_id, c.notification_id]
    assert ids(store.list(state=NotificationState.DISMISSED)) == [c.notification_id]
    assert ids(store.list(since=NOW)) == [b.notification_id, c.notification_id]
    assert ids(store.list(node_id=NODE)) == [a.notification_id, b.notification_id]
    assert ids(store.list(node_id=OTHER_NODE)) == [b.notification_id]
    assert ids(store.list(user_id=USER)) == [b.notification_id, c.notification_id]
    assert ids(store.list(node_id=NODE, user_id=USER)) == ids(store.list())


def test_set_state_shared_and_per_client():
    store = InMemoryNotificationStore()
    record = stored()
    store.upsert(record)
    store.set_state(record.notification_id, NotificationState.DISMISSED,
                    client_id=NODE)
    assert store.get(record.notification_id).state == NotificationState.ACTIVE
    assert store.get(record.notification_id).dismissed_by_clients == {NODE}
    store.set_state(record.notification_id, NotificationState.DISMISSED)
    assert store.get(record.notification_id).state == NotificationState.DISMISSED


def test_prune_tombstone_window_never_touches_live():
    store = InMemoryNotificationStore(clock=lambda: NOW)
    old = stored(state=NotificationState.EXPIRED, created=NOW - timedelta(days=8))
    live_old = stored(created=NOW - timedelta(days=40))
    recent = stored(state=NotificationState.DISMISSED,
                    created=NOW - timedelta(days=6))
    for record in (old, live_old, recent):
        store.upsert(record)
    assert store.prune(max_age=timedelta(days=7)) == 1
    remaining = sorted(r.notification_id for r in store.list())
    assert remaining == sorted([live_old.notification_id, recent.notification_id])


def test_since_filters_on_updated_at():
    store = InMemoryNotificationStore(clock=lambda: NOW + timedelta(hours=1))
    record = stored(created=NOW - timedelta(days=1))
    store.upsert(record)
    assert store.list(since=NOW) == []
    store.set_state(record.notification_id, NotificationState.DISMISSED)
    assert [r.notification_id for r in store.list(since=NOW)] == [record.notification_id]
    assert record.notification.updated_at == NOW + timedelta(hours=1)


def test_json_store_round_trip(tmp_path):
    path = str(tmp_path / "n.json")
    store = JsonNotificationStore(path=path, disable_lock=True)
    record = stored(state=NotificationState.SNOOZED)
    record.renotify_at = NOW + timedelta(minutes=1)
    store.upsert(record)
    store.set_state(record.notification_id, NotificationState.SNOOZED,
                    client_id=OTHER_NODE)

    reloaded = JsonNotificationStore(path=path, disable_lock=True)
    loaded = reloaded.get(record.notification_id)
    assert loaded is not None
    assert loaded.notification == record.notification
    assert loaded.state == NotificationState.SNOOZED
    assert loaded.renotify_at == record.renotify_at
    assert loaded.dismissed_by_clients == {OTHER_NODE}


def test_json_store_prune_persists(tmp_path):
    path = str(tmp_path / "n.json")
    store = JsonNotificationStore(path=path, disable_lock=True)
    store.upsert(stored(state=NotificationState.DISMISSED,
                        created=NOW - timedelta(days=60)))
    assert store.prune(timedelta(days=7)) == 1
    assert JsonNotificationStore(path=path, disable_lock=True).list() == []


def test_json_store_default_path_under_xdg(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    path = JsonNotificationStore.default_path("neon-phal-plugin-notifications")
    assert path.startswith(str(tmp_path))
    assert path.endswith("neon/neon-phal-plugin-notifications/notifications.json")
