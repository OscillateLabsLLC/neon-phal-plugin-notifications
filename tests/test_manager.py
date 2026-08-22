from datetime import timedelta

from neon_data_models.enum import (DismissPolicy, NotificationScope,
                                   NotificationState)
from neon_data_models.models.api.messagebus.notifications import (
    NotificationInteractionData, NotificationListData, NotificationRemoveData,
    NotificationSnoozeData, NotificationSyncRequestData)

from neon_phal_plugin_notifications.identity import FORWARDED_MARKER
from neon_phal_plugin_notifications.manager import (MSG_DISMISS,
                                                    MSG_INTERACTION,
                                                    MSG_NOTIFY, MSG_SNOOZED)
from tests.conftest import (ALERTS, NODE, OTHER_NODE, OTHER_SKILL, SKILL,
                            USER, make_action, make_notification,
                            node_context)


def remove(manager, skill_id=SKILL, notification_id=None, context=None,
           dismissed_by=None):
    return manager.remove(NotificationRemoveData(
        skill_id=skill_id, notification_id=notification_id,
        dismissed_by=dismissed_by), context or {})


def consumer_remove(manager, notification_id, context=None, **kwargs):
    """A consumer-shaped request: no skill_id, identity from context/data."""
    return remove(manager, skill_id=None, notification_id=notification_id,
                  context=context, **kwargs)


# ---- set ------------------------------------------------------------------
def test_set_accepts_and_notifies(manager, emitted, store, clock):
    response = manager.set(make_notification(notification_id="n1"), {})
    assert response.status == "ok" and response.notification_id == "n1"
    record = store.get("n1")
    assert record.state == NotificationState.ACTIVE
    assert record.notification.created_at == clock.now
    assert record.notification.updated_at == clock.now
    assert notify_has_updated_at(emitted)


def notify_has_updated_at(emitted) -> bool:
    return emitted.of(MSG_NOTIFY)[-1]["notification"]["updated_at"] is not None
    notify = emitted.of(MSG_NOTIFY)
    assert len(notify) == 1 and notify[0]["notification"]["notification_id"] == "n1"


def test_set_mints_id_when_absent(manager):
    response = manager.set(make_notification(), {})
    assert response.status == "ok" and len(response.notification_id) > 0


def test_set_upsert_replaces_and_reemits(manager, emitted, store, scheduler,
                                         clock):
    manager.set(make_notification(notification_id="n1", text="v1"), {})
    manager.snooze(NotificationSnoozeData(notification_id="n1", duration=60), {})
    emitted.clear()
    created = store.get("n1").created_at
    clock.advance(minutes=5)
    response = manager.set(make_notification(notification_id="n1", text="v2"), {})
    assert response.status == "ok"
    record = store.get("n1")
    assert record.notification.text == "v2"
    assert record.created_at == created
    assert record.notification.created_at == created
    assert record.updated_at == clock.now
    assert record.notification.updated_at == clock.now
    assert record.state == NotificationState.ACTIVE
    assert not scheduler.is_armed("n1")
    assert emitted.of(MSG_NOTIFY)[0]["notification"]["text"] == "v2"


def test_set_refuses_cross_skill_id(manager, emitted):
    manager.set(make_notification(notification_id="n1"), {})
    emitted.clear()
    response = manager.set(
        make_notification(skill_id=OTHER_SKILL, notification_id="n1"), {})
    assert response.status == "refused"
    assert SKILL in response.reason
    assert emitted.of(MSG_NOTIFY) == []


def test_set_refuses_policy_with_reason(manager, store):
    response = manager.set(make_notification(
        notification_id="g", scope=NotificationScope.GLOBAL, target=None), {})
    assert response.status == "refused" and "allow_global" in response.reason
    assert store.get("g") is None
    ok = manager.set(make_notification(
        skill_id=ALERTS, scope=NotificationScope.GLOBAL, target=None), {})
    assert ok.status == "ok"


def test_set_rate_limit_refuses(store, emitted, scheduler, clock):
    from neon_phal_plugin_notifications.manager import NotificationManager
    from neon_phal_plugin_notifications.policy import EmissionPolicy
    manager = NotificationManager(
        store, emitted, scheduler=scheduler, clock=clock,
        emission_policy=EmissionPolicy(rate_limit=1, clock=clock))
    assert manager.set(make_notification(), {}).status == "ok"
    refused = manager.set(make_notification(), {})
    assert refused.status == "refused" and "rate limit" in refused.reason


def test_set_resolves_client_target_from_context(manager, store):
    manager.set(make_notification(notification_id="n1", target=None),
                node_context(OTHER_NODE))
    assert store.get("n1").notification.target == OTHER_NODE
    manager.set(make_notification(notification_id="n2", target=None),
                {"session": {"session_id": "default"}})
    assert store.get("n2").notification.target is None
    manager.set(make_notification(notification_id="n3", target=None),
                {"session": {"session_id": "sess-9"}})
    assert store.get("n3").notification.target == "sess-9"


# ---- remove ----------------------------------------------------------------
def test_remove_by_producer_dismisses(manager, emitted, store):
    manager.set(make_notification(notification_id="n1"), {})
    emitted.clear()
    response = remove(manager, notification_id="n1")
    assert response.status == "ok" and response.notification_ids == ["n1"]
    assert store.get("n1").state == NotificationState.DISMISSED
    dismiss = emitted.of(MSG_DISMISS)[0]
    assert dismiss["notification_id"] == "n1"
    assert dismiss["skill_id"] == SKILL
    assert dismiss["scope"] == NotificationScope.CLIENT.value
    assert dismiss["target"] == NODE
    assert dismiss["dismissed_by"] == SKILL


def test_remove_all_for_skill(manager, store):
    manager.set(make_notification(notification_id="a"), {})
    manager.set(make_notification(notification_id="b"), {})
    manager.set(make_notification(skill_id=OTHER_SKILL, notification_id="c"), {})
    response = remove(manager)
    assert response.status == "ok" and sorted(response.notification_ids) == ["a", "b"]
    assert store.get("c").state == NotificationState.ACTIVE


def test_remove_unknown_or_wrong_skill_refused(manager):
    manager.set(make_notification(notification_id="n1"), {})
    assert remove(manager, notification_id="nope").status == "refused"
    wrong = remove(manager, skill_id=OTHER_SKILL, notification_id="n1")
    assert wrong.status == "refused" and wrong.notification_ids == []
    assert SKILL in wrong.reason and OTHER_SKILL in wrong.reason
    neither = remove(manager, skill_id=None)
    assert neither.status == "refused" and "requires" in neither.reason
    nothing = remove(manager, skill_id=OTHER_SKILL)
    assert nothing.status == "refused"


def test_remove_without_skill_id_resolves_producer(manager, store, emitted):
    manager.set(make_notification(notification_id="n1"), {})
    emitted.clear()
    response = consumer_remove(manager, "n1", dismissed_by=NODE)
    assert response.status == "ok" and response.notification_ids == ["n1"]
    assert store.get("n1").state == NotificationState.DISMISSED
    assert emitted.of(MSG_DISMISS)[0]["dismissed_by"] == NODE


def test_remove_dismissed_by_overrides_skill_id_identity(manager, store):
    manager.set(make_notification(notification_id="n1", removable_by_user=False), {})
    response = remove(manager, notification_id="n1", dismissed_by=NODE)
    assert response.status == "refused"
    assert store.get("n1").state == NotificationState.ACTIVE


def test_remove_non_removable_refused_for_consumer(manager, store):
    manager.set(make_notification(notification_id="n1", removable_by_user=False), {})
    response = consumer_remove(manager, "n1", context=node_context())
    assert response.status == "refused"
    assert "only removable by its producer" in response.reason
    assert store.get("n1").state == NotificationState.ACTIVE
    by_producer = remove(manager, notification_id="n1", context=node_context())
    assert by_producer.status == "ok"


def test_remove_blocked_consumer_refused(manager):
    manager.set(make_notification(notification_id="n1"), {})
    response = consumer_remove(manager, "n1", context=node_context("blocked-node"))
    assert response.status == "refused" and "blocked" in response.reason
    by_data = remove(manager, notification_id="n1", dismissed_by="blocked-node")
    assert by_data.status == "refused" and "blocked" in by_data.reason


def test_remove_shared_by_consumer(manager, store, emitted):
    manager.set(make_notification(notification_id="n1"), {})
    emitted.clear()
    response = consumer_remove(manager, "n1", context=node_context(OTHER_NODE))
    assert response.status == "ok"
    assert store.get("n1").state == NotificationState.DISMISSED
    assert emitted.of(MSG_DISMISS)[0]["dismissed_by"] == OTHER_NODE


def test_remove_per_client_records_client_only(manager, store, emitted):
    manager.set(make_notification(
        notification_id="n1", dismiss_policy=DismissPolicy.PER_CLIENT), {})
    emitted.clear()
    response = consumer_remove(manager, "n1", context=node_context(NODE))
    assert response.status == "ok"
    record = store.get("n1")
    assert record.state == NotificationState.ACTIVE
    assert record.dismissed_by_clients == {NODE}
    assert emitted.of(MSG_DISMISS)[0]["dismissed_by"] == NODE
    listed = manager.list(NotificationListData(node_id=NODE))
    assert listed.states["n1"] == NotificationState.DISMISSED
    assert manager.list(NotificationListData(node_id=OTHER_NODE)).states == {}
    assert manager.list(NotificationListData()).states["n1"] == NotificationState.ACTIVE


def test_remove_per_client_without_identity_refused(manager):
    manager.set(make_notification(
        notification_id="n1", dismiss_policy=DismissPolicy.PER_CLIENT), {})
    response = consumer_remove(manager, "n1", context={"client": "unknown"})
    assert response.status == "refused" and "PER_CLIENT" in response.reason


# ---- get / list ------------------------------------------------------------
def test_get(manager):
    manager.set(make_notification(notification_id="n1"), {})
    assert manager.get("n1").notification.notification_id == "n1"
    assert manager.get("missing").notification is None


def test_list_filters_include_global(manager, clock):
    manager.set(make_notification(notification_id="client"), {})
    manager.set(make_notification(skill_id=ALERTS, notification_id="global",
                                  scope=NotificationScope.GLOBAL, target=None), {})
    manager.set(make_notification(notification_id="user",
                                  scope=NotificationScope.USER, target=USER), {})
    clock.advance(seconds=5)
    manager.set(make_notification(notification_id="late"), {})
    ids = lambda r: sorted(n.notification_id for n in r.notifications)
    assert ids(manager.list(NotificationListData())) == ["client", "global", "late", "user"]
    assert ids(manager.list(NotificationListData(node_id=NODE))) == ["client", "global", "late"]
    assert ids(manager.list(NotificationListData(node_id=OTHER_NODE))) == ["global"]
    assert ids(manager.list(NotificationListData(user_id=USER))) == ["global", "user"]
    assert ids(manager.list(NotificationListData(skill_id=ALERTS))) == ["global"]
    since = clock.now - timedelta(seconds=1)
    assert ids(manager.list(NotificationListData(since=since))) == ["late"]
    clock.advance(seconds=5)
    remove(manager, notification_id="client")
    assert ids(manager.list(NotificationListData(
        state=NotificationState.DISMISSED))) == ["client"]
    # dismissal bumps updated_at, so the tombstone flows through `since`
    assert ids(manager.list(NotificationListData(since=since))) == ["client", "late"]


# ---- snooze ----------------------------------------------------------------
def test_snooze_then_renotify(manager, emitted, store, scheduler, clock):
    manager.set(make_notification(notification_id="n1"), {})
    emitted.clear()
    response = manager.snooze(
        NotificationSnoozeData(notification_id="n1", duration=300), {})
    assert response.status == "ok"
    assert response.renotify_at == clock.now + timedelta(seconds=300)
    assert store.get("n1").state == NotificationState.SNOOZED
    snoozed = emitted.of(MSG_SNOOZED)[0]
    assert snoozed["notification_id"] == "n1"
    assert snoozed["scope"] == NotificationScope.CLIENT.value
    assert snoozed["target"] == NODE
    assert scheduler.is_armed("n1")
    assert manager.active_records() == []
    scheduler.fire("n1")
    record = store.get("n1")
    assert record.state == NotificationState.ACTIVE and record.renotify_at is None
    assert emitted.of(MSG_NOTIFY)[0]["notification"]["notification_id"] == "n1"


def test_snooze_refusals(manager):
    assert manager.snooze(NotificationSnoozeData(
        notification_id="x", duration=5), {}).status == "refused"
    manager.set(make_notification(notification_id="n1"), {})
    assert manager.snooze(NotificationSnoozeData(
        notification_id="n1", duration=0), {}).status == "refused"
    remove(manager, notification_id="n1")
    refused = manager.snooze(NotificationSnoozeData(notification_id="n1", duration=5), {})
    assert refused.status == "refused" and "dismissed" in refused.reason


# ---- interaction -------------------------------------------------------------
def test_interaction_forwards_and_dismisses(manager, emitted, store):
    manager.set(make_notification(
        notification_id="n1", callback_data={"k": "v"},
        actions=[make_action("open"), make_action("later", dismiss=False)]), {})
    emitted.clear()
    manager.interact(NotificationInteractionData(
        notification_id="n1", action_id="later"), node_context())
    forwarded = [(d, c) for t, d, c in emitted.events if t == MSG_INTERACTION]
    assert forwarded[0][0]["callback_data"] == {"k": "v"}
    assert forwarded[0][1][FORWARDED_MARKER] is True
    assert store.get("n1").state == NotificationState.ACTIVE
    manager.interact(NotificationInteractionData(
        notification_id="n1", action_id="open"), node_context())
    assert store.get("n1").state == NotificationState.DISMISSED
    assert emitted.of(MSG_DISMISS)[0]["dismissed_by"] == NODE


def test_interaction_unknown_action_or_notification(manager, emitted, store):
    manager.set(make_notification(notification_id="n1", actions=[make_action()]), {})
    emitted.clear()
    manager.interact(NotificationInteractionData(
        notification_id="n1", action_id="nope"), {})
    assert store.get("n1").state == NotificationState.ACTIVE
    assert len(emitted.of(MSG_INTERACTION)) == 1
    manager.interact(NotificationInteractionData(
        notification_id="ghost", action_id="open"), {})
    assert len(emitted.of(MSG_INTERACTION)) == 1


def test_interaction_per_client_dismiss(manager, store):
    manager.set(make_notification(
        notification_id="n1", dismiss_policy=DismissPolicy.PER_CLIENT,
        actions=[make_action()]), {})
    manager.interact(NotificationInteractionData(
        notification_id="n1", action_id="open"), node_context(OTHER_NODE))
    record = store.get("n1")
    assert record.state == NotificationState.ACTIVE
    assert record.dismissed_by_clients == {OTHER_NODE}


# ---- sync / ready --------------------------------------------------------------
def test_sync_scopes_to_requester(manager, emitted, clock):
    manager.set(make_notification(notification_id="mine"), {})
    manager.set(make_notification(notification_id="theirs", target=OTHER_NODE), {})
    manager.set(make_notification(skill_id=ALERTS, notification_id="global",
                                  scope=NotificationScope.GLOBAL, target=None), {})
    manager.set(make_notification(notification_id="per", target=None,
                                  dismiss_policy=DismissPolicy.PER_CLIENT),
                node_context())
    consumer_remove(manager, "per", context=node_context())
    emitted.clear()
    response = manager.sync(NotificationSyncRequestData(), node_context())
    ids = sorted(n.notification_id for n in response.notifications)
    assert ids == ["global", "mine"]
    assert response.server_time == clock.now
    assert sorted(d["notification"]["notification_id"]
                  for d in emitted.of(MSG_NOTIFY)) == ["global", "mine"]
    later = manager.sync(NotificationSyncRequestData(since=clock.now), node_context())
    assert later.notifications == []


def test_announce_active(manager, emitted):
    manager.set(make_notification(notification_id="a"), {})
    manager.set(make_notification(notification_id="b"), {})
    manager.snooze(NotificationSnoozeData(notification_id="b", duration=9), {})
    emitted.clear()
    assert manager.announce_active() == 1
    assert emitted.of(MSG_NOTIFY)[0]["notification"]["notification_id"] == "a"


# ---- expiry / restore / prune ----------------------------------------------------
def test_expire_stale(manager, emitted, store, clock):
    manager.set(make_notification(notification_id="n1",
                                  expires_at=clock.now + timedelta(minutes=1)), {})
    emitted.clear()
    assert manager.expire_stale() == 0
    clock.advance(minutes=2)
    assert manager.expire_stale() == 1
    assert store.get("n1").state == NotificationState.EXPIRED
    assert emitted.of(MSG_DISMISS)[0]["dismissed_by"] == "expired"


def test_restore_rearms_and_wakes(manager, store, scheduler, emitted, clock):
    manager.set(make_notification(notification_id="future"), {})
    manager.snooze(NotificationSnoozeData(notification_id="future", duration=600), {})
    manager.set(make_notification(notification_id="past"), {})
    manager.snooze(NotificationSnoozeData(notification_id="past", duration=10), {})
    manager.set(make_notification(notification_id="stale",
                                  expires_at=clock.now + timedelta(seconds=5)), {})
    scheduler.cancel_all()
    emitted.clear()
    clock.advance(seconds=60)
    manager.restore()
    assert scheduler.is_armed("future")
    assert store.get("past").state == NotificationState.ACTIVE
    assert store.get("stale").state == NotificationState.EXPIRED
    assert [d["notification"]["notification_id"]
            for d in emitted.of(MSG_NOTIFY)] == ["past"]


def test_tick_prunes_tombstones_after_window(manager, store, clock):
    manager.set(make_notification(notification_id="old"), {})
    remove(manager, notification_id="old")
    manager.set(make_notification(notification_id="live"), {})
    clock.advance(days=6)
    manager.set(make_notification(notification_id="fresh"), {})
    remove(manager, notification_id="fresh")
    manager.tick()
    assert sorted(r.notification_id for r in store.list()) == ["fresh", "live", "old"]
    clock.advance(days=2)
    manager.tick()
    assert sorted(r.notification_id for r in store.list()) == ["fresh", "live"]
