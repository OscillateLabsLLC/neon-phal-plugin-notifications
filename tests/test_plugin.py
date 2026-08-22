import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pytest
from ovos_bus_client.message import Message
from ovos_utils.fakebus import FakeBus

from neon_data_models.enum import NotificationState

from neon_phal_plugin_notifications import (MSG_GET, MSG_INTERACTION,
                                            MSG_LIST, MSG_READY, MSG_REMOVE,
                                            MSG_REMOVE_CONTROLLED, MSG_SET,
                                            MSG_SET_CONTROLLED, MSG_SNOOZE,
                                            MSG_SYNC, NotificationManagerPlugin,
                                            build_store)
from neon_phal_plugin_notifications.manager import (MSG_DISMISS, MSG_NOTIFY,
                                                    MSG_SNOOZED)
from neon_phal_plugin_notifications.store import (JsonNotificationStore,
                                                  StoredNotification)
from tests.conftest import ALERTS, NODE, SKILL, FakeScheduler, \
    make_notification, node_context

WATCHED = [MSG_NOTIFY, MSG_DISMISS, MSG_SNOOZED, MSG_INTERACTION] + [
    f"{t}.response" for t in (MSG_SET, MSG_REMOVE, MSG_GET, MSG_LIST,
                              MSG_SNOOZE, MSG_SYNC)]


class Recorder:
    def __init__(self, bus: FakeBus):
        self.messages: Dict[str, List[Message]] = {t: [] for t in WATCHED}
        for msg_type in WATCHED:
            bus.on(msg_type, self._record)

    def _record(self, message: Message):
        self.messages[message.msg_type].append(message)

    def last(self, msg_type: str) -> Message:
        return self.messages[msg_type][-1]

    def clear(self):
        for queue in self.messages.values():
            queue.clear()


@pytest.fixture
def bus():
    return FakeBus()


@pytest.fixture
def recorder(bus):
    return Recorder(bus)


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "notifications.json")


@pytest.fixture
def config(store_path):
    return {"store_path": store_path, "tick_seconds": 3600,
            "allow_non_removable": [SKILL, ALERTS]}


@pytest.fixture
def plugin(bus, config, recorder):
    plugin = NotificationManagerPlugin(bus=bus, config=config)
    plugin.manager.scheduler = FakeScheduler()
    yield plugin
    plugin.shutdown()


def set_message(notification=None, context=None, **overrides) -> Message:
    notification = notification or make_notification(**overrides)
    return Message(MSG_SET, {"notification": notification.model_dump()},
                   context or {})


def test_build_store_rejects_unknown_adapter():
    with pytest.raises(ValueError, match="unsupported notification store"):
        build_store({"store": "postgres"})


def test_build_store_json_default(store_path):
    assert isinstance(build_store({"store_path": store_path}), JsonNotificationStore)


def test_set_round_trip(plugin, bus, recorder):
    context = {"mq": {"routing_key": "abc"}, "source": "skills",
               "destination": ["phal"]}
    bus.emit(set_message(notification_id="n1", context=context))
    response = recorder.last(f"{MSG_SET}.response")
    assert response.data["status"] == "ok"
    assert response.data["notification_id"] == "n1"
    assert response.context["mq"] == {"routing_key": "abc"}
    notify = recorder.last(MSG_NOTIFY)
    assert notify.data["notification"]["notification_id"] == "n1"
    assert notify.data["notification"]["created_at"] is not None
    assert notify.data["notification"]["updated_at"] is not None


def test_set_fills_session_from_context(plugin, bus, recorder):
    bus.emit(Message(MSG_SET, {"notification": {"skill_id": SKILL, "text": "t"}},
                     {"session": {"session_id": "sess-1", "lang": "en-us"}}))
    notification = recorder.last(MSG_NOTIFY).data["notification"]
    assert notification["session"]["session_id"] == "sess-1"
    assert notification["target"] == "sess-1"


def test_set_invalid_payload_refused(plugin, bus, recorder):
    bus.emit(Message(MSG_SET, {"notification": {"skill_id": SKILL}}, {}))
    response = recorder.last(f"{MSG_SET}.response")
    assert response.data["status"] == "refused"
    assert "invalid notification payload" in response.data["reason"]


def test_set_policy_refused_via_bus(plugin, bus, recorder):
    bus.emit(set_message(scope=2, target=None))
    response = recorder.last(f"{MSG_SET}.response")
    assert response.data["status"] == "refused"
    assert "allow_global" in response.data["reason"]
    assert recorder.messages[MSG_NOTIFY] == []


def test_set_controlled_maps_to_non_removable(plugin, bus, recorder):
    bus.emit(Message(MSG_SET_CONTROLLED,
                     {"notification": make_notification(
                         notification_id="c1").model_dump()}, {}))
    assert recorder.last(f"{MSG_SET}.response").data["status"] == "ok"
    assert recorder.last(MSG_NOTIFY).data["notification"]["removable_by_user"] is False
    bus.emit(Message(MSG_REMOVE, {"notification_id": "c1", "dismissed_by": NODE},
                     node_context()))
    assert recorder.last(f"{MSG_REMOVE}.response").data["status"] == "refused"
    bus.emit(Message(MSG_REMOVE, {"skill_id": SKILL, "notification_id": "c1"}, {}))
    assert recorder.last(f"{MSG_REMOVE}.response").data["status"] == "ok"


def test_legacy_payload_up_converted(plugin, bus, recorder):
    bus.emit(Message(MSG_SET, {"sender": "skill-legacy", "text": "old style",
                               "type": "sticky", "action": "go"}, {}))
    assert recorder.last(f"{MSG_SET}.response").data["status"] == "ok"
    notification = recorder.last(MSG_NOTIFY).data["notification"]
    assert notification["skill_id"] == "skill-legacy"
    assert notification["text"] == "old style"
    assert notification["actions"][0]["action_id"] == "go"


def test_sender_alias_in_nested_payload(plugin, bus, recorder):
    bus.emit(Message(MSG_SET, {"notification": {"sender": "s", "text": "t"}}, {}))
    assert recorder.last(MSG_NOTIFY).data["notification"]["skill_id"] == "s"


def test_remove_and_remove_controlled(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    bus.emit(set_message(notification_id="n2"))
    bus.emit(Message(MSG_REMOVE, {"skill_id": SKILL, "notification_id": "n1"}, {}))
    response = recorder.last(f"{MSG_REMOVE}.response")
    assert response.data == {"notification_ids": ["n1"], "status": "ok",
                             "reason": None}
    assert recorder.last(MSG_DISMISS).data["notification_id"] == "n1"
    bus.emit(Message(MSG_REMOVE_CONTROLLED, {"skill_id": SKILL}, {}))
    assert recorder.last(f"{MSG_REMOVE}.response").data["notification_ids"] == ["n2"]
    bus.emit(Message(MSG_REMOVE, {}, {}))
    assert recorder.last(f"{MSG_REMOVE}.response").data["status"] == "refused"


def test_remove_hana_rest_shape(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    bus.emit(Message(MSG_REMOVE, {"notification_id": "n1", "dismissed_by": NODE},
                     {"session": {"session_id": NODE}, "node": {"node_id": NODE}}))
    assert recorder.last(f"{MSG_REMOVE}.response").data["status"] == "ok"
    assert recorder.last(MSG_DISMISS).data["dismissed_by"] == NODE


def test_list_hana_shape_includes_global_user_client(plugin, bus, recorder):
    bus.emit(set_message(notification_id="client"))
    bus.emit(set_message(notification_id="user", scope=1, target="user-1"))
    bus.emit(set_message(notification_id="global", skill_id=ALERTS, scope=2,
                         target=None))
    bus.emit(set_message(notification_id="other", target="someone-else"))
    bus.emit(Message(MSG_LIST, {"node_id": NODE, "user_id": "user-1",
                                "state": "active", "since": None},
                     {"session": {"session_id": NODE},
                      "node": {"node_id": NODE}, "user_id": "user-1"}))
    listed = recorder.last(f"{MSG_LIST}.response").data
    assert sorted(n["notification_id"] for n in listed["notifications"]) == \
        ["client", "global", "user"]


def test_get_and_list(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    bus.emit(Message(MSG_GET, {"notification_id": "n1"}, {}))
    assert recorder.last(f"{MSG_GET}.response").data["notification"]["notification_id"] == "n1"
    bus.emit(Message(MSG_GET, {}, {}))
    assert recorder.last(f"{MSG_GET}.response").data["notification"] is None
    bus.emit(Message(MSG_LIST, {"node_id": NODE, "state": "active"}, {}))
    listed = recorder.last(f"{MSG_LIST}.response").data
    assert [n["notification_id"] for n in listed["notifications"]] == ["n1"]
    assert listed["states"] == {"n1": "active"}
    bus.emit(Message(MSG_LIST, {"state": "bogus"}, {}))
    assert recorder.last(f"{MSG_LIST}.response").data["notifications"] == []


def test_snooze_via_bus(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    recorder.clear()
    bus.emit(Message(MSG_SNOOZE, {"notification_id": "n1", "duration": 30}, {}))
    response = recorder.last(f"{MSG_SNOOZE}.response").data
    assert response["status"] == "ok" and response["renotify_at"]
    snoozed = recorder.last(MSG_SNOOZED).data
    assert snoozed["notification_id"] == "n1"
    assert snoozed["scope"] == 0 and snoozed["target"] == NODE
    plugin.manager.scheduler.fire("n1")
    assert recorder.last(MSG_NOTIFY).data["notification"]["notification_id"] == "n1"
    bus.emit(Message(MSG_SNOOZE, {"notification_id": "n1"}, {}))
    assert recorder.last(f"{MSG_SNOOZE}.response").data["status"] == "refused"


def test_interaction_via_bus_no_loop(plugin, bus, recorder):
    notification = make_notification(
        notification_id="n1", callback_data={"x": 1},
        actions=[{"action_id": "open", "label": "Open"}])
    bus.emit(set_message(notification))
    recorder.clear()
    bus.emit(Message(MSG_INTERACTION, {"notification_id": "n1",
                                       "action_id": "open"}, node_context()))
    forwarded = recorder.messages[MSG_INTERACTION]
    # the consumer's original plus exactly one forwarded copy
    assert len(forwarded) == 2
    assert forwarded[-1].data["callback_data"] == {"x": 1}
    assert recorder.last(MSG_DISMISS).data["dismissed_by"] == NODE
    assert plugin.manager.store.get("n1").state == NotificationState.DISMISSED


def test_sync_request_via_bus(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    recorder.clear()
    bus.emit(Message(MSG_SYNC, {}, node_context()))
    response = recorder.last(f"{MSG_SYNC}.response").data
    assert [n["notification_id"] for n in response["notifications"]] == ["n1"]
    assert response["server_time"] is not None
    assert len(recorder.messages[MSG_NOTIFY]) == 1


def test_ready_reemits_active(plugin, bus, recorder):
    bus.emit(set_message(notification_id="n1"))
    bus.emit(set_message(notification_id="n2"))
    recorder.clear()
    bus.emit(Message(MSG_READY, {}, {}))
    assert sorted(m.data["notification"]["notification_id"]
                  for m in recorder.messages[MSG_NOTIFY]) == ["n1", "n2"]


def test_restart_persists_rearms_and_expires(bus, config, store_path, recorder):
    first = NotificationManagerPlugin(bus=bus, config=config)
    bus.emit(set_message(notification_id="kept"))
    bus.emit(set_message(notification_id="snoozed"))
    bus.emit(Message(MSG_SNOOZE, {"notification_id": "snoozed",
                                  "duration": 3600}, {}))
    bus.emit(set_message(notification_id="stale",
                         expires_at=datetime.now(timezone.utc) - timedelta(days=1)))
    first.shutdown()
    assert not first.manager.scheduler.is_armed("snoozed")
    recorder.clear()

    second = NotificationManagerPlugin(bus=bus, config=config)
    try:
        store = second.manager.store
        assert store.get("kept").state == NotificationState.ACTIVE
        assert store.get("snoozed").state == NotificationState.SNOOZED
        assert second.manager.scheduler.is_armed("snoozed")
        assert store.get("stale").state == NotificationState.EXPIRED
        assert recorder.last(MSG_DISMISS).data["notification_id"] == "stale"
        with open(store_path) as f:
            assert set(json.load(f)) == {"kept", "snoozed", "stale"}
    finally:
        second.shutdown()
    assert not second.manager.scheduler.is_armed("snoozed")


def test_shutdown_removes_handlers(bus, config):
    plugin = NotificationManagerPlugin(bus=bus, config=config)
    assert bus.ee.listeners(MSG_SET)
    plugin.shutdown()
    assert not bus.ee.listeners(MSG_SET)
    assert not plugin.is_alive() or plugin._stop_event.is_set()
