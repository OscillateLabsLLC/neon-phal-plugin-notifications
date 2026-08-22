from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Tuple

import pytest

from neon_data_models.enum import NotificationScope
from neon_data_models.models.base.notifications import (Notification,
                                                        NotificationAction)

from neon_phal_plugin_notifications.manager import NotificationManager
from neon_phal_plugin_notifications.policy import (ConsumerDismissPolicy,
                                                   EmissionPolicy)
from neon_phal_plugin_notifications.store import InMemoryNotificationStore

SKILL = "skill-test.neongeckocom"
OTHER_SKILL = "skill-other.neongeckocom"
ALERTS = "skill-alerts.neongeckocom"
NODE = "node-abc"
OTHER_NODE = "node-xyz"
USER = "user-1"


class FakeClock:
    def __init__(self, start: datetime = None):
        self.now = start or datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs) -> None:
        self.now += timedelta(**kwargs)


class FakeScheduler:
    def __init__(self):
        self.armed: Dict[str, Tuple[datetime, Callable[[], None]]] = {}
        self.cancelled: List[str] = []

    def arm(self, notification_id, fire_at, callback):
        self.armed[notification_id] = (fire_at, callback)

    def cancel(self, notification_id):
        self.cancelled.append(notification_id)
        self.armed.pop(notification_id, None)

    def cancel_all(self):
        self.armed.clear()

    def is_armed(self, notification_id):
        return notification_id in self.armed

    def fire(self, notification_id):
        _, callback = self.armed.pop(notification_id)
        callback()


class Emitted:
    def __init__(self):
        self.events: List[Tuple[str, dict, dict]] = []

    def __call__(self, msg_type, data, context):
        self.events.append((msg_type, data.model_dump(), dict(context)))

    def of(self, msg_type) -> List[dict]:
        return [d for t, d, _ in self.events if t == msg_type]

    def clear(self):
        self.events.clear()


def make_notification(skill_id=SKILL, **overrides) -> Notification:
    fields = dict(skill_id=skill_id, text="hello", scope=NotificationScope.CLIENT,
                  target=NODE)
    fields.update(overrides)
    return Notification(**fields)


def make_action(action_id="open", dismiss=True) -> NotificationAction:
    return NotificationAction(action_id=action_id, label=action_id,
                              dismiss_on_activate=dismiss)


def node_context(node_id=NODE, **extra) -> dict:
    return dict({"node": {"node_id": node_id},
                 "session": {"session_id": node_id}}, **extra)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def emitted():
    return Emitted()


@pytest.fixture
def scheduler():
    return FakeScheduler()


@pytest.fixture
def store(clock):
    return InMemoryNotificationStore(clock=clock)


@pytest.fixture
def manager(store, emitted, scheduler, clock):
    return NotificationManager(
        store=store, emit=emitted, scheduler=scheduler, clock=clock,
        emission_policy=EmissionPolicy(allow_non_removable=[ALERTS, SKILL],
                                       clock=clock),
        dismiss_policy=ConsumerDismissPolicy(blocked=["blocked-node"]))
