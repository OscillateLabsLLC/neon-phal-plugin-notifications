# NEON AI (TM) SOFTWARE, Software Development Kit & Application Development System
# All trademark and other rights reserved by their respective owners
# Copyright 2008-2026 Neongecko.com Inc.
# BSD-3
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 1. Redistributions of source code must retain the above copyright notice,
#    this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from this
#    software without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
# PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
# CONTRIBUTORS  BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
# EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
# PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS;  OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE,  EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
Persistence port and the JSON adapter for the Notification Manager.

`NotificationStore` is the only seam between manager logic and storage. The
manager is the single writer; concurrency, durability, and serialization are
the adapter's concern.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from os import makedirs
from os.path import dirname, join
from typing import Callable, Dict, List, Optional, Set

from json_database import JsonStorage
from ovos_utils.xdg_utils import xdg_data_home

from neon_data_models.enum import NotificationScope, NotificationState
from neon_data_models.models.base.notifications import Notification

# States the manager may still act on; never pruned by retention
LIVE_STATES = (NotificationState.ACTIVE, NotificationState.SNOOZED)


@dataclass
class StoredNotification:
    """A `Notification` plus the manager-side state that wraps it."""
    notification: Notification
    state: NotificationState = NotificationState.ACTIVE
    renotify_at: Optional[datetime] = None
    dismissed_by_clients: Set[str] = field(default_factory=set)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def notification_id(self) -> str:
        return self.notification.notification_id

    @property
    def skill_id(self) -> str:
        return self.notification.skill_id

    def is_live(self) -> bool:
        return self.state in LIVE_STATES

    def touch(self, now: datetime) -> None:
        """Record a change; `updated_at` is the sync cursor on the wire."""
        self.updated_at = now
        self.notification.updated_at = now

    def state_for_client(self, client_id: Optional[str]) -> NotificationState:
        """State as seen by one client, honoring PER_CLIENT dismissals."""
        if client_id and client_id in self.dismissed_by_clients:
            return NotificationState.DISMISSED
        return self.state

    def addressed_to(self, node_id: Optional[str],
                     user_id: Optional[str]) -> bool:
        """
        True if this notification is in scope for the given identities. GLOBAL
        always matches; CLIENT/USER match their target. With neither filter
        set, everything matches.
        """
        if node_id is None and user_id is None:
            return True
        scope, target = self.notification.scope, self.notification.target
        if scope == NotificationScope.GLOBAL:
            return True
        if scope == NotificationScope.CLIENT:
            return node_id is not None and target == node_id
        return user_id is not None and target == user_id

    def to_dict(self) -> dict:
        return {
            "notification": self.notification.model_dump(),
            "state": self.state.value,
            "renotify_at": _to_timestamp(self.renotify_at),
            "dismissed_by_clients": sorted(self.dismissed_by_clients),
            "created_at": _to_timestamp(self.created_at),
            "updated_at": _to_timestamp(self.updated_at),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "StoredNotification":
        return cls(
            notification=Notification.model_validate(data["notification"]),
            state=NotificationState(data.get("state", "active")),
            renotify_at=_from_timestamp(data.get("renotify_at")),
            dismissed_by_clients=set(data.get("dismissed_by_clients", [])),
            created_at=_from_timestamp(data.get("created_at")),
            updated_at=_from_timestamp(data.get("updated_at")),
        )


def _to_timestamp(value: Optional[datetime]) -> Optional[float]:
    return value.timestamp() if value else None


def _from_timestamp(value: Optional[float]) -> Optional[datetime]:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


class NotificationStore(ABC):
    """Persistence port: the only seam between manager logic and storage."""

    @abstractmethod
    def upsert(self, record: StoredNotification) -> None: ...

    @abstractmethod
    def get(self, notification_id: str) -> Optional[StoredNotification]: ...

    @abstractmethod
    def list(self, *, skill_id: Optional[str] = None,
             state: Optional[NotificationState] = None,
             since: Optional[datetime] = None,
             node_id: Optional[str] = None,
             user_id: Optional[str] = None) -> List[StoredNotification]: ...

    @abstractmethod
    def set_state(self, notification_id: str, state: NotificationState,
                  client_id: Optional[str] = None) -> None: ...

    @abstractmethod
    def prune(self, max_age: timedelta) -> int: ...


class InMemoryNotificationStore(NotificationStore):
    """
    Dict-backed store. Base for the JSON adapter and usable directly in tests.
    """

    def __init__(self, clock: Callable[[], datetime] = None):
        self._records: Dict[str, StoredNotification] = {}
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def upsert(self, record: StoredNotification) -> None:
        self._records[record.notification_id] = record
        self._persist()

    def get(self, notification_id: str) -> Optional[StoredNotification]:
        return self._records.get(notification_id)

    def list(self, *, skill_id: Optional[str] = None,
             state: Optional[NotificationState] = None,
             since: Optional[datetime] = None,
             node_id: Optional[str] = None,
             user_id: Optional[str] = None) -> List[StoredNotification]:
        matches = [r for r in self._records.values()
                   if _matches(r, skill_id, state, since, node_id, user_id)]
        return sorted(matches, key=_created_key)

    def set_state(self, notification_id: str, state: NotificationState,
                  client_id: Optional[str] = None) -> None:
        record = self._records[notification_id]
        if client_id is not None:
            record.dismissed_by_clients.add(client_id)
        else:
            record.state = state
        record.touch(self._clock())
        self._persist()

    def prune(self, max_age: timedelta) -> int:
        """
        Drop retired (DISMISSED/EXPIRED) tombstones whose `updated_at` is
        older than `max_age`. Live records are never pruned.
        """
        cutoff = self._clock() - max_age
        doomed = [r for r in self._records.values()
                  if not r.is_live() and _updated_key(r) < cutoff]
        for record in doomed:
            del self._records[record.notification_id]
        if doomed:
            self._persist()
        return len(doomed)

    def _persist(self) -> None:
        """Hook for durable subclasses; the in-memory store has nothing to do."""


class JsonNotificationStore(InMemoryNotificationStore):
    """
    Default adapter: one JSON file under the plugin's XDG data directory,
    written through `json_database.JsonStorage` (the `ovos_utils` precedent).
    """
    FILE_NAME = "notifications.json"

    def __init__(self, path: Optional[str] = None, plugin_name: str = "",
                 disable_lock: bool = False,
                 clock: Callable[[], datetime] = None):
        super().__init__(clock=clock)
        path = path or self.default_path(plugin_name)
        makedirs(dirname(path), exist_ok=True)
        self._storage = JsonStorage(path, disable_lock=disable_lock)
        self._load()

    @staticmethod
    def default_path(plugin_name: str) -> str:
        return join(xdg_data_home(), "neon", plugin_name,
                    JsonNotificationStore.FILE_NAME)

    @property
    def path(self) -> str:
        return self._storage.path

    def _load(self) -> None:
        for notification_id, raw in self._storage.items():
            self._records[notification_id] = StoredNotification.from_dict(raw)

    def _persist(self) -> None:
        self._storage.clear()
        for notification_id, record in self._records.items():
            self._storage[notification_id] = record.to_dict()
        self._storage.store()


def _matches(record: StoredNotification, skill_id, state, since, node_id,
             user_id) -> bool:
    if skill_id is not None and record.skill_id != skill_id:
        return False
    if state is not None and record.state != state:
        return False
    if since is not None and _updated_key(record) <= since:
        return False
    return record.addressed_to(node_id, user_id)


def _created_key(record: StoredNotification) -> datetime:
    return record.created_at or datetime.fromtimestamp(0, tz=timezone.utc)


def _updated_key(record: StoredNotification) -> datetime:
    return record.updated_at or _created_key(record)
