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
Notification Manager core: accepts producer requests, owns lifecycle state,
and emits consumer events. Bus-independent; the plugin in
`__init__.py` adapts messagebus traffic to these methods.
"""

from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple

from ovos_utils.log import LOG
from pydantic import BaseModel

from neon_data_models.enum import NotificationScope, NotificationState
from neon_data_models.models.api.messagebus.notifications import (
    NotificationDismissData, NotificationGetResponseData,
    NotificationInteractionData, NotificationListData,
    NotificationListResponseData, NotificationNotifyData,
    NotificationRemoveData, NotificationRemoveResponseData,
    NotificationSetResponseData, NotificationSnoozeData,
    NotificationSnoozedData, NotificationSnoozeResponseData,
    NotificationSyncRequestData, NotificationSyncResponseData)
from neon_data_models.models.base.notifications import Notification

from neon_phal_plugin_notifications import identity
from neon_phal_plugin_notifications.authorization import (authorize_removal,
                                                          per_client_id,
                                                          snooze_refusal)
from neon_phal_plugin_notifications.policy import (ConsumerDismissPolicy,
                                                   EmissionPolicy, utc_now)
from neon_phal_plugin_notifications.scheduler import SnoozeScheduler
from neon_phal_plugin_notifications.store import (NotificationStore,
                                                  StoredNotification)

MSG_NOTIFY = "ovos.notification.api.notify"
MSG_DISMISS = "ovos.notification.api.dismiss"
MSG_SNOOZED = "ovos.notification.api.snoozed"
MSG_INTERACTION = "ovos.notification.api.interaction"
DISMISSED_BY_EXPIRY = "expired"
# Retired entries are kept as tombstones so an offline node learns on catch-up
# that a notification was dismissed elsewhere; this is not history
DEFAULT_RETENTION_MAX_AGE_DAYS = 7

Emitter = Callable[[str, BaseModel, dict], None]


class NotificationManager:
    def __init__(self, store: NotificationStore, emit: Emitter,
                 emission_policy: Optional[EmissionPolicy] = None,
                 dismiss_policy: Optional[ConsumerDismissPolicy] = None,
                 scheduler: Optional[SnoozeScheduler] = None,
                 retention_max_age: timedelta = timedelta(
                     days=DEFAULT_RETENTION_MAX_AGE_DAYS),
                 clock: Callable[[], datetime] = utc_now):
        self.store = store
        self._emit = emit
        self.emission_policy = emission_policy or EmissionPolicy(clock=clock)
        self.dismiss_policy = dismiss_policy or ConsumerDismissPolicy()
        self.scheduler = scheduler or SnoozeScheduler()
        self.retention_max_age = retention_max_age
        self._clock = clock

    # ---- producer API -----------------------------------------------------
    def set(self, notification: Notification, context: dict
            ) -> NotificationSetResponseData:
        """
        Upsert. Refuses cross-skill ID reuse and policy violations. An update
        keeps the original `created_at`; every accept bumps `updated_at`.
        """
        nid = notification.notification_id
        existing = self.store.get(nid)
        if existing and existing.skill_id != notification.skill_id:
            return self._refuse_set(nid, f"notification '{nid}' is owned by "
                                         f"'{existing.skill_id}'")
        allowed, reason = self.emission_policy.check(notification)
        if not allowed:
            return self._refuse_set(nid, reason)
        self._resolve_client_target(notification, context)
        now = self._clock()
        created_at = existing.created_at if existing else now
        notification.created_at = created_at
        self.scheduler.cancel(nid)
        record = StoredNotification(notification, NotificationState.ACTIVE,
                                    created_at=created_at)
        record.touch(now)
        self.store.upsert(record)
        self.emission_policy.record_accepted(notification.skill_id)
        self._notify(record, context)
        return NotificationSetResponseData(notification_id=nid, status="ok")

    def remove(self, data: NotificationRemoveData, context: dict
               ) -> NotificationRemoveResponseData:
        records, reason = self._records_for_removal(data)
        if reason:
            return NotificationRemoveResponseData(
                notification_ids=[], status="refused", reason=reason)
        removed, reasons = [], []
        for record in records:
            reason, dismissed_by, client_id = authorize_removal(
                record, data, context, self.dismiss_policy)
            if reason:
                reasons.append(reason)
                continue
            self._dismiss(record, dismissed_by, client_id)
            removed.append(record.notification_id)
        status = "ok" if removed else "refused"
        return NotificationRemoveResponseData(
            notification_ids=removed, status=status,
            reason="; ".join(reasons) or None)

    def get(self, notification_id: str) -> NotificationGetResponseData:
        record = self.store.get(notification_id)
        return NotificationGetResponseData(
            notification=record.notification if record else None)

    def list(self, data: NotificationListData) -> NotificationListResponseData:
        records = self.store.list(skill_id=data.skill_id, state=data.state,
                                  since=data.since, node_id=data.node_id,
                                  user_id=data.user_id)
        return NotificationListResponseData(
            notifications=[r.notification for r in records],
            states={r.notification_id: r.state_for_client(data.node_id)
                    for r in records})

    def snooze(self, data: NotificationSnoozeData, context: dict
               ) -> NotificationSnoozeResponseData:
        nid = data.notification_id
        record = self.store.get(nid)
        reason = snooze_refusal(record, data.duration)
        if reason:
            return NotificationSnoozeResponseData(
                notification_id=nid, status="refused", reason=reason)
        now = self._clock()
        record.state = NotificationState.SNOOZED
        record.renotify_at = now + timedelta(seconds=data.duration)
        record.touch(now)
        self.store.upsert(record)
        self._arm(record)
        self._emit(MSG_SNOOZED, NotificationSnoozedData(
            notification_id=nid, renotify_at=record.renotify_at,
            scope=record.notification.scope,
            target=record.notification.target), context)
        return NotificationSnoozeResponseData(
            notification_id=nid, status="ok", renotify_at=record.renotify_at)

    # ---- consumer API -----------------------------------------------------
    def interact(self, data: NotificationInteractionData, context: dict) -> None:
        """
        Forward the interaction for the producer (with stored `callback_data`
        filled in), then dismiss when the action asks for it.
        """
        record = self.store.get(data.notification_id)
        if record is None:
            LOG.warning("interaction for unknown notification '%s' ignored",
                        data.notification_id)
            return
        if not data.callback_data:
            data.callback_data = record.notification.callback_data
        forwarded = dict(context, **{identity.FORWARDED_MARKER: True})
        self._emit(MSG_INTERACTION, data, forwarded)
        action = next((a for a in record.notification.actions
                       if a.action_id == data.action_id), None)
        if action is None:
            LOG.warning("unknown action '%s' on notification '%s'; not "
                        "dismissing", data.action_id, data.notification_id)
            return
        if action.dismiss_on_activate and record.is_live():
            consumer = identity.consumer_id(context)
            client_id = per_client_id(record, consumer)
            self._dismiss(record, consumer or f"action:{action.action_id}",
                          client_id)

    def sync(self, data: NotificationSyncRequestData, context: dict
             ) -> NotificationSyncResponseData:
        """Re-emit `notify` for the requester's scope and return the set."""
        consumer = identity.consumer_id(context)
        records = [r for r in self.active_records(consumer,
                                                  identity.user_id(context))
                   if data.since is None
                   or _as_utc(r.updated_at) > _as_utc(data.since)]
        for record in records:
            self._notify(record, context)
        return NotificationSyncResponseData(
            notifications=[r.notification for r in records],
            states={r.notification_id: r.state for r in records},
            server_time=self._clock())

    # ---- lifecycle --------------------------------------------------------
    def active_records(self, node_id: Optional[str] = None,
                       user_id: Optional[str] = None) -> List[StoredNotification]:
        records = self.store.list(state=NotificationState.ACTIVE,
                                  node_id=node_id, user_id=user_id)
        return [r for r in records
                if r.state_for_client(node_id) == NotificationState.ACTIVE]

    def announce_active(self) -> int:
        """Re-emit `notify` for every ACTIVE notification (`mycroft.ready`)."""
        records = self.active_records()
        for record in records:
            self._notify(record, {})
        return len(records)

    def restore(self) -> None:
        """Reload persisted state: expire stale entries, re-arm snoozes."""
        self.expire_stale()
        for record in self.store.list(state=NotificationState.SNOOZED):
            if record.renotify_at and _as_utc(record.renotify_at) > self._clock():
                self._arm(record)
            else:
                self._wake(record.notification_id)

    def expire_stale(self) -> int:
        now = self._clock()
        expired = [r for r in self.store.list() if r.is_live()
                   and r.notification.expires_at
                   and _as_utc(r.notification.expires_at) <= now]
        for record in expired:
            self.scheduler.cancel(record.notification_id)
            self.store.set_state(record.notification_id,
                                 NotificationState.EXPIRED)
            self._emit_dismiss(record, DISMISSED_BY_EXPIRY)
        return len(expired)

    def tick(self) -> None:
        self.expire_stale()
        pruned = self.store.prune(self.retention_max_age)
        if pruned:
            LOG.debug("pruned %s retired notifications", pruned)

    def shutdown(self) -> None:
        self.scheduler.cancel_all()

    # ---- internals --------------------------------------------------------
    def _records_for_removal(self, data: NotificationRemoveData
                             ) -> Tuple[List[StoredNotification], Optional[str]]:
        """Resolve the removal set, or a refusal reason."""
        if data.notification_id is None:
            if not data.skill_id:
                return [], "remove requires `notification_id` or `skill_id`"
            records = [r for r in self.store.list(skill_id=data.skill_id)
                       if r.is_live()]
            if not records:
                return [], f"no live notifications for '{data.skill_id}'"
            return records, None
        record = self.store.get(data.notification_id)
        if record is None or not record.is_live():
            return [], f"no live notification '{data.notification_id}'"
        if data.skill_id and data.skill_id != record.skill_id:
            return [], (f"notification '{data.notification_id}' is owned by "
                        f"'{record.skill_id}', not '{data.skill_id}'")
        return [record], None

    def _dismiss(self, record: StoredNotification, dismissed_by: str,
                 client_id: Optional[str]) -> None:
        if client_id is None:
            self.scheduler.cancel(record.notification_id)
        self.store.set_state(record.notification_id,
                             NotificationState.DISMISSED, client_id=client_id)
        self._emit_dismiss(record, dismissed_by)

    def _emit_dismiss(self, record: StoredNotification,
                      dismissed_by: Optional[str]) -> None:
        notification = record.notification
        self._emit(MSG_DISMISS, NotificationDismissData(
            notification_id=notification.notification_id,
            skill_id=notification.skill_id, scope=notification.scope,
            target=notification.target, dismissed_by=dismissed_by), {})

    def _notify(self, record: StoredNotification, context: dict) -> None:
        self._emit(MSG_NOTIFY,
                   NotificationNotifyData(notification=record.notification),
                   context)

    def _arm(self, record: StoredNotification) -> None:
        nid = record.notification_id
        self.scheduler.arm(nid, _as_utc(record.renotify_at),
                           lambda: self._wake(nid))

    def _wake(self, notification_id: str) -> None:
        record = self.store.get(notification_id)
        if record is None or record.state != NotificationState.SNOOZED:
            return
        record.state = NotificationState.ACTIVE
        record.renotify_at = None
        record.touch(self._clock())
        self.store.upsert(record)
        self._notify(record, {})

    @staticmethod
    def _resolve_client_target(notification: Notification,
                               context: dict) -> None:
        if notification.scope != NotificationScope.CLIENT or notification.target:
            return
        notification.target = identity.consumer_id(context)
        if notification.target is None:
            LOG.info("CLIENT-scoped notification '%s' has no resolvable "
                     "target; leaving None", notification.notification_id)

    @staticmethod
    def _refuse_set(notification_id: str, reason: str
                    ) -> NotificationSetResponseData:
        LOG.warning("refused notification '%s': %s", notification_id, reason)
        return NotificationSetResponseData(
            notification_id=notification_id, status="refused", reason=reason)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value
