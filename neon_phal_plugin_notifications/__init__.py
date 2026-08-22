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
Notification Manager PHAL plugin. Bus in, bus out: this module
adapts `ovos.notification.api.*` traffic to `NotificationManager`. It never
renders anything and never talks MQ directly.
"""

from datetime import timedelta
from threading import Event
from typing import Optional

from ovos_bus_client.message import Message
from ovos_plugin_manager.templates.phal import PHALPlugin
from ovos_utils.log import LOG
from pydantic import BaseModel, ValidationError

from neon_data_models.models.api.messagebus.notifications import (
    NotificationGetData, NotificationGetResponseData,
    NotificationInteractionData, NotificationListData,
    NotificationListResponseData, NotificationRemoveData,
    NotificationRemoveResponseData, NotificationSetData,
    NotificationSetResponseData, NotificationSnoozeData,
    NotificationSnoozeResponseData, NotificationSyncRequestData)
from neon_data_models.models.base.notifications import Notification

from neon_phal_plugin_notifications.identity import is_forwarded
from neon_phal_plugin_notifications.legacy import (is_legacy_payload,
                                                   legacy_to_notification,
                                                   normalize_skill_id)
from neon_phal_plugin_notifications.manager import (
    DEFAULT_RETENTION_MAX_AGE_DAYS, NotificationManager)
from neon_phal_plugin_notifications.policy import (ConsumerDismissPolicy,
                                                   EmissionPolicy)
from neon_phal_plugin_notifications.store import (JsonNotificationStore,
                                                  NotificationStore)

PLUGIN_NAME = "neon-phal-plugin-notifications"
API = "ovos.notification.api"
MSG_SET = f"{API}.set"
MSG_SET_CONTROLLED = f"{API}.set.controlled"
MSG_REMOVE = f"{API}.remove"
MSG_REMOVE_CONTROLLED = f"{API}.remove.controlled"
MSG_GET = f"{API}.get"
MSG_LIST = f"{API}.list"
MSG_SNOOZE = f"{API}.snooze"
MSG_INTERACTION = f"{API}.interaction"
MSG_SYNC = f"{API}.sync.request"
MSG_READY = "mycroft.ready"
RESPONSE_SUFFIX = ".response"
DEFAULT_STORE = "json"
DEFAULT_TICK_SECONDS = 60


def build_store(config: dict, plugin_name: str = PLUGIN_NAME
                ) -> NotificationStore:
    """Select the persistence adapter by the `store` config key."""
    kind = config.get("store", DEFAULT_STORE)
    if kind != DEFAULT_STORE:
        raise ValueError(f"unsupported notification store '{kind}'; only "
                         f"'{DEFAULT_STORE}' ships in this version")
    return JsonNotificationStore(path=config.get("store_path"),
                                 plugin_name=plugin_name)


class NotificationManagerPlugin(PHALPlugin):
    def __init__(self, bus=None, config: Optional[dict] = None,
                 name: str = PLUGIN_NAME,
                 store: Optional[NotificationStore] = None):
        # The base __init__ starts the thread; `run` blocks on `_ready` until
        # the manager exists
        self._ready = Event()
        self._stop_event = Event()
        super().__init__(bus=bus, name=name, config=config)
        self.tick_seconds = self.config.get("tick_seconds", DEFAULT_TICK_SECONDS)
        self.manager = self._build_manager(store or build_store(self.config,
                                                                name))
        self.manager.restore()
        self._register_handlers()
        self._ready.set()

    def _build_manager(self, store: NotificationStore) -> NotificationManager:
        max_age = timedelta(days=self.config.get(
            "retention_max_age_days", DEFAULT_RETENTION_MAX_AGE_DAYS))
        return NotificationManager(
            store=store, emit=self._emit_event,
            emission_policy=EmissionPolicy.from_config(self.config),
            dismiss_policy=ConsumerDismissPolicy.from_config(self.config),
            retention_max_age=max_age)

    @property
    def _handlers(self) -> dict:
        return {
            MSG_SET: self.handle_set,
            MSG_SET_CONTROLLED: self.handle_set_controlled,
            MSG_REMOVE: self.handle_remove,
            MSG_REMOVE_CONTROLLED: self.handle_remove_controlled,
            MSG_GET: self.handle_get,
            MSG_LIST: self.handle_list,
            MSG_SNOOZE: self.handle_snooze,
            MSG_INTERACTION: self.handle_interaction,
            MSG_SYNC: self.handle_sync_request,
            MSG_READY: self.handle_ready,
        }

    def _register_handlers(self) -> None:
        for msg_type, handler in self._handlers.items():
            self.bus.on(msg_type, handler)

    def run(self):
        self._ready.wait()
        while not self._stop_event.wait(self.tick_seconds):
            self.manager.tick()

    def shutdown(self):
        self._stop_event.set()
        for msg_type, handler in self._handlers.items():
            self.bus.remove(msg_type, handler)
        self.manager.shutdown()
        super().shutdown()

    # ---- producer handlers --------------------------------------------------
    def handle_set(self, message: Message):
        self._handle_set(message, controlled=False)

    def handle_set_controlled(self, message: Message):
        LOG.warning("%s is deprecated; send %s with `removable_by_user: "
                    "false`", MSG_SET_CONTROLLED, MSG_SET)
        self._handle_set(message, controlled=True)

    def _handle_set(self, message: Message, controlled: bool) -> None:
        try:
            notification = self._parse_notification(message, controlled)
        except (ValidationError, ValueError, KeyError) as e:
            self._reply(message, MSG_SET, NotificationSetResponseData(
                notification_id=_requested_id(message.data), status="refused",
                reason=f"invalid notification payload: {e}"))
            return
        self._reply(message, MSG_SET,
                    self.manager.set(notification, message.context))

    @staticmethod
    def _parse_notification(message: Message, controlled: bool) -> Notification:
        data = dict(message.data)
        if is_legacy_payload(data):
            return legacy_to_notification(data, removable_by_user=not controlled)
        payload = normalize_skill_id(dict(data.get("notification") or {}))
        if "session" not in payload and message.context.get("session"):
            payload["session"] = message.context["session"]
        if controlled:
            payload["removable_by_user"] = False
        return NotificationSetData(notification=payload).notification

    def handle_remove(self, message: Message):
        try:
            data = NotificationRemoveData(**message.data)
        except ValidationError as e:
            self._reply(message, MSG_REMOVE, NotificationRemoveResponseData(
                notification_ids=[], status="refused",
                reason=f"invalid remove payload: {e}"))
            return
        self._reply(message, MSG_REMOVE,
                    self.manager.remove(data, message.context))

    def handle_remove_controlled(self, message: Message):
        LOG.warning("%s is deprecated; send %s", MSG_REMOVE_CONTROLLED,
                    MSG_REMOVE)
        self.handle_remove(message)

    def handle_get(self, message: Message):
        try:
            data = NotificationGetData(**message.data)
        except ValidationError as e:
            LOG.error("invalid get payload: %s", e)
            self._reply(message, MSG_GET, NotificationGetResponseData())
            return
        self._reply(message, MSG_GET, self.manager.get(data.notification_id))

    def handle_list(self, message: Message):
        try:
            data = NotificationListData(**message.data)
        except ValidationError as e:
            LOG.error("invalid list payload: %s", e)
            self._reply(message, MSG_LIST, NotificationListResponseData(
                notifications=[], states={}))
            return
        self._reply(message, MSG_LIST, self.manager.list(data))

    def handle_snooze(self, message: Message):
        try:
            data = NotificationSnoozeData(**message.data)
        except ValidationError as e:
            self._reply(message, MSG_SNOOZE, NotificationSnoozeResponseData(
                notification_id=_requested_id(message.data), status="refused",
                reason=f"invalid snooze payload: {e}"))
            return
        self._reply(message, MSG_SNOOZE,
                    self.manager.snooze(data, message.context))

    # ---- consumer handlers --------------------------------------------------
    def handle_interaction(self, message: Message):
        if is_forwarded(message.context):
            return
        try:
            data = NotificationInteractionData(**message.data)
        except ValidationError as e:
            LOG.error("invalid interaction payload: %s", e)
            return
        self.manager.interact(data, message.context)

    def handle_sync_request(self, message: Message):
        try:
            data = NotificationSyncRequestData(**message.data)
        except ValidationError as e:
            LOG.error("invalid sync payload: %s", e)
            data = NotificationSyncRequestData()
        self._reply(message, MSG_SYNC, self.manager.sync(data, message.context))

    def handle_ready(self, message: Message):
        count = self.manager.announce_active()
        LOG.info("re-emitted notify for %s active notifications", count)

    # ---- bus plumbing -------------------------------------------------------
    def _reply(self, message: Message, request_type: str,
               data: BaseModel) -> None:
        self.bus.emit(message.reply(request_type + RESPONSE_SUFFIX,
                                    data.model_dump()))

    def _emit_event(self, msg_type: str, data: BaseModel, context: dict) -> None:
        self.bus.emit(Message(msg_type, data.model_dump(), dict(context)))


def _requested_id(data: dict) -> str:
    nested = data.get("notification") or {}
    return str(nested.get("notification_id") or data.get("notification_id")
               or "")
