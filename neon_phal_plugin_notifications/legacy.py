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
Up-conversion of the legacy OVOS GUI notification payload.

The legacy `ovos.notification.api.set` carried flat `data` keys
(`sender`, `text`, `action`, `type`, `style`, `callback_data`). The manager
accepts that shape, logs a deprecation, and converts it to a `Notification`.
"""

from typing import Optional

from ovos_utils.log import LOG

from neon_data_models.enum import NotificationPersistence, NotificationStyle
from neon_data_models.models.base.notifications import (
    Notification, NotificationAction, NotificationDisplayHints)

LEGACY_TYPE_TO_TRAY = {
    "sticky": NotificationPersistence.STICKY,
    "transient": NotificationPersistence.EPHEMERAL,
}


def is_legacy_payload(data: dict) -> bool:
    """True when `data` is the flat GUI-API shape, not `{"notification": ...}`."""
    return "notification" not in data and "text" in data


def normalize_skill_id(payload: dict) -> dict:
    """Accept the deprecated `sender` key in place of `skill_id` (in place)."""
    if "skill_id" not in payload and payload.get("sender"):
        LOG.warning("`sender` is deprecated; use `skill_id` (sender=%s)",
                    payload["sender"])
        payload["skill_id"] = payload["sender"]
    return payload


def legacy_to_notification(data: dict, removable_by_user: bool = True
                           ) -> Notification:
    """
    Build a `Notification` from a legacy flat payload. Raises `ValueError`
    when no producer identity is present.
    """
    payload = normalize_skill_id(dict(data))
    if not payload.get("skill_id"):
        raise ValueError("legacy notification payload has no `sender`")
    LOG.warning("Legacy flat notification payload received from '%s'; send "
                "`{\"notification\": Notification}` instead",
                payload["skill_id"])
    return Notification(
        skill_id=payload["skill_id"],
        text=payload["text"],
        detail=payload.get("detail", ""),
        style=_legacy_style(payload.get("style")),
        display=_legacy_display(payload.get("type")),
        actions=_legacy_actions(payload.get("action")),
        callback_data=payload.get("callback_data") or {},
        removable_by_user=removable_by_user,
    )


def _legacy_style(style: Optional[str]) -> NotificationStyle:
    try:
        return NotificationStyle(style)
    except ValueError:
        return NotificationStyle.INFO


def _legacy_display(legacy_type: Optional[str]) -> NotificationDisplayHints:
    tray = LEGACY_TYPE_TO_TRAY.get(legacy_type or "", NotificationPersistence.STICKY)
    return NotificationDisplayHints(tray=tray)


def _legacy_actions(action: Optional[str]) -> list:
    if not action:
        return []
    return [NotificationAction(action_id=action, label=action)]
