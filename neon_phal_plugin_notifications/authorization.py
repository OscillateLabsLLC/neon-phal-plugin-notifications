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
Authorization decisions for consumer remove/dismiss requests.
"""

from typing import Optional, Tuple

from neon_data_models.enum import DismissPolicy
from neon_data_models.models.api.messagebus.notifications import \
    NotificationRemoveData

from neon_phal_plugin_notifications import identity
from neon_phal_plugin_notifications.policy import ConsumerDismissPolicy
from neon_phal_plugin_notifications.store import StoredNotification

UNKNOWN_CONSUMER = "unknown"

# (refusal_reason, dismissed_by, per_client_id); reason None means allowed
RemovalVerdict = Tuple[Optional[str], Optional[str], Optional[str]]


def requester_identity(data: NotificationRemoveData,
                       context: dict) -> Optional[str]:
    """
    Who is asking: `data.dismissed_by` (HANA REST dismiss), else the payload
    `skill_id` (a producer removing its own), else the context's consumer.
    """
    return (data.dismissed_by or data.skill_id
            or identity.consumer_id(context))


def per_client_id(record: StoredNotification,
                  consumer: Optional[str]) -> Optional[str]:
    """Client to record a dismissal against, for PER_CLIENT notifications."""
    if record.notification.dismiss_policy == DismissPolicy.PER_CLIENT:
        return consumer
    return None


def authorize_removal(record: StoredNotification, data: NotificationRemoveData,
                      context: dict, policy: ConsumerDismissPolicy
                      ) -> RemovalVerdict:
    """A requester identified as the producer bypasses `removable_by_user`."""
    consumer = requester_identity(data, context)
    if consumer == record.skill_id:
        return None, record.skill_id, None
    allowed, reason = policy.check(consumer)
    if not allowed:
        return reason, None, None
    if not record.notification.removable_by_user:
        return (f"notification '{record.notification_id}' is only removable "
                f"by its producer '{record.skill_id}'"), None, None
    client_id = per_client_id(record, consumer)
    if (record.notification.dismiss_policy == DismissPolicy.PER_CLIENT
            and client_id is None):
        return ("PER_CLIENT dismissal requires a client identity in the "
                "message context"), None, None
    return None, consumer or UNKNOWN_CONSUMER, client_id


def snooze_refusal(record: Optional[StoredNotification],
                   duration: int) -> Optional[str]:
    if record is None:
        return "unknown notification"
    if not record.is_live():
        return f"notification is {record.state.value}"
    if duration <= 0:
        return "duration must be a positive number of seconds"
    return None
