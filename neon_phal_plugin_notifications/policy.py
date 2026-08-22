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
Policy decisions for the Notification Manager.

Nothing here is security against a hostile local process; the messagebus has
no authentication. These checks contain buggy or obnoxious producers and keep
high-impact notifications deliberate.
"""

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Callable, Deque, Dict, Iterable, List, Optional, Tuple

from neon_data_models.enum import NotificationScope
from neon_data_models.models.base.notifications import Notification

# First-party producers are seeded onto the high-impact allowlists so that
# out-of-the-box alerts work without configuration.
DEFAULT_ALLOW_GLOBAL: List[str] = ["skill-alerts.neongeckocom"]
DEFAULT_ALLOW_NON_REMOVABLE: List[str] = ["skill-alerts.neongeckocom"]
DEFAULT_RATE_LIMIT = 30
DEFAULT_RATE_WINDOW_SECONDS = 3600

PolicyVerdict = Tuple[bool, Optional[str]]
Clock = Callable[[], datetime]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EmissionPolicy:
    """
    Producer-side policy: who may emit GLOBAL or non-removable notifications,
    and how many `set` requests a producer may have accepted per window.
    """

    def __init__(self, allow_global: Optional[Iterable[str]] = None,
                 allow_non_removable: Optional[Iterable[str]] = None,
                 rate_limit: int = DEFAULT_RATE_LIMIT,
                 rate_window_seconds: int = DEFAULT_RATE_WINDOW_SECONDS,
                 clock: Clock = utc_now):
        self.allow_global = set(allow_global if allow_global is not None
                                else DEFAULT_ALLOW_GLOBAL)
        self.allow_non_removable = set(
            allow_non_removable if allow_non_removable is not None
            else DEFAULT_ALLOW_NON_REMOVABLE)
        self.rate_limit = int(rate_limit)
        self.rate_window = timedelta(seconds=int(rate_window_seconds))
        self._clock = clock
        self._accepted: Dict[str, Deque[datetime]] = defaultdict(deque)

    @classmethod
    def from_config(cls, config: dict, clock: Clock = utc_now) -> "EmissionPolicy":
        return cls(allow_global=config.get("allow_global"),
                   allow_non_removable=config.get("allow_non_removable"),
                   rate_limit=config.get("rate_limit", DEFAULT_RATE_LIMIT),
                   rate_window_seconds=config.get("rate_window_seconds",
                                                  DEFAULT_RATE_WINDOW_SECONDS),
                   clock=clock)

    def check(self, notification: Notification) -> PolicyVerdict:
        """
        Decide whether `notification` may be accepted. Does NOT consume rate
        budget; call `record_accepted` once the manager stores it.
        """
        skill_id = notification.skill_id
        if (notification.scope == NotificationScope.GLOBAL
                and skill_id not in self.allow_global):
            return False, (f"'{skill_id}' is not permitted to emit GLOBAL "
                           f"notifications (config key `allow_global`)")
        if (not notification.removable_by_user
                and skill_id not in self.allow_non_removable):
            return False, (f"'{skill_id}' is not permitted to emit "
                           f"non-removable notifications (config key "
                           f"`allow_non_removable`)")
        if self._rate_exceeded(skill_id):
            return False, (f"'{skill_id}' exceeded the rate limit of "
                           f"{self.rate_limit} notifications per "
                           f"{int(self.rate_window.total_seconds())}s")
        return True, None

    def record_accepted(self, skill_id: str) -> None:
        self._accepted[skill_id].append(self._clock())

    def _rate_exceeded(self, skill_id: str) -> bool:
        if self.rate_limit <= 0:
            return False
        window_start = self._clock() - self.rate_window
        history = self._accepted[skill_id]
        while history and history[0] < window_start:
            history.popleft()
        return len(history) >= self.rate_limit


class ConsumerDismissPolicy:
    """
    Consumer-side policy: which consumers may dismiss or remove notifications.
    Defaults wide open. An allowlist, when set, wins over the
    blocklist: a consumer must be listed AND not blocked.
    """

    def __init__(self, allowed: Optional[Iterable[str]] = None,
                 blocked: Optional[Iterable[str]] = None):
        self.allowed = set(allowed) if allowed else None
        self.blocked = set(blocked or [])

    @classmethod
    def from_config(cls, config: dict) -> "ConsumerDismissPolicy":
        return cls(allowed=config.get("consumers_allowed_to_dismiss"),
                   blocked=config.get("consumers_blocked_from_dismiss"))

    def check(self, consumer_id: Optional[str]) -> PolicyVerdict:
        if consumer_id in self.blocked:
            return False, (f"consumer '{consumer_id}' is blocked from "
                           f"dismissing notifications")
        if self.allowed is not None and consumer_id not in self.allowed:
            return False, (f"consumer '{consumer_id}' is not on the dismiss "
                           f"allowlist")
        return True, None
