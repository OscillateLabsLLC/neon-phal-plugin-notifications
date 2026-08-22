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
Snooze timers. One `threading.Timer` per snoozed notification; the manager
re-arms them from persisted `renotify_at` on start, so snoozes survive
restarts and consumers never have to track snooze state themselves.
"""

from datetime import datetime, timezone
from threading import Lock, Timer
from typing import Callable, Dict


class SnoozeScheduler:
    def __init__(self):
        self._timers: Dict[str, Timer] = {}
        self._lock = Lock()

    def arm(self, notification_id: str, fire_at: datetime,
            callback: Callable[[], None]) -> None:
        delay = max(0.0, (fire_at - datetime.now(timezone.utc)).total_seconds())
        with self._lock:
            self._cancel_locked(notification_id)
            timer = Timer(delay, self._fire, args=(notification_id, callback))
            timer.daemon = True
            self._timers[notification_id] = timer
            timer.start()

    def cancel(self, notification_id: str) -> None:
        with self._lock:
            self._cancel_locked(notification_id)

    def cancel_all(self) -> None:
        with self._lock:
            for notification_id in list(self._timers):
                self._cancel_locked(notification_id)

    def is_armed(self, notification_id: str) -> bool:
        return notification_id in self._timers

    def _fire(self, notification_id: str, callback: Callable[[], None]) -> None:
        with self._lock:
            self._timers.pop(notification_id, None)
        callback()

    def _cancel_locked(self, notification_id: str) -> None:
        timer = self._timers.pop(notification_id, None)
        if timer:
            timer.cancel()
