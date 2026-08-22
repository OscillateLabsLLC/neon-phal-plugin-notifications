from datetime import datetime, timedelta, timezone
from threading import Event

from neon_phal_plugin_notifications.scheduler import SnoozeScheduler


def test_scheduler_fires_and_clears():
    scheduler = SnoozeScheduler()
    fired = Event()
    scheduler.arm("n1", datetime.now(timezone.utc), fired.set)
    assert fired.wait(2)
    assert not scheduler.is_armed("n1")


def test_scheduler_cancel_and_rearm():
    scheduler = SnoozeScheduler()
    fired = Event()
    far = datetime.now(timezone.utc) + timedelta(hours=1)
    scheduler.arm("n1", far, fired.set)
    scheduler.arm("n1", far, fired.set)
    assert scheduler.is_armed("n1")
    scheduler.cancel("n1")
    assert not scheduler.is_armed("n1")
    scheduler.arm("n2", far, fired.set)
    scheduler.cancel_all()
    assert not scheduler.is_armed("n2")
    assert not fired.is_set()
