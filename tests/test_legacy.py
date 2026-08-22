import pytest

from neon_data_models.enum import NotificationPersistence, NotificationStyle

from neon_phal_plugin_notifications.legacy import (is_legacy_payload,
                                                   legacy_to_notification,
                                                   normalize_skill_id)


def test_is_legacy_payload():
    assert is_legacy_payload({"sender": "x", "text": "hi"})
    assert not is_legacy_payload({"notification": {"text": "hi"}})
    assert not is_legacy_payload({})


def test_legacy_up_conversion():
    notification = legacy_to_notification({
        "sender": "skill-legacy", "text": "hi", "action": "open.it",
        "type": "sticky", "style": "warning", "callback_data": {"k": 1}})
    assert notification.skill_id == "skill-legacy"
    assert notification.text == "hi"
    assert notification.style == NotificationStyle.WARNING
    assert notification.display.tray == NotificationPersistence.STICKY
    assert [a.action_id for a in notification.actions] == ["open.it"]
    assert notification.callback_data == {"k": 1}
    assert notification.removable_by_user is True


def test_legacy_transient_and_unknown_style():
    notification = legacy_to_notification(
        {"sender": "s", "text": "t", "type": "transient", "style": "weird"},
        removable_by_user=False)
    assert notification.display.tray == NotificationPersistence.EPHEMERAL
    assert notification.style == NotificationStyle.INFO
    assert notification.actions == []
    assert notification.removable_by_user is False


def test_legacy_without_sender_raises():
    with pytest.raises(ValueError):
        legacy_to_notification({"text": "t"})


def test_normalize_skill_id_prefers_skill_id():
    assert normalize_skill_id({"sender": "a", "skill_id": "b"})["skill_id"] == "b"
    assert normalize_skill_id({"sender": "a"})["skill_id"] == "a"
