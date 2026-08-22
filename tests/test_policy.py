from datetime import datetime, timezone

from neon_data_models.enum import NotificationScope

from neon_phal_plugin_notifications.policy import (ConsumerDismissPolicy,
                                                   EmissionPolicy)
from tests.conftest import ALERTS, SKILL, FakeClock, make_notification


def test_global_refused_for_unlisted_producer():
    allowed, reason = EmissionPolicy().check(
        make_notification(scope=NotificationScope.GLOBAL, target=None))
    assert allowed is False
    assert "GLOBAL" in reason and SKILL in reason


def test_global_allowed_for_seeded_first_party():
    allowed, reason = EmissionPolicy().check(make_notification(
        skill_id=ALERTS, scope=NotificationScope.GLOBAL, target=None))
    assert allowed is True and reason is None


def test_non_removable_refused_unless_listed():
    notification = make_notification(removable_by_user=False)
    assert EmissionPolicy().check(notification)[0] is False
    assert EmissionPolicy(allow_non_removable=[SKILL]).check(notification)[0]


def test_from_config_reads_keys():
    policy = EmissionPolicy.from_config({"allow_global": ["a"],
                                         "allow_non_removable": ["b"],
                                         "rate_limit": 5,
                                         "rate_window_seconds": 10})
    assert policy.allow_global == {"a"}
    assert policy.allow_non_removable == {"b"}
    assert policy.rate_limit == 5
    assert policy.rate_window.total_seconds() == 10


def test_rate_limit_is_a_sliding_window():
    clock = FakeClock()
    policy = EmissionPolicy(rate_limit=2, rate_window_seconds=60, clock=clock)
    notification = make_notification()
    for _ in range(2):
        assert policy.check(notification)[0] is True
        policy.record_accepted(SKILL)
    allowed, reason = policy.check(notification)
    assert allowed is False and "rate limit" in reason
    clock.advance(seconds=61)
    assert policy.check(notification)[0] is True


def test_rate_limit_zero_disables():
    policy = EmissionPolicy(rate_limit=0)
    for _ in range(5):
        policy.record_accepted(SKILL)
    assert policy.check(make_notification())[0] is True


def test_consumer_policy_default_open():
    assert ConsumerDismissPolicy().check("anyone") == (True, None)
    assert ConsumerDismissPolicy().check(None) == (True, None)


def test_consumer_policy_blocklist_and_allowlist():
    blocked = ConsumerDismissPolicy(blocked=["bad"])
    assert blocked.check("bad")[0] is False
    assert blocked.check("good")[0] is True
    allow = ConsumerDismissPolicy(allowed=["good"], blocked=["good"])
    assert allow.check("other")[0] is False
    assert allow.check("good")[0] is False


def test_consumer_policy_from_config():
    policy = ConsumerDismissPolicy.from_config(
        {"consumers_allowed_to_dismiss": ["x"],
         "consumers_blocked_from_dismiss": ["y"]})
    assert policy.allowed == {"x"} and policy.blocked == {"y"}
