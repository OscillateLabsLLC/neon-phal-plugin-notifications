# neon-phal-plugin-notifications

Notification Manager for Neon/OVOS, packaged as a PHAL plugin. It is the single
chokepoint for `ovos.notification.api.*` traffic on the hub messagebus: it
validates producer requests, owns notification state (active / snoozed /
dismissed / expired), persists that state across restarts, enforces emission
and dismissal policy, and emits `notify` / `dismiss` / `snoozed` events for
consumers (the Node app today, other plugins later).

The plugin never renders anything and never talks to MQ. Bus in, bus out.
Transport lives in `neon-messagebus-mq-connector` and `neon-hana`; rendering
lives in consumers.

## Requirements

Notification data models come from `neon-data-models`. Until the next release
they live on the `FEAT_Notifications` branch; install that branch first:

```bash
pip install git+https://github.com/NeonGeckoCom/neon-data-models@FEAT_Notifications
pip install neon-phal-plugin-notifications
```

## Configuration

All keys live under the plugin's PHAL section. Every key is optional.

```yaml
PHAL:
  neon-phal-plugin-notifications:
    store: json                      # persistence adapter; only "json" ships
    store_path: null                 # override the JSON file path (default:
                                     # $XDG_DATA_HOME/neon/neon-phal-plugin-notifications/notifications.json)
    allow_global:                    # skill_ids permitted to use scope=GLOBAL
      - skill-alerts.neongeckocom
    allow_non_removable:             # skill_ids permitted to set removable_by_user=false
      - skill-alerts.neongeckocom
    rate_limit: 30                   # accepted `set` requests per producer per window
    rate_window_seconds: 3600
    consumers_allowed_to_dismiss: [] # empty/absent = every consumer may dismiss
    consumers_blocked_from_dismiss: []
    retention_max_age_days: 7        # tombstone window for dismissed/expired entries
    tick_seconds: 60                 # expiry + retention sweep interval
```

Policy defaults are asymmetric on purpose: dismissal is open, high-impact
emission (GLOBAL scope, non-removable) is closed except for seeded first-party
producers. An unlisted producer that requests either is refused with a reason
in `.set.response`; nothing is silently downgraded.

## Message API

Every request gets a `.response` reply on the request's own context, so MQ
routing survives the round trip.

| Consumed | Reply |
| --- | --- |
| `ovos.notification.api.set` `{notification}` | `.set.response` `{notification_id, status, reason?}` |
| `ovos.notification.api.remove` `{notification_id?, skill_id?, dismissed_by?}` | `.remove.response` `{notification_ids, status, reason?}` |
| `ovos.notification.api.get` `{notification_id}` | `.get.response` `{notification}` |
| `ovos.notification.api.list` `{skill_id?, state?, since?, node_id?, user_id?}` | `.list.response` `{notifications, states}` |
| `ovos.notification.api.snooze` `{notification_id, duration}` | `.snooze.response` `{notification_id, status, renotify_at?, reason?}` |
| `ovos.notification.api.interaction` `{notification_id, action_id, callback_data?}` | none; re-emitted for the producer, then dismissed if the action's `dismiss_on_activate` is true |
| `ovos.notification.api.sync.request` `{since?}` | `.sync.request.response` `{notifications, states, server_time}` plus one `notify` per entry |
| `ovos.notification.api.set.controlled` / `.remove.controlled` | deprecated wrappers; reply as `.set.response` / `.remove.response` |

| Emitted | When |
| --- | --- |
| `ovos.notification.api.notify` `{notification}` | a `set` is accepted, a snooze elapses, `mycroft.ready`, `sync.request` |
| `ovos.notification.api.dismiss` `{notification_id, skill_id, scope, target, dismissed_by}` | a notification is removed, dismissed, or expires (`dismissed_by: "expired"`) |
| `ovos.notification.api.snoozed` `{notification_id, renotify_at, scope, target}` | a snooze is granted |

Behavior notes:

- `set` is an upsert. The same `skill_id` re-sending an existing
  `notification_id` replaces it in place, resets it to ACTIVE, and re-emits
  `notify`. A `notification_id` owned by a different `skill_id` is refused.
- `created_at` is stamped by the manager on first accept and preserved on
  updates; `updated_at` is bumped on every change (upsert, snooze,
  re-notify, dismissal, expiry, per-client dismissal) and rides on the wire
  `Notification`. Every `since` filter (`list`, `sync.request`, and so the
  HANA REST catch-up) compares against `updated_at`.
- Retention is a tombstone window, not history: DISMISSED and EXPIRED entries
  stay for `retention_max_age_days` so a node that was offline learns on
  catch-up that a notification was dismissed elsewhere, then they are pruned.
  ACTIVE and SNOOZED entries are never pruned.
- A missing `session` is filled from `Message.context.session`. A CLIENT-scoped
  notification with no `target` takes the requester's node identity from the
  context (`context.node.node_id`, then `context.client_id`, then a non-local
  `session.session_id`).
- `remove` needs `notification_id` (the producer is resolved from the stored
  record; a `skill_id` that disagrees with it is refused) or `skill_id` alone
  (remove everything that producer owns). Neither is refused. The requester
  is `dismissed_by`, else `skill_id`, else the context's node/session
  identity. A requester equal to the producer bypasses `removable_by_user`;
  anyone else is a consumer subject to `removable_by_user` and the consumer
  dismiss lists.
- `dismiss_policy=PER_CLIENT`: a consumer dismissal records that client and
  leaves the shared state ACTIVE; `list` with `node_id` reports DISMISSED for
  that client only. `SHARED` dismissals set DISMISSED for everyone.
- Legacy flat GUI-API payloads (`sender`, `text`, `action`, `type`, `style`)
  are up-converted with a deprecation log.

## Testing

```bash
pip install -e .[test]
pytest tests/
```
