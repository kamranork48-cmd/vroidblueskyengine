# Account safety controls

The dashboard provides a persistent global emergency stop and a per-account audit history.

## Emergency stop

- Any editor can activate the stop from the dashboard header.
- Activation stops registered workers and blocks new worker starts, resumes, and manual post imports.
- A confirmed Bluesky account-takedown response automatically activates the stop and marks that account as suspended.
- Only an administrator can clear the global stop. Clearing it does not restart workers.
- An account marked suspended remains blocked from starting until an administrator records a review note. Do this only after checking the account status with Bluesky and resolving any enforcement action.

## API restrictions and audit history

- Explicit 429 responses and access restrictions pause the affected account; the classified category, HTTP status, response code, and any `Retry-After` header are recorded. The system does not automatically retry those operations.
- Account audit history records worker controls, completed follows, API errors, and suspension reviews. It is available in the selected account's Logs panel and is retained independently of the in-memory worker log.
- A rate-limit response is not a safe activity quota. Bluesky limits vary by endpoint, and its [Community Guidelines](https://bsky.social/about/support/community-guidelines) prohibit spammy or disruptive bulk activity. This project does not attempt to evade platform limits or enforcement.

## Investigation limits

Historical logs include an explicit `AccountTakedown` response during session creation and a separate 429 from `com.atproto.server.createSession` with a roughly 23-hour retry interval and a `10;w=86400` rate-limit policy. Those records confirm the returned API errors; without account/action/timestamp correlation, they do not establish what activity caused a takedown.
