---
'@backstage-community/plugin-copilot-backend': patch
---

Commit daily Copilot reports atomically so overlapping scheduled imports and
backfills can safely reuse completed data without expiring work leases.
