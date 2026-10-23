---
'@backstage-community/plugin-copilot-backend': minor
'@backstage-community/plugin-copilot': minor
'@backstage-community/plugin-copilot-common': minor
---

Add opt-in cost-center filtering and a separate **No cost center** dashboard scope. Incomplete GitHub membership snapshots are not applied, preventing partial rosters from skewing cost-center aggregates. When GitHub does not provide the organization's Copilot team report, enterprise team filtering can use the configured organization's memberships when `copilot.ingestTeams` is enabled. Backfills preserve existing totals and limit newly required user, team, and cost-center data to GitHub's rolling report window.
