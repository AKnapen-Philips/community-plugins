---
'@backstage-community/plugin-copilot': minor
'@backstage-community/plugin-copilot-backend': patch
---

Enable organization-team filtering in the enterprise dashboard when
`copilot.enterprise`, `copilot.organization`, and `copilot.ingestTeams` are
configured. The enterprise view's Team filter is now labelled Enterprise team
to distinguish it from organization teams. Known organization teams remain
selectable when the selected date range has partial report coverage; available
metrics are shown without filling missing days with zeros, and teams without
qualifying data display an empty state. Organization-team data that is still
syncing no longer triggers a warning, and failed options or dashboard requests
can be retried.
