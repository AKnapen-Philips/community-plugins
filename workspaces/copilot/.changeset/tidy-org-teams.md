---
'@backstage-community/plugin-copilot': minor
'@backstage-community/plugin-copilot-backend': patch
---

Enable organization-team filtering for the configured organization with
`copilot.ingestTeams`, or opt in to organization discovery with
`copilot.ingestOrganizationTeamReports`. The enterprise view's Team filter is
now labelled Enterprise team to distinguish it from organization teams.
Known teams remain selectable regardless of the date range; partial metric
days remain unfilled, and the organization-scoped dashboard applies the
existing five-user privacy check. Selecting a team scopes results to its
owning organization, so same-slug teams remain distinct. Failed options or
dashboard requests can be retried.
