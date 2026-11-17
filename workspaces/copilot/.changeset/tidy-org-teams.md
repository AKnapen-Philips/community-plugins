---
'@backstage-community/plugin-copilot': minor
'@backstage-community/plugin-copilot-backend': patch
---

Enable organization-team filtering for the configured organization with
`copilot.ingestTeams`, or opt in to organization discovery with
`copilot.ingestOrganizationTeamReports`. The enterprise view's Team filter is
now labelled Enterprise team to distinguish it from organization teams.
Organization-team options use the selected date range and existing five-user
eligibility threshold; partial metric days remain unfilled. Selecting a team
scopes results to its owning organization, so same-slug teams remain
distinct. Failed options or dashboard requests can be retried.
