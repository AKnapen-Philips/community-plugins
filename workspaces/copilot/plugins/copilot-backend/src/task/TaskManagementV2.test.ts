/*
 * Copyright 2026 The Backstage Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Config } from '@backstage/config';
import { LoggerService } from '@backstage/backend-plugin-api';
import { DateTime } from 'luxon';
import {
  V2UserMetricRow,
  V2UserTeamRow,
} from '@backstage-community/plugin-copilot-common';
import type { OrganizationTeamMembership } from '../client/GithubClientV2';
import type { UserBreakdownData } from '../utils/reportParser';
import { DatabaseHandlerV2 } from '../db/DatabaseHandlerV2';
import { GithubClientV2 } from '../client/GithubClientV2';
import { TaskManagementV2, runBackfill } from './TaskManagementV2';

describe('TaskManagementV2', () => {
  it('does not retry newly required user and team reports outside their retention window', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const rollingStart = DateTime.utc()
      .startOf('day')
      .minus({ years: 1 })
      .toISODate()!;
    const reportWindowStart =
      rollingStart > '2025-10-10' ? rollingStart : '2025-10-10';
    const backfillFromDate = DateTime.fromISO(reportWindowStart, {
      zone: 'utc',
    })
      .minus({ days: 1 })
      .toISODate()!;
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': backfillFromDate,
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });

    db.getMissingDays.mockResolvedValueOnce([]).mockResolvedValueOnce([]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      1,
      'enterprise',
      'ent-1',
      backfillFromDate,
      expect.any(String),
      ['totals'],
    );
    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      reportWindowStart,
      expect.any(String),
      ['totals', 'users', 'teams'],
    );
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
  });

  it('serializes overlapping backfills for the same scope', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({ 'copilot.backfillDelayMs': 0 });
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 3 }).toISODate()!;
    let startFirstQuery!: () => void;
    let releaseFirstQuery!: () => void;
    const firstQueryStarted = new Promise<void>(resolve => {
      startFirstQuery = resolve;
    });
    const firstQueryGate = new Promise<void>(resolve => {
      releaseFirstQuery = resolve;
    });
    db.getMissingDays
      .mockImplementationOnce(async () => {
        startFirstQuery();
        await firstQueryGate;
        return [];
      })
      .mockResolvedValue([]);

    const firstTask = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });
    const secondTask = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    const firstRun = firstTask.runBackfill(day, day, 'enterprise', 'ent-1');
    await firstQueryStarted;
    const secondRun = secondTask.runBackfill(day, day, 'enterprise', 'ent-1');
    await Promise.resolve();

    expect(db.getMissingDays).toHaveBeenCalledTimes(1);

    releaseFirstQuery();
    await Promise.all([firstRun, secondRun]);

    expect(db.getMissingDays).toHaveBeenCalledTimes(2);
  });

  it('continues loading totals outside the user and team report window', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const rollingStart = DateTime.utc()
      .startOf('day')
      .minus({ years: 1 })
      .toISODate()!;
    const reportWindowStart =
      rollingStart > '2025-10-10' ? rollingStart : '2025-10-10';
    const outsideWindowDay = DateTime.fromISO(reportWindowStart, {
      zone: 'utc',
    })
      .minus({ days: 1 })
      .toISODate()!;
    const config = createConfigMock({
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });

    db.getMissingDays
      .mockResolvedValueOnce([outsideWindowDay])
      .mockResolvedValueOnce([]);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/totals.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument(outsideWindowDay),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(
      outsideWindowDay,
      outsideWindowDay,
      'enterprise',
      'ent-1',
    );

    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      1,
      'enterprise',
      'ent-1',
      outsideWindowDay,
      outsideWindowDay,
      ['totals'],
    );
    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      reportWindowStart,
      outsideWindowDay,
      ['totals', 'users', 'teams'],
    );
    expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledWith(
      outsideWindowDay,
    );
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day: outsideWindowDay,
        status: 'success',
        components_loaded: '["totals"]',
      }),
    );
  });

  it('runAsync with no missing days exits without API calls', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue([]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getMissingDays).toHaveBeenCalledTimes(1);
    expect(db.getMissingDays).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      '2026-05-10',
      expect.any(String),
      ['totals'],
    );
    expect(api.fetchEnterpriseReportLinks).not.toHaveBeenCalled();
  });

  it('runAsync with one missing day ingests once', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10']);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-10'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-10'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledWith('2026-05-10');
    expect(db.insertByVscodeAgent).toHaveBeenCalledWith([
      expect.objectContaining({
        day: '2026-05-10',
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        session_count: 2,
        total_user_messages: 8,
      }),
    ]);
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day: '2026-05-10',
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status: 'success',
      }),
    );
  });

  it('runAsync continues to next day when one day fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10', '2026-05-11']);

    api.fetchEnterpriseReportLinks.mockImplementation(async day => {
      if (day === '2026-05-10') {
        throw new Error('enterprise report failed');
      }
      return {
        download_links: ['https://downloads.github.com/report-day2.json'],
      };
    });

    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams-day2.json'],
    });

    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-11'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-11'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledTimes(2);
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day: '2026-05-10', status: 'error' }),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day: '2026-05-11', status: 'success' }),
    );
    expect(logger.error).toHaveBeenCalled();
  });

  it('runBackfill processes only the specified range', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-12']);
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.fetchOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-12'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-12'),
    );

    await runBackfill(
      {
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config,
        logger,
      },
      '2026-05-12',
      '2026-05-12',
      'organization',
      'org-1',
    );

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'org-1',
      '2026-05-12',
      '2026-05-12',
      ['totals'],
    );
    expect(api.fetchOrganizationReportLinks).toHaveBeenCalledWith('2026-05-12');
    expect(api.fetchOrganizationUserTeamsLinks).not.toHaveBeenCalled();
  });

  it('ingestDay flow with ingestTeams false does not call team endpoints', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10']);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-10'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-10'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
  });

  it('runAsync records partial status when required team components are missing', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10']);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-10'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-10'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      '2026-05-10',
      expect.any(String),
      ['totals', 'users', 'teams'],
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day: '2026-05-10',
        status: 'partial',
        components_loaded: JSON.stringify(['totals']),
        error_message: 'Missing components: users, teams',
      }),
    );
  });

  it('reuses stored per-user reports when retrying a partial team day', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 5 }).toISODate()!;
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': day,
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const storedUserMetric = {
      day,
      metrics_type: 'enterprise',
      entity_id: 'ent-1',
      user_id: 1,
      user_login: 'octocat',
      used_agent: true,
      used_chat: true,
      used_cli: false,
      used_vscode_agent: false,
      code_acceptance_activity_count: 3,
      code_generation_activity_count: 4,
      loc_added_sum: 10,
      loc_deleted_sum: 2,
      loc_suggested_to_add_sum: 12,
      loc_suggested_to_delete_sum: 3,
      user_initiated_interaction_count: 7,
      ai_credits_used: 5.5,
    };
    const storedBreakdown = {
      user_id: 1,
      user_login: 'octocat',
      byFeature: [],
      byIde: [],
      byLanguageFeature: [],
      byModelFeature: [],
      byLanguageModel: [],
      customizations: [],
    };

    db.getMissingDays.mockResolvedValue([day]);
    db.getIngestionLog.mockResolvedValue([
      {
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status: 'partial',
        components_loaded: '["totals","users"]',
        source: 'scheduled',
      },
    ]);
    db.getUserMetricsForDay.mockResolvedValue([storedUserMetric]);
    db.getUserBreakdownsForDay.mockResolvedValue([storedBreakdown]);
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument(day),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getUserMetricsForDay).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
    );
    expect(db.getUserBreakdownsForDay).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
    );
    expect(api.fetchEnterpriseReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(day);
    expect(db.replaceTeamAggregates).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'native',
      [expect.objectContaining({ day, team_slug: 'platform' })],
      expect.objectContaining({
        dailyTotals: [expect.objectContaining({ day, team_slug: 'platform' })],
      }),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'success',
        components_loaded: '["totals","users","teams"]',
      }),
    );
  });

  it('switches to organization teams only after a complete snapshot rebuild', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const storedMetrics = [
      makeStoredUserMetric(day, {
        user_id: 7,
        user_login: 'user-seven',
        loc_added_sum: 17,
      }),
      makeStoredUserMetric(day, {
        user_id: 8,
        user_login: 'user-eight',
        loc_added_sum: 19,
      }),
    ];
    const memberships: OrganizationTeamMembership[] = [
      { teamId: 101, teamSlug: 'demo-parent', userId: 7 },
      { teamId: 101, teamSlug: 'demo-parent', userId: 8 },
      { teamId: 102, teamSlug: 'demo-child', userId: 8 },
    ];

    db.getMissingDays.mockResolvedValue([]);
    db.getTeamAggregationState
      .mockResolvedValueOnce({
        activeSource: 'native',
        activeGeneration: 'native-default',
      })
      .mockResolvedValue({
        activeSource: 'organization-fallback',
        activeGeneration: 'fallback-generation',
      });
    db.beginTeamAggregationTransition.mockResolvedValue({
      activeSource: 'native',
      activeGeneration: 'native-default',
      pendingSource: 'organization-fallback',
      pendingGeneration: 'fallback-generation',
    });
    db.getUserMetricDays.mockResolvedValue([day]);
    db.getMissingTeamAggregationDays.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue(storedMetrics);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(7, 'user-seven'),
      makeStoredUserBreakdown(8, 'user-eight'),
    ]);
    const membershipSnapshot = {
      generation: 'fallback-generation',
      attemptToken: 'fallback-attempt',
    };
    db.beginTeamMembershipSnapshot.mockResolvedValue({
      ...membershipSnapshot,
      cursor: null,
      complete: false,
    });
    db.getTeamMembershipSnapshotState.mockResolvedValue({
      ...membershipSnapshot,
      cursor: null,
      complete: true,
    });
    db.getTeamMembershipSnapshotBatchForDay
      .mockResolvedValueOnce({
        memberships: [
          makeStoredUserTeam(day, {
            user_id: 7,
            user_login: 'user-seven',
            team_id: 101,
            team_slug: 'demo-parent',
          }),
        ],
        nextCursor: 1,
      })
      .mockResolvedValueOnce({
        memberships: [
          makeStoredUserTeam(day, {
            user_id: 8,
            user_login: 'user-eight',
            team_id: 101,
            team_slug: 'demo-parent',
          }),
          makeStoredUserTeam(day, {
            user_id: 8,
            user_login: 'user-eight',
            team_id: 102,
            team_slug: 'demo-child',
          }),
        ],
        nextCursor: null,
      });
    db.completeStagedTeamAggregationDay.mockResolvedValue(true);
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    api.fetchOrganizationTeamMemberships.mockImplementation(
      async (options: any) => {
        await options.onPage({
          memberships,
          cursor: null,
          complete: true,
        });
        return [];
      },
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.probeOrganizationUserTeamsReport).toHaveBeenCalledWith(
      expect.any(String),
    );
    expect(api.fetchOrganizationTeamMemberships).toHaveBeenCalledTimes(1);
    expect(db.saveTeamMembershipSnapshotBatch).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      expect.objectContaining(membershipSnapshot),
      memberships,
      null,
      true,
    );
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.beginTeamAggregationTransition).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      'organization-fallback',
    );
    expect(db.beginTeamAggregationDayStaging).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      membershipSnapshot,
    );
    expect(db.getTeamMembershipSnapshotBatchForDay).toHaveBeenNthCalledWith(
      1,
      'enterprise',
      'ent-1',
      membershipSnapshot,
      day,
      0,
    );
    expect(db.getTeamMembershipSnapshotBatchForDay).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      membershipSnapshot,
      day,
      1,
    );
    expect(db.stageTeamAggregationBatch).toHaveBeenCalledTimes(2);
    expect(db.stageTeamAggregationBatch).toHaveBeenNthCalledWith(
      1,
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      [
        {
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 7,
          user_login: 'user-seven',
          team_id: 101,
          team_slug: 'demo-parent',
        },
      ],
      expect.objectContaining({
        dailyTotals: [
          expect.objectContaining({
            team_slug: 'demo-parent',
            daily_active_users: 1,
            loc_added_sum: 17,
          }),
        ],
      }),
      membershipSnapshot,
    );
    expect(db.stageTeamAggregationBatch).toHaveBeenNthCalledWith(
      2,
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      expect.arrayContaining([
        expect.objectContaining({ user_id: 8, team_slug: 'demo-parent' }),
        expect.objectContaining({ user_id: 8, team_slug: 'demo-child' }),
      ]),
      expect.objectContaining({
        dailyTotals: expect.arrayContaining([
          expect.objectContaining({
            team_slug: 'demo-parent',
            daily_active_users: 1,
            loc_added_sum: 19,
          }),
          expect.objectContaining({
            team_slug: 'demo-child',
            daily_active_users: 1,
            loc_added_sum: 19,
          }),
        ]),
      }),
      membershipSnapshot,
    );
    expect(db.completeStagedTeamAggregationDay).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      membershipSnapshot,
      [day],
    );
    expect(db.activateTeamAggregationSource).not.toHaveBeenCalled();
  });

  it('does not activate when the organization membership snapshot is incomplete', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue([]);
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    db.beginTeamAggregationTransition.mockResolvedValue({
      activeSource: 'native',
      activeGeneration: 'native-default',
      pendingSource: 'organization-fallback',
      pendingGeneration: 'pending-generation',
    });
    db.beginTeamMembershipSnapshot.mockResolvedValue({
      generation: 'pending-generation',
      attemptToken: 'pending-attempt',
      cursor: null,
      complete: false,
    });
    api.fetchOrganizationTeamMemberships.mockRejectedValue(
      new Error('incomplete GraphQL membership page'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.beginTeamAggregationTransition).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      'organization-fallback',
    );
    expect(api.fetchOrganizationTeamMemberships).toHaveBeenCalledWith(
      expect.objectContaining({ afterCursor: null }),
    );
    expect(db.stageTeamAggregationBatch).not.toHaveBeenCalled();
    expect(db.completeStagedTeamAggregationDay).not.toHaveBeenCalled();
    expect(db.activateTeamAggregationSource).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'Unable to complete the organization team membership snapshot',
      ),
      expect.any(Error),
    );
  });

  it('ingests fallback enterprise days without fetching native user-team reports', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const memberships: OrganizationTeamMembership[] = [
      { teamId: 101, teamSlug: 'demo-parent', userId: 1 },
      { teamId: 101, teamSlug: 'demo-parent', userId: 2 },
    ];

    db.getMissingDays.mockResolvedValueOnce([day]).mockResolvedValueOnce([]);
    db.getUserMetricsForDay.mockResolvedValue([]);
    db.getTeamAggregationState.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-generation',
    });
    const membershipSnapshot = {
      generation: 'run-snapshot',
      attemptToken: 'run-snapshot-attempt',
    };
    db.beginTeamMembershipSnapshot.mockResolvedValue({
      ...membershipSnapshot,
      cursor: null,
      complete: false,
    });
    db.getTeamMembershipSnapshotState.mockResolvedValue({
      ...membershipSnapshot,
      cursor: null,
      complete: true,
    });
    db.getTeamMembershipSnapshotBatchForDay.mockResolvedValue({
      memberships: [
        makeStoredUserTeam(day, {
          user_id: 1,
          user_login: 'octocat',
          team_id: 101,
          team_slug: 'demo-parent',
        }),
      ],
      nextCursor: null,
    });
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    api.fetchOrganizationTeamMemberships.mockImplementation(
      async (options: any) => {
        await options.onPage({
          memberships,
          cursor: null,
          complete: true,
        });
        return [];
      },
    );
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserMetricsDocument(day),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(api.fetchOrganizationUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.beginTeamAggregationDayStaging).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      membershipSnapshot,
    );
    expect(db.stageTeamAggregationBatch).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      [
        expect.objectContaining({
          user_id: 1,
          user_login: 'octocat',
          team_slug: 'demo-parent',
        }),
      ],
      expect.objectContaining({
        dailyTotals: [
          expect.objectContaining({
            team_slug: 'demo-parent',
            daily_active_users: 1,
          }),
        ],
      }),
      membershipSnapshot,
    );
    expect(db.replaceStagedTeamAggregates).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      membershipSnapshot,
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'success',
        components_loaded: JSON.stringify(['totals', 'users', 'teams']),
      }),
    );
  });

  it('rebuilds fallback teams from stored user metrics when the user report has no links', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const storedMetric = makeStoredUserMetric(day);
    const storedTeam = makeStoredUserTeam(day);

    db.getMissingDays.mockResolvedValueOnce([day]).mockResolvedValueOnce([]);
    db.getTeamAggregationState.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-generation',
    });
    db.getUserMetricsForDay.mockResolvedValue([storedMetric]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(storedMetric.user_id, storedMetric.user_login),
    ]);
    db.getTeamMembershipSnapshotBatchForDay.mockResolvedValue({
      memberships: [storedTeam],
      nextCursor: null,
    });
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    api.fetchEnterpriseReportLinks.mockResolvedValue({ download_links: [] });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: [],
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.getUserMetricsForDay).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
    );
    expect(db.beginTeamAggregationDayStaging).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      {
        generation: 'snapshot-generation',
        attemptToken: 'snapshot-generation-attempt',
      },
    );
    expect(db.stageTeamAggregationBatch).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      [storedTeam],
      expect.objectContaining({
        dailyTotals: expect.arrayContaining([
          expect.objectContaining({
            team_slug: 'platform',
            daily_active_users: 1,
          }),
        ]),
      }),
      {
        generation: 'snapshot-generation',
        attemptToken: 'snapshot-generation-attempt',
      },
    );
    expect(db.replaceStagedTeamAggregates).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'organization-fallback',
      'fallback-generation',
      {
        generation: 'snapshot-generation',
        attemptToken: 'snapshot-generation-attempt',
      },
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'partial',
        components_loaded: JSON.stringify(['teams']),
      }),
    );
  });

  it('does not replace fallback data when its pinned membership snapshot is superseded', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const storedMetric = makeStoredUserMetric(day);

    db.getMissingDays.mockResolvedValueOnce([day]).mockResolvedValueOnce([]);
    db.getTeamAggregationState.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-generation',
    });
    db.getUserMetricsForDay.mockResolvedValue([storedMetric]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(storedMetric.user_id, storedMetric.user_login),
    ]);
    db.getTeamMembershipSnapshotBatchForDay.mockRejectedValue(
      new Error(
        'Organization team membership snapshot is missing or incomplete.',
      ),
    );
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    api.fetchEnterpriseReportLinks.mockResolvedValue({ download_links: [] });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: [],
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getTeamMembershipSnapshotBatchForDay).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      {
        generation: 'snapshot-generation',
        attemptToken: 'snapshot-generation-attempt',
      },
      day,
      0,
    );
    expect(db.replaceStagedTeamAggregates).not.toHaveBeenCalled();
    expect(db.completeStagedTeamAggregationDay).not.toHaveBeenCalled();
    expect(db.activateTeamAggregationSource).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'error',
        error_message: expect.stringContaining('missing or incomplete'),
      }),
    );
  });

  it('does not replace or complete fallback teams when no stored user metrics exist', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValueOnce([day]).mockResolvedValueOnce([]);
    db.getUserMetricsForDay.mockResolvedValue([]);
    db.getTeamAggregationState.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-generation',
    });
    api.probeOrganizationUserTeamsReport.mockResolvedValue('unavailable');
    api.fetchEnterpriseReportLinks.mockResolvedValue({ download_links: [] });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: [],
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getUserMetricsForDay).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
    );
    expect(db.beginTeamAggregationDayStaging).not.toHaveBeenCalled();
    expect(db.stageTeamAggregationBatch).not.toHaveBeenCalled();
    expect(db.replaceStagedTeamAggregates).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'partial',
        components_loaded: JSON.stringify([]),
        error_message: 'Missing components: totals, users, teams',
      }),
    );
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'No stored per-user metrics for enterprise:ent-1',
      ),
    );
  });

  it('reverts to native enterprise team reports when organization reports become available', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const day = '2026-05-10';
    const nativeReport = [
      {
        user_id: 7,
        user_login: 'user-seven',
        day,
        enterprise_id: 'ent-1',
        team_id: 42,
        slug: 'native-team',
      },
    ];

    db.getMissingDays.mockResolvedValue([]);
    db.getTeamAggregationState
      .mockResolvedValueOnce({
        activeSource: 'organization-fallback',
        activeGeneration: 'fallback-generation',
      })
      .mockResolvedValue({
        activeSource: 'native',
        activeGeneration: 'native-generation',
      });
    db.beginTeamAggregationTransition.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-generation',
      pendingSource: 'native',
      pendingGeneration: 'native-generation',
    });
    db.getUserMetricDays.mockResolvedValue([day]);
    db.getMissingTeamAggregationDays.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { user_id: 7, user_login: 'user-seven' }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([]);
    db.stageTeamAggregates.mockResolvedValue(true);
    api.probeOrganizationUserTeamsReport.mockResolvedValue('available');
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/native-teams.json'],
    });
    api.downloadNdjsonDocument.mockResolvedValue(nativeReport);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger: createLoggerMock(),
    });

    await task.runAsync();

    expect(api.fetchOrganizationTeamMemberships).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(day);
    expect(db.stageTeamAggregates).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      'native',
      'native-generation',
      [day],
      [
        expect.objectContaining({
          team_slug: 'native-team',
          user_id: 7,
        }),
      ],
      expect.objectContaining({
        dailyTotals: expect.arrayContaining([
          expect.objectContaining({ team_slug: 'native-team' }),
        ]),
      }),
    );
  });

  it('does not refetch expired native team reports when reverting from fallback', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2025-10-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const rollingStart = DateTime.utc()
      .startOf('day')
      .minus({ years: 1 })
      .toISODate()!;
    const reportWindowStart =
      rollingStart > '2025-10-10' ? rollingStart : '2025-10-10';
    const outsideWindowDay = DateTime.fromISO(reportWindowStart, {
      zone: 'utc',
    })
      .minus({ days: 1 })
      .toISODate()!;
    const recentDay = DateTime.utc()
      .startOf('day')
      .minus({ days: 2 })
      .toISODate()!;

    db.getMissingDays.mockResolvedValue([]);
    db.getTeamAggregationState
      .mockResolvedValueOnce({
        activeSource: 'organization-fallback',
        activeGeneration: 'fallback-active',
      })
      .mockResolvedValue({
        activeSource: 'native',
        activeGeneration: 'native-next',
      });
    db.beginTeamAggregationTransition.mockResolvedValue({
      activeSource: 'organization-fallback',
      activeGeneration: 'fallback-active',
      pendingSource: 'native',
      pendingGeneration: 'native-next',
    });
    db.getUserMetricDays.mockResolvedValue([outsideWindowDay, recentDay]);
    db.getMissingTeamAggregationDays.mockResolvedValue([
      outsideWindowDay,
      recentDay,
    ]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(recentDay),
    ]);
    db.markTeamAggregationUnavailable.mockResolvedValue(false);
    api.probeOrganizationUserTeamsReport.mockResolvedValue('available');
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/native-teams.json'],
    });
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument(recentDay),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.markTeamAggregationUnavailable).toHaveBeenCalledWith(
      outsideWindowDay,
      'enterprise',
      'ent-1',
      'native',
      'native-next',
      [outsideWindowDay, recentDay],
      expect.stringContaining(reportWindowStart),
    );
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledTimes(1);
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(recentDay);
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalledWith(
      outsideWindowDay,
    );
  });

  it('persists per-user breakdown rows in addition to team aggregates when ingesting teams', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10']);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-10'),
    );
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      if (url.includes('users.json')) {
        return makeUserMetricsDocument('2026-05-10');
      }
      return makeUserTeamsDocument('2026-05-10');
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    // Per-user breakdown rows are persisted, keyed by the resolved user.
    expect(db.insertUserMetricsByFeature).toHaveBeenCalledWith([
      expect.objectContaining({
        day: '2026-05-10',
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        user_id: 1,
        user_login: 'octocat',
        feature: 'chat_panel',
      }),
    ]);
    expect(db.insertUserMetricsByIde).toHaveBeenCalledWith([
      expect.objectContaining({
        user_id: 1,
        user_login: 'octocat',
        ide: 'vscode',
      }),
    ]);
    // Team aggregates and memberships are replaced atomically.
    expect(db.replaceTeamAggregates).toHaveBeenCalledWith(
      '2026-05-10',
      'enterprise',
      'ent-1',
      'native',
      expect.arrayContaining([
        expect.objectContaining({ team_slug: 'platform' }),
      ]),
      expect.objectContaining({
        dailyTotals: expect.arrayContaining([
          expect.objectContaining({ team_slug: 'platform' }),
        ]),
      }),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day: '2026-05-10', status: 'success' }),
    );
  });

  it('idempotency: running same backfill twice skips API calls on second run', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
    });
    const logger = createLoggerMock();

    db.getMissingDays
      .mockResolvedValueOnce(['2026-05-10'])
      .mockResolvedValueOnce([]);

    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.fetchOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(
      makeEnterpriseDocument('2026-05-10'),
    );
    api.downloadNdjsonDocument.mockResolvedValueOnce(
      makeUserTeamsDocument('2026-05-10'),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill('2026-05-10', '2026-05-10', 'organization', 'org-1');
    await task.runBackfill('2026-05-10', '2026-05-10', 'organization', 'org-1');

    expect(db.getMissingDays).toHaveBeenCalledTimes(2);
    expect(api.fetchOrganizationReportLinks).toHaveBeenCalledTimes(1);
  });
});

it('ingestTotals returns partial status when downloaded document yields 0 parsed rows', async () => {
  // Regression test for: https://github.com/backstage/community-plugins/issues/9458
  // Before the fix, ingestTotals always returned true even when 0 rows were parsed,
  // causing the ingestion log to record status='success' and getMissingDays to
  // permanently skip those days on future runs.
  const db = createDbMock();
  const api = createApiMock();
  const config = createConfigMock({
    'copilot.enterprise': 'ent-1',
    'copilot.backfillFromDate': '2026-05-10',
    'copilot.backfillDelayMs': 0,
    'copilot.ingestTeams': false,
  });
  const logger = createLoggerMock();

  db.getMissingDays.mockResolvedValue(['2026-05-10']);
  api.fetchEnterpriseReportLinks.mockResolvedValue({
    download_links: ['https://downloads.github.com/report.json'],
  });
  // Return a payload that is structurally valid (an object) but has no 'day' field,
  // so parseEnterpriseDocument produces 0 dailyTotals rows.
  api.downloadDocument.mockResolvedValueOnce({ not_a_day: 'malformed' });

  const task = TaskManagementV2.create({
    db: db as unknown as DatabaseHandlerV2,
    api: api as unknown as GithubClientV2,
    config,
    logger,
  });

  await task.runAsync();

  // Should NOT be logged as success — totals component was not loaded
  expect(db.upsertIngestionLog).toHaveBeenCalledWith(
    expect.objectContaining({
      day: '2026-05-10',
      metrics_type: 'enterprise',
      entity_id: 'ent-1',
      status: 'partial',
    }),
  );
  expect(db.upsertIngestionLog).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: 'success' }),
  );
});

function createDbMock() {
  return {
    getMissingDays: jest.fn(),
    getIngestionLog: jest.fn().mockResolvedValue([]),
    getUserMetricsForDay: jest.fn(),
    getUserBreakdownsForDay: jest.fn(),
    insertDailyTotals: jest.fn().mockResolvedValue(undefined),
    insertPrMetrics: jest.fn().mockResolvedValue(undefined),
    insertByFeature: jest.fn().mockResolvedValue(undefined),
    insertByIde: jest.fn().mockResolvedValue(undefined),
    insertByLanguageFeature: jest.fn().mockResolvedValue(undefined),
    insertByModelFeature: jest.fn().mockResolvedValue(undefined),
    insertByLanguageModel: jest.fn().mockResolvedValue(undefined),
    insertByCli: jest.fn().mockResolvedValue(undefined),
    insertByVscodeAgent: jest.fn().mockResolvedValue(undefined),
    insertCustomizationMetrics: jest.fn().mockResolvedValue(undefined),
    insertCustomizationSummaries: jest.fn().mockResolvedValue(undefined),
    insertUserMetrics: jest.fn().mockResolvedValue(undefined),
    insertUserTeams: jest.fn().mockResolvedValue(undefined),
    getTeamAggregationState: jest.fn().mockResolvedValue({
      activeSource: 'native',
      activeGeneration: 'native-default',
    }),
    beginTeamAggregationTransition: jest.fn().mockResolvedValue(null),
    getUserMetricDays: jest.fn().mockResolvedValue([]),
    getMissingTeamAggregationDays: jest.fn().mockResolvedValue([]),
    beginTeamMembershipSnapshot: jest.fn().mockResolvedValue({
      generation: 'snapshot-generation',
      attemptToken: 'snapshot-generation-attempt',
      cursor: null,
      complete: true,
    }),
    saveTeamMembershipSnapshotBatch: jest.fn().mockResolvedValue(undefined),
    getTeamMembershipSnapshotState: jest.fn().mockResolvedValue({
      generation: 'snapshot-generation',
      attemptToken: 'snapshot-generation-attempt',
      cursor: null,
      complete: true,
    }),
    getTeamMembershipSnapshotBatchForDay: jest.fn().mockResolvedValue({
      memberships: [],
      nextCursor: null,
    }),

    insertUserMetricsByFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByIde: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByLanguageFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByModelFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByLanguageModel: jest.fn().mockResolvedValue(undefined),
    upsertIngestionLog: jest.fn().mockResolvedValue(undefined),
    beginTeamAggregationDayStaging: jest.fn().mockResolvedValue(undefined),
    stageTeamAggregationBatch: jest.fn().mockResolvedValue(undefined),
    completeStagedTeamAggregationDay: jest.fn().mockResolvedValue(false),
    replaceStagedTeamAggregates: jest.fn().mockResolvedValue(undefined),
    stageTeamAggregates: jest.fn().mockResolvedValue(false),
    activateTeamAggregationSource: jest.fn().mockResolvedValue(false),
    markTeamAggregationUnavailable: jest.fn().mockResolvedValue(false),
    recordTeamAggregationFailure: jest.fn().mockResolvedValue(undefined),
    replaceTeamAggregates: jest.fn().mockResolvedValue(undefined),
  };
}

function createApiMock() {
  return {
    fetchEnterpriseReportLinks: jest.fn(),
    fetchOrganizationReportLinks: jest.fn(),
    fetchEnterpriseUserReportLinks: jest.fn(),
    fetchOrganizationUserReportLinks: jest.fn(),
    fetchEnterpriseUserTeamsLinks: jest.fn(),
    fetchOrganizationUserTeamsLinks: jest.fn(),
    probeOrganizationUserTeamsReport: jest.fn().mockResolvedValue('available'),
    fetchOrganizationTeamMemberships: jest.fn().mockResolvedValue([]),
    downloadDocument: jest.fn(),
    downloadNdjsonDocument: jest.fn(),
  };
}

function createLoggerMock(): LoggerService {
  return {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    child: jest.fn(),
  };
}

function createConfigMock(values: Record<string, unknown>): Config {
  return {
    getOptionalString: jest.fn((key: string) => {
      const value = values[key];
      return typeof value === 'string' ? value : undefined;
    }),
    getOptionalNumber: jest.fn((key: string) => {
      const value = values[key];
      return typeof value === 'number' ? value : undefined;
    }),
    getOptionalBoolean: jest.fn((key: string) => {
      const value = values[key];
      return typeof value === 'boolean' ? value : undefined;
    }),
  } as unknown as Config;
}

function makeEnterpriseDocument(day: string) {
  return [
    {
      enterprise_id: 'ent-1',
      report_start_day: day,
      report_end_day: day,
      day_totals: [
        {
          day,
          enterprise_id: 'ent-1',
          daily_active_users: 2,
          weekly_active_users: 2,
          monthly_active_users: 2,
          code_acceptance_activity_count: 5,
          code_generation_activity_count: 6,
          loc_added_sum: 10,
          loc_deleted_sum: 3,
          loc_suggested_to_add_sum: 12,
          loc_suggested_to_delete_sum: 4,
          user_initiated_interaction_count: 1,
          totals_by_vscode_agent: {
            session_count: 2,
            total_user_messages: 8,
          },
        },
      ],
    },
  ];
}

function makeUserMetricsDocument(day: string) {
  return [
    {
      user_id: 1,
      user_login: 'octocat',
      day,
      enterprise_id: 'ent-1',
      used_agent: true,
      used_chat: true,
      used_cli: false,
      code_acceptance_activity_count: 3,
      code_generation_activity_count: 4,
      loc_added_sum: 10,
      loc_deleted_sum: 2,
      loc_suggested_to_add_sum: 12,
      loc_suggested_to_delete_sum: 3,
      user_initiated_interaction_count: 7,
      ai_credits_used: 5.5,
      totals_by_feature: [
        {
          feature: 'chat_panel',
          user_initiated_interaction_count: 7,
          code_generation_activity_count: 4,
          code_acceptance_activity_count: 3,
          loc_added_sum: 10,
          loc_deleted_sum: 2,
          loc_suggested_to_add_sum: 12,
          loc_suggested_to_delete_sum: 3,
        },
      ],
      totals_by_ide: [
        {
          ide: 'vscode',
          user_initiated_interaction_count: 7,
          code_generation_activity_count: 4,
          code_acceptance_activity_count: 3,
          loc_added_sum: 10,
          loc_deleted_sum: 2,
          loc_suggested_to_add_sum: 12,
          loc_suggested_to_delete_sum: 3,
        },
      ],
    },
  ];
}

function makeUserTeamsDocument(day: string) {
  return [
    {
      user_id: 1,
      user_login: 'octocat',
      day,
      enterprise_id: 'ent-1',
      team_id: 42,
      slug: 'platform',
    },
  ];
}

function makeStoredUserMetric(
  day: string,
  overrides: Partial<V2UserMetricRow> = {},
): V2UserMetricRow {
  return {
    day,
    metrics_type: 'enterprise',
    entity_id: 'ent-1',
    user_id: 1,
    user_login: 'octocat',
    used_agent: false,
    used_chat: false,
    used_cli: false,
    used_vscode_agent: false,
    code_acceptance_activity_count: 3,
    code_generation_activity_count: 4,
    loc_added_sum: 10,
    loc_deleted_sum: 2,
    loc_suggested_to_add_sum: 12,
    loc_suggested_to_delete_sum: 3,
    user_initiated_interaction_count: 7,
    ...overrides,
  };
}

function makeStoredUserTeam(
  day: string,
  overrides: Partial<V2UserTeamRow> = {},
): V2UserTeamRow {
  return {
    day,
    metrics_type: 'enterprise',
    entity_id: 'ent-1',
    user_id: 1,
    user_login: 'octocat',
    team_id: 42,
    team_slug: 'platform',
    ...overrides,
  };
}

function makeStoredUserBreakdown(
  userId: number,
  userLogin: string,
): UserBreakdownData {
  return {
    user_id: userId,
    user_login: userLogin,
    byFeature: [
      {
        feature: 'chat_panel',
        user_initiated_interaction_count: 7,
        code_generation_activity_count: 4,
        code_acceptance_activity_count: 3,
        loc_added_sum: 10,
        loc_deleted_sum: 2,
        loc_suggested_to_add_sum: 12,
        loc_suggested_to_delete_sum: 3,
      },
    ],
    byIde: [],
    byLanguageFeature: [],
    byModelFeature: [],
    byLanguageModel: [],
    customizations: [],
  };
}
