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
import {
  DatabaseHandlerV2,
  OrganizationIngestionLeaseLostError,
} from '../db/DatabaseHandlerV2';
import {
  GithubClientV2,
  InaccessibleEnterpriseOrganizationError,
} from '../client/GithubClientV2';
import { UserBreakdownData } from '../utils/reportParser';
import { TaskManagementV2, runBackfill } from './TaskManagementV2';

describe('TaskManagementV2', () => {
  it('prepares all reports before persisting a day in one transaction', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-10';
    const transaction = {};
    let inTransaction = false;
    db.getMissingDays.mockResolvedValue([day]);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['totals-1', 'totals-2'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['users'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['teams'],
    });
    api.downloadDocument.mockImplementation(async () => {
      expect(inTransaction).toBe(false);
      expect(db.insertDailyTotals).not.toHaveBeenCalled();
      return makeEnterpriseDocument(day);
    });
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      expect(inTransaction).toBe(false);
      expect(db.insertDailyTotals).not.toHaveBeenCalled();
      expect(db.insertUserMetrics).not.toHaveBeenCalled();
      return url === 'users'
        ? makeUserMetricsDocument(day)
        : makeUserTeamsDocument(day);
    });
    db.withIngestionDay.mockImplementation(
      async (lockedDay, scope, entity, operation) => {
        expect([lockedDay, scope, entity]).toEqual([
          day,
          'enterprise',
          'ent-1',
        ]);
        expect(api.downloadDocument).toHaveBeenCalledTimes(2);
        expect(api.downloadNdjsonDocument).toHaveBeenCalledTimes(2);
        inTransaction = true;
        try {
          return await operation(transaction, {
            components_loaded: '[]',
            status: 'partial',
          });
        } finally {
          inTransaction = false;
        }
      },
    );
    [
      db.insertDailyTotals,
      db.insertUserMetrics,
      db.insertUserMetricsByFeature,
      db.insertCustomizationMetrics,
      db.insertCustomizationSummaries,
    ].forEach(persist => {
      persist.mockImplementation(async (_rows, tx) => {
        expect(inTransaction).toBe(true);
        expect(tx).toBe(transaction);
      });
    });
    db.replaceTeamMemberships.mockImplementation(async (...args) => {
      expect(inTransaction).toBe(true);
      expect(args.at(-1)).toBe(transaction);
    });
    db.upsertIngestionLog.mockImplementation(async (log, tx) => {
      expect(inTransaction).toBe(true);
      expect(tx).toBe(transaction);
      expect(log).toEqual(
        expect.objectContaining({
          status: 'success',
          components_loaded: '["totals","users","userBreakdowns","teams"]',
        }),
      );
      expect(db.insertDailyTotals).toHaveBeenCalledTimes(2);
      expect(db.insertUserMetrics).toHaveBeenCalledTimes(1);
      expect(db.replaceTeamMemberships).toHaveBeenCalledTimes(1);
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestTeams': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger: createLoggerMock(),
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(db.withIngestionDay).toHaveBeenCalledTimes(1);
    expect(db.upsertIngestionLog).toHaveBeenCalledTimes(1);
  });

  it('skips prepared reports when another worker completed the locked day', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-10';
    db.getMissingDays.mockResolvedValue([day]);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['totals'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['users'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['teams'],
    });
    api.downloadDocument.mockResolvedValue(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url === 'users'
        ? makeUserMetricsDocument(day)
        : makeUserTeamsDocument(day),
    );
    db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat'),
    ]);
    db.withIngestionDay.mockImplementation(
      async (_day, _scope, _entity, operation) =>
        operation(
          {},
          {
            components_loaded: '["totals","users","teams"]',
            status: 'success',
          },
        ),
    );
    const logger = createLoggerMock();
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestTeams': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.downloadNdjsonDocument).toHaveBeenCalledTimes(2);
    expect(db.insertDailyTotals).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.insertCustomizationMetrics).not.toHaveBeenCalled();
    expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).not.toHaveBeenCalled();
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('preserves a concurrent successful commit when another worker fails during preparation', async () => {
    const db = createDbMock();
    const failingApi = createApiMock();
    const successfulApi = createApiMock();
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const failure = new Error('Failed to download totals');
    let announceFetchStarted!: () => void;
    const fetchStarted = new Promise<void>(resolve => {
      announceFetchStarted = resolve;
    });
    let rejectReport!: (error: Error) => void;
    const pendingReport = new Promise<never>((_resolve, reject) => {
      rejectReport = reject;
    });
    let committedLog: {
      status: string;
      components_loaded: string;
      error_message?: string;
      source?: string;
    } = {
      status: 'partial',
      components_loaded: '[]',
    };

    db.getMissingDays.mockResolvedValue([day]);
    db.getIngestionLog.mockResolvedValue([
      { status: 'partial', components_loaded: '[]' },
    ]);
    db.withIngestionDay.mockImplementation(
      async (_day, _scope, _entity, operation) => operation({}, committedLog),
    );
    db.upsertIngestionLog.mockImplementation(async log => {
      committedLog = log;
    });
    failingApi.fetchEnterpriseReportLinks.mockImplementation(() => {
      announceFetchStarted();
      return pendingReport;
    });
    successfulApi.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['totals'],
    });
    successfulApi.downloadDocument.mockResolvedValue(
      makeEnterpriseDocument(day),
    );

    const createTask = (api: ReturnType<typeof createApiMock>) =>
      TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
        }),
        logger,
      });
    const failingTask = createTask(failingApi);
    const successfulTask = createTask(successfulApi);

    const failingRun = failingTask.runBackfill(day, day, 'enterprise', 'ent-1');
    await fetchStarted;
    await successfulTask.runBackfill(day, day, 'enterprise', 'ent-1');
    expect(committedLog.status).toBe('success');

    rejectReport(failure);
    await failingRun;

    expect(db.withIngestionDay).toHaveBeenCalledTimes(2);
    expect(db.upsertIngestionLog).toHaveBeenCalledTimes(1);
    expect(committedLog).toEqual(
      expect.objectContaining({
        status: 'success',
        components_loaded: '["totals"]',
        source: 'backfill',
      }),
    );
    expect(committedLog.error_message).toBeUndefined();
  });

  it.each(['totals', 'users', 'teams'])(
    'keeps only previously committed components when a later %s file fails to download',
    async component => {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const day = '2026-05-10';
      const committed = component === 'totals' ? [] : ['totals'];
      db.getMissingDays.mockResolvedValue([day]);
      db.getIngestionLog.mockResolvedValue([
        { components_loaded: JSON.stringify(committed) },
      ]);
      api.fetchEnterpriseReportLinks.mockResolvedValue({
        download_links:
          component === 'totals' ? ['totals-1', 'totals-2'] : ['totals-1'],
      });
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links:
          component === 'users' ? ['users-1', 'users-2'] : ['users-1'],
      });
      api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
        download_links: ['teams-1', 'teams-2'],
      });
      const downloadError = new Error(`Failed to download ${component}-2`);
      api.downloadDocument.mockImplementation(async (url: string) => {
        if (url === `${component}-2`) {
          throw downloadError;
        }
        return makeEnterpriseDocument(day);
      });
      api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
        if (url === `${component}-2`) {
          throw downloadError;
        }
        return url.startsWith('users')
          ? makeUserMetricsDocument(day)
          : makeUserTeamsDocument(day);
      });
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.ingestTeams': true,
          'copilot.backfillDelayMs': 0,
        }),
        logger,
      });

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(db.insertDailyTotals).not.toHaveBeenCalled();
      expect(db.insertPrMetrics).not.toHaveBeenCalled();
      expect(db.insertUserMetrics).not.toHaveBeenCalled();
      expect(db.insertUserMetricsByFeature).not.toHaveBeenCalled();
      expect(db.insertCustomizationMetrics).not.toHaveBeenCalled();
      expect(db.insertCustomizationSummaries).not.toHaveBeenCalled();
      expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
      expect(db.upsertIngestionLog).toHaveBeenCalledTimes(1);
      expect(db.upsertIngestionLog).toHaveBeenCalledWith(
        expect.objectContaining({
          status: 'error',
          components_loaded: JSON.stringify(committed),
          error_message: downloadError.message,
        }),
        expect.any(Object),
      );
    },
  );

  it('logs only committed components after a daily persistence transaction fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const dataTransaction = {};
    const errorTransaction = {};
    const failure = new Error('team insert failed');
    db.getMissingDays.mockResolvedValue([day]);
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['totals'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['users'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['teams'],
    });
    api.downloadDocument.mockResolvedValue(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url === 'users'
        ? makeUserMetricsDocument(day)
        : makeUserTeamsDocument(day),
    );
    db.withIngestionDay
      .mockImplementationOnce(async (_day, _scope, _entity, operation) =>
        operation(dataTransaction, {
          components_loaded: '[]',
          status: 'partial',
        }),
      )
      .mockImplementationOnce(async (_day, _scope, _entity, operation) =>
        operation(errorTransaction, {
          components_loaded: '[]',
          status: 'partial',
        }),
      );
    db.replaceTeamMemberships.mockRejectedValue(failure);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestTeams': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(db.insertDailyTotals).toHaveBeenCalledWith(
      expect.any(Array),
      dataTransaction,
    );
    expect(db.insertUserMetrics).toHaveBeenCalledWith(
      expect.any(Array),
      dataTransaction,
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledTimes(1);
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'error',
        components_loaded: '[]',
        error_message: failure.message,
      }),
      errorTransaction,
    );
    expect(db.withIngestionDay).toHaveBeenCalledTimes(2);
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('Failed ingest'),
      failure,
    );
  });

  it('commits team memberships independently of concurrent changes to raw users', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-10';
    const stored = makeStoredUserMetric(day);
    db.getMissingDays.mockResolvedValue([day]);
    db.getIngestionLog.mockResolvedValue([
      { components_loaded: '["totals","users"]' },
    ]);
    db.getUserMetricsForDay
      .mockResolvedValueOnce([stored])
      .mockResolvedValue([{ ...stored, loc_added_sum: 999 }]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat'),
    ]);
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['teams'],
    });
    api.downloadNdjsonDocument.mockResolvedValue(makeUserTeamsDocument(day));
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestTeams': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger: createLoggerMock(),
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.downloadNdjsonDocument).toHaveBeenCalledWith('teams');
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [expect.objectContaining({ user_id: 1, team_slug: 'platform' })],
      expect.any(Object),
    );
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'success',
        components_loaded: '["totals","users","teams"]',
      }),
      expect.any(Object),
    );
  });

  it.each([false, true])(
    'commits cost-center memberships independently of changed raw users (legacy repair=%s)',
    async legacyRepair => {
      const db = createDbMock();
      const api = createApiMock();
      const day = '2026-05-10';
      const stored = makeStoredUserMetric(day);
      db.getMissingDays.mockResolvedValue([]);
      db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
      db.getUserMetricsForDay
        .mockResolvedValueOnce([stored])
        .mockResolvedValue([{ ...stored, loc_added_sum: 999 }]);
      db.getUserBreakdownsForDay.mockResolvedValue(
        legacyRepair ? [] : [makeStoredUserBreakdown(1, 'octocat')],
      );
      api.fetchCostCenterMemberships.mockResolvedValue(
        makeCostCenterSnapshot(
          new Map([
            [1, { costCenterId: 'cc-1', login: 'octocat' }],
            [2, { costCenterId: 'cc-2', login: 'not-yet-active' }],
          ]),
        ),
      );
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links: ['users'],
      });
      api.downloadNdjsonDocument.mockResolvedValue(
        makeUserMetricsDocument(day),
      );
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.ingestCostCenters': true,
          'copilot.backfillDelayMs': 0,
        }),
        logger: createLoggerMock(),
      });

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(db.withIngestionDay).toHaveBeenCalledWith(
        day,
        'enterprise',
        'ent-1',
        expect.any(Function),
      );
      expect(api.downloadNdjsonDocument).toHaveBeenCalledTimes(
        legacyRepair ? 1 : 0,
      );
      expect(db.insertUserMetrics).toHaveBeenCalledTimes(legacyRepair ? 1 : 0);
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
        day,
        'enterprise',
        'ent-1',
        [
          expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' }),
          expect.objectContaining({
            user_id: 2,
            user_login: 'not-yet-active',
            cost_center_id: 'cc-2',
          }),
        ],
        true,
        expect.any(Object),
      );
    },
  );

  it.each([false, true])(
    'does not overwrite a concurrently completed cost-center snapshot (unassigned complete=%s)',
    async unassignedComplete => {
      const db = createDbMock();
      const api = createApiMock();
      const day = '2026-05-10';
      const transaction = {};
      db.getMissingDays.mockResolvedValue([]);
      db.getDaysMissingCostCenterMemberships
        .mockResolvedValueOnce([day])
        .mockResolvedValue([]);
      db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
      api.fetchCostCenterMemberships.mockResolvedValue(
        makeCostCenterSnapshot(
          new Map([[1, { costCenterId: 'cc-stale', login: 'octocat' }]]),
          [],
          true,
          unassignedComplete,
        ),
      );
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links: ['users'],
      });
      api.downloadNdjsonDocument.mockResolvedValue(
        makeUserMetricsDocument(day),
      );
      db.withIngestionDay.mockImplementation(
        async (_day, _scope, _entity, operation) =>
          operation(transaction, {
            components_loaded: '["totals","users","userBreakdowns"]',
          }),
      );
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.ingestCostCenters': true,
          'copilot.backfillDelayMs': 0,
        }),
        logger: createLoggerMock(),
      });

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(api.downloadNdjsonDocument).toHaveBeenCalledWith('users', {
        strict: true,
      });
      expect(db.getDaysMissingCostCenterMemberships).toHaveBeenLastCalledWith(
        'enterprise',
        'ent-1',
        day,
        day,
        transaction,
      );
      expect(
        db.getDaysWithIncompleteCostCenterMemberships,
      ).toHaveBeenLastCalledWith('enterprise', 'ent-1', day, day, transaction);
      expect(db.insertUserMetrics).not.toHaveBeenCalled();
      expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
      expect(db.upsertIngestionLog).not.toHaveBeenCalled();
    },
  );

  it.each([true, false])(
    'refreshes completed cost-center membership after user-report refresh and passes completeness to the database guard (unassigned complete=%s)',
    async unassignedComplete => {
      const db = createDbMock();
      const api = createApiMock();
      const day = '2026-05-10';
      db.getMissingDays.mockResolvedValue([day]);
      db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);
      db.getDaysWithIncompleteCostCenterMemberships.mockResolvedValue([]);
      db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
      db.getUserBreakdownsForDay.mockResolvedValue([
        makeStoredUserBreakdown(1, 'octocat'),
      ]);
      api.fetchCostCenterMemberships.mockResolvedValue({
        ...makeCostCenterSnapshot(
          new Map([
            [1, { costCenterId: 'cc-1', login: 'octocat' }],
            [2, { costCenterId: 'cc-2', login: 'new-user' }],
          ]),
          [],
          true,
          unassignedComplete,
        ),
        costCenters: [
          { id: 'cc-1', name: 'Engineering' },
          { id: 'cc-2', name: 'Product' },
        ],
      });
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.ingestCostCenters': true,
          'copilot.backfillDelayMs': 0,
        }),
        logger: createLoggerMock(),
      });
      jest.spyOn(task as any, 'ingestDay').mockResolvedValue({
        loaded: true,
        refreshed: true,
        complete: true,
        totalsLoaded: true,
      });

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
        day,
        'enterprise',
        'ent-1',
        expect.arrayContaining([
          expect.objectContaining({
            user_id: 2,
            user_login: 'new-user',
            cost_center_id: 'cc-2',
          }),
        ]),
        unassignedComplete,
        expect.any(Object),
      );
    },
  );

  it('skips a prepared legacy raw repair already completed by another worker without losing memberships', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-10';
    const transaction = {};
    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat' }]]),
      ),
    );
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['users'],
    });
    api.downloadNdjsonDocument.mockResolvedValue(makeUserMetricsDocument(day));
    db.withIngestionDay.mockImplementation(
      async (_day, _scope, _entity, operation) =>
        operation(transaction, {
          components_loaded: '["totals","users","userBreakdowns"]',
        }),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestCostCenters': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger: createLoggerMock(),
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.downloadNdjsonDocument).toHaveBeenCalledWith('users', {
      strict: true,
    });
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' })],
      true,
      transaction,
    );
  });

  it('reuses legitimately empty raw breakdowns when completing team and cost-center memberships', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-10';
    db.getMissingDays.mockResolvedValue([day]);
    db.getIngestionLog.mockResolvedValue([
      { components_loaded: '["totals","users","userBreakdowns"]' },
    ]);
    db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['teams'],
    });
    api.downloadNdjsonDocument.mockResolvedValue(makeUserTeamsDocument(day));
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat' }]]),
      ),
    );
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestTeams': true,
        'copilot.ingestCostCenters': true,
        'copilot.backfillDelayMs': 0,
      }),
      logger: createLoggerMock(),
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.replaceTeamMemberships).toHaveBeenCalledTimes(1);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'success',
        components_loaded: '["totals","users","userBreakdowns","teams"]',
      }),
      expect.any(Object),
    );
  });

  it.each([false, true])(
    'uses the start of organization access verification for state updates (inaccessible=%s)',
    async inaccessible => {
      jest.useFakeTimers();
      const startedAt = new Date('2026-11-18T10:00:00Z');
      jest.setSystemTime(startedAt);
      try {
        const db = createDbMock();
        const api = createApiMock();
        const day = '2026-11-17';
        const lease = {
          enterpriseId: 'ent-1',
          organization: 'Org-A',
          owner: 'worker',
        };
        db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
        db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
          'Org-A',
        );
        db.getMissingDays.mockResolvedValue([day]);
        api.verifyEnterpriseOrganizationAccess.mockImplementation(async () => {
          jest.setSystemTime(new Date('2026-11-18T10:15:00Z'));
          if (inaccessible) {
            throw new InaccessibleEnterpriseOrganizationError();
          }
        });
        api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
          download_links: [],
        });
        api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
          download_links: [],
        });
        api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
          download_links: [],
        });
        const task = TaskManagementV2.create({
          db: db as unknown as DatabaseHandlerV2,
          api: api as unknown as GithubClientV2,
          config: createConfigMock({
            'copilot.enterprise': 'ent-1',
            'copilot.ingestOrganizationTeamReports': true,
            'copilot.backfillDelayMs': 0,
          }),
          logger: createLoggerMock(),
        });

        const result = await task
          .runOrganizationBackfill('org-a', day, day)
          .catch(error => error);
        const expectedResult = inaccessible
          ? new Error('Organization is no longer available.')
          : undefined;
        expect(result).toEqual(expectedResult);

        expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
          'ent-1',
          'Org-A',
          startedAt,
          expect.any(Date),
          true,
        );
        expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
          lease,
          expect.objectContaining({
            accessState: inaccessible ? 'inaccessible' : 'accessible',
            lastAttemptedAt: new Date('2026-11-18T10:15:00Z'),
          }),
        );
      } finally {
        jest.useRealTimers();
      }
    },
  );

  it('keeps the configured access observation timestamp through other organization work and legacy ingestion', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-11-18T12:00:00Z'));
    try {
      const db = createDbMock();
      const api = createApiMock();
      const lease = {
        enterpriseId: 'ent-1',
        organization: 'org-1',
        owner: 'worker',
      };
      db.getMissingDays.mockResolvedValue(['2026-11-17']);
      api.fetchEnterpriseOrganizations.mockResolvedValue(['org-1']);
      api.fetchEnterpriseReportLinks.mockImplementation(async () => {
        expect(db.claimEnterpriseOrganizationLease).not.toHaveBeenCalled();
        jest.setSystemTime(new Date('2026-11-18T12:11:00Z'));
        return { download_links: [] };
      });
      api.fetchOrganizationReportLinks.mockResolvedValue({
        download_links: [],
      });
      api.fetchOrganizationUserReportLinks.mockResolvedValue({
        download_links: [],
      });
      api.fetchOrganizationUserTeamsLinks.mockResolvedValue({
        download_links: [],
      });
      db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.organization': 'org-1',
          'copilot.ingestOrganizationTeamReports': true,
          'copilot.backfillFromDate': '2026-11-17',
          'copilot.backfillDelayMs': 0,
        }),
        logger: createLoggerMock(),
      });

      await task.runAsync();

      expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
        'ent-1',
        'org-1',
        new Date('2026-11-18T12:11:00Z'),
        new Date('2026-11-18T12:21:00Z'),
        true,
        '1',
      );
      expect(api.fetchOrganizationReportLinks).toHaveBeenCalledWith(
        '2026-11-17',
      );
      expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
    } finally {
      jest.useRealTimers();
    }
  });

  it('does not discover enterprise organizations unless multi-org ingestion is enabled', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    db.getMissingDays.mockResolvedValue([]);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestOrganizationTeamReports': false,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseOrganizations).not.toHaveBeenCalled();
    expect(db.syncEnterpriseOrganizations).not.toHaveBeenCalled();
  });

  it('syncs the enterprise directory and leaves the configured organization to legacy ingestion', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    db.getMissingDays.mockResolvedValue([]);
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue([]);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-1', 'org-2']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-1',
        'copilot.ingestTeams': false,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.syncEnterpriseOrganizations).toHaveBeenCalledWith(
      'ent-1',
      '1',
      ['org-1', 'org-2'],
      expect.any(Date),
      'org-1',
    );
    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'org-1',
      expect.any(String),
    );
    expect(db.markLegacyConfiguredOrganization).toHaveBeenCalledWith(
      'ent-1',
      'org-1',
      expect.any(Date),
      expect.any(Date),
      undefined,
      '1',
    );
    expect(db.getEnterpriseOrganizationsToIngest).toHaveBeenCalledWith(
      'ent-1',
      expect.any(Date),
      6,
      1,
      'org-1',
      '1',
    );
    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'org-1',
      expect.any(String),
      expect.any(String),
      ['totals', 'users', 'teams'],
    );
    expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
      'ent-1',
      'org-1',
      expect.any(Date),
      expect.any(Date),
      true,
      '1',
    );
  });

  it('does not replace the directory when enterprise discovery fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const discoveryError = new Error('GraphQL page request failed');
    db.getMissingDays.mockResolvedValue([]);
    api.fetchEnterpriseOrganizations.mockRejectedValue(discoveryError);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.syncEnterpriseOrganizations).not.toHaveBeenCalled();
    expect(db.markEnterpriseOrganizationDirectoryFailed).toHaveBeenCalledWith(
      'ent-1',
      '1',
      expect.any(Date),
    );
    expect(db.getEnterpriseOrganizationsToIngest).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining(
        'Failed to refresh the enterprise organization directory',
      ),
      discoveryError,
    );
  });

  it('stops using a directory snapshot superseded during discovery', async () => {
    const db = createDbMock();
    const api = createApiMock();
    db.getMissingDays.mockResolvedValue([]);
    db.syncEnterpriseOrganizations.mockResolvedValue('superseded');
    api.fetchEnterpriseOrganizations.mockImplementation(async () => {
      expect(db.beginEnterpriseOrganizationDirectorySync).toHaveBeenCalledWith(
        'ent-1',
      );
      return ['stale-org'];
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger: createLoggerMock(),
    });

    await task.runAsync();

    expect(db.getEnterpriseOrganizationsToIngest).not.toHaveBeenCalled();
    expect(db.claimEnterpriseOrganizationLease).not.toHaveBeenCalled();
    expect(db.markLegacyConfiguredOrganization).not.toHaveBeenCalled();
  });

  it('does not report a stale discovery failure over a newer success', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const discoveryError = new Error('Older discovery failed');
    db.getMissingDays.mockResolvedValue([]);
    db.markEnterpriseOrganizationDirectoryFailed.mockResolvedValue(
      'superseded',
    );
    api.fetchEnterpriseOrganizations.mockRejectedValue(discoveryError);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.markEnterpriseOrganizationDirectoryFailed).toHaveBeenCalledWith(
      'ent-1',
      '1',
      expect.any(Date),
    );
    expect(logger.error).not.toHaveBeenCalledWith(
      expect.stringContaining('Failed to refresh the enterprise organization'),
      discoveryError,
    );
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining(
        'Ignored an enterprise organization directory failure',
      ),
    );
    expect(db.getEnterpriseOrganizationsToIngest).not.toHaveBeenCalled();
  });

  it('ingests configured-organization teams through the existing path when multi-org is enabled', async () => {
    const db = createDbMock();
    const transactionDb = createDbMock();
    db.withOrganizationIngestionLease.mockImplementation(
      async (_lease, operation) =>
        operation(transactionDb as unknown as DatabaseHandlerV2),
    );
    const api = createApiMock();
    const logger = createLoggerMock();
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-1',
      owner: 'worker',
    };
    const day = DateTime.utc().minus({ days: 1 }).toISODate()!;
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    db.getMissingDays.mockImplementation(
      async (metricsType: string, entityId: string) =>
        metricsType === 'organization' && entityId === 'org-1' ? [day] : [],
    );
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue([]);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-1']);
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.fetchOrganizationUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.fetchOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValue([
      { day, daily_active_users: 1, weekly_active_users: 1 },
    ]);
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url.endsWith('/users.json')
        ? makeUserMetricsDocument(day)
        : makeUserTeamsDocument(day),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-1',
        'copilot.backfillFromDate': day,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestTeams': false,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.fetchOrganizationUserReportLinks).toHaveBeenCalledWith(day);
    expect(api.fetchOrganizationUserTeamsLinks).toHaveBeenCalledWith(day);
    for (const method of [
      'insertDailyTotals',
      'insertUserMetrics',
      'insertUserMetricsByFeature',
      'replaceTeamMemberships',
      'upsertIngestionLog',
    ] as const) {
      expect(transactionDb[method]).toHaveBeenCalled();
      expect(db[method]).not.toHaveBeenCalled();
    }
    expect(transactionDb.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        metrics_type: 'organization',
        entity_id: 'org-1',
        status: 'success',
        components_loaded: '["totals","users","userBreakdowns","teams"]',
      }),
      expect.any(Object),
    );
  });

  it('canonicalizes configured organizations before legacy backfill writes', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ years: 1, days: 2 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-1',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-1',
    );
    db.getMissingDays.mockImplementation(
      async (_type, _entityId, _from, _to, requiredComponents) =>
        requiredComponents.length === 1 ? [day] : [],
    );
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'oRG-1',
        'copilot.ingestOrganizationTeamReports': true,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestCostCenters': true,
      }),
      logger,
    });
    const ingestDay = jest.spyOn(task as any, 'ingestDay').mockResolvedValue({
      loaded: false,
      refreshed: false,
      complete: false,
      totalsLoaded: false,
      accessVerified: true,
    });

    await task.runBackfill(day, day, 'organization', 'oRg-1');

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'Org-1',
      day,
      day,
      ['totals'],
    );
    expect(db.getDaysMissingCostCenterMemberships).toHaveBeenCalledWith(
      'organization',
      'Org-1',
      expect.any(String),
      day,
    );
    expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
      'ent-1',
      'Org-1',
      expect.any(Date),
      expect.any(Date),
      true,
    );
    expect(ingestDay).toHaveBeenCalledWith(
      day,
      'organization',
      'Org-1',
      expect.objectContaining({ organizationLease: lease }),
    );
    expect(db.markLegacyConfiguredOrganization).toHaveBeenCalledWith(
      'ent-1',
      'Org-1',
      expect.any(Date),
      expect.any(Date),
      lease,
    );
  });

  it('backfills only missing organization days using the configured delay', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const lastDay = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const middleDay = DateTime.fromISO(lastDay).minus({ days: 1 }).toISODate()!;
    const firstDay = DateTime.fromISO(middleDay)
      .minus({ days: 1 })
      .toISODate()!;
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-1',
    );
    db.getMissingDays.mockResolvedValue([firstDay, lastDay]);
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-1',
      owner: 'worker',
    };
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 5,
        'copilot.ingestOrganizationTeamReports': true,
        'copilot.ingestCostCenters': true,
        'copilot.organization': 'configured-org',
      }),
      logger,
    });
    const ingestDay = jest.spyOn(task as any, 'ingestDay').mockResolvedValue({
      loaded: true,
      refreshed: false,
      complete: true,
      totalsLoaded: true,
    });
    const setTimeoutSpy = jest.spyOn(global, 'setTimeout');

    try {
      await task.runOrganizationBackfill('oRg-1', firstDay, lastDay);
      expect(ingestDay).toHaveBeenCalledTimes(2);
      expect(
        db.getCurrentAccessibleEnterpriseOrganizationLogin,
      ).toHaveBeenCalledWith('ent-1', 'oRg-1');
      expect(db.getMissingDays).toHaveBeenCalledWith(
        'organization',
        'Org-1',
        firstDay,
        lastDay,
        ['totals', 'users', 'teams'],
      );
      expect(ingestDay).toHaveBeenNthCalledWith(
        1,
        firstDay,
        'organization',
        'Org-1',
        expect.objectContaining({
          source: 'backfill',
          organizationLease: lease,
          organizationReportTarget: 'Org-1',
        }),
      );
      expect(ingestDay).toHaveBeenNthCalledWith(
        2,
        lastDay,
        'organization',
        'Org-1',
        expect.objectContaining({ organizationLease: lease }),
      );
      expect(setTimeoutSpy).toHaveBeenCalledWith(expect.any(Function), 5);
      expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
        'ent-1',
        'Org-1',
        expect.any(Date),
        expect.any(Date),
        true,
      );
      expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
      expect(db.getDaysMissingCostCenterMemberships).not.toHaveBeenCalled();
      expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
        'Org-1',
        expect.any(String),
      );
    } finally {
      setTimeoutSpy.mockRestore();
    }
  });

  it('keeps organization totals outside the user-report window without querying old user reports', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2027-02-01T00:00:00Z'));
    try {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
        'Org-1',
      );
      const lease = {
        enterpriseId: 'ent-1',
        organization: 'Org-1',
        owner: 'worker',
      };
      const reportWindowStart = DateTime.utc()
        .startOf('day')
        .minus({ years: 1 })
        .toISODate()!;
      const outsideWindowDay = DateTime.fromISO(reportWindowStart, {
        zone: 'utc',
      })
        .minus({ days: 1 })
        .toISODate()!;
      db.getMissingDays.mockImplementation(
        async (
          _metricsType: string,
          _entityId: string,
          _from: string,
          _to: string,
          requiredComponents: string[],
        ) =>
          requiredComponents.length === 1
            ? [outsideWindowDay]
            : [reportWindowStart],
      );
      db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
      api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
      api.fetchEnterpriseOrganizationReportLinks.mockImplementation(
        async (_organization: string, day: string) => ({
          download_links: [`https://downloads.github.com/totals-${day}.json`],
        }),
      );
      api.downloadDocument.mockImplementation(async (url: string) => {
        const day = url.match(/totals-(\d{4}-\d{2}-\d{2})/)![1];
        return [
          {
            day,
            daily_active_users: 1,
            weekly_active_users: 1,
            monthly_active_users: 1,
          },
        ];
      });
      api.fetchEnterpriseOrganizationUserReportLinks.mockImplementation(
        async () => ({
          download_links: ['https://downloads.github.com/users.json'],
        }),
      );
      api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/user-teams.json'],
      });
      api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
        url.endsWith('/users.json')
          ? makeUserMetricsDocument(reportWindowStart)
          : makeUserTeamsDocument(reportWindowStart),
      );
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });

      await task.runOrganizationBackfill(
        'oRg-1',
        outsideWindowDay,
        reportWindowStart,
      );

      expect(
        db.getCurrentAccessibleEnterpriseOrganizationLogin,
      ).toHaveBeenCalledWith('ent-1', 'oRg-1');
      expect(api.fetchEnterpriseOrganizationReportLinks).toHaveBeenCalledWith(
        'Org-1',
        outsideWindowDay,
      );
      expect(api.fetchEnterpriseOrganizationReportLinks).toHaveBeenCalledWith(
        'Org-1',
        reportWindowStart,
      );
      expect(
        api.fetchEnterpriseOrganizationUserReportLinks,
      ).toHaveBeenCalledTimes(1);
      expect(
        api.fetchEnterpriseOrganizationUserReportLinks,
      ).toHaveBeenCalledWith('Org-1', reportWindowStart);
      expect(
        api.fetchEnterpriseOrganizationUserTeamsLinks,
      ).toHaveBeenCalledWith('Org-1', reportWindowStart);
      expect(api.fetchOrganizationReportLinks).not.toHaveBeenCalled();
      expect(api.fetchOrganizationUserReportLinks).not.toHaveBeenCalled();
      expect(api.fetchOrganizationUserTeamsLinks).not.toHaveBeenCalled();
      expect(db.insertDailyTotals.mock.calls.flatMap(([rows]) => rows)).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            day: outsideWindowDay,
            metrics_type: 'organization',
            entity_id: 'Org-1',
          }),
          expect.objectContaining({
            day: reportWindowStart,
            metrics_type: 'organization',
            entity_id: 'Org-1',
          }),
        ]),
      );
      expect(db.insertUserMetrics.mock.calls.flatMap(([rows]) => rows)).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            day: reportWindowStart,
            metrics_type: 'organization',
            entity_id: 'Org-1',
          }),
        ]),
      );
      expect(db.replaceTeamMemberships).toHaveBeenCalledTimes(1);
      expect(db.upsertIngestionLog).toHaveBeenCalledWith(
        expect.objectContaining({
          day: outsideWindowDay,
          metrics_type: 'organization',
          entity_id: 'Org-1',
          status: 'success',
          components_loaded: '["totals"]',
          source: 'backfill',
        }),
        expect.any(Object),
      );
      expect(
        db.updateEnterpriseOrganizationState.mock.calls.some(
          ([, state]) => state.accessState === 'inaccessible',
        ),
      ).toBe(false);
      expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
        lease,
        expect.objectContaining({
          accessState: 'accessible',
          reportsReady: true,
        }),
      );
    } finally {
      jest.useRealTimers();
    }
  });

  it('records partial components for discovered organizations without replacing loaded totals', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 2 }).toISODate()!;
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([day]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/totals.json'],
    });
    api.downloadDocument.mockResolvedValue({
      day,
      daily_active_users: 3,
      weekly_active_users: 3,
    });
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: [],
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runOrganizationBackfill('org-a', day, yesterday);

    expect(db.insertDailyTotals).toHaveBeenCalledWith(
      expect.arrayContaining([
        expect.objectContaining({
          day,
          metrics_type: 'organization',
          entity_id: 'Org-A',
        }),
      ]),
      expect.any(Object),
    );
    expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        metrics_type: 'organization',
        entity_id: 'Org-A',
        status: 'partial',
        components_loaded: '["totals"]',
        error_message: 'Missing components: users, teams',
        source: 'backfill',
      }),
      expect.any(Object),
    );
    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({
        accessState: 'accessible',
        reportsReady: true,
        nextAttemptAt: expect.any(Date),
      }),
    );
    const partialState = db.updateEnterpriseOrganizationState.mock.calls[0][1];
    expect(partialState.nextAttemptAt.getTime()).toBe(
      partialState.lastAttemptedAt.getTime() + 60 * 60 * 1000,
    );
  });

  it('preserves the scheduled retry time after a complete latest-day backfill', async () => {
    jest.useFakeTimers().setSystemTime(new Date('2026-11-18T12:00:00.000Z'));
    try {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
      const lease = {
        enterpriseId: 'ent-1',
        organization: 'Org-A',
        owner: 'worker',
      };
      db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
        'Org-A',
      );
      db.getMissingDays.mockResolvedValue([yesterday]);
      db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
      api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });
      jest.spyOn(task as any, 'ingestDay').mockResolvedValue({
        loaded: true,
        refreshed: false,
        complete: true,
        totalsLoaded: true,
      });

      await task.runOrganizationBackfill('org-a', yesterday, yesterday);

      const state = db.updateEnterpriseOrganizationState.mock.calls[0][1];
      expect(state).toEqual(
        expect.objectContaining({
          accessState: 'accessible',
          reportsReady: true,
          lastAttemptedAt: expect.any(Date),
        }),
      );
      expect(state).not.toHaveProperty('nextAttemptAt');
      expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
    } finally {
      jest.useRealTimers();
    }
  });

  it('preserves the scheduled retry time after a complete historical backfill', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 2 }).toISODate()!;
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([day]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });
    jest.spyOn(task as any, 'ingestDay').mockResolvedValue({
      loaded: true,
      refreshed: false,
      complete: true,
      totalsLoaded: true,
    });

    await task.runOrganizationBackfill('org-a', day, yesterday);

    expect(db.claimEnterpriseOrganizationLease).toHaveBeenCalledWith(
      'ent-1',
      'Org-A',
      expect.any(Date),
      expect.any(Date),
      true,
    );
    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({
        accessState: 'accessible',
        reportsReady: true,
        lastAttemptedAt: expect.any(Date),
      }),
    );
    expect(
      db.updateEnterpriseOrganizationState.mock.calls[0][1],
    ).not.toHaveProperty('nextAttemptAt');
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
  });

  it('marks a discovered organization inaccessible when a manual backfill fails access verification', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([day]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.verifyEnterpriseOrganizationAccess.mockRejectedValue(
      new InaccessibleEnterpriseOrganizationError(),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await expect(
      task.runOrganizationBackfill('org-a', day, day),
    ).rejects.toThrow('Organization is no longer available.');

    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({
        accessState: 'inaccessible',
        reportsReady: false,
        nextAttemptAt: expect.any(Date),
      }),
    );
    const inaccessibleState =
      db.updateEnterpriseOrganizationState.mock.calls[0][1];
    expect(inaccessibleState.nextAttemptAt.getTime()).toBe(
      inaccessibleState.lastAttemptedAt.getTime() + 24 * 60 * 60 * 1000,
    );
    expect(api.fetchEnterpriseOrganizationReportLinks).not.toHaveBeenCalled();
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
  });

  it('limits each organization run to three backlog days plus the recent refresh window', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const bootstrapStart = DateTime.utc()
      .startOf('day')
      .minus({ days: 30 })
      .toISODate()!;
    const missingDays: string[] = [];
    for (
      let day = DateTime.fromISO(bootstrapStart, { zone: 'utc' });
      day <= DateTime.fromISO(yesterday, { zone: 'utc' });
      day = day.plus({ days: 1 })
    ) {
      missingDays.push(day.toISODate()!);
    }
    db.getMissingDays.mockImplementation(
      async (_type: string, entityId: string) =>
        entityId === 'org-a' ? missingDays : [],
    );
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
    db.claimEnterpriseOrganizationLease.mockResolvedValue({
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
    });
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
    api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: [],
    });
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    const trailingDays = missingDays.slice(-3);
    expect(
      api.fetchEnterpriseOrganizationReportLinks.mock.calls.map(
        ([, day]) => day,
      ),
    ).toEqual([...missingDays.slice(0, 3), ...trailingDays]);
    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'org-a',
      yesterday,
    );
    expect(db.renewEnterpriseOrganizationLease).toHaveBeenCalledTimes(8);
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledTimes(1);
  });

  it('continues configured scheduled ingestion after yesterday is unavailable and uses the adopted login', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const historicalDay = DateTime.fromISO(yesterday)
      .minus({ days: 1 })
      .toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    const reportNotFound = Object.assign(new Error('HTTP 404'), {
      status: 404,
    });
    db.syncEnterpriseOrganizations.mockResolvedValue({
      configuredOrganizationLogin: 'Org-A',
      configuredOrganizationAmbiguous: false,
    });
    db.getMissingDays.mockImplementation(
      async (metricsType: string, entityId: string) =>
        metricsType === 'organization' && entityId === 'Org-A'
          ? [historicalDay, yesterday]
          : [],
    );
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue([]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['Org-A']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(false);
    api.fetchOrganizationReportLinks.mockImplementation(async day => {
      if (day === historicalDay) {
        throw reportNotFound;
      }
      return { download_links: [] };
    });
    api.fetchOrganizationUserReportLinks.mockImplementation(async day => {
      if (day === historicalDay) {
        throw reportNotFound;
      }
      return { download_links: [] };
    });
    api.fetchOrganizationUserTeamsLinks.mockImplementation(async day => {
      if (day === historicalDay) {
        throw reportNotFound;
      }
      return { download_links: [] };
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-a',
        'copilot.backfillFromDate': historicalDay,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'Org-A',
      yesterday,
    );
    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'Org-A',
      historicalDay,
      yesterday,
      ['totals', 'users', 'teams'],
    );
    expect(api.fetchOrganizationReportLinks.mock.calls).toEqual([
      [historicalDay],
      [yesterday],
    ]);
    expect(db.markLegacyConfiguredOrganization).toHaveBeenCalledWith(
      'ent-1',
      'Org-A',
      expect.any(Date),
      expect.any(Date),
      lease,
    );
  });

  it('records organization access when report links succeed before a download fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 2 }).toISODate()!;
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://example.test/report'],
    });
    api.downloadDocument.mockRejectedValue(new Error('Download failed'));
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({}),
      logger,
    });

    const result = await (task as any).ingestDay(day, 'organization', 'Org-A', {
      ingestTeams: false,
      ingestUsers: false,
      ingestCostCenters: false,
      source: 'backfill',
    });

    expect(result).toMatchObject({ accessVerified: true, complete: false });
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ status: 'error' }),
      expect.any(Object),
    );
  });

  it('proves discovered organization access from an available historical day after yesterday returns 404', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const historicalDay = DateTime.fromISO(yesterday)
      .minus({ days: 2 })
      .toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
    };
    const reportNotFound = Object.assign(new Error('HTTP 404'), {
      status: 404,
    });
    db.getMissingDays.mockResolvedValue([]);
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(false);
    api.fetchEnterpriseOrganizationReportLinks.mockImplementation(
      async (_organization: string, day: string) => {
        if (day === historicalDay) {
          return { download_links: [] };
        }
        throw reportNotFound;
      },
    );
    api.fetchEnterpriseOrganizationUserReportLinks.mockRejectedValue(
      reportNotFound,
    );
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockRejectedValue(
      reportNotFound,
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'org-a',
      yesterday,
    );
    expect(
      api.fetchEnterpriseOrganizationReportLinks.mock.calls.map(
        ([, day]) => day,
      ),
    ).toEqual([
      historicalDay,
      DateTime.fromISO(yesterday).minus({ days: 1 }).toISODate(),
      yesterday,
    ]);
    const updatedState = db.updateEnterpriseOrganizationState.mock.calls[0][1];
    expect(updatedState.accessState).toBe('accessible');
    expect(updatedState.reportsReady).toBeUndefined();
  });

  it('keeps a new organization unverified when every report day returns 404', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-new',
      owner: 'worker',
    };
    const reportNotFound = Object.assign(new Error('HTTP 404'), {
      status: 404,
    });
    db.getMissingDays.mockResolvedValue([]);
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-new']);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-new']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(false);
    api.fetchEnterpriseOrganizationReportLinks.mockRejectedValue(
      reportNotFound,
    );
    api.fetchEnterpriseOrganizationUserReportLinks.mockRejectedValue(
      reportNotFound,
    );
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockRejectedValue(
      reportNotFound,
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    const updatedState = db.updateEnterpriseOrganizationState.mock.calls[0][1];
    expect(updatedState.accessState).toBeUndefined();
    expect(updatedState.reportsReady).toBeUndefined();
    expect(updatedState.lastAttemptedAt).toEqual(expect.any(Date));
  });

  it.each([
    ['per-user', 403],
    ['user-teams', 403],
    ['per-user', 404],
    ['user-teams', 404],
  ] as const)(
    'revokes a discovered organization after a %s report permission denial (HTTP %i) despite earlier success',
    async (report, status) => {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const lease = {
        enterpriseId: 'ent-1',
        organization: 'org-a',
        owner: 'worker',
        directoryGeneration: '1',
      };
      const permissionError = Object.assign(
        new Error(
          status === 404
            ? 'Resource not accessible by integration'
            : 'Forbidden',
        ),
        { status },
      );
      db.getMissingDays.mockResolvedValue([]);
      db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
      db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
      api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
      api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
      api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
        download_links: [],
      });
      if (report === 'per-user') {
        api.fetchEnterpriseOrganizationUserReportLinks.mockRejectedValue(
          permissionError,
        );
      } else {
        db.getIngestionLog.mockResolvedValue([
          {
            components_loaded: '["userBreakdowns"]',
            status: 'partial',
          },
        ]);
        api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
          download_links: [],
        });
        api.fetchEnterpriseOrganizationUserTeamsLinks.mockRejectedValue(
          permissionError,
        );
      }
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });

      await task.runAsync();

      expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
        'org-a',
        expect.any(String),
      );
      expect(api.fetchEnterpriseOrganizationReportLinks).toHaveBeenCalled();
      expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
        lease,
        expect.objectContaining({
          accessState: 'inaccessible',
          reportsReady: false,
        }),
      );
      expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
    },
  );

  it('revokes a configured organization when totals are forbidden after a successful access check', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
      directoryGeneration: '1',
    };
    const deniedTotals = Object.assign(new Error('Forbidden'), { status: 403 });
    db.syncEnterpriseOrganizations.mockResolvedValue({
      configuredOrganizationLogin: 'Org-A',
      configuredOrganizationAmbiguous: false,
    });
    db.getMissingDays.mockImplementation(
      async (metricsType: string, entityId: string) =>
        metricsType === 'organization' && entityId === 'Org-A'
          ? [yesterday]
          : [],
    );
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue([]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['Org-A']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    api.fetchOrganizationReportLinks.mockRejectedValue(deniedTotals);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-a',
        'copilot.backfillFromDate': yesterday,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'Org-A',
      expect.any(String),
    );
    expect(
      api.verifyEnterpriseOrganizationAccess.mock.invocationCallOrder[0],
    ).toBeLessThan(
      api.fetchOrganizationReportLinks.mock.invocationCallOrder[0],
    );
    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({
        accessState: 'inaccessible',
        reportsReady: false,
      }),
    );
    expect(db.markLegacyConfiguredOrganization).toHaveBeenCalledWith(
      'ent-1',
      'Org-A',
      expect.any(Date),
      expect.any(Date),
      undefined,
      '1',
    );
    expect(db.markLegacyConfiguredOrganization).not.toHaveBeenCalledWith(
      'ent-1',
      'Org-A',
      expect.any(Date),
      expect.any(Date),
      lease,
    );
  });

  it('does not apply a configured-organization denial after its directory generation is superseded', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
      directoryGeneration: '1',
    };
    db.syncEnterpriseOrganizations.mockResolvedValue({
      configuredOrganizationLogin: 'Org-A',
      configuredOrganizationAmbiguous: false,
    });
    db.getMissingDays.mockImplementation(
      async (metricsType: string, entityId: string) =>
        metricsType === 'organization' && entityId === 'Org-A'
          ? [yesterday]
          : [],
    );
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue([]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    db.updateEnterpriseOrganizationState.mockRejectedValueOnce(
      new OrganizationIngestionLeaseLostError(),
    );
    api.fetchEnterpriseOrganizations.mockResolvedValue(['Org-A']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    api.fetchOrganizationReportLinks.mockRejectedValue(
      Object.assign(new Error('Forbidden'), { status: 403 }),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-a',
        'copilot.backfillFromDate': yesterday,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await expect(task.runAsync()).resolves.toBeUndefined();

    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({
        accessState: 'inaccessible',
        reportsReady: false,
      }),
    );
    expect(logger.debug).toHaveBeenCalledWith(
      expect.stringContaining('stale configured-organization denial'),
    );
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
  });

  it.each([
    [
      'rate-limited 403',
      Object.assign(new Error('API rate limit exceeded'), {
        status: 403,
        response: { headers: { 'x-ratelimit-remaining': '0' } },
      }),
    ],
    [
      'transient 500',
      Object.assign(new Error('Internal Server Error'), { status: 500 }),
    ],
  ])(
    'does not revoke a discovered organization for a %s',
    async (_description, reportError) => {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const lease = {
        enterpriseId: 'ent-1',
        organization: 'org-a',
        owner: 'worker',
        directoryGeneration: '1',
      };
      db.getMissingDays.mockResolvedValue([]);
      db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
      db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
      api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
      api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
      api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
        download_links: [],
      });
      api.fetchEnterpriseOrganizationUserReportLinks
        .mockRejectedValueOnce(reportError)
        .mockResolvedValue({ download_links: [] });
      api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
        download_links: [],
      });
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });

      await task.runAsync();

      expect(
        db.updateEnterpriseOrganizationState.mock.calls.some(
          ([, state]) => state.accessState === 'inaccessible',
        ),
      ).toBe(false);
      expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
        lease,
        expect.objectContaining({ accessState: 'accessible' }),
      );
    },
  );

  it('backfills a discovered organization from a historical day after yesterday returns 404', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const historicalDay = DateTime.fromISO(yesterday)
      .minus({ days: 2 })
      .toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([historicalDay]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(false);
    api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: [],
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runOrganizationBackfill('org-a', historicalDay, historicalDay);

    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'Org-A',
      yesterday,
    );
    expect(api.fetchEnterpriseOrganizationReportLinks).toHaveBeenCalledWith(
      'Org-A',
      historicalDay,
    );
    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      lease,
      expect.objectContaining({ accessState: 'accessible' }),
    );
    expect(
      db.updateEnterpriseOrganizationState.mock.calls[0][1].reportsReady,
    ).toBeUndefined();
  });

  it('does not add a yesterday access gate to configured organization backfills', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 2 }).toISODate()!;
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'Org-A',
      owner: 'worker',
    };
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([day]);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchOrganizationReportLinks.mockResolvedValue({ download_links: [] });
    api.fetchOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: [],
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-a',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runBackfill(day, day, 'organization', 'org-a');

    expect(api.verifyEnterpriseOrganizationAccess).not.toHaveBeenCalled();
    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'Org-A',
      day,
      day,
      ['totals', 'users', 'teams'],
    );
    expect(db.markLegacyConfiguredOrganization).toHaveBeenCalledWith(
      'ent-1',
      'Org-A',
      expect.any(Date),
      expect.any(Date),
      lease,
    );
  });

  it('refreshes recent team data without refetching complete totals and user reports', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
    };
    db.getMissingDays.mockResolvedValue([]);
    db.getIngestionLog.mockResolvedValue([
      {
        components_loaded: '["totals","users","userBreakdowns","teams"]',
      },
    ]);
    db.getUserMetricsForDay.mockImplementation(
      async (_metricsType: string, _entityId: string, day: string) => [
        makeStoredUserMetric(day, {
          metrics_type: 'organization',
          entity_id: 'org-a',
        }),
      ],
    );
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat'),
    ]);
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
    api.verifyEnterpriseOrganizationAccess.mockResolvedValue(true);
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockImplementation(
      async (_organization: string, day: string) => ({
        download_links: [`https://downloads.github.com/user-teams-${day}.json`],
      }),
    );
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      const day = url.match(/user-teams-(\d{4}-\d{2}-\d{2})/)![1];
      return makeUserTeamsDocument(day);
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.verifyEnterpriseOrganizationAccess).toHaveBeenCalledWith(
      'org-a',
      yesterday,
    );
    expect(api.fetchEnterpriseOrganizationReportLinks).not.toHaveBeenCalled();
    expect(
      api.fetchEnterpriseOrganizationUserReportLinks,
    ).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseOrganizationUserTeamsLinks.mock.calls).toEqual(
      days.map(day => ['org-a', day]),
    );
    expect(db.withIngestionDay.mock.calls.map(([day]) => day)).toEqual(days);
    expect(db.replaceTeamMemberships).toHaveBeenCalledTimes(3);
    expect(db.replaceTeamMemberships.mock.calls.map(([day]) => day)).toEqual(
      days,
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledTimes(3);
    expect(db.insertDailyTotals).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
  });

  it('repairs unmarked user breakdowns before refreshing team memberships', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const repairDay = days[1];
    configureRecentOrganizationTeamRefresh(db, api, repairDay);
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [`users-${repairDay}`],
    });
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      const day = url.replace(/^(users|teams)-/, '');
      if (url.startsWith('users-')) {
        return [
          {
            ...makeUserMetricsDocument(day)[0],
            user_id: 2,
            user_login: 'new-user',
            totals_by_feature: [
              {
                feature: 'new-feature',
                code_generation_activity_count: 4,
                loc_added_sum: 10,
              },
            ],
            totals_by_skill: [{ skill: 'new-skill', interaction_count: 4 }],
          },
        ];
      }
      return [
        {
          ...makeUserTeamsDocument(day)[0],
          user_id: 2,
          user_login: 'new-user',
          team_id: 99,
          slug: 'new-team',
        },
      ];
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(
      api.fetchEnterpriseOrganizationUserReportLinks,
    ).toHaveBeenCalledTimes(1);
    expect(api.fetchEnterpriseOrganizationUserReportLinks).toHaveBeenCalledWith(
      'org-a',
      repairDay,
    );
    expect(db.deleteUserReportForDay).toHaveBeenCalledWith(
      repairDay,
      'organization',
      'org-a',
      expect.any(Object),
    );
    expect(db.insertUserMetrics).toHaveBeenCalledWith(
      [expect.objectContaining({ day: repairDay, user_id: 2 })],
      expect.any(Object),
    );
    expect(db.insertUserMetricsByFeature).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          day: repairDay,
          user_id: 2,
          feature: 'new-feature',
        }),
      ],
      expect.any(Object),
    );
    expect(db.insertCustomizationMetrics).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          day: repairDay,
          user_id: 2,
          category: 'skill',
          item_name: 'new-skill',
          interaction_count: 4,
        }),
      ],
      expect.any(Object),
    );
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      repairDay,
      'organization',
      'org-a',
      [expect.objectContaining({ user_id: 2, team_slug: 'new-team' })],
      expect.any(Object),
    );
    const repairTransaction = db.deleteUserReportForDay.mock.calls[0][3];
    const repairedMembership = db.replaceTeamMemberships.mock.calls.find(
      ([day]) => day === repairDay,
    );
    const repairLog = db.upsertIngestionLog.mock.calls.find(
      ([row]) => row.day === repairDay,
    );
    expect(db.insertUserMetrics.mock.calls[0][1]).toBe(repairTransaction);
    expect(repairedMembership?.[4]).toBe(repairTransaction);
    expect(repairLog?.[1]).toBe(repairTransaction);
    expect(repairLog?.[0]).toEqual(
      expect.objectContaining({
        status: 'success',
      }),
    );
    expect(JSON.parse(repairLog?.[0].components_loaded)).toEqual(
      expect.arrayContaining(['totals', 'users', 'teams', 'userBreakdowns']),
    );
    expect(logger.error).not.toHaveBeenCalled();
  });

  it('marks a successfully fetched empty user report complete and replaces memberships with empty', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const repairDay = days[1];
    configureRecentOrganizationTeamRefresh(db, api, repairDay);
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [`users-${repairDay}`],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockImplementation(
      async (_organization: string, day: string) => ({
        download_links: [`teams-${day}`],
      }),
    );
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url === `users-${repairDay}` || url === `teams-${repairDay}`
        ? []
        : makeUserTeamsDocument(url.replace(/^teams-/, '')),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.deleteUserReportForDay).toHaveBeenCalledWith(
      repairDay,
      'organization',
      'org-a',
      expect.any(Object),
    );
    expect(db.insertUserMetrics).toHaveBeenCalledWith([], expect.any(Object));
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      repairDay,
      'organization',
      'org-a',
      [],
      expect.any(Object),
    );
    const repairLog = db.upsertIngestionLog.mock.calls.find(
      ([row]) => row.day === repairDay,
    );
    expect(repairLog?.[0].status).toBe('success');
    expect(JSON.parse(repairLog?.[0].components_loaded)).toEqual(
      expect.arrayContaining(['totals', 'users', 'teams', 'userBreakdowns']),
    );
  });

  it('preserves existing user data and memberships when a repair report is unavailable', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const repairDay = days[1];
    configureRecentOrganizationTeamRefresh(db, api, repairDay);
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      makeUserTeamsDocument(url.replace(/^teams-/, '')),
    );
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.deleteUserReportForDay).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.insertCustomizationMetrics).not.toHaveBeenCalled();
    expect(
      api.fetchEnterpriseOrganizationUserTeamsLinks.mock.calls.some(
        ([, day]) => day === repairDay,
      ),
    ).toBe(false);
    expect(
      db.replaceTeamMemberships.mock.calls.some(([day]) => day === repairDay),
    ).toBe(false);
    const repairLog = db.upsertIngestionLog.mock.calls.find(
      ([row]) => row.day === repairDay,
    );
    expect(repairLog?.[0].status).toBe('partial');
    expect(repairLog?.[0].components_loaded).not.toContain('userBreakdowns');
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(repairDay),
    );
    expect(
      db.updateEnterpriseOrganizationState.mock.calls.some(
        ([, state]) => state.nextAttemptAt instanceof Date,
      ),
    ).toBe(true);
  });

  it.each(['user', 'team'] as const)(
    'preserves existing history when strict %s repair parsing fails',
    async input => {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
      const days = [2, 1, 0].map(
        offset =>
          DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
      );
      const repairDay = days[1];
      configureRecentOrganizationTeamRefresh(db, api, repairDay);
      api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
        download_links: [`users-${repairDay}`],
      });
      api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
        if (url === `users-${repairDay}`) {
          return input === 'user'
            ? [
                ...makeUserMetricsDocument(repairDay),
                { day: repairDay, user_id: 2 },
              ]
            : makeUserMetricsDocument(repairDay);
        }
        if (url === `teams-${repairDay}` && input === 'team') {
          return [
            {
              ...makeUserTeamsDocument(repairDay)[0],
              team_id: 0,
            },
          ];
        }
        return makeUserTeamsDocument(url.replace(/^teams-/, ''));
      });
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });

      await task.runAsync();

      expect(api.downloadNdjsonDocument).toHaveBeenCalledWith(
        `users-${repairDay}`,
        { strict: true },
      );
      const teamReportWasDownloadedStrictly =
        api.downloadNdjsonDocument.mock.calls.some(
          ([url, options]) =>
            url === `teams-${repairDay}` &&
            (options as { strict?: boolean } | undefined)?.strict === true,
        );
      expect(teamReportWasDownloadedStrictly).toBe(input === 'team');
      expect(db.deleteUserReportForDay).not.toHaveBeenCalled();
      expect(
        db.insertUserMetrics.mock.calls.some(([rows]) =>
          rows.some((row: { day: string }) => row.day === repairDay),
        ),
      ).toBe(false);
      expect(
        db.replaceTeamMemberships.mock.calls.some(([day]) => day === repairDay),
      ).toBe(false);
      const repairLog = db.upsertIngestionLog.mock.calls.find(
        ([row]) => row.day === repairDay,
      );
      expect(repairLog?.[0]).toEqual(
        expect.objectContaining({
          status: 'error',
          components_loaded: '["totals","users","teams"]',
        }),
      );
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining(
          `Failed ingest for organization:org-a on ${repairDay}`,
        ),
        expect.any(Error),
      );
      expect(
        db.updateEnterpriseOrganizationState.mock.calls.some(
          ([, state]) => state.nextAttemptAt instanceof Date,
        ),
      ).toBe(true);
    },
  );

  it.each(['per-user', 'user-teams'])(
    'routes inaccessible %s report errors through organization handling during repair',
    async report => {
      const db = createDbMock();
      const api = createApiMock();
      const logger = createLoggerMock();
      const accessError = new InaccessibleEnterpriseOrganizationError();
      const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
      const days = [2, 1, 0].map(
        offset =>
          DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
      );
      const repairDay = days[1];
      configureRecentOrganizationTeamRefresh(db, api, repairDay);

      if (report === 'per-user') {
        api.fetchEnterpriseOrganizationUserReportLinks.mockRejectedValue(
          accessError,
        );
      } else {
        api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
          download_links: [`users-${repairDay}`],
        });
        api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
          url === `users-${repairDay}`
            ? makeUserMetricsDocument(repairDay)
            : makeUserTeamsDocument(url.replace(/^teams-/, '')),
        );
        api.fetchEnterpriseOrganizationUserTeamsLinks.mockImplementation(
          async (_organization: string, day: string) => {
            if (day === repairDay) {
              throw accessError;
            }
            return { download_links: [`teams-${day}`] };
          },
        );
      }

      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config: createConfigMock({
          'copilot.enterprise': 'ent-1',
          'copilot.backfillDelayMs': 0,
          'copilot.ingestOrganizationTeamReports': true,
        }),
        logger,
      });

      await task.runAsync();

      expect(
        api.fetchEnterpriseOrganizationUserReportLinks,
      ).toHaveBeenCalledWith('org-a', repairDay);
      expect(db.deleteUserReportForDay).not.toHaveBeenCalled();
      expect(
        db.insertUserMetrics.mock.calls.some(([rows]) =>
          rows.some((row: { day: string }) => row.day === repairDay),
        ),
      ).toBe(false);
      expect(
        db.replaceTeamMemberships.mock.calls.some(([day]) => day === repairDay),
      ).toBe(false);
      const repairLog = db.upsertIngestionLog.mock.calls.find(
        ([row]) => row.day === repairDay,
      );
      expect(repairLog?.[0]).toEqual(
        expect.objectContaining({
          status: 'error',
          components_loaded: '["totals","users","teams"]',
        }),
      );
      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining(
          `Failed ingest for organization:org-a on ${repairDay}`,
        ),
        accessError,
      );
      expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
        expect.objectContaining({
          enterpriseId: 'ent-1',
          organization: 'org-a',
        }),
        expect.objectContaining({ accessState: 'inaccessible' }),
      );
      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining(
          'Organization-team ingestion is inaccessible for organization:org-a',
        ),
      );
    },
  );

  it('does not commit a complete user report when refreshed memberships are unavailable', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const repairDay = days[1];
    configureRecentOrganizationTeamRefresh(db, api, repairDay);
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [`users-${repairDay}`],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockImplementation(
      async (_organization: string, day: string) => ({
        download_links: day === repairDay ? [] : [`teams-${day}`],
      }),
    );
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      if (url.startsWith('users-')) {
        return [
          {
            ...makeUserMetricsDocument(repairDay)[0],
            user_id: 2,
            user_login: 'new-user',
            totals_by_skill: [{ skill: 'new-skill', interaction_count: 4 }],
          },
        ];
      }
      return makeUserTeamsDocument(url.replace(/^teams-/, ''));
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(api.fetchEnterpriseOrganizationUserTeamsLinks).toHaveBeenCalledWith(
      'org-a',
      repairDay,
    );
    expect(db.deleteUserReportForDay).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.insertCustomizationMetrics).not.toHaveBeenCalled();
    expect(
      db.replaceTeamMemberships.mock.calls.some(([day]) => day === repairDay),
    ).toBe(false);
    const repairLog = db.upsertIngestionLog.mock.calls.find(
      ([row]) => row.day === repairDay,
    );
    expect(repairLog?.[0].status).toBe('partial');
    expect(repairLog?.[0].components_loaded).not.toContain('userBreakdowns');
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(repairDay),
    );
  });

  it('does not persist partial customizations or memberships when a repair download fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate()!;
    const days = [2, 1, 0].map(
      offset =>
        DateTime.fromISO(yesterday).minus({ days: offset }).toISODate()!,
    );
    const repairDay = days[1];
    configureRecentOrganizationTeamRefresh(db, api, repairDay);
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [
        `users-partial-${repairDay}`,
        `users-failed-${repairDay}`,
      ],
    });
    api.downloadNdjsonDocument.mockImplementation(async (url: string) => {
      if (url === `users-partial-${repairDay}`) {
        return [
          {
            ...makeUserMetricsDocument(repairDay)[0],
            totals_by_skill: [{ skill: 'partial-skill', interaction_count: 8 }],
          },
        ];
      }
      if (url === `users-failed-${repairDay}`) {
        throw new Error('download failed');
      }
      return makeUserTeamsDocument(url.replace(/^teams-/, ''));
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.deleteUserReportForDay).not.toHaveBeenCalled();
    expect(db.insertUserMetrics).not.toHaveBeenCalled();
    expect(db.insertCustomizationMetrics).not.toHaveBeenCalled();
    expect(
      api.fetchEnterpriseOrganizationUserTeamsLinks.mock.calls.some(
        ([, day]) => day === repairDay,
      ),
    ).toBe(false);
    expect(
      db.replaceTeamMemberships.mock.calls.some(([day]) => day === repairDay),
    ).toBe(false);
    const repairLog = db.upsertIngestionLog.mock.calls.find(
      ([row]) => row.day === repairDay,
    );
    expect(repairLog?.[0].status).toBe('error');
    expect(repairLog?.[0].components_loaded).not.toContain('userBreakdowns');
    expect(repairLog?.[0].error_message).toBe('download failed');
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining(repairDay),
      expect.any(Error),
    );
  });

  it('skips discovered organization backfills when every requested day is complete', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = DateTime.utc().minus({ days: 1 }).toISODate()!;
    db.getCurrentAccessibleEnterpriseOrganizationLogin.mockResolvedValue(
      'Org-A',
    );
    db.getMissingDays.mockResolvedValue([]);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runOrganizationBackfill('org-a', day, day);

    expect(db.claimEnterpriseOrganizationLease).not.toHaveBeenCalled();
    expect(api.verifyEnterpriseOrganizationAccess).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseOrganizationReportLinks).not.toHaveBeenCalled();
    expect(db.updateEnterpriseOrganizationState).not.toHaveBeenCalled();
  });

  it('hides an inaccessible organization and continues with the next organization', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-a',
      owner: 'worker',
    };
    db.getMissingDays.mockResolvedValue([]);
    db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a', 'org-b']);
    db.claimEnterpriseOrganizationLease.mockImplementation(
      async (_enterprise: string, organization: string) => ({
        ...lease,
        organization,
        owner: `worker-${organization}`,
      }),
    );
    api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a', 'org-b']);
    api.verifyEnterpriseOrganizationAccess.mockImplementation(
      async (organization: string) => {
        if (organization === 'org-a') {
          throw new InaccessibleEnterpriseOrganizationError();
        }
      },
    );
    api.fetchEnterpriseOrganizationReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserReportLinks.mockResolvedValue({
      download_links: [],
    });
    api.fetchEnterpriseOrganizationUserTeamsLinks.mockResolvedValue({
      download_links: [],
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });

    await task.runAsync();

    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      expect.objectContaining({ organization: 'org-a' }),
      expect.objectContaining({
        accessState: 'inaccessible',
        reportsReady: false,
        nextAttemptAt: expect.any(Date),
      }),
    );
    expect(db.updateEnterpriseOrganizationState).toHaveBeenCalledWith(
      expect.objectContaining({ organization: 'org-b' }),
      expect.objectContaining({ accessState: 'accessible' }),
    );
    expect(api.verifyEnterpriseOrganizationAccess.mock.calls).toEqual([
      ['org-a', expect.any(String)],
      ['org-b', expect.any(String)],
    ]);
    expect(db.insertDailyTotals).not.toHaveBeenCalled();
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledTimes(2);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining('imported history will be retained'),
    );
  });

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
      reportWindowStart,
      expect.any(String),
      ['totals', 'users', 'teams'],
    );
    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      backfillFromDate,
      expect.any(String),
      ['totals'],
    );
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
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
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([outsideWindowDay]);
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
      reportWindowStart,
      outsideWindowDay,
      ['totals', 'users', 'teams'],
    );
    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      outsideWindowDay,
      outsideWindowDay,
      ['totals'],
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
      expect.any(Object),
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
      'copilot.ingestCostCenters': false,
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
    expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.getDaysMissingCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.upsertCostCenters).not.toHaveBeenCalled();
    expect(db.upsertUserCostCenters).not.toHaveBeenCalled();
    expect(db.getUserMetricsForDay).not.toHaveBeenCalled();
    expect(db.getUserBreakdownsForDay).not.toHaveBeenCalled();
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
  });

  it('does not fetch a cost-center snapshot when no days need processing', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.upsertCostCenters).not.toHaveBeenCalled();
  });

  it('warns without an enterprise and continues organization ingestion', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();

    db.getMissingDays.mockResolvedValue(['2026-05-10']);
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.downloadDocument.mockResolvedValueOnce({
      day: '2026-05-10',
      organization_id: 'org-1',
      daily_active_users: 1,
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'copilot.ingestCostCenters is enabled, but copilot.enterprise is not configured',
      ),
    );
    expect(api.fetchOrganizationReportLinks).toHaveBeenCalledWith('2026-05-10');
    expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.getDaysMissingCostCenterMemberships).not.toHaveBeenCalled();
  });

  it('warns and skips organization cost centers when no organization is configured', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValue([day]);
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.downloadDocument.mockResolvedValueOnce({
      day,
      organization_id: 'org-1',
      daily_active_users: 1,
      weekly_active_users: 1,
      monthly_active_users: 1,
      code_acceptance_activity_count: 0,
      code_generation_activity_count: 0,
      loc_added_sum: 0,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
      user_initiated_interaction_count: 0,
    });

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'organization', 'org-1');

    expect(logger.warn).toHaveBeenCalledTimes(1);
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'Organization cost-center ingestion requires copilot.enterprise and the configured copilot.organization',
      ),
    );
    expect(api.fetchOrganizationReportLinks).toHaveBeenCalledWith(day);
    expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.getDaysMissingCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
  });

  it('ingests organization cost centers independently of team ingestion', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const organizationUsers = [
      {
        user_id: 1,
        user_login: 'octocat-user-1',
        day,
        used_agent: true,
        loc_added_sum: 10,
      },
      {
        user_id: 2,
        user_login: 'hubot',
        day,
        used_chat: true,
        loc_added_sum: 20,
      },
    ];
    db.getMissingDays.mockImplementation(async metricsType =>
      metricsType === 'organization' ? [day] : [],
    );
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, {
        metrics_type: 'organization',
        entity_id: 'org-1',
        loc_added_sum: 10,
      }),
      makeStoredUserMetric(day, {
        metrics_type: 'organization',
        entity_id: 'org-1',
        user_id: 2,
        user_login: 'hubot',
        loc_added_sum: 20,
      }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
      makeStoredUserBreakdown(2, 'hubot'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-totals.json'],
    });
    api.fetchOrganizationUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-users.json'],
    });
    api.downloadDocument.mockResolvedValueOnce({
      day,
      organization_id: 'org-1',
      daily_active_users: 2,
    });
    api.downloadNdjsonDocument.mockResolvedValue(organizationUsers);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'organization',
      'org-1',
      '2026-05-10',
      expect.any(String),
      ['totals', 'users'],
    );
    expect(db.getDaysMissingCostCenterMemberships).toHaveBeenCalledWith(
      'organization',
      'org-1',
      '2026-05-10',
      expect.any(String),
    );
    expect(api.fetchCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(api.fetchOrganizationUserReportLinks).toHaveBeenCalledWith(day);
    expect(api.fetchOrganizationUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.upsertCostCenters).toHaveBeenCalledWith([
      {
        metrics_type: 'organization',
        entity_id: 'org-1',
        cost_center_id: 'cc-1',
        name: 'Engineering',
      },
    ]);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'organization',
      'org-1',
      [
        expect.objectContaining({
          metrics_type: 'organization',
          entity_id: 'org-1',
          user_id: 1,
          cost_center_id: 'cc-1',
        }),
      ],
      true,
      expect.any(Object),
    );
    expect(logger.warn).not.toHaveBeenCalled();
  });

  it('stops scheduled cost-center aggregation when organization lease renewal fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const lease = {
      enterpriseId: 'ent-1',
      organization: 'org-1',
      owner: 'worker',
    };
    const days = ['2026-05-10', '2026-05-11'];
    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockImplementation(
      async metricsType => (metricsType === 'organization' ? days : []),
    );
    db.claimEnterpriseOrganizationLease.mockResolvedValue(lease);
    db.renewEnterpriseOrganizationLease
      .mockResolvedValueOnce(true)
      .mockResolvedValueOnce(false);
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config: createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.organization': 'org-1',
        'copilot.ingestCostCenters': true,
        'copilot.ingestOrganizationTeamReports': true,
      }),
      logger,
    });
    jest.spyOn(task as any, 'runOrganizationTeamIngestion').mockResolvedValue({
      generation: '1',
      configuredOrganizationAvailable: true,
    });
    jest
      .spyOn(task as any, 'loadCostCenterSnapshot')
      .mockResolvedValue(makeCostCenterSnapshot(new Map()));
    const ingestCostCenterMemberships = jest
      .spyOn(task as any, 'ingestCostCenterMemberships')
      .mockResolvedValue(undefined);

    await expect(task.runAsync()).rejects.toBeInstanceOf(
      OrganizationIngestionLeaseLostError,
    );

    expect(db.renewEnterpriseOrganizationLease).toHaveBeenCalledTimes(2);
    expect(ingestCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(ingestCostCenterMemberships).toHaveBeenCalledWith(
      days[0],
      'organization',
      'org-1',
      expect.any(Object),
      true,
      false,
      lease,
    );
    expect(db.releaseEnterpriseOrganizationLease).toHaveBeenCalledWith(lease);
  });

  it('shares one enterprise membership snapshot across enabled cost-center scopes', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.ingestCostCenters': true,
    });
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockImplementation(
      async (metricsType, entityId) => [
        makeStoredUserMetric(day, {
          metrics_type: metricsType,
          entity_id: entityId,
        }),
      ],
    );
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger: createLoggerMock(),
    });

    await task.runAsync();

    expect(api.fetchCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(db.upsertCostCenters).toHaveBeenCalledTimes(2);
    expect(db.upsertCostCenters).toHaveBeenCalledWith([
      expect.objectContaining({
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        cost_center_id: 'cc-1',
      }),
    ]);
    expect(db.upsertCostCenters).toHaveBeenCalledWith([
      expect.objectContaining({
        metrics_type: 'organization',
        entity_id: 'org-1',
        cost_center_id: 'cc-1',
      }),
    ]);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(2);
  });

  it('ingests enterprise users for cost centers without fetching teams', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 10 }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
        ['Skipping a cost-center organization with no members.'],
      ),
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

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      '2026-05-10',
      expect.any(String),
      ['totals', 'users'],
    );
    expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledWith(day);
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
    expect(db.upsertCostCenters).toHaveBeenCalledWith([
      {
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        cost_center_id: 'cc-1',
        name: 'Engineering',
      },
    ]);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [
        expect.objectContaining({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 1,
          user_login: 'octocat-user-1',
          cost_center_id: 'cc-1',
        }),
      ],
      true,
      expect.any(Object),
    );
    expect(db.upsertUserCostCenters).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
  });

  it('persists raw customization metrics alongside cost-center memberships', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-10-28';
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': day,
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const userDocument = [
      {
        ...makeUserMetricsDocument(day)[0],
        totals_by_skill: [{ skill: 'release-checks', interaction_count: 4 }],
      },
    ];
    const userBreakdown = makeStoredUserBreakdown(1, 'octocat-user-1');
    userBreakdown.customizations = [
      {
        category: 'skill',
        item_name: 'release-checks',
        interaction_count: 4,
      },
    ];

    db.getMissingDays.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 10 }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([userBreakdown]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/totals.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockResolvedValueOnce(userDocument);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger: createLoggerMock(),
    });

    await task.runAsync();

    expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledWith(day);
    expect(db.insertCustomizationMetrics).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          scope: 'user',
          category: 'skill',
          item_name: 'release-checks',
          interaction_count: 4,
        }),
      ],
      expect.any(Object),
    );
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      expect.arrayContaining([
        expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' }),
      ]),
      true,
      expect.any(Object),
    );
  });

  it.each(
    ['scheduled', 'backfill'].flatMap(source =>
      [
        {
          component: 'totals',
          costCenters: 'complete',
          unassignedComplete: false,
        },
        {
          component: 'teams',
          costCenters: 'complete',
          unassignedComplete: true,
        },
        {
          component: 'teams',
          costCenters: 'missing',
          unassignedComplete: true,
        },
        {
          component: 'totals',
          costCenters: 'incomplete',
          unassignedComplete: true,
        },
      ].map(scenario => ({ source, ...scenario })),
    ),
  )(
    'preserves cached memberships on $source $component retries with $costCenters cost centers',
    async ({ source, component, costCenters, unassignedComplete }) => {
      const db = createDbMock();
      const api = createApiMock();
      const day = DateTime.utc().minus({ days: 5 }).toISODate()!;
      const config = createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillFromDate': day,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestTeams': true,
        'copilot.ingestCostCenters': true,
      });
      db.getMissingDays.mockResolvedValue([day]);
      db.getIngestionLog.mockResolvedValue([
        {
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          status: 'partial',
          components_loaded:
            component === 'totals' ? '["users","teams"]' : '["totals","users"]',
          source,
        },
      ]);
      db.getDaysMissingCostCenterMemberships.mockResolvedValue(
        costCenters === 'missing' ? [day] : [],
      );
      db.getDaysWithIncompleteCostCenterMemberships.mockResolvedValue(
        costCenters === 'incomplete' ? [day] : [],
      );
      db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
      db.getUserBreakdownsForDay.mockResolvedValue([
        makeStoredUserBreakdown(1, 'octocat-user-1'),
      ]);
      api.fetchCostCenterMemberships.mockResolvedValue(
        makeCostCenterSnapshot(
          new Map([[1, { costCenterId: 'cc-new', login: 'octocat-user-1' }]]),
          [],
          true,
          unassignedComplete,
        ),
      );
      api.fetchEnterpriseReportLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/totals.json'],
      });
      api.downloadDocument.mockResolvedValue(makeEnterpriseDocument(day));
      api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/teams.json'],
      });
      api.downloadNdjsonDocument.mockResolvedValue(makeUserTeamsDocument(day));

      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config,
        logger: createLoggerMock(),
      });
      if (source === 'scheduled') {
        await task.runAsync();
      } else {
        await task.runBackfill(day, day, 'enterprise', 'ent-1');
      }

      expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
      expect(db.replaceTeamMemberships).toHaveBeenCalledTimes(
        component === 'teams' ? 1 : 0,
      );
      expect(db.upsertIngestionLog).toHaveBeenCalledWith(
        expect.objectContaining({ day, status: 'success' }),
        expect.any(Object),
      );
      const expectedReplacement = [
        day,
        'enterprise',
        'ent-1',
        [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-new' })],
        true,
        expect.any(Object),
      ];
      expect(db.replaceCostCenterMemberships.mock.calls).toEqual(
        costCenters === 'complete' ? [] : [expectedReplacement],
      );
    },
  );

  it('keeps team and cost-center memberships in separate scopes', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const usersDocument = makeUserMetricsDocument(day);
    usersDocument.push({
      ...usersDocument[0],
      user_id: 2,
      user_login: 'hubot',
      loc_added_sum: 20,
    });
    const teamsDocument = [
      ...makeUserTeamsDocument(day),
      {
        user_id: 2,
        user_login: 'hubot',
        day,
        enterprise_id: 'ent-1',
        team_id: 42,
        slug: 'platform',
      },
    ];

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 10 }),
      makeStoredUserMetric(day, {
        user_id: 2,
        user_login: 'hubot',
        loc_added_sum: 20,
      }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
      makeStoredUserBreakdown(2, 'hubot'),
    ]);
    db.getUserTeamsForDay.mockResolvedValue([
      makeStoredUserTeam(day, { user_id: 1, user_login: 'octocat-user-1' }),
      makeStoredUserTeam(day, { user_id: 2, user_login: 'hubot' }),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url.includes('users.json') ? usersDocument : teamsDocument,
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(1);
    const [, , , userCostCenters] =
      db.replaceCostCenterMemberships.mock.calls[0];
    expect(userCostCenters).toEqual([
      expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' }),
    ]);
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [
        expect.objectContaining({ user_id: 1, team_slug: 'platform' }),
        expect.objectContaining({ user_id: 2, team_slug: 'platform' }),
      ],
      expect.any(Object),
    );
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(day);
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
  });

  it('marks a successfully loaded day with no per-user rows without aggregating', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(new Map()),
    );
    api.fetchEnterpriseReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/report.json'],
    });
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.downloadDocument.mockResolvedValueOnce(makeEnterpriseDocument(day));
    api.downloadNdjsonDocument.mockResolvedValueOnce([]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [],
      true,
      expect.any(Object),
    );
    expect(db.insertDailyTotals).toHaveBeenCalledTimes(1);
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
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
    expect(db.insertByVscodeAgent).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          day: '2026-05-10',
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          session_count: 2,
          total_user_messages: 8,
        }),
      ],
      expect.any(Object),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day: '2026-05-10',
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status: 'success',
      }),
      expect.any(Object),
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
      expect.any(Object),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day: '2026-05-11', status: 'success' }),
      expect.any(Object),
    );
    expect(logger.error).toHaveBeenCalled();
  });

  it('preserves totals-only history and backfills users within the report window', async () => {
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
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': outsideWindowDay,
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });

    db.getMissingDays.mockResolvedValue([reportWindowStart]);
    db.getIngestionLog.mockResolvedValue([
      {
        day: reportWindowStart,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status: 'success',
        components_loaded: '["totals"]',
        source: 'scheduled',
      },
    ]);
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadNdjsonDocument
      .mockResolvedValueOnce(makeUserMetricsDocument(reportWindowStart))
      .mockResolvedValueOnce(makeUserTeamsDocument(reportWindowStart));

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
      reportWindowStart,
      expect.any(String),
      ['totals', 'users', 'teams'],
    );
    expect(db.getIngestionLog).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      reportWindowStart,
      reportWindowStart,
    );
    expect(api.fetchEnterpriseReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledWith(
      reportWindowStart,
    );
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(
      reportWindowStart,
    );
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalledWith(
      outsideWindowDay,
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day: reportWindowStart,
        status: 'success',
        components_loaded: JSON.stringify([
          'totals',
          'users',
          'userBreakdowns',
          'teams',
        ]),
      }),
      expect.any(Object),
    );
  });

  it('limits newly enabled cost-center aggregation to the report window', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
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

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);

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

    expect(db.getMissingDays).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      reportWindowStart,
      outsideWindowDay,
      ['totals', 'users'],
    );
    expect(db.getDaysMissingCostCenterMemberships).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      reportWindowStart,
      outsideWindowDay,
    );
    expect(api.fetchCostCenterMemberships).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
  });

  it('keeps core totals backfill behavior outside the user-report window', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
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

    db.getMissingDays
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([outsideWindowDay]);
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
      reportWindowStart,
      outsideWindowDay,
      ['totals', 'users', 'teams'],
    );
    expect(db.getMissingDays).toHaveBeenNthCalledWith(
      2,
      'enterprise',
      'ent-1',
      outsideWindowDay,
      outsideWindowDay,
      ['totals'],
    );
    expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledWith(
      outsideWindowDay,
    );
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
  });

  it('retries an in-window generic HTTP 400 for a newly required team report', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });
    const logger = createLoggerMock();
    const day = DateTime.utc().startOf('day').minus({ days: 2 }).toISODate()!;
    const genericBadRequest = Object.assign(new Error('Validation failed'), {
      status: 400,
      response: {
        status: 400,
        data: { message: 'Validation failed' },
      },
    });

    db.getMissingDays.mockResolvedValue([day]);
    db.getIngestionLog.mockResolvedValue([
      {
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status: 'success',
        components_loaded: '["totals","users"]',
        source: 'scheduled',
      },
    ]);
    db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchEnterpriseUserTeamsLinks.mockRejectedValue(genericBadRequest);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');
    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.fetchEnterpriseReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledTimes(2);
  });

  it('logs organization user-teams permission errors and continues ingestion', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const error = Object.assign(
      new Error('Resource not accessible by integration'),
      { status: 404 },
    );
    const config = createConfigMock({
      'copilot.organization': 'org-1',
      'copilot.backfillFromDate': day,
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
    });

    db.getMissingDays.mockResolvedValue([day]);
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.fetchOrganizationUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-users.json'],
    });
    api.fetchOrganizationUserTeamsLinks.mockRejectedValue(error);
    api.downloadDocument.mockResolvedValueOnce([
      {
        day,
        daily_active_users: 1,
        weekly_active_users: 1,
        monthly_active_users: 1,
      },
    ]);
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

    expect(api.fetchOrganizationUserTeamsLinks).toHaveBeenCalledWith(day);
    expect(db.insertDailyTotals).toHaveBeenCalledTimes(1);
    expect(db.insertUserMetrics).toHaveBeenCalledTimes(1);
    expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'partial',
        components_loaded: '["totals","users","userBreakdowns"]',
      }),
      expect.any(Object),
    );
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('GitHub returned HTTP 404'),
      error,
    );
    const permissionLog = (logger.error as jest.Mock).mock.calls.find(
      ([message]) =>
        typeof message === 'string' &&
        message.includes('organization user-teams report'),
    )?.[0] as string;
    expect(permissionLog).toContain(error.message);
    expect(permissionLog).toContain('likely due to missing permissions');
    expect(permissionLog).toContain('Organization Copilot metrics: read');
    expect(permissionLog).toContain('read:org');
    expect(permissionLog).toContain('manage_billing:copilot');
    expect(permissionLog).toContain('read:enterprise');
    expect(permissionLog).toContain(
      'https://github.com/backstage/community-plugins/blob/main/workspaces/copilot/plugins/copilot-backend/README.md#required-github-permissions',
    );
  });

  it('logs organization per-user permission errors and skips cost-center aggregation', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const error = Object.assign(new Error('Not Found'), { status: 403 });
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(new Map()),
    );
    api.fetchOrganizationReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/org-report.json'],
    });
    api.fetchOrganizationUserReportLinks.mockRejectedValue(error);
    api.downloadDocument.mockResolvedValueOnce([
      {
        day,
        daily_active_users: 1,
        weekly_active_users: 1,
        monthly_active_users: 1,
      },
    ]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'organization', 'org-1');

    expect(db.insertDailyTotals).toHaveBeenCalledTimes(1);
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        status: 'partial',
        components_loaded: '["totals"]',
      }),
      expect.any(Object),
    );
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('GitHub returned HTTP 403'),
      error,
    );
    const permissionLog = (logger.error as jest.Mock).mock.calls.find(
      ([message]) =>
        typeof message === 'string' &&
        message.includes('organization per-user report'),
    )?.[0] as string;
    expect(permissionLog).toContain(error.message);
    expect(permissionLog).toContain('likely due to missing permissions');
    expect(permissionLog).toContain('Organization Copilot metrics: read');
    expect(permissionLog).toContain('read:org');
    expect(permissionLog).toContain('manage_billing:copilot');
    expect(permissionLog).toContain('read:enterprise');
    expect(permissionLog).toContain(
      'https://github.com/backstage/community-plugins/blob/main/workspaces/copilot/plugins/copilot-backend/README.md#required-github-permissions',
    );
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

  it('backfills cost-center memberships alongside stored users without downloading reports', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 15 }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(db.getDaysMissingCostCenterMemberships).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
      day,
    );
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
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
    expect(api.downloadNdjsonDocument).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' })],
      true,
      expect.any(Object),
    );
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
  });

  it.each(['missing links', 'download failure', 'empty report'])(
    'refreshes legacy cost-center breakdowns and retries after %s',
    async failure => {
      const db = createDbMock();
      const api = createApiMock();
      const day = DateTime.utc().minus({ days: 30 }).toISODate()!;
      const config = createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillDelayMs': 0,
        'copilot.ingestTeams': true,
        'copilot.ingestCostCenters': true,
      });
      db.getMissingDays.mockResolvedValue([]);
      db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
      db.getIngestionLog.mockResolvedValue([
        {
          day,
          status: 'success',
          components_loaded: '["totals","users","teams"]',
        },
      ]);
      db.getUserMetricsForDay.mockResolvedValue([
        makeStoredUserMetric(day, { loc_added_sum: 15 }),
      ]);
      api.fetchCostCenterMemberships.mockResolvedValue(
        makeCostCenterSnapshot(
          new Map([[1, { costCenterId: 'cc-1', login: 'octocat' }]]),
        ),
      );
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/users.json'],
      });
      if (failure === 'missing links') {
        api.fetchEnterpriseUserReportLinks.mockResolvedValueOnce({
          download_links: [],
        });
      } else if (failure === 'download failure') {
        api.downloadNdjsonDocument.mockRejectedValueOnce(
          new Error('Report download failed'),
        );
      } else {
        api.downloadNdjsonDocument.mockResolvedValueOnce([]);
      }
      api.downloadNdjsonDocument.mockResolvedValue(
        makeUserMetricsDocument(day),
      );
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config,
        logger: createLoggerMock(),
      });

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
      expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();

      await task.runBackfill(day, day, 'enterprise', 'ent-1');

      expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledTimes(2);
      expect(api.fetchEnterpriseUserReportLinks).toHaveBeenLastCalledWith(day);
      expect(db.insertUserMetricsByFeature).toHaveBeenLastCalledWith(
        [expect.objectContaining({ day, feature: 'chat_panel' })],
        expect.any(Object),
      );
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(1);
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
        day,
        'enterprise',
        'ent-1',
        [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' })],
        true,
        expect.any(Object),
      );
      expect(api.fetchEnterpriseReportLinks).not.toHaveBeenCalled();
      expect(api.fetchEnterpriseUserTeamsLinks).not.toHaveBeenCalled();
      expect(db.insertDailyTotals).not.toHaveBeenCalled();
      expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
      expect(db.upsertIngestionLog).toHaveBeenCalledWith(
        expect.objectContaining({
          components_loaded: '["totals","users","teams","userBreakdowns"]',
        }),
        expect.any(Object),
      );
    },
  );

  it('repairs legacy raw metrics and persists the full assigned snapshot atomically', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const day = '2026-05-12';
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
      'copilot.ingestCostCenters': true,
    });
    const original = makeStoredUserMetric(day, { loc_added_sum: 15 });
    const transaction = {};
    let inTransaction = false;
    const refreshed = makeUserMetricsDocument(day).map(metric => ({
      ...metric,
      used_vscode_agent: true,
    }));
    refreshed.push({
      ...refreshed[0],
      user_id: 2,
      user_login: 'octocat-user-2',
    });
    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([original]);
    db.withIngestionDay.mockImplementation(
      async (_day, _scope, _entity, operation) => {
        expect(api.downloadNdjsonDocument).toHaveBeenCalledTimes(1);
        expect(db.insertUserMetrics).not.toHaveBeenCalled();
        inTransaction = true;
        try {
          return await operation(transaction, {
            components_loaded: '["totals","users","teams"]',
          });
        } finally {
          inTransaction = false;
        }
      },
    );
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([
          [1, { costCenterId: 'cc-1', login: 'octocat' }],
          [2, { costCenterId: 'cc-1', login: 'octocat-user-2' }],
        ]),
      ),
    );
    api.fetchEnterpriseUserReportLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/users.json'],
    });
    api.downloadNdjsonDocument.mockImplementation(async () => {
      expect(inTransaction).toBe(false);
      return refreshed;
    });
    db.insertUserMetrics.mockImplementation(async (_rows, tx) => {
      expect(inTransaction).toBe(true);
      expect(tx).toBe(transaction);
    });
    db.replaceCostCenterMemberships.mockImplementation(async (...args) => {
      expect(inTransaction).toBe(true);
      expect(args.at(-1)).toBe(transaction);
    });
    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger: createLoggerMock(),
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(db.getUserMetricsForDay).toHaveBeenCalledTimes(2);
    expect(db.getUserMetricsForDay).toHaveBeenLastCalledWith(
      'enterprise',
      'ent-1',
      day,
      transaction,
    );
    expect(db.getUserBreakdownsForDay).toHaveBeenLastCalledWith(
      'enterprise',
      'ent-1',
      day,
      transaction,
    );
    expect(db.withIngestionDay).toHaveBeenCalledTimes(1);
    expect(db.insertUserMetrics).toHaveBeenCalledTimes(1);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [
        expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' }),
        expect.objectContaining({ user_id: 2, cost_center_id: 'cc-1' }),
      ],
      true,
      expect.any(Object),
    );
    expect(db.insertDailyTotals).not.toHaveBeenCalled();
    expect(db.replaceTeamMemberships).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        components_loaded: '["totals","users","teams","userBreakdowns"]',
      }),
      transaction,
    );
  });

  it('replaces incomplete memberships after the snapshot becomes complete', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);
    db.getDaysWithIncompleteCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 15 }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(db.getDaysWithIncompleteCostCenterMemberships).toHaveBeenCalledWith(
      'enterprise',
      'ent-1',
      day,
      day,
    );
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' })],
      true,
      expect.any(Object),
    );
  });

  it('does not rewrite an incomplete day while the snapshot still has unknown members', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);
    db.getDaysWithIncompleteCostCenterMemberships.mockResolvedValue([day]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(new Map(), [], true, false),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.fetchCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
  });

  it('ingests named centers but omits no-cost-center data when an org installation is missing', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { user_id: 1, loc_added_sum: 15 }),
      makeStoredUserMetric(day, {
        user_id: 2,
        user_login: 'unassigned-user',
        loc_added_sum: 25,
      }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
      makeStoredUserBreakdown(2, 'unassigned-user'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
        [
          'Skipping organization resource "demo-org" in cost center "Demo"; the GitHub App is not installed for this organization.',
        ],
        true,
        false,
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    const [, , , userCostCenters, unassignedComplete] =
      db.replaceCostCenterMemberships.mock.calls[0];
    expect(userCostCenters).toEqual([
      expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' }),
    ]);
    expect(unassignedComplete).toBe(false);
  });

  it('backfills organization cost-center marker gaps from stored user data', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.organization': 'org-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': true,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, {
        metrics_type: 'organization',
        entity_id: 'org-1',
        loc_added_sum: 15,
      }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'organization', 'org-1');

    expect(db.getDaysMissingCostCenterMemberships).toHaveBeenCalledWith(
      'organization',
      'org-1',
      day,
      day,
    );
    expect(db.getUserMetricsForDay).toHaveBeenCalledWith(
      'organization',
      'org-1',
      day,
    );
    expect(db.getUserTeamsForDay).not.toHaveBeenCalled();
    expect(api.fetchCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(api.fetchOrganizationUserReportLinks).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'organization',
      'org-1',
      [expect.objectContaining({ user_id: 1, cost_center_id: 'cc-1' })],
      true,
      expect.any(Object),
    );
  });

  it('preserves existing cost-center data when the membership snapshot is incomplete', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';
    const persistedState = {
      memberships: [{ user_id: 4, cost_center_id: 'cc-previous' }],
      aggregates: [{ day, cost_center_id: 'cc-previous', active_users: 5 }],
    };
    const originalState = structuredClone(persistedState);

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-partial', login: 'octocat-user-1' }]]),
        ['An organization roster could not be resolved.'],
        false,
      ),
    );
    db.replaceCostCenterMemberships.mockImplementation(
      async (_day, _metricsType, _entityId, memberships) => {
        persistedState.memberships = memberships;
      },
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(persistedState).toEqual(originalState);
    expect(db.upsertCostCenters).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'preserving existing membership and aggregate data',
      ),
    );
  });

  it('marks a backfill day with an ingested but empty user report', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([day]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(new Map()),
    );
    db.getUserMetricsForDay.mockResolvedValue([]);

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.fetchEnterpriseUserReportLinks).not.toHaveBeenCalled();
    expect(api.downloadNdjsonDocument).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [],
      true,
      expect.any(Object),
    );
  });

  it('does not reprocess cost-center days after their marker is written', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-12';

    db.getMissingDays.mockResolvedValue([]);
    db.getDaysMissingCostCenterMemberships
      .mockResolvedValueOnce([day])
      .mockResolvedValueOnce([day])
      .mockResolvedValue([]);
    db.getUserMetricsForDay.mockResolvedValue([
      makeStoredUserMetric(day, { loc_added_sum: 15 }),
    ]);
    db.getUserBreakdownsForDay.mockResolvedValue([
      makeStoredUserBreakdown(1, 'octocat-user-1'),
    ]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      ),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runBackfill(day, day, 'enterprise', 'ent-1');
    await task.runBackfill(day, day, 'enterprise', 'ent-1');

    expect(api.fetchCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(db.getUserMetricsForDay).toHaveBeenCalledTimes(2);
    expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(1);
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
  });

  it.each(['scheduled', 'backfill'] as const)(
    'retries missing memberships after snapshot and replacement failures (%s)',
    async source => {
      const db = createDbMock();
      const api = createApiMock();
      const day = '2026-05-10';
      const config = createConfigMock({
        'copilot.enterprise': 'ent-1',
        'copilot.backfillFromDate': day,
        'copilot.backfillDelayMs': 0,
        'copilot.ingestTeams': false,
        'copilot.ingestCostCenters': true,
      });
      let pending = true;
      db.getMissingDays.mockResolvedValue([day]);
      db.getDaysMissingCostCenterMemberships.mockImplementation(async () =>
        pending ? [day] : [],
      );
      const snapshot = makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-1', login: 'octocat-user-1' }]]),
      );
      if (source === 'backfill') {
        snapshot.unassignedComplete = false;
        db.getDaysWithIncompleteCostCenterMemberships.mockResolvedValue([day]);
      }
      if (source === 'scheduled') {
        api.fetchCostCenterMemberships.mockRejectedValueOnce(
          new Error('unavailable'),
        );
      } else {
        api.fetchCostCenterMemberships.mockResolvedValueOnce(
          makeCostCenterSnapshot(new Map(), ['incomplete'], false),
        );
      }
      api.fetchCostCenterMemberships.mockResolvedValue(snapshot);
      api.fetchEnterpriseReportLinks.mockResolvedValue({
        download_links: ['totals'],
      });
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links: ['users'],
      });
      api.downloadDocument.mockResolvedValue(makeEnterpriseDocument(day));
      api.downloadNdjsonDocument.mockResolvedValue(
        makeUserMetricsDocument(day),
      );
      db.replaceCostCenterMemberships
        .mockRejectedValueOnce(new Error('replacement failed'))
        .mockImplementation(async () => {
          pending = false;
        });
      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config,
        logger: createLoggerMock(),
      });
      const run = () =>
        source === 'scheduled'
          ? task.runAsync()
          : task.runBackfill(day, day, 'enterprise', 'ent-1');

      await run();
      expect(pending).toBe(true);
      expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();

      db.getMissingDays.mockResolvedValue([]);
      db.getUserMetricsForDay.mockResolvedValue([makeStoredUserMetric(day)]);
      db.getUserBreakdownsForDay.mockResolvedValue([
        makeStoredUserBreakdown(1, 'octocat-user-1'),
      ]);
      await run();
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(1);
      expect(pending).toBe(true);
      await run();
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(2);
      expect(pending).toBe(false);

      // Retrying raw ingestion must not requeue a completed membership snapshot.
      db.getMissingDays.mockResolvedValue([day]);
      db.getIngestionLog.mockResolvedValue([
        { components_loaded: '["totals","users"]' },
      ]);
      await run();
      expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledTimes(1);
      expect(db.replaceCostCenterMemberships).toHaveBeenCalledTimes(2);
      expect(pending).toBe(false);
    },
  );

  it('continues core ingestion and leaves cost-center days unmarked when listing fails', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);
    api.fetchCostCenterMemberships.mockRejectedValue(
      new Error('cost-center list failed'),
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

    expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledWith(day);
    expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledWith(day);
    expect(db.insertUserMetrics).toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day, status: 'success' }),
      expect.any(Object),
    );
    expect(db.upsertCostCenters).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining(
        'Failed to load cost-center membership for enterprise:ent-1',
      ),
      expect.any(Error),
    );
  });

  it('does not replace cost-center data when a scheduled roster snapshot is incomplete', async () => {
    const db = createDbMock();
    const api = createApiMock();
    const config = createConfigMock({
      'copilot.enterprise': 'ent-1',
      'copilot.backfillFromDate': '2026-05-10',
      'copilot.backfillDelayMs': 0,
      'copilot.ingestTeams': false,
      'copilot.ingestCostCenters': true,
    });
    const logger = createLoggerMock();
    const day = '2026-05-10';
    const persistedState = {
      memberships: [{ user_id: 4, cost_center_id: 'cc-previous' }],
      aggregates: [{ day, cost_center_id: 'cc-previous', active_users: 5 }],
    };
    const originalState = structuredClone(persistedState);

    db.getMissingDays.mockResolvedValue([day]);
    db.getDaysMissingCostCenterMemberships.mockResolvedValue([]);
    api.fetchCostCenterMemberships.mockResolvedValue(
      makeCostCenterSnapshot(
        new Map([[1, { costCenterId: 'cc-partial', login: 'octocat-user-1' }]]),
        ['An organization roster could not be resolved.'],
        false,
      ),
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
    db.replaceCostCenterMemberships.mockImplementation(
      async (_day, _metricsType, _entityId, memberships) => {
        persistedState.memberships = memberships;
      },
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.insertUserMetrics).toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day, status: 'success' }),
      expect.any(Object),
    );
    expect(persistedState).toEqual(originalState);
    expect(db.upsertCostCenters).not.toHaveBeenCalled();
    expect(db.replaceCostCenterMemberships).not.toHaveBeenCalled();
    expect(db.markCostCenterMembershipsDone).not.toHaveBeenCalled();
    expect(logger.warn).toHaveBeenCalledWith(
      expect.stringContaining(
        'preserving existing membership and aggregate data',
      ),
    );
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
      expect.any(Object),
    );
  });

  it.each([
    { status: 'partial', cached: true },
    { status: 'partial', cached: false },
    { status: 'error', cached: false },
  ])('retries $status (breakdowns: $cached)', async ({ status, cached }) => {
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
    const userDocument = makeUserMetricsDocument(day);
    const storedUserMetric = {
      day,
      metrics_type: 'enterprise',
      entity_id: 'ent-1',
      user_id: 1,
      user_login: 'user-1',
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
      user_login: 'user-1',
      byFeature: userDocument[0].totals_by_feature,
      byIde: userDocument[0].totals_by_ide,
      byLanguageFeature: [],
      byModelFeature: [],
      byLanguageModel: [],
      customizations: [],
    };

    db.getMissingDays
      .mockResolvedValueOnce([day])
      .mockResolvedValue(cached ? [] : [day]);
    db.getIngestionLog.mockResolvedValue([
      {
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        status,
        components_loaded: '["totals","users"]',
        source: 'scheduled',
      },
    ]);
    db.getUserMetricsForDay.mockResolvedValue([storedUserMetric]);
    db.getUserBreakdownsForDay.mockResolvedValue(
      cached ? [storedBreakdown] : [],
    );
    api.fetchEnterpriseUserReportLinks
      .mockResolvedValueOnce({ download_links: [] })
      .mockResolvedValue({
        download_links: ['https://downloads.github.com/users.json'],
      });
    api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
      download_links: ['https://downloads.github.com/user-teams.json'],
    });
    api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
      url.endsWith('/users.json') ? userDocument : makeUserTeamsDocument(day),
    );

    const task = TaskManagementV2.create({
      db: db as unknown as DatabaseHandlerV2,
      api: api as unknown as GithubClientV2,
      config,
      logger,
    });

    await task.runAsync();

    expect(db.insertDailyTotals).not.toHaveBeenCalled();
    expect(db.upsertIngestionLog).toHaveBeenLastCalledWith(
      expect.objectContaining({
        day,
        status: cached ? 'success' : 'partial',
        components_loaded: cached
          ? '["totals","users","teams"]'
          : '["totals","users"]',
      }),
      expect.any(Object),
    );
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
    expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledTimes(
      cached ? 0 : 2,
    );
    expect(db.insertUserMetricsByFeature).toHaveBeenCalledTimes(cached ? 0 : 1);
    expect(api.fetchEnterpriseUserTeamsLinks).toHaveBeenCalledWith(day);
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      day,
      'enterprise',
      'ent-1',
      [expect.objectContaining({ day, team_slug: 'platform' })],
      expect.any(Object),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({
        day,
        status: 'success',
        components_loaded: cached
          ? '["totals","users","teams"]'
          : '["totals","users","userBreakdowns","teams"]',
      }),
      expect.any(Object),
    );
  });

  it.each([
    { report: 'empty', userRows: [] },
    { report: 'invalid', userRows: [{}] },
  ])(
    'refetches $report user reports on partial team retries until user rows become available',
    async ({ userRows }) => {
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

      db.getMissingDays.mockResolvedValue([day]);
      db.getIngestionLog
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValue([
          {
            day,
            metrics_type: 'enterprise',
            entity_id: 'ent-1',
            status: 'partial',
            components_loaded: '["totals","users"]',
            source: 'scheduled',
          },
        ]);
      db.getUserMetricsForDay.mockResolvedValue([]);
      db.getUserBreakdownsForDay.mockResolvedValue([]);
      api.fetchEnterpriseReportLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/totals.json'],
      });
      api.downloadDocument.mockResolvedValue(makeEnterpriseDocument(day));
      api.fetchEnterpriseUserReportLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/users.json'],
      });
      api.fetchEnterpriseUserTeamsLinks.mockResolvedValue({
        download_links: ['https://downloads.github.com/user-teams.json'],
      });
      const userReports = [userRows, userRows, makeUserMetricsDocument(day)];
      api.downloadNdjsonDocument.mockImplementation(async (url: string) =>
        url.endsWith('/users.json')
          ? userReports.shift() ?? []
          : makeUserTeamsDocument(day),
      );

      const task = TaskManagementV2.create({
        db: db as unknown as DatabaseHandlerV2,
        api: api as unknown as GithubClientV2,
        config,
        logger,
      });

      for (const attempt of [1, 2]) {
        await task.runAsync();

        expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledTimes(
          attempt,
        );
        expect(db.upsertIngestionLog).toHaveBeenLastCalledWith(
          expect.objectContaining({
            day,
            status: 'partial',
            components_loaded: '["totals","users","userBreakdowns"]',
            error_message: 'Missing components: teams',
          }),
          expect.any(Object),
        );
        expect(db.insertUserMetrics).toHaveBeenLastCalledWith(
          [],
          expect.any(Object),
        );
        expect(db.insertDailyTotals).toHaveBeenCalledTimes(1);
      }

      await task.runAsync();

      expect(api.fetchEnterpriseReportLinks).toHaveBeenCalledTimes(1);
      expect(api.fetchEnterpriseUserReportLinks).toHaveBeenCalledTimes(3);
      expect(db.getUserMetricsForDay).toHaveBeenCalledTimes(6);
      expect(db.insertUserMetrics).toHaveBeenLastCalledWith(
        [expect.objectContaining({ day, user_id: 1 })],
        expect.any(Object),
      );
      expect(db.replaceTeamMemberships).toHaveBeenLastCalledWith(
        day,
        'enterprise',
        'ent-1',
        [expect.objectContaining({ day, team_slug: 'platform' })],
        expect.any(Object),
      );
      expect(db.upsertIngestionLog).toHaveBeenLastCalledWith(
        expect.objectContaining({
          day,
          status: 'success',
          components_loaded: '["totals","users","userBreakdowns","teams"]',
        }),
        expect.any(Object),
      );
      expect(logger.error).not.toHaveBeenCalled();
    },
  );

  it('persists per-user breakdown rows alongside team memberships', async () => {
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
    expect(db.insertUserMetricsByFeature).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          day: '2026-05-10',
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 1,
          user_login: 'octocat',
          feature: 'chat_panel',
        }),
      ],
      expect.any(Object),
    );
    expect(db.insertUserMetricsByIde).toHaveBeenCalledWith(
      [
        expect.objectContaining({
          user_id: 1,
          user_login: 'octocat',
          ide: 'vscode',
        }),
      ],
      expect.any(Object),
    );
    expect(db.replaceTeamMemberships).toHaveBeenCalledWith(
      '2026-05-10',
      'enterprise',
      'ent-1',
      expect.arrayContaining([
        expect.objectContaining({ team_slug: 'platform' }),
      ]),
      expect.any(Object),
    );
    expect(db.upsertIngestionLog).toHaveBeenCalledWith(
      expect.objectContaining({ day: '2026-05-10', status: 'success' }),
      expect.any(Object),
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
    expect.any(Object),
  );
  expect(db.upsertIngestionLog).not.toHaveBeenCalledWith(
    expect.objectContaining({ status: 'success' }),
    expect.any(Object),
  );
});

function makeCostCenterSnapshot(
  members: Map<number, { costCenterId: string; login: string }>,
  warnings: string[] = [],
  complete = true,
  unassignedComplete = true,
) {
  return {
    costCenters: [{ id: 'cc-1', name: 'Engineering' }],
    members,
    warnings,
    complete,
    unassignedComplete,
  };
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
    user_login: 'octocat-user-1',
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
    user_login: 'octocat-user-1',
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

function createDbMock() {
  const getIngestionLog = jest.fn().mockResolvedValue([]);
  const withIngestionDay = jest.fn(
    async (
      day: string,
      metricsType: string,
      entityId: string,
      operation: (
        transaction: object,
        existingLog: { components_loaded: string; status: string },
      ) => Promise<unknown>,
    ) => {
      const [existingLog] = await getIngestionLog(
        metricsType,
        entityId,
        day,
        day,
      );
      return operation(
        {},
        existingLog ?? { components_loaded: '[]', status: 'partial' },
      );
    },
  );
  const db = {
    getMissingDays: jest.fn(),
    getDaysMissingCostCenterMemberships: jest.fn(
      async (_scope, _entity, from, _to, transaction?) =>
        transaction ? [from] : [],
    ),
    getDaysWithIncompleteCostCenterMemberships: jest.fn().mockResolvedValue([]),
    getIngestionLog,
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
    insertUserMetricsByFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByIde: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByLanguageFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByModelFeature: jest.fn().mockResolvedValue(undefined),
    insertUserMetricsByLanguageModel: jest.fn().mockResolvedValue(undefined),
    deleteUserReportForDay: jest.fn().mockResolvedValue(undefined),
    getUserMetricsForDay: jest.fn().mockResolvedValue([]),
    getUserBreakdownsForDay: jest.fn().mockResolvedValue([]),
    getUserTeamsForDay: jest.fn().mockResolvedValue([]),
    replaceTeamMemberships: jest.fn().mockResolvedValue(undefined),
    withIngestionDay,
    syncEnterpriseOrganizations: jest.fn().mockResolvedValue({
      configuredOrganizationLogin: undefined,
      configuredOrganizationAmbiguous: false,
    }),
    beginEnterpriseOrganizationDirectorySync: jest.fn().mockResolvedValue('1'),
    isEnterpriseOrganizationDirectoryGenerationCurrent: jest
      .fn()
      .mockResolvedValue(true),
    markEnterpriseOrganizationDirectoryFailed: jest
      .fn()
      .mockResolvedValue('marked'),
    markLegacyConfiguredOrganization: jest.fn().mockResolvedValue('updated'),
    markEnterpriseOrganizationInaccessible: jest
      .fn()
      .mockResolvedValue(undefined),
    getCurrentAccessibleEnterpriseOrganizationLogin: jest
      .fn()
      .mockResolvedValue(undefined),
    getEnterpriseOrganizationsToIngest: jest.fn().mockResolvedValue([]),
    claimEnterpriseOrganizationLease: jest.fn().mockResolvedValue(undefined),
    renewEnterpriseOrganizationLease: jest.fn().mockResolvedValue(true),
    releaseEnterpriseOrganizationLease: jest.fn().mockResolvedValue(undefined),
    withOrganizationIngestionLease: jest.fn(
      async (
        _lease: unknown,
        operation: (database: DatabaseHandlerV2) => Promise<void>,
      ): Promise<void> => operation(db as unknown as DatabaseHandlerV2),
    ),
    updateEnterpriseOrganizationState: jest.fn().mockResolvedValue(undefined),
    upsertCostCenters: jest.fn().mockResolvedValue(undefined),
    upsertUserCostCenters: jest.fn().mockResolvedValue(undefined),
    markCostCenterMembershipsDone: jest.fn().mockResolvedValue(undefined),
    replaceCostCenterMemberships: jest.fn().mockResolvedValue(undefined),
    upsertIngestionLog: jest.fn().mockResolvedValue(undefined),
  };
  return db;
}

function configureRecentOrganizationTeamRefresh(
  db: ReturnType<typeof createDbMock>,
  api: ReturnType<typeof createApiMock>,
  unmarkedDay: string,
) {
  db.getMissingDays.mockResolvedValue([]);
  db.getIngestionLog.mockImplementation(
    async (_metricsType: string, _entityId: string, day: string) => [
      {
        components_loaded:
          day === unmarkedDay
            ? '["totals","users","teams"]'
            : '["totals","users","userBreakdowns","teams"]',
      },
    ],
  );
  db.getUserMetricsForDay.mockImplementation(
    async (_metricsType: string, _entityId: string, day: string) => [
      makeStoredUserMetric(day, {
        metrics_type: 'organization',
        entity_id: 'org-a',
      }),
    ],
  );
  db.getUserBreakdownsForDay.mockResolvedValue([
    makeStoredUserBreakdown(1, 'octocat'),
  ]);
  db.getEnterpriseOrganizationsToIngest.mockResolvedValue(['org-a']);
  db.claimEnterpriseOrganizationLease.mockResolvedValue({
    enterpriseId: 'ent-1',
    organization: 'org-a',
    owner: 'worker',
    directoryGeneration: '1',
  });
  api.fetchEnterpriseOrganizations.mockResolvedValue(['org-a']);
  api.verifyEnterpriseOrganizationAccess.mockResolvedValue(undefined);
  api.fetchEnterpriseOrganizationUserTeamsLinks.mockImplementation(
    async (_organization: string, day: string) => ({
      download_links: [`teams-${day}`],
    }),
  );
}

function createApiMock() {
  return {
    fetchEnterpriseReportLinks: jest.fn(),
    fetchOrganizationReportLinks: jest.fn(),
    fetchEnterpriseOrganizations: jest.fn(),
    verifyEnterpriseOrganizationAccess: jest.fn().mockResolvedValue(true),
    fetchEnterpriseOrganizationReportLinks: jest.fn(),
    fetchEnterpriseOrganizationUserReportLinks: jest.fn(),
    fetchEnterpriseOrganizationUserTeamsLinks: jest.fn(),
    fetchEnterpriseUserReportLinks: jest.fn(),
    fetchOrganizationUserReportLinks: jest.fn(),
    fetchEnterpriseUserTeamsLinks: jest.fn(),
    fetchOrganizationUserTeamsLinks: jest.fn(),
    fetchCostCenterMemberships: jest.fn(),
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
