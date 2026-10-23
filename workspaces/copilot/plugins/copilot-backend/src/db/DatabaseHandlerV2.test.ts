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

import { TestDatabaseId, TestDatabases } from '@backstage/backend-test-utils';
import { Knex } from 'knex';
import {
  NO_COST_CENTER_ID,
  V2DailyTotal,
  V2IngestionLogRow,
  V2UserMetricRow,
  V2UserCostCenterRow,
  V2UserMetricsByFeatureRow,
  V2UserMetricsByIdeRow,
  V2UserMetricsByLanguageFeatureRow,
  V2UserMetricsByModelFeatureRow,
  V2UserMetricsByLanguageModelRow,
  V2UserTeamRow,
} from '@backstage-community/plugin-copilot-common';
import { migrationsDir } from './DatabaseHandler';
import type { CostCenterAggregateRows } from './DatabaseHandlerV2';
import { DatabaseHandlerV2 } from './DatabaseHandlerV2';

jest.setTimeout(60_000);

const costCenterMigration = jest.requireActual<{
  up(knex: Knex): Promise<void>;
  down(knex: Knex): Promise<void>;
}>('../../migrations/202610220000_add_cost_centers.js');

const costCenterAggregateUniqueColumns: Record<string, string[]> = {
  copilot_daily_totals: ['day', 'metrics_type', 'entity_id', 'team_slug'],
  copilot_pr_metrics: ['day', 'metrics_type', 'entity_id', 'team_slug'],
  copilot_metrics_by_feature: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
    'feature',
  ],
  copilot_metrics_by_ide: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
    'ide',
  ],
  copilot_metrics_by_language_feature: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
    'language',
    'feature',
  ],
  copilot_metrics_by_model_feature: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
    'model_id',
    'feature',
  ],
  copilot_metrics_by_language_model: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
    'language',
    'model_id',
  ],
  copilot_metrics_by_cli: ['day', 'metrics_type', 'entity_id', 'team_slug'],
  copilot_metrics_by_vscode_agent: [
    'day',
    'metrics_type',
    'entity_id',
    'team_slug',
  ],
};

const costCenterAggregateTables = Object.keys(costCenterAggregateUniqueColumns);

async function createLegacyAggregateSchema(knex: Knex): Promise<void> {
  for (const [tableName, uniqueColumns] of Object.entries(
    costCenterAggregateUniqueColumns,
  )) {
    await knex.schema.createTable(tableName, table => {
      table.increments('id').primary();
      table.date('day').notNullable();
      table.string('metrics_type', 64).notNullable();
      table.string('entity_id', 64).notNullable();
      table.string('team_slug', 64).notNullable().defaultTo('');

      for (const column of uniqueColumns.slice(4)) {
        table.string(column, 64).notNullable();
      }

      if (knex.client.config.client === 'mysql2') {
        table.unique(
          uniqueColumns,
          `uq_legacy_${tableName.replace(/^copilot_/, '')}`,
        );
      } else {
        table.unique(uniqueColumns);
      }
    });

    await knex.schema.alterTable(tableName, table => {
      table.index(
        ['metrics_type', 'entity_id', 'team_slug', 'day'],
        `idx_${tableName}_type_entity_team_day`,
      );
    });
  }
}

describe('DatabaseHandlerV2', () => {
  const databases = TestDatabases.create();

  async function createDatabase(databaseId: TestDatabaseId) {
    const knex = await databases.init(databaseId);
    await knex.migrate.latest({ directory: migrationsDir });
    return knex;
  }

  describe.each(databases.eachSupportedId())('database: %s', databaseId => {
    let knex: Knex;
    let handler: DatabaseHandlerV2;

    if (databaseId.startsWith('MYSQL')) {
      it('adds and reverses the cost-center migration on MySQL', async () => {
        const mysqlKnex = await databases.init(databaseId);

        try {
          await createLegacyAggregateSchema(mysqlKnex);
          await costCenterMigration.up(mysqlKnex);

          await mysqlKnex('copilot_daily_totals').insert([
            {
              day: '2026-05-01',
              metrics_type: 'organization',
              entity_id: 'org-1',
              team_slug: '',
              cost_center_id: '',
            },
            {
              day: '2026-05-01',
              metrics_type: 'organization',
              entity_id: 'org-1',
              team_slug: '',
              cost_center_id: 'cc-a',
            },
            {
              day: '2026-05-01',
              metrics_type: 'organization',
              entity_id: 'org-1',
              team_slug: '',
              cost_center_id: 'cc-b',
            },
          ]);

          await costCenterMigration.down(mysqlKnex);

          for (const tableName of costCenterAggregateTables) {
            expect(
              await mysqlKnex.schema.hasColumn(tableName, 'cost_center_id'),
            ).toBe(false);
          }
          expect(await mysqlKnex.schema.hasTable('copilot_cost_centers')).toBe(
            false,
          );
          expect(
            await mysqlKnex.schema.hasTable('copilot_user_cost_centers'),
          ).toBe(false);
          expect(
            await mysqlKnex.schema.hasTable(
              'copilot_cost_center_aggregation_log',
            ),
          ).toBe(false);
          const rows = await mysqlKnex('copilot_daily_totals').select('*');
          expect(rows).toHaveLength(1);
          expect(rows[0]).not.toHaveProperty('cost_center_id');
        } finally {
          await mysqlKnex.destroy();
        }
      });
      return;
    }

    beforeEach(async () => {
      knex = await createDatabase(databaseId);
      handler = await DatabaseHandlerV2.create({
        database: {
          getClient: async () => knex,
          migrations: { skip: true },
        } as any,
      });
    });

    afterEach(async () => {
      await knex?.destroy();
    });

    it('adds and reverses the cost-center schema migration', async () => {
      await costCenterMigration.down(knex);

      for (const tableName of costCenterAggregateTables) {
        expect(await knex.schema.hasColumn(tableName, 'cost_center_id')).toBe(
          false,
        );
      }
      await expect(knex.schema.hasTable('copilot_cost_centers')).resolves.toBe(
        false,
      );
      await expect(
        knex.schema.hasTable('copilot_user_cost_centers'),
      ).resolves.toBe(false);
      await expect(
        knex.schema.hasTable('copilot_cost_center_aggregation_log'),
      ).resolves.toBe(false);

      await costCenterMigration.up(knex);

      for (const tableName of costCenterAggregateTables) {
        expect(await knex.schema.hasColumn(tableName, 'cost_center_id')).toBe(
          true,
        );
      }
      expect(
        await knex.schema.hasColumn('copilot_cost_centers', 'metrics_type'),
      ).toBe(true);

      await handler.insertDailyTotals([
        buildDailyTotal({ cost_center_id: 'cc-a' }),
        buildDailyTotal({ cost_center_id: 'cc-b' }),
      ]);
      const rows = await knex('copilot_daily_totals').where({
        day: '2026-05-01',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });
      expect(rows).toHaveLength(2);
    });

    it('preserves legacy aggregate rows and merges them after migration', async () => {
      await costCenterMigration.down(knex);
      await knex('copilot_daily_totals').insert(
        buildDailyTotal({
          day: '2026-05-09',
          total_ai_credits_used: 3,
          daily_active_vscode_agent_users: 7,
        }),
      );

      await costCenterMigration.up(knex);

      const migratedRows = await knex('copilot_daily_totals').where({
        day: '2026-05-09',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });
      expect(migratedRows).toHaveLength(1);
      expect(migratedRows[0].cost_center_id).toBe('');

      await handler.insertDailyTotals([
        buildDailyTotal({
          day: '2026-05-09',
          total_ai_credits_used: 9,
          daily_active_vscode_agent_users: 11,
        }),
      ]);

      const mergedRows = await knex('copilot_daily_totals').where({
        day: '2026-05-09',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
        cost_center_id: '',
      });
      expect(mergedRows).toHaveLength(1);
      expect(mergedRows[0].total_ai_credits_used).toBe(9);
      expect(mergedRows[0].daily_active_vscode_agent_users).toBe(11);

      await costCenterMigration.down(knex);
      const rolledBackRows = await knex('copilot_daily_totals').where({
        day: '2026-05-09',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });
      expect(rolledBackRows).toHaveLength(1);
      expect(rolledBackRows[0]).not.toHaveProperty('cost_center_id');
      expect(rolledBackRows[0].total_ai_credits_used).toBe(9);
    });

    it('getMissingDays returns all days when ingestion log is empty', async () => {
      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(result).toEqual(['2026-05-01', '2026-05-02', '2026-05-03']);
    });

    it('getMissingDays skips successful days and returns remaining days', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-01', status: 'success' }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-03', status: 'success' }),
      );

      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(result).toEqual(['2026-05-02']);
    });

    it('getMissingDays returns empty array when all days are successful', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-01', status: 'success' }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-02', status: 'success' }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-03', status: 'success' }),
      );

      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(result).toEqual([]);
    });

    it('getMissingDays does not skip error days', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-01', status: 'success' }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-02', status: 'error' }),
      );

      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(result).toEqual(['2026-05-02', '2026-05-03']);
    });

    it('getMissingDays only skips success rows with all required components', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-01',
          status: 'success',
          components_loaded: '["totals"]',
        }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-02',
          status: 'success',
          components_loaded: '["totals","users","teams"]',
        }),
      );

      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
        ['totals', 'users', 'teams'],
      );

      expect(result).toEqual(['2026-05-01', '2026-05-03']);
    });

    it('upsertIngestionLog inserts a new row', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-10',
          status: 'success',
          components_loaded: '["totals","users"]',
        }),
      );

      const logs = await handler.getIngestionLog('organization', 'org-1');
      expect(logs).toHaveLength(1);
      expect(typeof logs[0].day).toBe('string');
      expect(logs[0].day).toBe('2026-05-10');
      expect(logs[0].status).toBe('success');
      expect(logs[0].components_loaded).toBe('["totals","users"]');
    });

    it('upsertIngestionLog updates existing row on conflict', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-11', status: 'success' }),
      );

      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-11',
          status: 'error',
          components_loaded: '["totals"]',
          error_message: 'download failed',
          source: 'backfill',
        }),
      );

      const logs = await handler.getIngestionLog('organization', 'org-1');
      expect(logs).toHaveLength(1);
      expect(logs[0].status).toBe('error');
      expect(logs[0].components_loaded).toBe('["totals"]');
      expect(logs[0].error_message).toBe('download failed');
      expect(logs[0].source).toBe('backfill');
    });

    it('tracks cost-center aggregation idempotently and reads back a day for backfill', async () => {
      await handler.insertUserMetrics([
        buildUserMetric({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
        buildUserMetric({
          day: '2026-05-02',
          user_id: 2,
          user_login: 'octodog',
        }),
      ]);
      await handler.insertUserMetricsByFeature([
        buildUserMetricsByFeature({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);
      await handler.insertUserMetricsByIde([
        buildUserMetricsByIde({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);
      await handler.insertUserMetricsByLanguageFeature([
        buildUserMetricsByLanguageFeature({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);
      await handler.insertUserMetricsByModelFeature([
        buildUserMetricsByModelFeature({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);
      await handler.insertUserMetricsByLanguageModel([
        buildUserMetricsByLanguageModel({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);
      await handler.insertUserTeams([
        buildUserTeam({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
          team_slug: 'alpha',
        }),
      ]);

      await expect(
        handler.getDaysMissingCostCenterAggregation(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-03',
        ),
      ).resolves.toEqual(['2026-05-01', '2026-05-02']);
      await handler.markCostCenterAggregationDone(
        '2026-05-01',
        'organization',
        'org-1',
      );
      await handler.markCostCenterAggregationDone(
        '2026-05-01',
        'organization',
        'org-1',
      );

      const [metric] = await handler.getUserMetricsForDay(
        'organization',
        'org-1',
        '2026-05-01',
      );
      const [breakdown] = await handler.getUserBreakdownsForDay(
        'organization',
        'org-1',
        '2026-05-01',
      );
      const teams = await handler.getUserTeamsForDay(
        'organization',
        'org-1',
        '2026-05-01',
      );

      expect(metric).toEqual(
        expect.objectContaining({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      );
      expect(breakdown).toEqual(
        expect.objectContaining({
          user_id: 1,
          user_login: 'octocat',
          byFeature: [expect.objectContaining({ feature: 'chat_panel' })],
          byIde: [expect.objectContaining({ ide: 'vscode' })],
          byLanguageFeature: [
            expect.objectContaining({
              language: 'typescript',
              feature: 'chat_panel',
            }),
          ],
          byModelFeature: [
            expect.objectContaining({
              model_id: 'gpt-4o',
              feature: 'chat_panel',
            }),
          ],
          byLanguageModel: [
            expect.objectContaining({
              language: 'typescript',
              model_id: 'gpt-4o',
            }),
          ],
        }),
      );
      expect(teams).toEqual([
        expect.objectContaining({
          day: '2026-05-01',
          user_id: 1,
          team_slug: 'alpha',
        }),
      ]);
      expect(
        await knex('copilot_cost_center_aggregation_log').where({
          day: '2026-05-01',
          metrics_type: 'organization',
          entity_id: 'org-1',
        }),
      ).toHaveLength(1);
      await expect(
        handler.getDaysMissingCostCenterAggregation(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-03',
        ),
      ).resolves.toEqual(['2026-05-02']);

      await handler.markCostCenterAggregationDone(
        '2026-05-02',
        'organization',
        'org-1',
      );
      await expect(
        handler.getDaysMissingCostCenterAggregation(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-03',
        ),
      ).resolves.toEqual([]);
    });

    it('includes successfully ingested empty user reports in cost-center backfills', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-03',
          components_loaded: '["totals","users"]',
        }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-04',
          components_loaded: '["totals"]',
        }),
      );

      await expect(
        handler.getDaysMissingCostCenterAggregation(
          'organization',
          'org-1',
          '2026-05-03',
          '2026-05-04',
        ),
      ).resolves.toEqual(['2026-05-03']);
    });

    it('atomically replaces memberships and all scoped aggregates when a snapshot changes', async () => {
      const day = '2026-05-10';
      const scope = {
        day,
        metrics_type: 'enterprise' as const,
        entity_id: 'ent-1',
      };
      const memberships = (costCenterForUserSix: string) =>
        Array.from({ length: 6 }, (_, index) => ({
          ...scope,
          user_id: index + 1,
          user_login: `user-${index + 1}`,
          cost_center_id: index === 5 ? costCenterForUserSix : 'cc-a',
        }));

      await handler.insertDailyTotals([
        buildDailyTotal({
          ...scope,
          team_slug: '',
          cost_center_id: '',
          daily_active_users: 88,
        }),
        buildDailyTotal({
          ...scope,
          team_slug: 'alpha',
          cost_center_id: '',
          daily_active_users: 99,
        }),
      ]);

      const firstGroups = makeCostCenterAggregateRows(day, [
        { costCenterId: 'cc-a', teamSlug: '', activeUsers: 6, locAdded: 60 },
        {
          costCenterId: 'cc-a',
          teamSlug: 'alpha',
          activeUsers: 6,
          locAdded: 60,
        },
        {
          costCenterId: 'cc-a',
          teamSlug: 'legacy',
          activeUsers: 1,
          locAdded: 10,
        },
      ]);
      await handler.replaceCostCenterAggregates(
        day,
        'enterprise',
        'ent-1',
        memberships('cc-a'),
        firstGroups,
      );

      const updatedGroups = makeCostCenterAggregateRows(day, [
        { costCenterId: 'cc-a', teamSlug: '', activeUsers: 5, locAdded: 50 },
        {
          costCenterId: 'cc-a',
          teamSlug: 'alpha',
          activeUsers: 5,
          locAdded: 50,
        },
        { costCenterId: 'cc-b', teamSlug: '', activeUsers: 1, locAdded: 10 },
        {
          costCenterId: 'cc-b',
          teamSlug: 'alpha',
          activeUsers: 1,
          locAdded: 10,
        },
        {
          costCenterId: 'cc-b',
          teamSlug: 'legacy',
          activeUsers: 1,
          locAdded: 10,
        },
      ]);
      await handler.replaceCostCenterAggregates(
        day,
        'enterprise',
        'ent-1',
        memberships('cc-b'),
        updatedGroups,
      );

      const membershipRows = await knex('copilot_user_cost_centers').where(
        scope,
      );
      expect(membershipRows).toHaveLength(6);
      expect(
        membershipRows.filter(row => row.cost_center_id === 'cc-a'),
      ).toHaveLength(5);
      expect(
        membershipRows.find(row => Number(row.user_id) === 6)?.cost_center_id,
      ).toBe('cc-b');

      const costCenterATeam = await knex('copilot_daily_totals')
        .where({
          ...scope,
          team_slug: 'alpha',
          cost_center_id: 'cc-a',
        })
        .first();
      expect(costCenterATeam.daily_active_users).toBe(5);
      expect(costCenterATeam.loc_added_sum).toBe(50);

      const costCenterA = await knex('copilot_daily_totals')
        .where({ ...scope, team_slug: '', cost_center_id: 'cc-a' })
        .first();
      expect(costCenterA.daily_active_users).toBe(5);
      expect(costCenterA.loc_added_sum).toBe(50);

      for (const tableName of costCenterAggregateTables) {
        await expect(
          knex(tableName).where({
            ...scope,
            team_slug: 'legacy',
            cost_center_id: 'cc-a',
          }),
        ).resolves.toEqual([]);
      }

      const unscopedRows = await knex('copilot_daily_totals')
        .where(scope)
        .where('cost_center_id', '')
        .orderBy('team_slug', 'asc');
      expect(unscopedRows).toHaveLength(2);
      expect(unscopedRows.map(row => row.daily_active_users)).toEqual([88, 99]);
      await expect(
        knex('copilot_cost_center_aggregation_log').where(scope),
      ).resolves.toHaveLength(1);
    });

    it('insertDailyTotals is idempotent with conflict ignore', async () => {
      const row = buildDailyTotal({ day: '2026-05-20', team_slug: '' });

      await handler.insertDailyTotals([row]);
      await handler.insertDailyTotals([row]);

      const rows = await knex('copilot_daily_totals').where({
        day: '2026-05-20',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });

      expect(rows).toHaveLength(1);
    });

    it('persists daily_active_vscode_agent_users and backfills it on re-ingestion', async () => {
      const row = buildDailyTotal({
        day: '2026-05-21',
        team_slug: '',
        daily_active_vscode_agent_users: undefined,
      });

      await handler.insertDailyTotals([row]);
      await handler.insertDailyTotals([
        { ...row, daily_active_vscode_agent_users: 9 },
      ]);

      const [stored] = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-21',
        '2026-05-21',
      );

      expect(stored.daily_active_vscode_agent_users).toBe(9);
    });

    it('insertByVscodeAgent persists team-level session/message totals and merges on re-ingestion', async () => {
      await handler.insertByVscodeAgent([
        {
          day: '2026-05-21',
          metrics_type: 'organization',
          entity_id: 'org-1',
          team_slug: '',
          session_count: 3,
          total_user_messages: 12,
        },
      ]);

      await handler.insertByVscodeAgent([
        {
          day: '2026-05-21',
          metrics_type: 'organization',
          entity_id: 'org-1',
          team_slug: '',
          session_count: 5,
          total_user_messages: 20,
        },
      ]);

      const rows = await knex('copilot_metrics_by_vscode_agent').where({
        day: '2026-05-21',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });

      expect(rows).toHaveLength(1);
      expect(rows[0].session_count).toBe(5);
      expect(rows[0].total_user_messages).toBe(20);
    });

    it('persists used_vscode_agent on per-user metrics and backfills it on re-ingestion', async () => {
      const row = buildUserMetric({
        day: '2026-05-21',
        used_vscode_agent: false,
      });

      await handler.insertUserMetrics([row]);
      await handler.insertUserMetrics([{ ...row, used_vscode_agent: true }]);

      const [stored] = await handler.getUserMetrics(
        'organization',
        'org-1',
        row.user_login,
        '2026-05-21',
        '2026-05-21',
      );

      expect(stored.used_vscode_agent).toBeTruthy();
    });

    it('getDailyTotals filters by date range and team', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-01', team_slug: 'team-a' }),
        buildDailyTotal({ day: '2026-05-02', team_slug: 'team-a' }),
        buildDailyTotal({ day: '2026-05-02', team_slug: 'team-b' }),
        buildDailyTotal({ day: '2026-05-04', team_slug: 'team-a' }),
      ]);

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
        'team-a',
      );

      expect(rows).toHaveLength(2);
      expect(rows.every(r => typeof r.day === 'string')).toBe(true);
      expect(rows.map(r => r.day)).toEqual(['2026-05-01', '2026-05-02']);
      expect(rows.every(r => r.team_slug === 'team-a')).toBe(true);
    });

    it('scopes aggregate inserts and reads by team and cost center', async () => {
      const scopes = [
        { team_slug: '', cost_center_id: '' },
        { team_slug: 'team-a', cost_center_id: '' },
        { team_slug: '', cost_center_id: 'cc-a' },
        { team_slug: 'team-a', cost_center_id: 'cc-a' },
      ].map(scope => ({
        day: '2026-05-01',
        metrics_type: 'organization' as const,
        entity_id: 'org-1',
        ...scope,
      }));

      await handler.insertDailyTotals(
        scopes.map(scope => ({
          ...buildDailyTotal(),
          ...scope,
        })),
      );
      await handler.insertPrMetrics(
        scopes.map(scope => ({
          ...scope,
          total_created: 1,
          total_merged: 1,
          total_reviewed: 1,
          total_created_by_copilot: 1,
          total_merged_created_by_copilot: 1,
          total_merged_reviewed_by_copilot: 1,
          total_reviewed_by_copilot: 1,
          total_suggestions: 1,
          total_applied_suggestions: 1,
          total_copilot_suggestions: 1,
          total_copilot_applied_suggestions: 1,
        })),
      );
      await handler.insertByFeature(
        scopes.map(scope => ({
          ...scope,
          feature: 'chat_panel',
          code_acceptance_activity_count: 1,
          code_generation_activity_count: 1,
          loc_added_sum: 1,
          loc_deleted_sum: 1,
          loc_suggested_to_add_sum: 1,
          loc_suggested_to_delete_sum: 1,
          user_initiated_interaction_count: 1,
        })),
      );
      await handler.insertByIde(
        scopes.map(scope => ({
          ...scope,
          ide: 'vscode',
          code_acceptance_activity_count: 1,
          code_generation_activity_count: 1,
          loc_added_sum: 1,
          loc_deleted_sum: 1,
          loc_suggested_to_add_sum: 1,
          loc_suggested_to_delete_sum: 1,
          user_initiated_interaction_count: 1,
        })),
      );
      await handler.insertByLanguageFeature(
        scopes.map(scope => ({
          ...scope,
          language: 'typescript',
          feature: 'chat_panel',
          code_acceptance_activity_count: 1,
          code_generation_activity_count: 1,
          loc_added_sum: 1,
          loc_deleted_sum: 1,
          loc_suggested_to_add_sum: 1,
          loc_suggested_to_delete_sum: 1,
        })),
      );
      await handler.insertByModelFeature(
        scopes.map(scope => ({
          ...scope,
          model_id: 'gpt-4o',
          feature: 'chat_panel',
          user_initiated_interaction_count: 1,
          code_generation_activity_count: 1,
          code_acceptance_activity_count: 1,
          loc_added_sum: 1,
          loc_deleted_sum: 1,
          loc_suggested_to_add_sum: 1,
          loc_suggested_to_delete_sum: 1,
        })),
      );
      await handler.insertByLanguageModel(
        scopes.map(scope => ({
          ...scope,
          language: 'typescript',
          model_id: 'gpt-4o',
          request_count: 1,
          code_generation_activity_count: 1,
          code_acceptance_activity_count: 1,
          loc_added_sum: 1,
          loc_deleted_sum: 1,
          loc_suggested_to_add_sum: 1,
          loc_suggested_to_delete_sum: 1,
        })),
      );
      await handler.insertByCli(
        scopes.map(scope => ({
          ...scope,
          prompt_count: 1,
          request_count: 1,
          session_count: 1,
          avg_tokens_per_request: 1,
          output_tokens_sum: 1,
          prompt_tokens_sum: 1,
        })),
      );
      await handler.insertByVscodeAgent(
        scopes.map(scope => ({
          ...scope,
          session_count: 1,
          total_user_messages: 1,
        })),
      );

      for (const scope of scopes) {
        const teamSlug = scope.team_slug || undefined;
        const costCenter = scope.cost_center_id || undefined;
        const [
          daily,
          prs,
          byFeature,
          byIde,
          byLanguageFeature,
          byModelFeature,
          byLanguageModel,
        ] = await Promise.all([
          handler.getDailyTotals(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
          handler.getPrMetrics(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
          handler.getByFeature(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
          handler.getByIde(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
          handler.getByLanguageFeature(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            undefined,
            costCenter,
          ),
          handler.getByModelFeature(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
          handler.getByLanguageModel(
            'organization',
            'org-1',
            '2026-05-01',
            '2026-05-01',
            teamSlug,
            costCenter,
          ),
        ]);

        expect(daily).toHaveLength(1);
        expect(prs).toHaveLength(1);
        expect(byFeature).toHaveLength(1);
        expect(byIde).toHaveLength(1);
        expect(byLanguageFeature).toHaveLength(1);
        expect(byModelFeature).toHaveLength(1);
        expect(byLanguageModel).toHaveLength(1);
        expect(daily[0].team_slug).toBe(scope.team_slug);
        expect(daily[0].cost_center_id).toBe(scope.cost_center_id);
      }

      const cliRows = await knex('copilot_metrics_by_cli').where({
        day: '2026-05-01',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });
      const vscodeAgentRows = await knex(
        'copilot_metrics_by_vscode_agent',
      ).where({
        day: '2026-05-01',
        metrics_type: 'organization',
        entity_id: 'org-1',
        team_slug: '',
      });
      expect(cliRows).toHaveLength(2);
      expect(vscodeAgentRows).toHaveLength(2);
    });

    it('getDailyTotals always returns day as a plain YYYY-MM-DD string, regardless of database driver (regression test for #9540)', async () => {
      // On Postgres, Knex deserializes a `date` column as a JS Date object.
      // Before the fix, that Date was returned as-is and serialized by
      // res.json() into a full ISO timestamp, breaking the frontend's
      // formatDay() parsing. This test guards against that regression on
      // every database backend the suite runs against, including Postgres.
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-26', team_slug: '' }),
      ]);

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-26',
        '2026-05-26',
      );

      expect(rows).toHaveLength(1);
      expect(typeof rows[0].day).toBe('string');
      expect(rows[0].day).toBe('2026-05-26');
    });

    it('getDailyTotals computes rolling weekly/monthly active users for teams', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-10', team_slug: 'team-a' }),
      ]);

      // user 1: active 05-04 (agent) and again 05-10 — distinct counts once
      // user 2: active 05-08 (chat)
      // user 3: active 05-01 — inside 28-day window, outside 7-day window
      const activity: Array<{
        day: string;
        user_id: number;
        used_agent?: boolean;
        used_chat?: boolean;
      }> = [
        { day: '2026-05-04', user_id: 1, used_agent: true },
        { day: '2026-05-10', user_id: 1 },
        { day: '2026-05-08', user_id: 2, used_chat: true },
        { day: '2026-05-01', user_id: 3 },
      ];

      await handler.insertUserMetrics(
        activity.map(a =>
          buildUserMetric({
            day: a.day,
            user_id: a.user_id,
            user_login: `u${a.user_id}`,
            used_agent: a.used_agent ?? false,
            used_chat: a.used_chat ?? false,
          }),
        ),
      );
      await handler.insertUserTeams(
        activity.map(a =>
          buildUserTeam({
            day: a.day,
            user_id: a.user_id,
            user_login: `u${a.user_id}`,
            team_slug: 'team-a',
            team_id: 1,
          }),
        ),
      );

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-10',
        '2026-05-10',
        'team-a',
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].weekly_active_users).toBe(2);
      expect(rows[0].monthly_active_users).toBe(3);
      expect(rows[0].monthly_active_agent_users).toBe(1);
      expect(rows[0].monthly_active_chat_users).toBe(1);
    });

    it('getDailyTotals anchors rolling windows to each returned day', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-08', team_slug: 'team-a' }),
        buildDailyTotal({ day: '2026-05-10', team_slug: 'team-a' }),
      ]);

      const activity: Array<{
        day: string;
        user_id: number;
        used_agent?: boolean;
        used_chat?: boolean;
      }> = [
        { day: '2026-05-02', user_id: 1 },
        { day: '2026-05-04', user_id: 2, used_agent: true },
        { day: '2026-05-08', user_id: 3, used_chat: true },
        { day: '2026-05-10', user_id: 4 },
      ];

      await handler.insertUserMetrics(
        activity.map(a =>
          buildUserMetric({
            day: a.day,
            user_id: a.user_id,
            user_login: `u${a.user_id}`,
            used_agent: a.used_agent ?? false,
            used_chat: a.used_chat ?? false,
          }),
        ),
      );
      await handler.insertUserTeams(
        activity.map(a =>
          buildUserTeam({
            day: a.day,
            user_id: a.user_id,
            user_login: `u${a.user_id}`,
            team_slug: 'team-a',
            team_id: 1,
          }),
        ),
      );

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-08',
        '2026-05-10',
        'team-a',
      );

      expect(rows).toHaveLength(2);
      expect(typeof rows[0].day).toBe('string');
      expect(rows[0].day).toBe('2026-05-08');
      expect(rows[0].weekly_active_users).toBe(3);
      expect(rows[0].monthly_active_users).toBe(3);
      expect(rows[0].monthly_active_agent_users).toBe(1);
      expect(rows[0].monthly_active_chat_users).toBe(1);

      expect(typeof rows[1].day).toBe('string');
      expect(rows[1].day).toBe('2026-05-10');
      expect(rows[1].weekly_active_users).toBe(3);
      expect(rows[1].monthly_active_users).toBe(4);
      expect(rows[1].monthly_active_agent_users).toBe(1);
      expect(rows[1].monthly_active_chat_users).toBe(1);
    });

    it('getDailyTotals computes rolling users when dates come back as Date objects', async () => {
      const originalDb = (handler as any).db;
      const fakeActivityRows = [
        {
          day: new Date('2026-05-04T00:00:00.000Z'),
          user_id: 1,
          used_agent: true,
          used_chat: false,
        },
        {
          day: new Date('2026-05-08T00:00:00.000Z'),
          user_id: 2,
          used_agent: false,
          used_chat: true,
        },
        {
          day: new Date('2026-05-10T00:00:00.000Z'),
          user_id: 1,
          used_agent: false,
          used_chat: false,
        },
      ];
      const fakeQuery = {
        join: jest.fn().mockReturnThis(),
        where: jest.fn().mockReturnThis(),
        whereBetween: jest.fn().mockReturnThis(),
        select: jest.fn().mockResolvedValue(fakeActivityRows),
      };
      (handler as any).db = jest.fn().mockReturnValue(fakeQuery);

      try {
        const rows = await (handler as any).enrichScopedRollingActiveUsers(
          [
            buildDailyTotal({
              day: new Date('2026-05-10T00:00:00.000Z') as any,
              team_slug: 'team-a',
            }),
          ],
          'organization',
          'org-1',
          '2026-05-10',
          '2026-05-10',
          'team-a',
        );

        expect(rows).toHaveLength(1);
        expect(rows[0].weekly_active_users).toBe(2);
        expect(rows[0].monthly_active_users).toBe(2);
        expect(rows[0].monthly_active_agent_users).toBe(1);
        expect(rows[0].monthly_active_chat_users).toBe(1);
      } finally {
        (handler as any).db = originalDb;
      }
    });

    it('getDailyTotals only counts users active within the selected team', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-10', team_slug: 'team-a' }),
      ]);

      await handler.insertUserMetrics([
        buildUserMetric({ day: '2026-05-10', user_id: 1, user_login: 'u1' }),
        buildUserMetric({ day: '2026-05-10', user_id: 2, user_login: 'u2' }),
      ]);
      // user 1 is in team-a, user 2 is in team-b
      await handler.insertUserTeams([
        buildUserTeam({
          day: '2026-05-10',
          user_id: 1,
          user_login: 'u1',
          team_slug: 'team-a',
          team_id: 1,
        }),
        buildUserTeam({
          day: '2026-05-10',
          user_id: 2,
          user_login: 'u2',
          team_slug: 'team-b',
          team_id: 2,
        }),
      ]);

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-10',
        '2026-05-10',
        'team-a',
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].weekly_active_users).toBe(1);
      expect(rows[0].monthly_active_users).toBe(1);
    });

    it('computes rolling active users for cost centers and team intersections', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({
          day: '2026-05-10',
          team_slug: '',
          cost_center_id: 'cc-a',
        }),
        buildDailyTotal({
          day: '2026-05-10',
          team_slug: 'team-a',
          cost_center_id: 'cc-a',
        }),
      ]);

      const activity: Array<{
        day: string;
        user_id: number;
        cost_center_id: string;
        team_slug: string;
        used_agent?: boolean;
        used_chat?: boolean;
      }> = [
        {
          day: '2026-05-04',
          user_id: 1,
          cost_center_id: 'cc-a',
          team_slug: 'team-a',
          used_agent: true,
        },
        {
          day: '2026-05-10',
          user_id: 1,
          cost_center_id: 'cc-a',
          team_slug: 'team-a',
        },
        {
          day: '2026-05-08',
          user_id: 2,
          cost_center_id: 'cc-a',
          team_slug: 'team-a',
          used_chat: true,
        },
        {
          day: '2026-05-01',
          user_id: 3,
          cost_center_id: 'cc-a',
          team_slug: 'team-b',
        },
        {
          day: '2026-05-09',
          user_id: 4,
          cost_center_id: 'cc-a',
          team_slug: 'team-b',
        },
        {
          day: '2026-05-10',
          user_id: 5,
          cost_center_id: 'cc-b',
          team_slug: 'team-a',
        },
      ];

      await handler.insertUserMetrics(
        activity.map(row =>
          buildUserMetric({
            day: row.day,
            user_id: row.user_id,
            user_login: `u${row.user_id}`,
            used_agent: row.used_agent ?? false,
            used_chat: row.used_chat ?? false,
          }),
        ),
      );
      await handler.upsertUserCostCenters(
        activity.map(row => ({
          day: row.day,
          metrics_type: 'organization',
          entity_id: 'org-1',
          user_id: row.user_id,
          user_login: `u${row.user_id}`,
          cost_center_id: row.cost_center_id,
        })),
      );
      await handler.insertUserTeams(
        activity.map(row =>
          buildUserTeam({
            day: row.day,
            user_id: row.user_id,
            user_login: `u${row.user_id}`,
            team_slug: row.team_slug,
            team_id: row.team_slug === 'team-a' ? 1 : 2,
          }),
        ),
      );

      const costCenterRows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-10',
        '2026-05-10',
        undefined,
        'cc-a',
      );
      const pairRows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-10',
        '2026-05-10',
        'team-a',
        'cc-a',
      );

      expect(costCenterRows[0]).toEqual(
        expect.objectContaining({
          weekly_active_users: 3,
          monthly_active_users: 4,
          monthly_active_agent_users: 1,
          monthly_active_chat_users: 1,
        }),
      );
      expect(pairRows[0]).toEqual(
        expect.objectContaining({
          weekly_active_users: 2,
          monthly_active_users: 2,
          monthly_active_agent_users: 1,
          monthly_active_chat_users: 1,
        }),
      );
    });

    it('getDailyTotals leaves org-level rolling counts untouched', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({
          day: '2026-05-10',
          team_slug: '',
          weekly_active_users: 42,
          monthly_active_users: 99,
        }),
      ]);

      const rows = await handler.getDailyTotals(
        'organization',
        'org-1',
        '2026-05-10',
        '2026-05-10',
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].weekly_active_users).toBe(42);
      expect(rows[0].monthly_active_users).toBe(99);
    });

    it('getPeriodRange returns min/max day from daily totals', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-03', team_slug: '' }),
        buildDailyTotal({ day: '2026-05-01', team_slug: '' }),
        buildDailyTotal({ day: '2026-05-02', team_slug: '' }),
      ]);

      const range = await handler.getPeriodRange('organization', 'org-1');

      expect(range).toEqual({
        minDate: '2026-05-01',
        maxDate: '2026-05-03',
      });
    });

    it('getPeriodRange scopes its date range by cost center', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-01', cost_center_id: '' }),
        buildDailyTotal({ day: '2026-05-03', cost_center_id: 'cc-a' }),
        buildDailyTotal({ day: '2026-05-05', cost_center_id: 'cc-b' }),
      ]);

      await expect(
        handler.getPeriodRange('organization', 'org-1'),
      ).resolves.toEqual({
        minDate: '2026-05-01',
        maxDate: '2026-05-01',
      });
      await expect(
        handler.getPeriodRange('organization', 'org-1', 'cc-a'),
      ).resolves.toEqual({
        minDate: '2026-05-03',
        maxDate: '2026-05-03',
      });
    });

    it('getTeams returns only team slugs with 5 or more distinct members', async () => {
      // 'alpha' has 5 members — should be returned
      await handler.insertUserTeams([
        buildUserTeam({ team_slug: 'alpha', user_id: 1, user_login: 'u1' }),
        buildUserTeam({ team_slug: 'alpha', user_id: 2, user_login: 'u2' }),
        buildUserTeam({ team_slug: 'alpha', user_id: 3, user_login: 'u3' }),
        buildUserTeam({ team_slug: 'alpha', user_id: 4, user_login: 'u4' }),
        buildUserTeam({ team_slug: 'alpha', user_id: 5, user_login: 'u5' }),
        // 'beta' has only 4 members — should be excluded
        buildUserTeam({ team_slug: 'beta', user_id: 1, user_login: 'u1' }),
        buildUserTeam({ team_slug: 'beta', user_id: 2, user_login: 'u2' }),
        buildUserTeam({ team_slug: 'beta', user_id: 3, user_login: 'u3' }),
        buildUserTeam({ team_slug: 'beta', user_id: 4, user_login: 'u4' }),
      ]);
      const teams = await handler.getTeams(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(teams).toEqual(['alpha']);
    });

    it('getCostCenters applies the distinct-user threshold, names and none sentinel', async () => {
      await handler.upsertCostCenters([
        {
          metrics_type: 'organization',
          entity_id: 'org-1',
          cost_center_id: 'cc-alpha',
          name: 'Alpha',
        },
      ]);

      const membershipRows: V2UserCostCenterRow[] = [];
      const addMemberships = (
        day: string,
        costCenterId: string,
        userIds: number[],
      ) => {
        membershipRows.push(
          ...userIds.map(userId => ({
            day,
            metrics_type: 'organization' as const,
            entity_id: 'org-1',
            user_id: userId,
            user_login: `u${userId}`,
            cost_center_id: costCenterId,
          })),
        );
      };

      addMemberships('2026-05-01', 'cc-alpha', [1, 2, 3, 4, 5]);
      addMemberships('2026-05-02', 'cc-alpha', [1]);
      addMemberships('2026-05-01', 'cc-small', [11, 12, 13, 14]);
      addMemberships('2026-05-01', NO_COST_CENTER_ID, [21, 22, 23, 24, 25]);
      addMemberships('2026-05-01', 'cc-unknown', [31, 32, 33, 34, 35]);
      addMemberships('2026-05-03', 'cc-outside-range', [41, 42, 43, 44, 45]);
      await handler.upsertUserCostCenters(membershipRows);
      await handler.insertUserMetrics([
        ...[1, 2, 3, 4, 5, 21, 22, 23, 24, 25, 31, 32, 33, 34, 35].map(userId =>
          buildUserMetric({ user_id: userId, user_login: `u${userId}` }),
        ),
        ...[11, 12, 13, 14].map(userId =>
          buildUserMetric({ user_id: userId, user_login: `u${userId}` }),
        ),
        ...[41, 42, 43, 44, 45].map(userId =>
          buildUserMetric({
            day: '2026-05-03',
            user_id: userId,
            user_login: `u${userId}`,
          }),
        ),
        buildUserMetric({
          day: '2026-05-02',
          user_id: 1,
          user_login: 'u1',
        }),
      ]);

      const costCenters = await handler.getCostCenters(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-02',
      );

      expect(costCenters).toEqual(
        expect.arrayContaining([
          { id: 'cc-alpha', name: 'Alpha' },
          { id: NO_COST_CENTER_ID, name: null },
          { id: 'cc-unknown', name: 'cc-unknown' },
        ]),
      );
      expect(costCenters.map(costCenter => costCenter.id)).not.toContain(
        'cc-small',
      );
      expect(costCenters.map(costCenter => costCenter.id)).not.toContain(
        'cc-outside-range',
      );
      expect(
        costCenters
          .filter(costCenter => costCenter.name !== null)
          .map(costCenter => costCenter.name),
      ).toEqual(['Alpha', 'cc-unknown']);
      await expect(
        handler.getCostCenters(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-02',
          4,
        ),
      ).resolves.toEqual(
        expect.arrayContaining([
          expect.objectContaining({ id: 'cc-small', name: 'cc-small' }),
        ]),
      );
    });

    it('keeps cost-center names isolated by metrics scope', async () => {
      await handler.upsertCostCenters([
        {
          metrics_type: 'enterprise',
          entity_id: 'shared',
          cost_center_id: 'cc-shared',
          name: 'Enterprise center',
        },
        {
          metrics_type: 'organization',
          entity_id: 'shared',
          cost_center_id: 'cc-shared',
          name: 'Organization center',
        },
      ]);

      const addScopeRows = (metricsType: 'enterprise' | 'organization') => {
        const members = [1, 2, 3, 4, 5].map(userId => ({
          day: '2026-05-01',
          metrics_type: metricsType,
          entity_id: 'shared',
          user_id: userId,
          user_login: `${metricsType}-${userId}`,
          cost_center_id: 'cc-shared',
        }));
        return {
          members,
          metrics: members.map(member =>
            buildUserMetric({
              metrics_type: metricsType,
              entity_id: 'shared',
              user_id: member.user_id,
              user_login: member.user_login,
            }),
          ),
        };
      };
      const enterpriseRows = addScopeRows('enterprise');
      const organizationRows = addScopeRows('organization');
      await handler.upsertUserCostCenters([
        ...enterpriseRows.members,
        ...organizationRows.members,
      ]);
      await handler.insertUserMetrics([
        ...enterpriseRows.metrics,
        ...organizationRows.metrics,
      ]);

      await expect(
        handler.getCostCenters('enterprise', 'shared'),
      ).resolves.toEqual([{ id: 'cc-shared', name: 'Enterprise center' }]);
      await expect(
        handler.getCostCenters('organization', 'shared'),
      ).resolves.toEqual([{ id: 'cc-shared', name: 'Organization center' }]);
    });

    it('does not count membership-only users toward cost-center or team thresholds', async () => {
      await handler.upsertUserCostCenters(
        [1, 2, 3, 4, 5].map(userId => ({
          day: '2026-05-01',
          metrics_type: 'organization' as const,
          entity_id: 'org-1',
          user_id: userId,
          user_login: `u${userId}`,
          cost_center_id: 'cc-membership-only',
        })),
      );
      await handler.insertUserTeams(
        [1, 2, 3, 4, 5].map(userId =>
          buildUserTeam({
            user_id: userId,
            user_login: `u${userId}`,
            team_slug: 'membership-only-team',
          }),
        ),
      );
      await handler.insertUserMetrics(
        [1, 2, 3, 4].map(userId =>
          buildUserMetric({ user_id: userId, user_login: `u${userId}` }),
        ),
      );

      await expect(
        handler.getCostCenters(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-01',
        ),
      ).resolves.toEqual([]);
      await expect(
        handler.getTeams(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-01',
          5,
          'cc-membership-only',
        ),
      ).resolves.toEqual([]);
    });

    it('getTeams applies the threshold to the same-day team/cost-center intersection', async () => {
      const teamRows = [
        ...[1, 2, 3, 4, 5, 6].map(userId =>
          buildUserTeam({
            day: '2026-05-01',
            user_id: userId,
            user_login: `u${userId}`,
            team_id: 1,
            team_slug: 'alpha',
          }),
        ),
        ...[11, 12, 13, 14, 15, 16].map(userId =>
          buildUserTeam({
            day: '2026-05-01',
            user_id: userId,
            user_login: `u${userId}`,
            team_id: 2,
            team_slug: 'beta',
          }),
        ),
      ];
      await handler.insertUserTeams(teamRows);
      await handler.insertUserMetrics(
        [...[1, 2, 3, 4, 5, 6], ...[11, 12, 13, 14, 15]].map(userId =>
          buildUserMetric({
            day: '2026-05-01',
            user_id: userId,
            user_login: `u${userId}`,
          }),
        ),
      );
      await handler.upsertUserCostCenters([
        ...[1, 2, 3, 4, 5, 11, 12, 13, 14].map(userId => ({
          day: '2026-05-01',
          metrics_type: 'organization' as const,
          entity_id: 'org-1',
          user_id: userId,
          user_login: `u${userId}`,
          cost_center_id: 'cc-a',
        })),
        ...[6, 15].map(userId => ({
          day: '2026-05-01',
          metrics_type: 'organization' as const,
          entity_id: 'org-1',
          user_id: userId,
          user_login: `u${userId}`,
          cost_center_id: 'cc-b',
        })),
      ]);

      await expect(
        handler.getTeams('organization', 'org-1', '2026-05-01', '2026-05-01'),
      ).resolves.toEqual(['alpha', 'beta']);
      await expect(
        handler.getTeams(
          'organization',
          'org-1',
          '2026-05-01',
          '2026-05-01',
          5,
          'cc-a',
        ),
      ).resolves.toEqual(['alpha']);
    });

    it('getTeams counts distinct members across days, not per-day rows', async () => {
      // same user appears on two days in 'gamma' — should still count as 1 member
      await handler.insertUserTeams([
        buildUserTeam({
          team_slug: 'gamma',
          user_id: 1,
          user_login: 'u1',
          day: '2026-05-01',
        }),
        buildUserTeam({
          team_slug: 'gamma',
          user_id: 1,
          user_login: 'u1',
          day: '2026-05-02',
        }),
        buildUserTeam({
          team_slug: 'gamma',
          user_id: 2,
          user_login: 'u2',
          day: '2026-05-01',
        }),
        buildUserTeam({
          team_slug: 'gamma',
          user_id: 3,
          user_login: 'u3',
          day: '2026-05-01',
        }),
        buildUserTeam({
          team_slug: 'gamma',
          user_id: 4,
          user_login: 'u4',
          day: '2026-05-01',
        }),
        // only 4 distinct users — should be excluded
      ]);
      const teams = await handler.getTeams(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-02',
      );

      expect(teams).toEqual([]);
    });

    it('getDashboardData returns all chart data in a single call', async () => {
      await handler.insertDailyTotals([
        buildDailyTotal({ day: '2026-05-01', team_slug: '' }),
        buildDailyTotal({ day: '2026-05-02', team_slug: '' }),
      ]);

      const result = await handler.getDashboardData(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-02',
      );

      expect(result).toHaveProperty('daily');
      expect(result).toHaveProperty('byFeature');
      expect(result).toHaveProperty('byLanguage');
      expect(result).toHaveProperty('byModelFeature');
      expect(result).toHaveProperty('byLanguageModel');
      expect(result).toHaveProperty('prMetrics');
      expect(result.daily).toHaveLength(2);
    });

    it('getUserMetrics only returns rows for the requested user login', async () => {
      await handler.insertUserMetrics([
        buildUserMetric({ user_login: 'octocat', day: '2026-05-01' }),
        buildUserMetric({
          user_login: 'octocat',
          user_id: 1,
          day: '2026-05-02',
        }),
        buildUserMetric({
          user_login: 'someone-else',
          user_id: 2,
          day: '2026-05-01',
        }),
      ]);

      const rows = await handler.getUserMetrics(
        'organization',
        'org-1',
        'octocat',
        '2026-05-01',
        '2026-05-03',
      );

      expect(rows).toHaveLength(2);
      expect(rows.every(r => r.user_login === 'octocat')).toBe(true);
    });

    it('getUserMetrics matches user login case-insensitively', async () => {
      await handler.insertUserMetrics([
        buildUserMetric({ user_login: 'OctoCat', day: '2026-05-01' }),
        buildUserMetric({
          user_login: 'someone-else',
          user_id: 2,
          day: '2026-05-01',
        }),
      ]);

      const rows = await handler.getUserMetrics(
        'organization',
        'org-1',
        'octocat',
        '2026-05-01',
        '2026-05-03',
      );

      expect(rows).toHaveLength(1);
      expect(rows[0].user_login).toBe('OctoCat');
    });

    it('hasUserMetrics finds historical data independent of the date range', async () => {
      await handler.insertUserMetrics([
        buildUserMetric({ user_login: 'OctoCat', day: '2026-05-01' }),
      ]);

      await expect(
        handler.hasUserMetrics('organization', 'org-1', 'octocat'),
      ).resolves.toBe(true);
      await expect(
        handler.hasUserMetrics('organization', 'org-1', 'someone-else'),
      ).resolves.toBe(false);
    });

    it('getUserDashboardData scopes every dimension to a single user login', async () => {
      await handler.insertUserMetrics([
        buildUserMetric({ user_login: 'octocat', user_id: 1 }),
        buildUserMetric({ user_login: 'someone-else', user_id: 2 }),
      ]);
      await handler.insertUserMetricsByFeature([
        buildUserMetricsByFeature({ user_login: 'octocat', user_id: 1 }),
        buildUserMetricsByFeature({
          user_login: 'someone-else',
          user_id: 2,
          feature: 'agent',
        }),
      ]);
      await handler.insertUserMetricsByIde([
        buildUserMetricsByIde({ user_login: 'octocat', user_id: 1 }),
      ]);
      await handler.insertUserMetricsByLanguageFeature([
        buildUserMetricsByLanguageFeature({
          user_login: 'octocat',
          user_id: 1,
        }),
      ]);
      await handler.insertUserMetricsByModelFeature([
        buildUserMetricsByModelFeature({ user_login: 'octocat', user_id: 1 }),
      ]);
      await handler.insertUserMetricsByLanguageModel([
        buildUserMetricsByLanguageModel({ user_login: 'octocat', user_id: 1 }),
      ]);

      const result = await handler.getUserDashboardData(
        'organization',
        'org-1',
        'octocat',
        '2026-05-01',
        '2026-05-01',
      );

      expect(result.userLogin).toBe('octocat');
      expect(result.daily).toHaveLength(1);
      expect(result.daily[0].user_login).toBe('octocat');
      expect(result.byFeature).toHaveLength(1);
      expect(result.byFeature[0].feature).toBe('chat_panel');
      expect(result.byIde).toHaveLength(1);
      expect(result.byLanguage).toHaveLength(1);
      expect(result.byModelFeature).toHaveLength(1);
      expect(result.byLanguageModel).toHaveLength(1);

      // Never returns another user's rows, even though they exist in the
      // same day/entity/metrics_type scope.
      const otherUser = await handler.getUserDashboardData(
        'organization',
        'org-1',
        'someone-else',
        '2026-05-01',
        '2026-05-01',
      );
      expect(otherUser.byFeature).toEqual([
        expect.objectContaining({ feature: 'agent' }),
      ]);
    });
  });
});

function buildIngestionLog(
  overrides: Partial<V2IngestionLogRow> = {},
): V2IngestionLogRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    status: 'success',
    components_loaded: '["totals"]',
    source: 'scheduled',
    ...overrides,
  };
}

function buildDailyTotal(overrides: Partial<V2DailyTotal> = {}): V2DailyTotal {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    team_slug: '',
    daily_active_users: 10,
    weekly_active_users: 20,
    monthly_active_users: 30,
    daily_active_cli_users: 4,
    monthly_active_agent_users: 5,
    monthly_active_chat_users: 6,
    code_acceptance_activity_count: 100,
    code_generation_activity_count: 120,
    loc_added_sum: 1000,
    loc_deleted_sum: 500,
    loc_suggested_to_add_sum: 1400,
    loc_suggested_to_delete_sum: 700,
    user_initiated_interaction_count: 33,
    ...overrides,
  };
}

function makeCostCenterAggregateRows(
  day: string,
  groups: Array<{
    costCenterId: string;
    teamSlug: string;
    activeUsers: number;
    locAdded: number;
  }>,
): CostCenterAggregateRows {
  const scopes = groups.map(
    ({ costCenterId, teamSlug, activeUsers, locAdded }) => ({
      scope: {
        day,
        metrics_type: 'enterprise' as const,
        entity_id: 'ent-1',
        team_slug: teamSlug,
        cost_center_id: costCenterId,
      },
      activeUsers,
      locAdded,
    }),
  );

  return {
    dailyTotals: scopes.map(({ activeUsers, locAdded, scope }) =>
      buildDailyTotal({
        ...scope,
        daily_active_users: activeUsers,
        loc_added_sum: locAdded,
      }),
    ),
    prMetrics: scopes.map(({ activeUsers, scope }) => ({
      ...scope,
      total_created: activeUsers,
      total_merged: activeUsers,
      total_reviewed: activeUsers,
      total_created_by_copilot: activeUsers,
      total_merged_created_by_copilot: activeUsers,
      total_merged_reviewed_by_copilot: activeUsers,
      total_reviewed_by_copilot: activeUsers,
      total_suggestions: activeUsers,
      total_applied_suggestions: activeUsers,
      total_copilot_suggestions: activeUsers,
      total_copilot_applied_suggestions: activeUsers,
    })),
    byFeature: scopes.map(({ activeUsers, locAdded, scope }) => ({
      ...scope,
      feature: 'chat_panel',
      code_acceptance_activity_count: activeUsers,
      code_generation_activity_count: activeUsers,
      loc_added_sum: locAdded,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
      user_initiated_interaction_count: activeUsers,
    })),
    byIde: scopes.map(({ activeUsers, locAdded, scope }) => ({
      ...scope,
      ide: 'vscode',
      code_acceptance_activity_count: activeUsers,
      code_generation_activity_count: activeUsers,
      loc_added_sum: locAdded,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
      user_initiated_interaction_count: activeUsers,
    })),
    byLanguageFeature: scopes.map(({ activeUsers, locAdded, scope }) => ({
      ...scope,
      language: 'typescript',
      feature: 'chat_panel',
      code_acceptance_activity_count: activeUsers,
      code_generation_activity_count: activeUsers,
      loc_added_sum: locAdded,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
    })),
    byModelFeature: scopes.map(({ activeUsers, locAdded, scope }) => ({
      ...scope,
      model_id: 'gpt-4o',
      feature: 'chat_panel',
      user_initiated_interaction_count: activeUsers,
      code_generation_activity_count: activeUsers,
      code_acceptance_activity_count: activeUsers,
      loc_added_sum: locAdded,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
    })),
    byLanguageModel: scopes.map(({ activeUsers, locAdded, scope }) => ({
      ...scope,
      language: 'typescript',
      model_id: 'gpt-4o',
      request_count: activeUsers,
      code_generation_activity_count: activeUsers,
      code_acceptance_activity_count: activeUsers,
      loc_added_sum: locAdded,
      loc_deleted_sum: 0,
      loc_suggested_to_add_sum: 0,
      loc_suggested_to_delete_sum: 0,
    })),
    byCli: scopes.map(({ activeUsers, scope }) => ({
      ...scope,
      prompt_count: activeUsers,
      request_count: activeUsers,
      session_count: activeUsers,
      avg_tokens_per_request: 1,
      output_tokens_sum: activeUsers,
      prompt_tokens_sum: activeUsers,
    })),
    byVscodeAgent: scopes.map(({ activeUsers, scope }) => ({
      ...scope,
      session_count: activeUsers,
      total_user_messages: activeUsers * 2,
    })),
  };
}

function buildUserTeam(overrides: Partial<V2UserTeamRow> = {}): V2UserTeamRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    team_id: 100,
    team_slug: 'alpha',
    ...overrides,
  };
}

function buildUserMetric(
  overrides: Partial<V2UserMetricRow> = {},
): V2UserMetricRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
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
    ...overrides,
  };
}

function buildUserMetricsByFeature(
  overrides: Partial<V2UserMetricsByFeatureRow> = {},
): V2UserMetricsByFeatureRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    feature: 'chat_panel',
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

function buildUserMetricsByIde(
  overrides: Partial<V2UserMetricsByIdeRow> = {},
): V2UserMetricsByIdeRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    ide: 'vscode',
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

function buildUserMetricsByLanguageFeature(
  overrides: Partial<V2UserMetricsByLanguageFeatureRow> = {},
): V2UserMetricsByLanguageFeatureRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    language: 'typescript',
    feature: 'chat_panel',
    code_acceptance_activity_count: 3,
    code_generation_activity_count: 4,
    loc_added_sum: 10,
    loc_deleted_sum: 2,
    loc_suggested_to_add_sum: 12,
    loc_suggested_to_delete_sum: 3,
    ...overrides,
  };
}

function buildUserMetricsByModelFeature(
  overrides: Partial<V2UserMetricsByModelFeatureRow> = {},
): V2UserMetricsByModelFeatureRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    model_id: 'gpt-4o',
    feature: 'chat_panel',
    user_initiated_interaction_count: 7,
    code_generation_activity_count: 4,
    code_acceptance_activity_count: 3,
    loc_added_sum: 10,
    loc_deleted_sum: 2,
    loc_suggested_to_add_sum: 12,
    loc_suggested_to_delete_sum: 3,
    ...overrides,
  };
}

function buildUserMetricsByLanguageModel(
  overrides: Partial<V2UserMetricsByLanguageModelRow> = {},
): V2UserMetricsByLanguageModelRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    user_id: 1,
    user_login: 'octocat',
    language: 'typescript',
    model_id: 'gpt-4o',
    request_count: 4,
    code_generation_activity_count: 4,
    code_acceptance_activity_count: 3,
    loc_added_sum: 10,
    loc_deleted_sum: 2,
    loc_suggested_to_add_sum: 12,
    loc_suggested_to_delete_sum: 3,
    ...overrides,
  };
}
