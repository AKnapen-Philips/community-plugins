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
import knexFactory, { Knex } from 'knex';
import {
  V2DailyTotal,
  V2CustomizationMetricRow,
  V2CustomizationSummaryRow,
  V2IngestionLogRow,
  V2UserMetricRow,
  V2UserMetricsByFeatureRow,
  V2UserMetricsByIdeRow,
  V2UserMetricsByLanguageFeatureRow,
  V2UserMetricsByModelFeatureRow,
  V2UserMetricsByLanguageModelRow,
  V2UserTeamRow,
} from '@backstage-community/plugin-copilot-common';
import { migrationsDir } from './DatabaseHandler';
import { DatabaseHandlerV2 } from './DatabaseHandlerV2';
import { aggregateTeamMetrics } from '../utils/teamAggregator';

const teamAggregationMigration = jest.requireActual<{
  up(knex: Knex): Promise<void>;
  down(knex: Knex): Promise<void>;
}>('../../migrations/202611020000_add_team_aggregation_sources.js');

jest.setTimeout(60_000);

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

    // Skip MySQL tests due to known migration issues in earlier migrations.
    if (databaseId.startsWith('MYSQL')) {
      // eslint-disable-next-line jest/no-disabled-tests, jest/expect-expect
      it.skip('tests for MySQL due to pre-existing migration issue', () => {});
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

    async function createCompletedMembershipSnapshot(
      entityId: string,
      generation: string,
    ) {
      const snapshot = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        entityId,
        generation,
      );
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        entityId,
        snapshot,
        [],
        null,
        true,
      );
      return snapshot;
    }

    it('adds and reverses team aggregation tables without a membership source column', async () => {
      await expect(
        knex.schema.hasColumn('copilot_user_teams', 'source'),
      ).resolves.toBe(false);

      await teamAggregationMigration.down(knex);
      await expect(
        knex.schema.hasTable('copilot_team_aggregation_state'),
      ).resolves.toBe(false);
      await expect(
        knex.schema.hasTable('copilot_team_aggregation_log'),
      ).resolves.toBe(false);
      await expect(
        knex.schema.hasColumn('copilot_user_teams', 'source'),
      ).resolves.toBe(false);

      await teamAggregationMigration.up(knex);
      await expect(
        knex.schema.hasTable('copilot_team_aggregation_state'),
      ).resolves.toBe(true);
      await expect(
        knex.schema.hasTable('copilot_team_aggregation_log'),
      ).resolves.toBe(true);
      await expect(
        knex.schema.hasColumn('copilot_user_teams', 'source'),
      ).resolves.toBe(false);

      const membership = buildUserTeam({ team_slug: 'native-team' });
      await handler.insertUserTeams([membership]);
      await handler.insertUserTeams([membership]);
      await expect(
        knex('copilot_user_teams').where({
          day: membership.day,
          metrics_type: membership.metrics_type,
          entity_id: membership.entity_id,
          user_id: membership.user_id,
          team_id: membership.team_id,
        }),
      ).resolves.toHaveLength(1);
    });

    it('allows direct membership writes only while native aggregation is idle', async () => {
      const day = '2026-05-01';
      const userMetrics = [
        buildUserMetric({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 1,
          user_login: 'u1',
        }),
      ];
      const nativeMemberships = [
        buildUserTeam({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 1,
          user_login: 'u1',
          team_slug: 'native-team',
        }),
      ];
      const fallbackMemberships = [
        buildUserTeam({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 1,
          user_login: 'u1',
          team_slug: 'fallback-team',
        }),
      ];
      const aggregate = (memberships: V2UserTeamRow[]) =>
        aggregateTeamMetrics(
          userMetrics,
          memberships,
          [],
          day,
          'enterprise',
          'ent-1',
        );

      await handler.insertUserMetrics(userMetrics);
      const fallbackTransition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'organization-fallback',
      );
      const membershipSnapshot = await createCompletedMembershipSnapshot(
        'ent-1',
        fallbackTransition!.pendingGeneration,
      );
      await expect(handler.insertUserTeams(nativeMemberships)).rejects.toThrow(
        'Cannot insert native team memberships',
      );
      await expect(
        handler.stageTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'organization-fallback',
          fallbackTransition!.pendingGeneration,
          [day],
          fallbackMemberships,
          aggregate(fallbackMemberships),
          membershipSnapshot,
        ),
      ).resolves.toBe(true);
      await expect(handler.insertUserTeams(nativeMemberships)).rejects.toThrow(
        'Cannot insert native team memberships',
      );

      const nativeTransition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'native',
      );
      await expect(handler.insertUserTeams(nativeMemberships)).rejects.toThrow(
        'Cannot insert native team memberships',
      );
      await expect(
        handler.stageTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'native',
          nativeTransition!.pendingGeneration,
          [day],
          nativeMemberships,
          aggregate(nativeMemberships),
        ),
      ).resolves.toBe(true);
      await expect(
        handler.insertUserTeams(nativeMemberships),
      ).resolves.toBeUndefined();
    });

    it('removes active fallback team data while preserving native data on rollback', async () => {
      const fallbackDay = '2026-05-01';
      const nativeDay = '2026-05-02';
      await knex('copilot_team_aggregation_state').insert([
        {
          metrics_type: 'enterprise',
          entity_id: 'fallback-enterprise',
          active_source: 'organization-fallback',
          active_generation: 'fallback-generation',
        },
        {
          metrics_type: 'enterprise',
          entity_id: 'native-enterprise',
          active_source: 'native',
          active_generation: 'native-default',
        },
      ]);
      await knex('copilot_team_aggregation_log').insert({
        day: fallbackDay,
        metrics_type: 'enterprise',
        entity_id: 'fallback-enterprise',
        source: 'organization-fallback',
        generation: 'fallback-generation',
        status: 'complete',
      });
      await knex('copilot_user_teams').insert([
        buildUserTeam({
          day: fallbackDay,
          metrics_type: 'enterprise',
          entity_id: 'fallback-enterprise',
          team_slug: 'fallback-team',
        }),
        buildUserTeam({
          day: nativeDay,
          metrics_type: 'enterprise',
          entity_id: 'native-enterprise',
          team_slug: 'native-team',
        }),
      ]);
      await handler.insertDailyTotals([
        buildDailyTotal({
          day: fallbackDay,
          metrics_type: 'enterprise',
          entity_id: 'fallback-enterprise',
          team_slug: 'fallback-team',
        }),
        buildDailyTotal({
          day: nativeDay,
          metrics_type: 'enterprise',
          entity_id: 'native-enterprise',
          team_slug: 'native-team',
        }),
      ]);

      await teamAggregationMigration.down(knex);

      await expect(
        knex('copilot_user_teams').where({ entity_id: 'fallback-enterprise' }),
      ).resolves.toHaveLength(0);
      await expect(
        knex('copilot_user_teams').where({ entity_id: 'native-enterprise' }),
      ).resolves.toHaveLength(1);
      await expect(
        knex('copilot_daily_totals').where({
          entity_id: 'fallback-enterprise',
          team_slug: 'fallback-team',
        }),
      ).resolves.toHaveLength(0);
      await expect(
        knex('copilot_daily_totals').where({
          entity_id: 'native-enterprise',
          team_slug: 'native-team',
        }),
      ).resolves.toHaveLength(1);
    });

    it('supersedes organization membership pagination when a new attempt starts', async () => {
      const firstState = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
        'snapshot-generation',
      );
      expect(firstState).toEqual({
        generation: 'snapshot-generation',
        attemptToken: expect.any(String),
        cursor: null,
        complete: false,
      });
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        firstState,
        [{ teamId: 101, teamSlug: 'demo-parent', userId: 7 }],
        'cursor-after-first-page',
        false,
      );

      const restartedHandler = await DatabaseHandlerV2.create({
        database: {
          getClient: async () => knex,
          migrations: { skip: true },
        } as any,
      });
      const newState = await restartedHandler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
        'snapshot-generation',
      );
      expect(newState).toEqual({
        generation: 'snapshot-generation',
        attemptToken: expect.any(String),
        cursor: null,
        complete: false,
      });
      expect(newState.attemptToken).not.toBe(firstState.attemptToken);
      await expect(
        handler.saveTeamMembershipSnapshotBatch(
          'enterprise',
          'ent-1',
          firstState,
          [{ teamId: 102, teamSlug: 'stale-team', userId: 8 }],
          null,
          true,
        ),
      ).rejects.toThrow('attempt was superseded');
      await restartedHandler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        newState,
        [{ teamId: 102, teamSlug: 'demo-child', userId: 8 }],
        null,
        true,
      );

      await expect(
        restartedHandler.getTeamMembershipSnapshot(
          'enterprise',
          'ent-1',
          newState,
        ),
      ).resolves.toEqual({
        state: {
          generation: 'snapshot-generation',
          attemptToken: newState.attemptToken,
          cursor: null,
          complete: true,
        },
        memberships: [{ teamId: 102, teamSlug: 'demo-child', userId: 8 }],
      });
    });

    it('pages only snapshot memberships matching users in the requested metric day', async () => {
      const day = '2026-05-12';
      const snapshot = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
        'streaming-snapshot',
      );
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        snapshot,
        [
          { teamId: 101, teamSlug: 'demo-parent', userId: 7 },
          { teamId: 102, teamSlug: 'demo-child', userId: 8 },
          { teamId: 102, teamSlug: 'demo-child', userId: 9 },
        ],
        null,
        true,
      );
      await handler.insertUserMetrics([
        buildUserMetric({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 7,
          user_login: 'user-seven',
        }),
        buildUserMetric({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 8,
          user_login: 'user-eight',
        }),
        buildUserMetric({
          day: '2026-05-11',
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 9,
          user_login: 'user-nine',
        }),
      ]);

      const firstPage = await handler.getTeamMembershipSnapshotBatchForDay(
        'enterprise',
        'ent-1',
        snapshot,
        day,
        0,
        1,
      );
      const secondPage = await handler.getTeamMembershipSnapshotBatchForDay(
        'enterprise',
        'ent-1',
        snapshot,
        day,
        firstPage.nextCursor!,
        1,
      );
      const finalPage = await handler.getTeamMembershipSnapshotBatchForDay(
        'enterprise',
        'ent-1',
        snapshot,
        day,
        secondPage.nextCursor!,
        1,
      );

      expect(firstPage.memberships).toHaveLength(1);
      expect(secondPage.memberships).toHaveLength(1);
      expect(finalPage.memberships).toEqual([]);
      expect([...firstPage.memberships, ...secondPage.memberships]).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            team_slug: 'demo-parent',
            user_id: 7,
            user_login: 'user-seven',
          }),
          expect.objectContaining({
            team_slug: 'demo-child',
            user_id: 8,
            user_login: 'user-eight',
          }),
        ]),
      );
    });

    it('omits unavailable teams from backfill status without downgrading totals', async () => {
      const day = '2025-10-28';
      await knex('copilot_team_aggregation_state').insert({
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        active_source: 'native',
        active_generation: 'native-generation',
        pending_source: null,
        pending_generation: null,
        updated_at: new Date(),
      });
      await knex('copilot_team_aggregation_log').insert({
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        source: 'native',
        generation: 'native-generation',
        status: 'unavailable',
        error_message: 'Outside the GitHub report availability window.',
      });
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          status: 'success',
          components_loaded: '["totals","users","teams"]',
        }),
      );

      const [status] = await handler.getBackfillStatus(
        'enterprise',
        'ent-1',
        day,
        day,
      );
      const [ingestionLog] = await handler.getIngestionLog(
        'enterprise',
        'ent-1',
        day,
        day,
      );

      expect(status.status).toBe('success');
      expect(JSON.parse(status.components_loaded)).toEqual(['totals', 'users']);
      expect(ingestionLog.components_loaded).toBe('["totals","users","teams"]');
    });

    it('rebuilds team sources atomically, resumes staged days, and supports reversion', async () => {
      const days = ['2026-05-01', '2026-05-02'];
      const metricUsersByDay = new Map([
        [days[0], [1, 2, 3, 4, 5]],
        [days[1], [1, 2, 3, 4, 5, 6, 7]],
      ]);
      const membershipsFor = (
        day: string,
        userIds: number[],
        teamSlug: string,
      ) =>
        userIds.map(userId =>
          buildUserTeam({
            day,
            metrics_type: 'enterprise',
            entity_id: 'ent-1',
            user_id: userId,
            user_login: `u${userId}`,
            team_id: 17,
            team_slug: teamSlug,
          }),
        );
      const aggregatesFor = async (day: string, userTeams: V2UserTeamRow[]) =>
        aggregateTeamMetrics(
          userTeams.map(row =>
            buildUserMetric({
              day,
              metrics_type: 'enterprise',
              entity_id: 'ent-1',
              user_id: row.user_id,
              user_login: row.user_login,
            }),
          ),
          userTeams,
          await handler.getUserBreakdownsForDay('enterprise', 'ent-1', day),
          day,
          'enterprise',
          'ent-1',
        );

      await handler.insertUserMetrics(
        days.flatMap(day =>
          (metricUsersByDay.get(day) ?? []).map(userId =>
            buildUserMetric({
              day,
              metrics_type: 'enterprise',
              entity_id: 'ent-1',
              user_id: userId,
              user_login: `u${userId}`,
            }),
          ),
        ),
      );
      await handler.insertCustomizationMetrics(
        days.flatMap(day =>
          (metricUsersByDay.get(day) ?? []).map(userId =>
            buildCustomizationMetric({
              day,
              metrics_type: 'enterprise',
              entity_id: 'ent-1',
              scope: 'user',
              team_slug: '',
              user_id: userId,
              user_login: `u${userId}`,
              category: 'skill',
              item_name: 'shared-skill',
              interaction_count: 1,
            }),
          ),
        ),
      );
      for (const day of days) {
        const nativeRows = membershipsFor(
          day,
          metricUsersByDay.get(day) ?? [],
          'shared',
        );
        await handler.replaceTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'native',
          nativeRows,
          await aggregatesFor(day, nativeRows),
        );
      }

      const transition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'organization-fallback',
      );
      const membershipSnapshot = await createCompletedMembershipSnapshot(
        'ent-1',
        transition!.pendingGeneration,
      );
      expect(transition).toEqual(
        expect.objectContaining({
          activeSource: 'native',
          pendingSource: 'organization-fallback',
        }),
      );
      const fallbackDayOne = membershipsFor(days[0], [1, 2], 'shared');
      await expect(
        handler.stageTeamAggregates(
          days[0],
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition!.pendingGeneration,
          days,
          fallbackDayOne,
          await aggregatesFor(days[0], fallbackDayOne),
          membershipSnapshot,
        ),
      ).resolves.toBe(false);
      await expect(
        handler.activateTeamAggregationSource(
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition!.pendingGeneration,
          days,
          membershipSnapshot,
        ),
      ).resolves.toBe(false);
      await expect(
        handler.getTeamAggregationState('enterprise', 'ent-1'),
      ).resolves.toEqual(expect.objectContaining({ activeSource: 'native' }));
      await expect(
        handler.getTeams('enterprise', 'ent-1', days[0], days[1]),
      ).resolves.toEqual(['shared']);

      const resumedHandler = await DatabaseHandlerV2.create({
        database: {
          getClient: async () => knex,
          migrations: { skip: true },
        } as any,
      });
      const resumedTransition =
        await resumedHandler.beginTeamAggregationTransition(
          'enterprise',
          'ent-1',
          'organization-fallback',
        );
      expect(resumedTransition?.pendingGeneration).toBe(
        transition!.pendingGeneration,
      );
      await expect(
        resumedHandler.getMissingTeamAggregationDays(
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition!.pendingGeneration,
        ),
      ).resolves.toEqual([days[1]]);

      const fallbackDayTwo = membershipsFor(days[1], [6, 7], 'shared');
      await expect(
        resumedHandler.stageTeamAggregates(
          days[1],
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition!.pendingGeneration,
          days,
          fallbackDayTwo,
          await aggregatesFor(days[1], fallbackDayTwo),
          membershipSnapshot,
        ),
      ).resolves.toBe(true);
      await expect(
        handler.getTeamAggregationState('enterprise', 'ent-1'),
      ).resolves.toEqual(
        expect.objectContaining({
          activeSource: 'organization-fallback',
        }),
      );
      await expect(
        handler.getTeams('enterprise', 'ent-1', days[0], days[1]),
      ).resolves.toEqual([]);
      await expect(
        handler.getTeams('enterprise', 'ent-1', days[0], days[1], 4),
      ).resolves.toEqual(['shared']);
      const activeFallbackMemberships = await knex('copilot_user_teams')
        .where({
          day: days[0],
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
        })
        .select('team_slug', 'user_id');
      expect(activeFallbackMemberships).toHaveLength(2);
      expect(
        activeFallbackMemberships.map(row => Number(row.user_id)).sort(),
      ).toEqual([1, 2]);
      expect(
        activeFallbackMemberships.every(row => row.team_slug === 'shared'),
      ).toBe(true);
      const fallbackDashboard = await handler.getDashboardData(
        'enterprise',
        'ent-1',
        days[0],
        days[0],
        'shared',
      );
      expect(fallbackDashboard.customizationMetrics).toEqual([
        expect.objectContaining({
          scope: 'team',
          team_slug: 'shared',
          category: 'skill',
          item_name: 'shared-skill',
          interaction_count: 2,
        }),
      ]);

      const reversion = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'native',
      );
      expect(reversion?.pendingSource).toBe('native');
      expect(
        await handler.getMissingTeamAggregationDays(
          'enterprise',
          'ent-1',
          'native',
          reversion!.pendingGeneration,
        ),
      ).toEqual(days);
      for (const day of days) {
        const nativeRows = membershipsFor(
          day,
          metricUsersByDay.get(day) ?? [],
          'native-again',
        );
        await handler.stageTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'native',
          reversion!.pendingGeneration,
          days,
          nativeRows,
          await aggregatesFor(day, nativeRows),
        );
      }

      await expect(
        handler.getTeamAggregationState('enterprise', 'ent-1'),
      ).resolves.toEqual(expect.objectContaining({ activeSource: 'native' }));
      await expect(
        handler.getTeams('enterprise', 'ent-1', days[0], days[1]),
      ).resolves.toEqual(['native-again']);
      const nativeDashboard = await handler.getDashboardData(
        'enterprise',
        'ent-1',
        days[0],
        days[0],
        'native-again',
      );
      expect(nativeDashboard.customizationMetrics).toEqual([
        expect.objectContaining({
          scope: 'team',
          team_slug: 'native-again',
          category: 'skill',
          item_name: 'shared-skill',
          interaction_count: 5,
        }),
      ]);
    });

    it('completes native transitions without retrying out-of-window report days', async () => {
      const oldDay = '2025-10-28';
      const recentDay = '2026-05-01';
      const days = [oldDay, recentDay];
      const users = [1, 2, 3, 4, 5].map(userId =>
        buildUserMetric({
          day: recentDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          user_id: userId,
          user_login: `u${userId}`,
        }),
      );
      await handler.insertUserMetrics([
        ...users,
        buildUserMetric({
          day: oldDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          user_id: 1,
          user_login: 'u1',
        }),
      ]);
      await knex('copilot_team_aggregation_state').insert({
        metrics_type: 'enterprise',
        entity_id: 'demo-enterprise',
        active_source: 'organization-fallback',
        active_generation: 'fallback-generation',
      });
      await knex('copilot_user_teams').insert(
        buildUserTeam({
          day: oldDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          user_id: 1,
          user_login: 'u1',
          team_slug: 'old-team',
        }),
      );
      await handler.insertDailyTotals([
        buildDailyTotal({
          day: oldDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          team_slug: 'old-team',
        }),
      ]);

      const transition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'demo-enterprise',
        'native',
      );
      expect(transition?.pendingGeneration).toBeTruthy();

      await expect(
        handler.markTeamAggregationUnavailable(
          oldDay,
          'enterprise',
          'demo-enterprise',
          'native',
          transition!.pendingGeneration,
          days,
          'Outside the GitHub report availability window.',
        ),
      ).resolves.toBe(false);
      await expect(
        handler.getMissingTeamAggregationDays(
          'enterprise',
          'demo-enterprise',
          'native',
          transition!.pendingGeneration,
        ),
      ).resolves.toEqual([recentDay]);

      const recentMemberships = users.map(user =>
        buildUserTeam({
          day: recentDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          user_id: user.user_id,
          user_login: user.user_login,
          team_slug: 'recent-team',
        }),
      );
      await expect(
        handler.stageTeamAggregates(
          recentDay,
          'enterprise',
          'demo-enterprise',
          'native',
          transition!.pendingGeneration,
          days,
          recentMemberships,
          aggregateTeamMetrics(
            users,
            recentMemberships,
            [],
            recentDay,
            'enterprise',
            'demo-enterprise',
          ),
        ),
      ).resolves.toBe(true);

      await expect(
        handler.getTeamAggregationState('enterprise', 'demo-enterprise'),
      ).resolves.toEqual(expect.objectContaining({ activeSource: 'native' }));
      await expect(
        knex('copilot_team_aggregation_log')
          .where({
            day: oldDay,
            metrics_type: 'enterprise',
            entity_id: 'demo-enterprise',
            source: 'native',
            generation: transition!.pendingGeneration,
          })
          .first('status'),
      ).resolves.toEqual({ status: 'unavailable' });
      await expect(
        knex('copilot_daily_totals').where({
          day: oldDay,
          metrics_type: 'enterprise',
          entity_id: 'demo-enterprise',
          team_slug: 'old-team',
        }),
      ).resolves.toHaveLength(0);
      await expect(
        handler.getMissingTeamAggregationDays(
          'enterprise',
          'demo-enterprise',
          'native',
          transition!.pendingGeneration,
        ),
      ).resolves.toEqual([]);
    });

    it('serializes the first transition after seeding the default native state row', async () => {
      const [first, second] = await Promise.all([
        handler.beginTeamAggregationTransition(
          'enterprise',
          'ent-1',
          'organization-fallback',
        ),
        handler.beginTeamAggregationTransition(
          'enterprise',
          'ent-1',
          'organization-fallback',
        ),
      ]);

      expect(first?.pendingGeneration).toBeTruthy();
      expect(second?.pendingGeneration).toBe(first?.pendingGeneration);
      await expect(
        knex('copilot_team_aggregation_state')
          .where({ metrics_type: 'enterprise', entity_id: 'ent-1' })
          .first(),
      ).resolves.toEqual(
        expect.objectContaining({
          active_source: 'native',
          active_generation: 'native-default',
          pending_source: 'organization-fallback',
          pending_generation: first?.pendingGeneration,
        }),
      );
    });

    it('does not replace fallback data after its pinned membership snapshot is superseded', async () => {
      const day = '2026-05-12';
      const snapshot = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
        'pinned-snapshot',
      );
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        snapshot,
        [{ teamId: 101, teamSlug: 'new-team', userId: 7 }],
        null,
        true,
      );

      const transition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'organization-fallback',
      );
      if (!transition) {
        throw new Error('Expected an organization-fallback transition.');
      }
      await expect(
        handler.activateTeamAggregationSource(
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition.pendingGeneration,
          [],
          snapshot,
        ),
      ).resolves.toBe(true);

      await handler.insertUserMetrics([
        buildUserMetric({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 7,
          user_login: 'user-seven',
        }),
      ]);
      await knex('copilot_user_teams').insert(
        buildUserTeam({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          user_id: 9,
          user_login: 'previous-user',
          team_slug: 'previous-team',
        }),
      );
      await handler.insertDailyTotals([
        buildDailyTotal({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          team_slug: 'previous-team',
        }),
      ]);

      const refreshedSnapshot = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
      );
      expect(refreshedSnapshot.generation).not.toBe(snapshot.generation);
      expect(refreshedSnapshot.complete).toBe(false);

      await expect(
        handler.getTeamMembershipSnapshotBatchForDay(
          'enterprise',
          'ent-1',
          snapshot,
          day,
        ),
      ).rejects.toThrow('attempt was superseded');
      await expect(
        handler.getTeamMembershipSnapshotBatchForDay(
          'enterprise',
          'ent-1',
          refreshedSnapshot,
          day,
        ),
      ).rejects.toThrow('missing or incomplete');
      await expect(
        handler.replaceStagedTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition.pendingGeneration,
          snapshot,
        ),
      ).rejects.toThrow('attempt was superseded');

      const membershipsAfterRejectedReplacement = await knex(
        'copilot_user_teams',
      ).where({
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
      });
      expect(membershipsAfterRejectedReplacement).toHaveLength(1);
      expect(membershipsAfterRejectedReplacement[0].team_slug).toBe(
        'previous-team',
      );
      expect(Number(membershipsAfterRejectedReplacement[0].user_id)).toBe(9);
      await expect(
        knex('copilot_daily_totals').where({
          day,
          metrics_type: 'enterprise',
          entity_id: 'ent-1',
          team_slug: 'previous-team',
        }),
      ).resolves.toHaveLength(1);
      await expect(
        handler.getMissingTeamAggregationDays(
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition.pendingGeneration,
        ),
      ).resolves.toEqual([day]);
    });

    it('prevents separate snapshot initializers from mixing membership pages', async () => {
      const day = '2026-05-12';
      const metric = buildUserMetric({
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        user_id: 7,
        user_login: 'user-seven',
      });
      await handler.insertUserMetrics([metric]);

      const transition = await handler.beginTeamAggregationTransition(
        'enterprise',
        'ent-1',
        'organization-fallback',
      );
      if (!transition) {
        throw new Error('Expected an organization-fallback transition.');
      }

      const firstAttempt = await handler.beginTeamMembershipSnapshot(
        'enterprise',
        'ent-1',
        transition.pendingGeneration,
      );
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        firstAttempt,
        [{ teamId: 101, teamSlug: 'stale-page-team', userId: 7 }],
        'cursor-from-first-initializer',
        false,
      );
      await handler.saveTeamMembershipSnapshotBatch(
        'enterprise',
        'ent-1',
        firstAttempt,
        [{ teamId: 102, teamSlug: 'stale-second-page', userId: 8 }],
        null,
        true,
      );
      const staleTeam = buildUserTeam({
        day,
        metrics_type: 'enterprise',
        entity_id: 'ent-1',
        user_id: 7,
        user_login: 'user-seven',
        team_slug: 'stale-page-team',
      });
      await expect(
        handler.stageTeamAggregates(
          day,
          'enterprise',
          'ent-1',
          'organization-fallback',
          transition.pendingGeneration,
          [day, '2026-05-13'],
          [staleTeam],
          aggregateTeamMetrics(
            [metric],
            [staleTeam],
            [],
            day,
            'enterprise',
            'ent-1',
          ),
          firstAttempt,
        ),
      ).resolves.toBe(false);

      const independentKnex =
        databaseId === 'POSTGRES_18'
          ? knexFactory(knex.client.config as Knex.Config)
          : knex;
      const independentHandler = await DatabaseHandlerV2.create({
        database: {
          getClient: async () => independentKnex,
          migrations: { skip: true },
        } as any,
      });

      try {
        const secondAttempt =
          await independentHandler.beginTeamMembershipSnapshot(
            'enterprise',
            'ent-1',
            transition.pendingGeneration,
          );
        expect(secondAttempt.attemptToken).not.toBe(firstAttempt.attemptToken);
        await expect(
          handler.getMissingTeamAggregationDays(
            'enterprise',
            'ent-1',
            'organization-fallback',
            transition.pendingGeneration,
          ),
        ).resolves.toEqual([day]);
        await expect(
          knex('copilot_user_teams_staging').where({
            day,
            metrics_type: 'enterprise',
            entity_id: 'ent-1',
            generation: transition.pendingGeneration,
          }),
        ).resolves.toHaveLength(0);
        await expect(
          knex('copilot_team_aggregates_staging').where({
            day,
            metrics_type: 'enterprise',
            entity_id: 'ent-1',
            generation: transition.pendingGeneration,
          }),
        ).resolves.toHaveLength(0);

        await expect(
          handler.saveTeamMembershipSnapshotBatch(
            'enterprise',
            'ent-1',
            firstAttempt,
            [{ teamId: 102, teamSlug: 'late-stale-page', userId: 8 }],
            null,
            true,
          ),
        ).rejects.toThrow('attempt was superseded');
        await independentHandler.saveTeamMembershipSnapshotBatch(
          'enterprise',
          'ent-1',
          secondAttempt,
          [{ teamId: 202, teamSlug: 'fresh-team', userId: 7 }],
          null,
          true,
        );

        await expect(
          independentHandler.getTeamMembershipSnapshot(
            'enterprise',
            'ent-1',
            secondAttempt,
          ),
        ).resolves.toEqual({
          state: {
            generation: transition.pendingGeneration,
            attemptToken: secondAttempt.attemptToken,
            cursor: null,
            complete: true,
          },
          memberships: [{ teamId: 202, teamSlug: 'fresh-team', userId: 7 }],
        });
        await expect(
          handler.activateTeamAggregationSource(
            'enterprise',
            'ent-1',
            'organization-fallback',
            transition.pendingGeneration,
            [],
            firstAttempt,
          ),
        ).rejects.toThrow('attempt was superseded');
        await expect(
          independentHandler.stageTeamAggregates(
            day,
            'enterprise',
            'ent-1',
            'organization-fallback',
            transition.pendingGeneration,
            [day],
            [
              buildUserTeam({
                day,
                metrics_type: 'enterprise',
                entity_id: 'ent-1',
                user_id: 7,
                user_login: 'user-seven',
                team_slug: 'fresh-team',
              }),
            ],
            aggregateTeamMetrics(
              [metric],
              [
                buildUserTeam({
                  day,
                  metrics_type: 'enterprise',
                  entity_id: 'ent-1',
                  user_id: 7,
                  user_login: 'user-seven',
                  team_slug: 'fresh-team',
                }),
              ],
              [],
              day,
              'enterprise',
              'ent-1',
            ),
            secondAttempt,
          ),
        ).resolves.toBe(true);
        await expect(
          handler.getTeamAggregationState('enterprise', 'ent-1'),
        ).resolves.toEqual(
          expect.objectContaining({ activeSource: 'organization-fallback' }),
        );
        await expect(
          knex('copilot_user_teams').where({
            day,
            metrics_type: 'enterprise',
            entity_id: 'ent-1',
          }),
        ).resolves.toEqual(
          expect.arrayContaining([
            expect.objectContaining({ team_slug: 'fresh-team' }),
          ]),
        );
      } finally {
        if (independentKnex !== knex) {
          await independentKnex.destroy();
        }
      }
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

    it('getMissingDays does not skip error days missing required components', async () => {
      await handler.upsertIngestionLog(
        buildIngestionLog({ day: '2026-05-01', status: 'success' }),
      );
      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-02',
          status: 'error',
          components_loaded: '[]',
        }),
      );

      const result = await handler.getMissingDays(
        'organization',
        'org-1',
        '2026-05-01',
        '2026-05-03',
      );

      expect(result).toEqual(['2026-05-02', '2026-05-03']);
    });

    it('does not retry components already loaded before a later error', async () => {
      await knex('copilot_ingestion_log').insert(
        buildIngestionLog({
          day: '2026-05-02',
          status: 'error',
          components_loaded: '["totals"]',
        }),
      );

      await expect(
        handler.getMissingDays(
          'organization',
          'org-1',
          '2026-05-02',
          '2026-05-02',
          ['totals'],
        ),
      ).resolves.toEqual([]);
      await expect(
        handler.getMissingDays(
          'organization',
          'org-1',
          '2026-05-02',
          '2026-05-02',
          ['totals', 'users'],
        ),
      ).resolves.toEqual(['2026-05-02']);
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
        buildIngestionLog({
          day: '2026-05-11',
          status: 'success',
          components_loaded: '["totals"]',
        }),
      );

      await handler.upsertIngestionLog(
        buildIngestionLog({
          day: '2026-05-11',
          status: 'error',
          components_loaded: '[]',
          error_message: 'download failed',
          source: 'backfill',
        }),
      );

      const logs = await handler.getIngestionLog('organization', 'org-1');
      expect(logs).toHaveLength(1);
      expect(logs[0].status).toBe('success');
      expect(logs[0].components_loaded).toBe('["totals"]');
      expect(logs[0].error_message).toBe('download failed');
      expect(logs[0].source).toBe('backfill');
      await expect(
        handler.getMissingDays(
          'organization',
          'org-1',
          '2026-05-11',
          '2026-05-11',
          ['totals'],
        ),
      ).resolves.toEqual([]);
      await expect(
        handler.getMissingDays(
          'organization',
          'org-1',
          '2026-05-11',
          '2026-05-11',
          ['totals', 'users'],
        ),
      ).resolves.toEqual(['2026-05-11']);
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
        const rows = await (handler as any).enrichTeamRollingActiveUsers(
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
      await handler.insertCustomizationMetrics([
        {
          day: '2026-05-01',
          metrics_type: 'organization',
          entity_id: 'org-1',
          scope: 'aggregate',
          team_slug: '',
          user_id: 0,
          user_login: '',
          category: 'skill',
          item_name: 'release-checks',
          interaction_count: 7,
        },
      ]);
      await handler.insertCustomizationSummaries([
        {
          day: '2026-05-01',
          metrics_type: 'organization',
          entity_id: 'org-1',
          scope: 'aggregate',
          team_slug: '',
          user_id: 0,
          user_login: '',
          category: 'skill',
          distinct_item_count: 1,
        },
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
      expect(result.customizationMetrics).toEqual([
        expect.objectContaining({
          category: 'skill',
          item_name: 'release-checks',
          interaction_count: 7,
        }),
      ]);
      expect(result.customizationSummaries).toEqual([
        expect.objectContaining({
          category: 'skill',
          distinct_item_count: 1,
        }),
      ]);
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

    it('reads stored per-user reports for a day', async () => {
      await handler.insertUserMetrics([buildUserMetric({ day: '2026-05-01' })]);
      await handler.insertUserMetricsByFeature([
        buildUserMetricsByFeature({ day: '2026-05-01' }),
      ]);

      await expect(
        handler.getUserMetricsForDay('organization', 'org-1', '2026-05-01'),
      ).resolves.toEqual([
        expect.objectContaining({
          day: '2026-05-01',
          user_id: 1,
          user_login: 'octocat',
        }),
      ]);

      await expect(
        handler.getUserBreakdownsForDay('organization', 'org-1', '2026-05-01'),
      ).resolves.toEqual([
        expect.objectContaining({
          user_id: 1,
          user_login: 'octocat',
          byFeature: [
            expect.objectContaining({
              feature: 'chat_panel',
              code_generation_activity_count: 4,
            }),
          ],
          byIde: [],
          byLanguageFeature: [],
          byModelFeature: [],
          byLanguageModel: [],
          customizations: [],
        }),
      ]);
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
      await handler.insertCustomizationMetrics([
        buildCustomizationMetric({ user_login: 'OctoCat', user_id: 1 }),
        buildCustomizationMetric({
          user_login: 'someone-else',
          user_id: 2,
          item_name: 'other-skill',
        }),
        buildCustomizationMetric({
          scope: 'aggregate',
          user_id: 0,
          user_login: '',
          item_name: 'aggregate-skill',
        }),
        buildCustomizationMetric({
          scope: 'team',
          team_slug: 'alpha',
          user_id: 0,
          user_login: '',
          item_name: 'team-skill',
        }),
      ]);
      await handler.insertCustomizationSummaries([
        buildCustomizationSummary({ user_login: 'OctoCat', user_id: 1 }),
        buildCustomizationSummary({
          user_login: 'someone-else',
          user_id: 2,
          distinct_item_count: 2,
        }),
        buildCustomizationSummary({
          scope: 'aggregate',
          user_id: 0,
          user_login: '',
          distinct_item_count: 3,
        }),
      ]);

      const result = await handler.getUserDashboardData(
        'organization',
        'org-1',
        'OCTOCAT',
        '2026-05-01',
        '2026-05-01',
      );

      expect(result.userLogin).toBe('OCTOCAT');
      expect(result.daily).toHaveLength(1);
      expect(result.daily[0].user_login).toBe('octocat');
      expect(result.byFeature).toHaveLength(1);
      expect(result.byFeature[0].feature).toBe('chat_panel');
      expect(result.byIde).toHaveLength(1);
      expect(result.byLanguage).toHaveLength(1);
      expect(result.byModelFeature).toHaveLength(1);
      expect(result.byLanguageModel).toHaveLength(1);
      expect(result.customizationMetrics).toEqual([
        expect.objectContaining({
          scope: 'user',
          user_login: 'octocat',
          item_name: 'release-checks',
        }),
      ]);
      expect(result.customizationSummaries).toEqual([
        expect.objectContaining({
          scope: 'user',
          user_login: 'octocat',
          distinct_item_count: 1,
        }),
      ]);

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
      expect(otherUser.customizationMetrics).toEqual([
        expect.objectContaining({
          user_login: 'someone-else',
          item_name: 'other-skill',
        }),
      ]);
      expect(otherUser.customizationSummaries).toEqual([
        expect.objectContaining({
          user_login: 'someone-else',
          distinct_item_count: 2,
        }),
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

function buildCustomizationMetric(
  overrides: Partial<V2CustomizationMetricRow> = {},
): V2CustomizationMetricRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    scope: 'user',
    team_slug: '',
    user_id: 1,
    user_login: 'octocat',
    category: 'skill',
    item_name: 'release-checks',
    interaction_count: 7,
    ...overrides,
  };
}

function buildCustomizationSummary(
  overrides: Partial<V2CustomizationSummaryRow> = {},
): V2CustomizationSummaryRow {
  return {
    day: '2026-05-01',
    metrics_type: 'organization',
    entity_id: 'org-1',
    scope: 'user',
    team_slug: '',
    user_id: 1,
    user_login: 'octocat',
    category: 'skill',
    distinct_item_count: 1,
    ...overrides,
  };
}
