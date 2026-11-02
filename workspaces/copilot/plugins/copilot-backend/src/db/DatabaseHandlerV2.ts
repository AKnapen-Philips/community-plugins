/*
 * Copyright 2024 The Backstage Authors
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
import { DatabaseService } from '@backstage/backend-plugin-api';
import {
  PeriodRange,
  V2CustomizationMetricRow,
  V2CustomizationSummaryRow,
  V2DailyTotal,
  V2DashboardData,
  V2IngestionLogRow,
  V2MetricsByCliRow,
  V2MetricsByFeatureRow,
  V2MetricsByIdeRow,
  V2MetricsByLanguageFeatureRow,
  V2MetricsByModelFeatureRow,
  V2MetricsByLanguageModelRow,
  V2MetricsByVscodeAgentRow,
  V2PrMetricsRow,
  V2UserDashboardData,
  V2UserMetricRow,
  V2UserMetricsByFeatureRow,
  V2UserMetricsByIdeRow,
  V2UserMetricsByLanguageFeatureRow,
  V2UserMetricsByModelFeatureRow,
  V2UserMetricsByLanguageModelRow,
  V2UserTeamRow,
  MetricsScope,
} from '@backstage-community/plugin-copilot-common';
import { DateTime } from 'luxon';
import { Knex } from 'knex';
import { randomUUID } from 'node:crypto';
import { migrationsDir } from './DatabaseHandler';
import { batchInsertInChunks } from '../utils/batchInsert';
import { UserBreakdownData } from '../utils/reportParser';
import type { TeamAggregates } from '../utils/teamAggregator';

type Options = {
  database: DatabaseService;
};

type DayValue = string | Date;

type TeamActivityRow = {
  day: DayValue;
  user_id: number;
  used_agent: boolean | number | null;
  used_chat: boolean | number | null;
};

export type TeamAggregationSource = 'native' | 'organization-fallback';

export type TeamAggregationState = {
  activeSource: TeamAggregationSource;
  activeGeneration: string;
  pendingSource?: TeamAggregationSource;
  pendingGeneration?: string;
};

export type TeamAggregationTransition = TeamAggregationState & {
  pendingSource: TeamAggregationSource;
  pendingGeneration: string;
};

export type TeamMembershipSnapshotAttempt = {
  generation: string;
  attemptToken: string;
};

export type TeamMembershipSnapshotState = TeamMembershipSnapshotAttempt & {
  cursor: string | null;
  complete: boolean;
};

type TeamAggregationStateRow = {
  active_source: TeamAggregationSource;
  active_generation: string;
  pending_source: TeamAggregationSource | null;
  pending_generation: string | null;
};

type TeamAggregationLogRow = {
  day: string;
  metrics_type: MetricsScope;
  entity_id: string;
  source: TeamAggregationSource;
  generation: string;
  status: 'staged' | 'complete' | 'failed' | 'abandoned' | 'unavailable';
  error_message?: string | null;
  completed_at?: Date | null;
};

type TeamMembershipStagingRow = Omit<V2UserTeamRow, 'id'> & {
  generation: string;
};

type TeamAggregateStagingRow = {
  day: string;
  metrics_type: MetricsScope;
  entity_id: string;
  generation: string;
  table_name: string;
  row_key: string;
  row_data: string;
};

type TeamMembershipSnapshotRow = {
  id?: number;
  generation: string;
  metrics_type: MetricsScope;
  entity_id: string;
  team_id: number;
  team_slug: string;
  user_id: number;
};

type TeamMembershipSnapshotPageRow = TeamMembershipSnapshotRow & {
  id: number;
  user_login: string;
};

type TeamMembershipSnapshotStateRow = {
  generation: string;
  attempt_token: string;
  cursor: string | null;
  complete: boolean | number;
};

const teamAggregateTables = [
  'copilot_daily_totals',
  'copilot_pr_metrics',
  'copilot_metrics_by_feature',
  'copilot_metrics_by_ide',
  'copilot_metrics_by_language_feature',
  'copilot_metrics_by_model_feature',
  'copilot_metrics_by_language_model',
  'copilot_metrics_by_cli',
  'copilot_metrics_by_vscode_agent',
  'copilot_customization_metrics',
] as const;

const teamAggregationCopyBatchSize = 500;

const teamMembershipPageSize = 500;

const teamAggregateRowDimensions: Record<string, string[]> = {
  copilot_daily_totals: ['team_slug'],
  copilot_pr_metrics: ['team_slug'],
  copilot_metrics_by_feature: ['team_slug', 'feature'],
  copilot_metrics_by_ide: ['team_slug', 'ide'],
  copilot_metrics_by_language_feature: ['team_slug', 'language', 'feature'],
  copilot_metrics_by_model_feature: ['team_slug', 'model_id', 'feature'],
  copilot_metrics_by_language_model: ['team_slug', 'language', 'model_id'],
  copilot_metrics_by_cli: ['team_slug'],
  copilot_metrics_by_vscode_agent: ['team_slug'],
  copilot_customization_metrics: [
    'scope',
    'team_slug',
    'user_id',
    'category',
    'item_name',
  ],
};

const teamAggregateAdditiveColumns: Record<string, string[]> = {
  copilot_daily_totals: [
    'daily_active_users',
    'daily_active_cli_users',
    'daily_active_vscode_agent_users',
    'monthly_active_agent_users',
    'monthly_active_chat_users',
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
    'user_initiated_interaction_count',
    'total_ai_credits_used',
  ],
  copilot_pr_metrics: [
    'total_created',
    'total_merged',
    'total_reviewed',
    'total_created_by_copilot',
    'total_merged_created_by_copilot',
    'total_merged_reviewed_by_copilot',
    'total_reviewed_by_copilot',
    'total_suggestions',
    'total_applied_suggestions',
    'total_copilot_suggestions',
    'total_copilot_applied_suggestions',
  ],
  copilot_metrics_by_feature: [
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
    'user_initiated_interaction_count',
  ],
  copilot_metrics_by_ide: [
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
    'user_initiated_interaction_count',
  ],
  copilot_metrics_by_language_feature: [
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
  ],
  copilot_metrics_by_model_feature: [
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
    'user_initiated_interaction_count',
  ],
  copilot_metrics_by_language_model: [
    'request_count',
    'code_acceptance_activity_count',
    'code_generation_activity_count',
    'loc_added_sum',
    'loc_deleted_sum',
    'loc_suggested_to_add_sum',
    'loc_suggested_to_delete_sum',
  ],
  copilot_metrics_by_cli: [
    'prompt_count',
    'request_count',
    'session_count',
    'output_tokens_sum',
    'prompt_tokens_sum',
  ],
  copilot_metrics_by_vscode_agent: ['session_count', 'total_user_messages'],
  copilot_customization_metrics: ['interaction_count'],
};

const teamAggregateRowsByTable = (
  aggregates: TeamAggregates,
): Array<[string, object[]]> => [
  ['copilot_daily_totals', aggregates.dailyTotals],
  ['copilot_metrics_by_feature', aggregates.byFeature],
  ['copilot_metrics_by_ide', aggregates.byIde],
  ['copilot_metrics_by_language_feature', aggregates.byLanguageFeature],
  ['copilot_metrics_by_model_feature', aggregates.byModelFeature],
  ['copilot_metrics_by_language_model', aggregates.byLanguageModel],
  ['copilot_customization_metrics', aggregates.customizationMetrics],
];

function getTeamAggregateRowKey(
  tableName: string,
  row: Record<string, unknown>,
): string {
  const dimensions = teamAggregateRowDimensions[tableName];
  if (!dimensions) {
    throw new Error(`Unexpected team aggregate staging table "${tableName}".`);
  }
  return JSON.stringify(dimensions.map(dimension => row[dimension] ?? ''));
}

function mergeTeamAggregateRows(
  tableName: string,
  existing: Record<string, unknown>,
  next: Record<string, unknown>,
): Record<string, unknown> {
  const merged = { ...existing };
  for (const column of teamAggregateAdditiveColumns[tableName] ?? []) {
    const currentValue = merged[column];
    const nextValue = next[column];
    if (typeof currentValue === 'number' || typeof nextValue === 'number') {
      merged[column] =
        (typeof currentValue === 'number' ? currentValue : 0) +
        (typeof nextValue === 'number' ? nextValue : 0);
    }
  }
  return merged;
}

export class DatabaseHandlerV2 {
  static async create(options: Options): Promise<DatabaseHandlerV2> {
    const { database } = options;
    const client = await database.getClient();

    if (!database.migrations?.skip) {
      await client.migrate.latest({ directory: migrationsDir });
    }

    return new DatabaseHandlerV2(client);
  }

  private constructor(private readonly db: Knex) {}

  async getTeamAggregationState(
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<TeamAggregationState> {
    const row = (await this.db('copilot_team_aggregation_state')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .first()) as TeamAggregationStateRow | undefined;

    return {
      activeSource: row?.active_source ?? 'native',
      activeGeneration: row?.active_generation ?? 'native-default',
      ...(row?.pending_source ? { pendingSource: row.pending_source } : {}),
      ...(row?.pending_generation
        ? { pendingGeneration: row.pending_generation }
        : {}),
    };
  }

  private async ensureTeamAggregationStateRow(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    await transaction('copilot_team_aggregation_state')
      .insert({
        metrics_type: metricsType,
        entity_id: entityId,
        active_source: 'native',
        active_generation: 'native-default',
        pending_source: null,
        pending_generation: null,
        updated_at: new Date(),
      })
      .onConflict(['metrics_type', 'entity_id'])
      .ignore();
  }

  async beginTeamMembershipSnapshot(
    metricsType: MetricsScope,
    entityId: string,
    generation?: string,
  ): Promise<TeamMembershipSnapshotState> {
    return this.db.transaction(async transaction => {
      const snapshotGeneration = generation ?? randomUUID();
      const attemptToken = randomUUID();
      const now = new Date();
      const aggregationState = await this.lockTeamAggregationState(
        transaction,
        metricsType,
        entityId,
      );

      await transaction('copilot_team_membership_snapshot_state')
        .insert({
          metrics_type: metricsType,
          entity_id: entityId,
          generation: snapshotGeneration,
          attempt_token: attemptToken,
          cursor: null,
          complete: false,
          updated_at: now,
        })
        .onConflict(['metrics_type', 'entity_id'])
        .merge({
          generation: snapshotGeneration,
          attempt_token: attemptToken,
          cursor: null,
          complete: false,
          updated_at: now,
        });
      await transaction('copilot_team_membership_snapshot_staging')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .del();
      if (
        aggregationState.pending_source === 'organization-fallback' &&
        aggregationState.pending_generation === snapshotGeneration
      ) {
        await transaction('copilot_user_teams_staging')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: snapshotGeneration,
          })
          .del();
        await transaction('copilot_team_aggregates_staging')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: snapshotGeneration,
          })
          .del();
        await transaction('copilot_team_aggregation_log')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            source: 'organization-fallback',
            generation: snapshotGeneration,
            status: 'staged',
          })
          .update({
            status: 'failed',
            error_message:
              'Organization team membership snapshot attempt was superseded.',
            completed_at: null,
          });
      }

      return {
        generation: snapshotGeneration,
        attemptToken,
        cursor: null,
        complete: false,
      };
    });
  }

  async getTeamMembershipSnapshot(
    metricsType: MetricsScope,
    entityId: string,
    attempt: TeamMembershipSnapshotAttempt,
  ): Promise<{
    state: TeamMembershipSnapshotState;
    memberships: Array<{ teamId: number; teamSlug: string; userId: number }>;
  }> {
    return this.db.transaction(async transaction => {
      const state = await this.assertCurrentTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        attempt,
        true,
      );
      const rows = await transaction<TeamMembershipSnapshotRow>(
        'copilot_team_membership_snapshot_staging',
      )
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          generation: attempt.generation,
        })
        .orderBy('team_id', 'asc')
        .orderBy('user_id', 'asc')
        .select('team_id', 'team_slug', 'user_id');

      return {
        state: this.mapTeamMembershipSnapshotState(state),
        memberships: rows.map(row => ({
          teamId: Number(row.team_id),
          teamSlug: row.team_slug,
          userId: Number(row.user_id),
        })),
      };
    });
  }

  async getTeamMembershipSnapshotState(
    metricsType: MetricsScope,
    entityId: string,
    attempt: TeamMembershipSnapshotAttempt,
  ): Promise<TeamMembershipSnapshotState> {
    return this.db.transaction(async transaction => {
      const state = await this.assertCurrentTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        attempt,
      );
      return this.mapTeamMembershipSnapshotState(state);
    });
  }

  private mapTeamMembershipSnapshotState(
    state: TeamMembershipSnapshotStateRow,
  ): TeamMembershipSnapshotState {
    return {
      generation: state.generation,
      attemptToken: state.attempt_token,
      cursor: state.cursor ?? null,
      complete: state.complete === true || Number(state.complete) === 1,
    };
  }

  private async assertCurrentTeamMembershipSnapshot(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
    attempt: TeamMembershipSnapshotAttempt,
    requireComplete = false,
  ): Promise<TeamMembershipSnapshotStateRow> {
    const state = (await transaction('copilot_team_membership_snapshot_state')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .forUpdate()
      .first('generation', 'attempt_token', 'cursor', 'complete')) as
      | TeamMembershipSnapshotStateRow
      | undefined;

    if (
      !state ||
      state.generation !== attempt.generation ||
      state.attempt_token !== attempt.attemptToken
    ) {
      throw new Error(
        'Organization team membership snapshot attempt was superseded.',
      );
    }

    if (
      requireComplete &&
      state.complete !== true &&
      Number(state.complete) !== 1
    ) {
      throw new Error(
        'Organization team membership snapshot is missing or incomplete.',
      );
    }

    return state;
  }

  private async assertRequiredTeamMembershipSnapshot(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
    attempt?: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    if (!attempt) {
      throw new Error(
        'Organization team membership snapshot attempt is required.',
      );
    }

    await this.assertCurrentTeamMembershipSnapshot(
      transaction,
      metricsType,
      entityId,
      attempt,
      true,
    );
  }

  async getTeamMembershipSnapshotBatchForDay(
    metricsType: MetricsScope,
    entityId: string,
    attempt: TeamMembershipSnapshotAttempt,
    day: string,
    afterId: number = 0,
    pageSize: number = teamMembershipPageSize,
  ): Promise<{ memberships: V2UserTeamRow[]; nextCursor: number | null }> {
    return this.db.transaction(async transaction => {
      await this.assertCurrentTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        attempt,
        true,
      );

      const rows = await transaction<TeamMembershipSnapshotPageRow>(
        'copilot_team_membership_snapshot_staging as s',
      )
        .join('copilot_user_metrics as m', function joinDailyMetrics() {
          this.on('s.metrics_type', '=', 'm.metrics_type')
            .andOn('s.entity_id', '=', 'm.entity_id')
            .andOn('s.user_id', '=', 'm.user_id');
        })
        .where('s.metrics_type', metricsType)
        .where('s.entity_id', entityId)
        .where('s.generation', attempt.generation)
        .where('m.day', day)
        .where('s.id', '>', afterId)
        .orderBy('s.id', 'asc')
        .limit(pageSize)
        .select(
          's.id',
          's.team_id',
          's.team_slug',
          's.user_id',
          'm.user_login',
        );

      return {
        memberships: rows.map(row => ({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          user_id: Number(row.user_id),
          user_login: row.user_login,
          team_id: Number(row.team_id),
          team_slug: row.team_slug,
        })),
        nextCursor:
          rows.length === pageSize ? Number(rows[rows.length - 1].id) : null,
      };
    });
  }

  async saveTeamMembershipSnapshotBatch(
    metricsType: MetricsScope,
    entityId: string,
    attempt: TeamMembershipSnapshotAttempt,
    memberships: Array<{ teamId: number; teamSlug: string; userId: number }>,
    cursor: string | null,
    complete: boolean,
  ): Promise<void> {
    await this.db.transaction(async transaction => {
      const state = await this.assertCurrentTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        attempt,
      );
      if (state.complete === true || Number(state.complete) === 1) {
        throw new Error(
          'Organization team membership snapshot is already complete.',
        );
      }

      const rows: TeamMembershipSnapshotRow[] = memberships.map(
        ({ teamId, teamSlug, userId }) => ({
          generation: attempt.generation,
          metrics_type: metricsType,
          entity_id: entityId,
          team_id: teamId,
          team_slug: teamSlug,
          user_id: userId,
        }),
      );
      await batchInsertInChunks(rows, 100, async chunk => {
        await transaction<TeamMembershipSnapshotRow>(
          'copilot_team_membership_snapshot_staging',
        )
          .insert(chunk)
          .onConflict([
            'metrics_type',
            'entity_id',
            'generation',
            'team_id',
            'user_id',
          ])
          .merge(['team_slug']);
      });

      await transaction('copilot_team_membership_snapshot_state')
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          generation: attempt.generation,
          attempt_token: attempt.attemptToken,
        })
        .update({
          cursor,
          complete,
          updated_at: new Date(),
        });
    });
  }

  async beginTeamAggregationTransition(
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
  ): Promise<TeamAggregationTransition | null> {
    return this.db.transaction(async transaction => {
      await this.ensureTeamAggregationStateRow(
        transaction,
        metricsType,
        entityId,
      );
      const state = (await transaction('copilot_team_aggregation_state')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .forUpdate()
        .first()) as TeamAggregationStateRow | undefined;
      const activeSource = state?.active_source ?? 'native';
      const activeGeneration = state?.active_generation ?? 'native-default';
      const pendingSource = state?.pending_source ?? undefined;
      const pendingGeneration = state?.pending_generation ?? undefined;

      if (activeSource === source) {
        if (pendingGeneration) {
          await transaction('copilot_team_aggregation_log')
            .where({
              metrics_type: metricsType,
              entity_id: entityId,
              generation: pendingGeneration,
            })
            .update({ status: 'abandoned' });
          await transaction('copilot_user_teams_staging')
            .where({
              metrics_type: metricsType,
              entity_id: entityId,
              generation: pendingGeneration,
            })
            .del();
          await transaction('copilot_team_aggregates_staging')
            .where({
              metrics_type: metricsType,
              entity_id: entityId,
              generation: pendingGeneration,
            })
            .del();
          await transaction('copilot_team_membership_snapshot_staging')
            .where({
              metrics_type: metricsType,
              entity_id: entityId,
              generation: pendingGeneration,
            })
            .del();
          await transaction('copilot_team_membership_snapshot_state')
            .where({
              metrics_type: metricsType,
              entity_id: entityId,
              generation: pendingGeneration,
            })
            .del();
          await transaction('copilot_team_aggregation_state')
            .where({ metrics_type: metricsType, entity_id: entityId })
            .update({
              pending_source: null,
              pending_generation: null,
              updated_at: new Date(),
            });
        }
        return null;
      }

      if (pendingSource === source && pendingGeneration) {
        return {
          activeSource,
          activeGeneration,
          pendingSource,
          pendingGeneration,
        };
      }

      if (pendingGeneration) {
        await transaction('copilot_team_aggregation_log')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: pendingGeneration,
          })
          .update({ status: 'abandoned' });
        await transaction('copilot_user_teams_staging')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: pendingGeneration,
          })
          .del();
        await transaction('copilot_team_aggregates_staging')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: pendingGeneration,
          })
          .del();
        await transaction('copilot_team_membership_snapshot_staging')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: pendingGeneration,
          })
          .del();
        await transaction('copilot_team_membership_snapshot_state')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: pendingGeneration,
          })
          .del();
      }

      const nextGeneration = randomUUID();
      const nextState = {
        metrics_type: metricsType,
        entity_id: entityId,
        active_source: activeSource,
        active_generation: activeGeneration,
        pending_source: source,
        pending_generation: nextGeneration,
        updated_at: new Date(),
      };

      if (state) {
        await transaction('copilot_team_aggregation_state')
          .where({ metrics_type: metricsType, entity_id: entityId })
          .update(nextState);
      } else {
        await transaction('copilot_team_aggregation_state').insert(nextState);
      }

      return {
        activeSource,
        activeGeneration,
        pendingSource: source,
        pendingGeneration: nextGeneration,
      };
    });
  }

  async getUserMetricDays(
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<string[]> {
    const rows = await this.db('copilot_user_metrics')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .distinct('day')
      .orderBy('day', 'asc')
      .select('day');

    return rows
      .map(row => this.normalizeDay(row.day))
      .filter((day): day is string => Boolean(day));
  }

  async getMissingTeamAggregationDays(
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
  ): Promise<string[]> {
    const [metricDays, stagedRows] = await Promise.all([
      this.getUserMetricDays(metricsType, entityId),
      this.db<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
        })
        .whereIn('status', ['staged', 'complete', 'unavailable'])
        .select('day'),
    ]);
    const completedDays = new Set(
      stagedRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    return metricDays.filter(day => !completedDays.has(day));
  }

  async recordTeamAggregationFailure(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    errorMessage: string,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    await this.db.transaction(async transaction => {
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }

      await transaction<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
          status: 'failed',
          error_message: errorMessage,
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'failed',
          error_message: errorMessage,
          completed_at: null,
        });
    });
  }

  async markTeamAggregationUnavailable(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
    reason: string,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    return this.db.transaction(async transaction => {
      const state = await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        false,
      );
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }
      if (
        state.pending_source !== source ||
        state.pending_generation !== generation
      ) {
        throw new Error('Team aggregation transition is no longer active.');
      }

      await transaction('copilot_user_teams_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      await transaction('copilot_team_aggregates_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      await transaction<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
          status: 'unavailable',
          error_message: reason,
          completed_at: new Date(),
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'unavailable',
          error_message: reason,
          completed_at: new Date(),
        });

      return this.tryActivateTeamAggregationSource(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        requiredDays,
        membershipAttempt,
      );
    });
  }

  private async lockTeamAggregationState(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<TeamAggregationStateRow> {
    await this.ensureTeamAggregationStateRow(
      transaction,
      metricsType,
      entityId,
    );
    const state = (await transaction('copilot_team_aggregation_state')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .forUpdate()
      .first()) as TeamAggregationStateRow | undefined;
    if (!state) {
      throw new Error('Team aggregation state could not be initialized.');
    }
    return state;
  }

  private async assertTeamAggregationGeneration(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    allowActiveGeneration: boolean,
  ): Promise<TeamAggregationStateRow> {
    const state = await this.lockTeamAggregationState(
      transaction,
      metricsType,
      entityId,
    );
    const isPending =
      state.pending_source === source &&
      state.pending_generation === generation;
    const isActive =
      allowActiveGeneration &&
      state.active_source === source &&
      state.active_generation === generation &&
      !state.pending_source;

    if (!isPending && !isActive) {
      throw new Error('Team aggregation transition is no longer active.');
    }
    return state;
  }

  async beginTeamAggregationDayStaging(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    await this.db.transaction(async transaction => {
      await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        true,
      );
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }
      await transaction('copilot_user_teams_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      await transaction('copilot_team_aggregates_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
    });
  }

  async stageTeamAggregationBatch(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    userTeams: V2UserTeamRow[],
    aggregates: TeamAggregates,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    this.validateTeamReplacementRows(
      day,
      metricsType,
      entityId,
      userTeams,
      aggregates,
    );

    await this.db.transaction(async transaction => {
      await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        true,
      );
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }

      const stagedMemberships: TeamMembershipStagingRow[] = userTeams.map(
        row => ({ ...row, generation }),
      );
      await batchInsertInChunks(stagedMemberships, 100, async chunk => {
        await transaction<TeamMembershipStagingRow>(
          'copilot_user_teams_staging',
        )
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'generation',
            'user_id',
            'team_id',
          ])
          .ignore();
      });

      const aggregateRows: TeamAggregateStagingRow[] = [];
      for (const [tableName, rows] of teamAggregateRowsByTable(aggregates)) {
        for (const rowValue of rows) {
          const row = rowValue as Record<string, unknown>;
          const rowData = row;
          aggregateRows.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            generation,
            table_name: tableName,
            row_key: getTeamAggregateRowKey(tableName, rowData),
            row_data: JSON.stringify(rowData),
          });
        }
      }
      await batchInsertInChunks(aggregateRows, 100, async chunk => {
        await transaction<TeamAggregateStagingRow>(
          'copilot_team_aggregates_staging',
        ).insert(chunk);
      });
    });
  }

  private async compactTeamAggregateStagingRows(
    transaction: Knex.Transaction,
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    generation: string,
  ): Promise<void> {
    let cursor: { table_name: string; row_key: string; id: number } | undefined;
    let current:
      | {
          id: number;
          table_name: string;
          row_key: string;
          row_data: Record<string, unknown>;
        }
      | undefined;
    let duplicateIds: number[] = [];

    const flushCurrent = async () => {
      if (!current) {
        return;
      }
      await transaction('copilot_team_aggregates_staging')
        .where({ id: current.id })
        .update({
          row_key: current.row_key,
          row_data: JSON.stringify(current.row_data),
        });
      if (duplicateIds.length > 0) {
        for (let index = 0; index < duplicateIds.length; index += 100) {
          await transaction('copilot_team_aggregates_staging')
            .whereIn('id', duplicateIds.slice(index, index + 100))
            .del();
        }
        duplicateIds = [];
      }
    };

    for (;;) {
      const query = transaction<TeamAggregateStagingRow & { id: number }>(
        'copilot_team_aggregates_staging',
      ).where({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        generation,
      });
      const cursorForQuery = cursor;
      if (cursorForQuery) {
        const { table_name: cursorTableName, row_key: cursorRowKey } =
          cursorForQuery;
        const cursorId = cursorForQuery.id;
        query.where(function afterCursor() {
          this.where('table_name', '>', cursorTableName)
            .orWhere(function sameTable() {
              this.where('table_name', cursorTableName).andWhere(
                'row_key',
                '>',
                cursorRowKey,
              );
            })
            .orWhere(function sameGroup() {
              this.where('table_name', cursorTableName)
                .andWhere('row_key', cursorRowKey)
                .andWhere('id', '>', cursorId);
            });
        });
      }

      const rows = await query
        .orderBy('table_name', 'asc')
        .orderBy('row_key', 'asc')
        .orderBy('id', 'asc')
        .limit(teamAggregationCopyBatchSize)
        .select('id', 'table_name', 'row_key', 'row_data');
      if (rows.length === 0) {
        break;
      }

      for (const row of rows) {
        if (
          !teamAggregateTables.includes(
            row.table_name as (typeof teamAggregateTables)[number],
          )
        ) {
          throw new Error(
            `Unexpected team aggregate staging table "${row.table_name}".`,
          );
        }
        const rowData = JSON.parse(row.row_data) as Record<string, unknown>;
        const rowKey =
          row.row_key || getTeamAggregateRowKey(row.table_name, rowData);
        if (
          current &&
          current.table_name === row.table_name &&
          current.row_key === rowKey
        ) {
          current.row_data = mergeTeamAggregateRows(
            row.table_name,
            current.row_data,
            rowData,
          );
          duplicateIds.push(Number(row.id));
        } else {
          await flushCurrent();
          current = {
            id: Number(row.id),
            table_name: row.table_name,
            row_key: rowKey,
            row_data: rowData,
          };
        }
        cursor = {
          table_name: row.table_name,
          row_key: row.row_key,
          id: Number(row.id),
        };
      }
    }

    await flushCurrent();
  }

  async completeStagedTeamAggregationDay(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    membershipAttempt: TeamMembershipSnapshotAttempt,
    requiredDays: string[],
  ): Promise<boolean> {
    return this.db.transaction(async transaction => {
      const state = await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        false,
      );
      if (
        state.pending_source !== source ||
        state.pending_generation !== generation
      ) {
        throw new Error('Team aggregation transition is no longer active.');
      }
      await this.assertRequiredTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        membershipAttempt,
      );

      await this.compactTeamAggregateStagingRows(
        transaction,
        day,
        metricsType,
        entityId,
        generation,
      );
      await transaction<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
          status: 'staged',
          error_message: null,
          completed_at: new Date(),
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'staged',
          error_message: null,
          completed_at: new Date(),
        });

      return this.tryActivateTeamAggregationSource(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        requiredDays,
        membershipAttempt,
      );
    });
  }

  async replaceStagedTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    membershipAttempt: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    await this.db.transaction(async transaction => {
      const state = await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        true,
      );
      if (
        state.active_source !== source ||
        state.active_generation !== generation ||
        state.pending_source
      ) {
        throw new Error(
          'Cannot replace team aggregates while a different source is active or transitioning.',
        );
      }
      await this.assertRequiredTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        membershipAttempt,
      );

      await this.compactTeamAggregateStagingRows(
        transaction,
        day,
        metricsType,
        entityId,
        generation,
      );
      await transaction('copilot_user_teams')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();

      let lastMembershipId = 0;
      for (;;) {
        const stagedMemberships = await transaction<
          TeamMembershipStagingRow & { id: number }
        >('copilot_user_teams_staging')
          .where({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            generation,
          })
          .where('id', '>', lastMembershipId)
          .orderBy('id', 'asc')
          .limit(teamAggregationCopyBatchSize)
          .select(
            'id',
            'day',
            'metrics_type',
            'entity_id',
            'user_id',
            'user_login',
            'team_id',
            'team_slug',
          );
        if (stagedMemberships.length === 0) {
          break;
        }

        const memberships: V2UserTeamRow[] = stagedMemberships.map(row => ({
          day: this.normalizeRequiredDay(row.day),
          metrics_type: row.metrics_type,
          entity_id: row.entity_id,
          user_id: Number(row.user_id),
          user_login: row.user_login,
          team_id: Number(row.team_id),
          team_slug: row.team_slug,
        }));
        await batchInsertInChunks(memberships, 100, async chunk => {
          await transaction<V2UserTeamRow>('copilot_user_teams').insert(chunk);
        });
        lastMembershipId = Number(
          stagedMemberships[stagedMemberships.length - 1].id,
        );
      }

      await this.deleteTeamAggregateRows(
        transaction,
        day,
        metricsType,
        entityId,
      );

      let lastAggregateId = 0;
      for (;;) {
        const aggregateRows = await transaction<
          TeamAggregateStagingRow & { id: number }
        >('copilot_team_aggregates_staging')
          .where({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            generation,
          })
          .where('id', '>', lastAggregateId)
          .orderBy('id', 'asc')
          .limit(teamAggregationCopyBatchSize)
          .select('id', 'table_name', 'row_data');
        if (aggregateRows.length === 0) {
          break;
        }

        const rowsByTable = new Map<string, object[]>();
        for (const row of aggregateRows) {
          if (
            !teamAggregateTables.includes(
              row.table_name as (typeof teamAggregateTables)[number],
            )
          ) {
            throw new Error(
              `Unexpected team aggregate staging table "${row.table_name}".`,
            );
          }
          const tableRows = rowsByTable.get(row.table_name) ?? [];
          tableRows.push(JSON.parse(row.row_data) as object);
          rowsByTable.set(row.table_name, tableRows);
        }
        for (const [tableName, tableRows] of rowsByTable) {
          await batchInsertInChunks(tableRows, 100, async chunk => {
            await transaction(tableName).insert(chunk);
          });
        }
        lastAggregateId = Number(aggregateRows[aggregateRows.length - 1].id);
      }

      await transaction<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
          status: 'complete',
          error_message: null,
          completed_at: new Date(),
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'complete',
          error_message: null,
          completed_at: new Date(),
        });
      await transaction('copilot_user_teams_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      await transaction('copilot_team_aggregates_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
    });
  }

  private validateTeamReplacementRows(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userTeams: V2UserTeamRow[],
    aggregates: TeamAggregates,
  ): void {
    if (
      userTeams.some(
        row =>
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId,
      )
    ) {
      throw new Error(
        'Team replacement memberships must match the requested day and scope.',
      );
    }

    for (const [, rows] of teamAggregateRowsByTable(aggregates)) {
      for (const rowValue of rows) {
        const row = rowValue as {
          day?: string;
          metrics_type?: MetricsScope;
          entity_id?: string;
          team_slug?: string;
        };
        if (
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId ||
          !row.team_slug
        ) {
          throw new Error(
            'Team replacement aggregates must match the requested day and scope.',
          );
        }
      }
    }
  }

  private async insertTeamAggregateRows(
    transaction: Knex.Transaction,
    aggregates: TeamAggregates,
  ): Promise<void> {
    for (const [tableName, rows] of teamAggregateRowsByTable(aggregates)) {
      if (!rows.length) {
        continue;
      }
      await batchInsertInChunks(rows, 100, async chunk => {
        await transaction(tableName).insert(chunk);
      });
    }
  }

  private async deleteTeamAggregateRows(
    transaction: Knex.Transaction,
    day: string | undefined,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    for (const tableName of teamAggregateTables) {
      const query = transaction(tableName).where({
        metrics_type: metricsType,
        entity_id: entityId,
      });
      if (day) {
        query.where({ day });
      }
      query.whereNot('team_slug', '');
      if (tableName === 'copilot_customization_metrics') {
        query.where('scope', 'team');
      }
      await query.del();
    }
  }

  private async tryActivateTeamAggregationSource(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    const state = await this.lockTeamAggregationState(
      transaction,
      metricsType,
      entityId,
    );
    if (source === 'organization-fallback') {
      await this.assertRequiredTeamMembershipSnapshot(
        transaction,
        metricsType,
        entityId,
        membershipAttempt,
      );
    }
    if (
      state.pending_source !== source ||
      state.pending_generation !== generation
    ) {
      throw new Error('Team aggregation transition is no longer active.');
    }

    const metricRows = await transaction('copilot_user_metrics')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .distinct('day')
      .select('day');
    const currentDays = metricRows
      .map(row => this.normalizeDay(row.day))
      .filter((day): day is string => Boolean(day));
    const allRequiredDays = [...new Set([...requiredDays, ...currentDays])];
    const stagedRows = await transaction<TeamAggregationLogRow>(
      'copilot_team_aggregation_log',
    )
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        source,
        generation,
      })
      .whereIn('status', ['staged', 'unavailable'])
      .select('day');
    const stagedDays = new Set(
      stagedRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    if (allRequiredDays.some(day => !stagedDays.has(day))) {
      return false;
    }

    await transaction('copilot_user_teams')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .del();

    let lastMembershipId = 0;
    for (;;) {
      const stagedMemberships = await transaction<
        TeamMembershipStagingRow & { id: number }
      >('copilot_user_teams_staging')
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .where('id', '>', lastMembershipId)
        .orderBy('id', 'asc')
        .limit(teamAggregationCopyBatchSize)
        .select(
          'id',
          'day',
          'metrics_type',
          'entity_id',
          'user_id',
          'user_login',
          'team_id',
          'team_slug',
        );
      if (stagedMemberships.length === 0) break;

      const memberships: V2UserTeamRow[] = stagedMemberships.map(row => ({
        day: this.normalizeRequiredDay(row.day),
        metrics_type: row.metrics_type,
        entity_id: row.entity_id,
        user_id: Number(row.user_id),
        user_login: row.user_login,
        team_id: Number(row.team_id),
        team_slug: row.team_slug,
      }));
      await batchInsertInChunks(memberships, 100, async chunk => {
        await transaction<V2UserTeamRow>('copilot_user_teams').insert(chunk);
      });
      lastMembershipId = Number(
        stagedMemberships[stagedMemberships.length - 1].id,
      );
    }

    await this.deleteTeamAggregateRows(
      transaction,
      undefined,
      metricsType,
      entityId,
    );

    let lastAggregateId = 0;
    for (;;) {
      const aggregateRows = await transaction<
        TeamAggregateStagingRow & { id: number }
      >('copilot_team_aggregates_staging')
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .where('id', '>', lastAggregateId)
        .orderBy('id', 'asc')
        .limit(teamAggregationCopyBatchSize)
        .select('id', 'table_name', 'row_data');
      if (aggregateRows.length === 0) break;

      const rowsByTable = new Map<string, object[]>();
      for (const row of aggregateRows) {
        if (
          !teamAggregateTables.includes(
            row.table_name as (typeof teamAggregateTables)[number],
          )
        ) {
          throw new Error(
            `Unexpected team aggregate staging table "${row.table_name}".`,
          );
        }
        const rows = rowsByTable.get(row.table_name) ?? [];
        rows.push(JSON.parse(row.row_data) as object);
        rowsByTable.set(row.table_name, rows);
      }
      for (const [tableName, rows] of rowsByTable) {
        await batchInsertInChunks(rows, 100, async chunk => {
          await transaction(tableName).insert(chunk);
        });
      }
      lastAggregateId = Number(aggregateRows[aggregateRows.length - 1].id);
    }

    await transaction('copilot_team_aggregation_log')
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        source,
        generation,
        status: 'staged',
      })
      .update({
        status: 'complete',
        completed_at: new Date(),
        error_message: null,
      });
    await transaction('copilot_team_aggregation_state')
      .where({ metrics_type: metricsType, entity_id: entityId })
      .update({
        active_source: source,
        active_generation: generation,
        pending_source: null,
        pending_generation: null,
        updated_at: new Date(),
      });
    await transaction('copilot_user_teams_staging')
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        generation,
      })
      .del();
    await transaction('copilot_team_aggregates_staging')
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        generation,
      })
      .del();
    if (source === 'native') {
      await transaction('copilot_team_membership_snapshot_staging')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .del();
      await transaction('copilot_team_membership_snapshot_state')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .del();
    }

    return true;
  }

  async stageTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
    userTeams: V2UserTeamRow[],
    aggregates: TeamAggregates,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    this.validateTeamReplacementRows(
      day,
      metricsType,
      entityId,
      userTeams,
      aggregates,
    );

    return this.db.transaction(async transaction => {
      const state = await this.assertTeamAggregationGeneration(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        false,
      );
      if (
        state.pending_source !== source ||
        state.pending_generation !== generation
      ) {
        throw new Error('Team aggregation transition is no longer active.');
      }
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }

      await transaction('copilot_user_teams_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      const stagedMemberships: TeamMembershipStagingRow[] = userTeams.map(
        row => ({ ...row, generation }),
      );
      await batchInsertInChunks(stagedMemberships, 100, async chunk => {
        await transaction<TeamMembershipStagingRow>(
          'copilot_user_teams_staging',
        ).insert(chunk);
      });

      await transaction('copilot_team_aggregates_staging')
        .where({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          generation,
        })
        .del();
      const aggregateRows: TeamAggregateStagingRow[] = [];
      for (const [tableName, rows] of teamAggregateRowsByTable(aggregates)) {
        for (const rowValue of rows) {
          const row = rowValue as Record<string, unknown>;
          const rowData = row;
          aggregateRows.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            generation,
            table_name: tableName,
            row_key: getTeamAggregateRowKey(tableName, rowData),
            row_data: JSON.stringify(rowData),
          });
        }
      }
      await batchInsertInChunks(aggregateRows, 100, async chunk => {
        await transaction<TeamAggregateStagingRow>(
          'copilot_team_aggregates_staging',
        ).insert(chunk);
      });

      await transaction<TeamAggregationLogRow>('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation,
          status: 'staged',
          error_message: null,
          completed_at: new Date(),
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'staged',
          error_message: null,
          completed_at: new Date(),
        });

      return this.tryActivateTeamAggregationSource(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        requiredDays,
        membershipAttempt,
      );
    });
  }

  async activateTeamAggregationSource(
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    return this.db.transaction(transaction =>
      this.tryActivateTeamAggregationSource(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        requiredDays,
        membershipAttempt,
      ),
    );
  }

  async replaceTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    userTeams: V2UserTeamRow[],
    aggregates: TeamAggregates,
    membershipAttempt?: TeamMembershipSnapshotAttempt,
  ): Promise<void> {
    this.validateTeamReplacementRows(
      day,
      metricsType,
      entityId,
      userTeams,
      aggregates,
    );

    await this.db.transaction(async transaction => {
      const state = await this.lockTeamAggregationState(
        transaction,
        metricsType,
        entityId,
      );
      if (state.active_source !== source || state.pending_source) {
        throw new Error(
          'Cannot replace team aggregates while a different source is active or transitioning.',
        );
      }
      if (source === 'organization-fallback') {
        await this.assertRequiredTeamMembershipSnapshot(
          transaction,
          metricsType,
          entityId,
          membershipAttempt,
        );
      }

      await transaction('copilot_user_teams')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();
      const memberships: V2UserTeamRow[] = userTeams.map(row => ({
        ...row,
      }));
      await batchInsertInChunks(memberships, 100, async chunk => {
        await transaction<V2UserTeamRow>('copilot_user_teams').insert(chunk);
      });

      await this.deleteTeamAggregateRows(
        transaction,
        day,
        metricsType,
        entityId,
      );
      await this.insertTeamAggregateRows(transaction, aggregates);

      await transaction('copilot_team_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          source,
          generation: state.active_generation,
          status: 'complete',
          error_message: null,
          completed_at: new Date(),
        })
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'generation',
        ])
        .merge({
          status: 'complete',
          error_message: null,
          completed_at: new Date(),
        });
    });
  }

  async getBackfillStatus(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
  ): Promise<V2IngestionLogRow[]> {
    const rows = await this.getIngestionLog(metricsType, entityId, from, to);
    if (rows.length === 0) {
      return rows;
    }

    const { activeSource, activeGeneration } =
      await this.getTeamAggregationState(metricsType, entityId);
    const unavailableRows = await this.db<TeamAggregationLogRow>(
      'copilot_team_aggregation_log',
    )
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        source: activeSource,
        generation: activeGeneration,
        status: 'unavailable',
      })
      .whereIn(
        'day',
        rows.map(row => row.day),
      )
      .select('day');
    const unavailableDays = new Set(
      unavailableRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    return rows.map(row => {
      if (!unavailableDays.has(row.day)) {
        return row;
      }

      const componentsLoaded = this.parseComponentsLoaded(
        row.components_loaded,
      );
      return {
        ...row,
        components_loaded: JSON.stringify(
          componentsLoaded.filter(component => component !== 'teams'),
        ),
      };
    });
  }

  async insertDailyTotals(rows: V2DailyTotal[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2DailyTotal>('copilot_daily_totals')
        .insert(chunk)
        // Merge `total_ai_credits_used`/`daily_active_vscode_agent_users` on
        // conflict (rather than ignoring) so that re-ingesting a day that
        // was already stored before these columns existed backfills the new
        // metrics in place.
        .onConflict(['day', 'metrics_type', 'entity_id', 'team_slug'])
        .merge(['total_ai_credits_used', 'daily_active_vscode_agent_users']);
    });
  }

  async insertPrMetrics(rows: V2PrMetricsRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2PrMetricsRow>('copilot_pr_metrics')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'team_slug'])
        .ignore();
    });
  }

  async insertByFeature(rows: V2MetricsByFeatureRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'team_slug',
          'feature',
        ])
        .ignore();
    });
  }

  async insertByIde(rows: V2MetricsByIdeRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'team_slug', 'ide'])
        .ignore();
    });
  }

  async insertByLanguageFeature(
    rows: V2MetricsByLanguageFeatureRow[],
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByLanguageFeatureRow>(
        'copilot_metrics_by_language_feature',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'team_slug',
          'language',
          'feature',
        ])
        .ignore();
    });
  }

  async insertByModelFeature(
    rows: V2MetricsByModelFeatureRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByModelFeatureRow>(
        'copilot_metrics_by_model_feature',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'team_slug',
          'model_id',
          'feature',
        ])
        .ignore();
    });
  }

  async insertByLanguageModel(
    rows: V2MetricsByLanguageModelRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByLanguageModelRow>(
        'copilot_metrics_by_language_model',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'team_slug',
          'language',
          'model_id',
        ])
        .ignore();
    });
  }

  async insertByCli(rows: V2MetricsByCliRow[]): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByCliRow>('copilot_metrics_by_cli')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'team_slug'])
        // Merge (rather than ignore) on conflict so that re-ingesting a day
        // updates any stale/zeroed row already stored for that key — e.g.
        // rows written before the `output_tokens_sum`/`prompt_tokens_sum`
        // columns were widened to `bigint` (see
        // 202608240000_widen_cli_token_sum_columns.js), which would
        // otherwise be silently skipped forever by `.ignore()`.
        .merge([
          'prompt_count',
          'request_count',
          'session_count',
          'avg_tokens_per_request',
          'output_tokens_sum',
          'prompt_tokens_sum',
        ]);
    });
  }

  async insertByVscodeAgent(rows: V2MetricsByVscodeAgentRow[]): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2MetricsByVscodeAgentRow>(
        'copilot_metrics_by_vscode_agent',
      )
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'team_slug'])
        // Merge (rather than ignore) on conflict so that re-ingesting a day
        // updates any stale/zeroed row already stored for that key, mirroring
        // insertByCli above.
        .merge(['session_count', 'total_user_messages']);
    });
  }

  async insertCustomizationMetrics(
    rows: V2CustomizationMetricRow[],
  ): Promise<void> {
    if (!rows.length) return;

    const normalizedRows = rows.map(row => ({
      ...row,
      user_login: row.user_login.toLowerCase(),
    }));

    await batchInsertInChunks(normalizedRows, 100, async chunk => {
      await this.db<V2CustomizationMetricRow>('copilot_customization_metrics')
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'scope',
          'team_slug',
          'user_id',
          'category',
          'item_name',
        ])
        .merge(['interaction_count', 'user_login']);
    });
  }

  async insertCustomizationSummaries(
    rows: V2CustomizationSummaryRow[],
  ): Promise<void> {
    if (!rows.length) return;

    const normalizedRows = rows.map(row => ({
      ...row,
      user_login: row.user_login.toLowerCase(),
    }));

    await batchInsertInChunks(normalizedRows, 100, async chunk => {
      await this.db<V2CustomizationSummaryRow>(
        'copilot_customization_summaries',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'scope',
          'team_slug',
          'user_id',
          'category',
        ])
        .merge(['distinct_item_count', 'user_login']);
    });
  }

  async insertUserMetrics(rows: V2UserMetricRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricRow>('copilot_user_metrics')
        .insert(chunk)
        // Merge `ai_credits_used`/`used_vscode_agent` on conflict (rather
        // than ignoring) so that re-ingesting a day that was already stored
        // before these columns existed backfills the new metrics in place.
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id'])
        .merge(['ai_credits_used', 'used_vscode_agent']);
    });
  }

  async insertUserTeams(rows: V2UserTeamRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    const scopes = new Map<
      string,
      { metricsType: MetricsScope; entityId: string }
    >();
    for (const row of rows) {
      scopes.set(JSON.stringify([row.metrics_type, row.entity_id]), {
        metricsType: row.metrics_type,
        entityId: row.entity_id,
      });
    }

    await this.db.transaction(async transaction => {
      for (const { metricsType, entityId } of scopes.values()) {
        const state = await this.lockTeamAggregationState(
          transaction,
          metricsType,
          entityId,
        );
        if (state.active_source !== 'native' || state.pending_source) {
          throw new Error(
            'Cannot insert native team memberships while a different source is active or transitioning.',
          );
        }
      }

      await batchInsertInChunks(rows, 100, async chunk => {
        await transaction<V2UserTeamRow>('copilot_user_teams')
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'user_id',
            'team_id',
          ])
          .ignore();
      });
    });
  }

  async insertUserMetricsByFeature(
    rows: V2UserMetricsByFeatureRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricsByFeatureRow>(
        'copilot_user_metrics_by_feature',
      )
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id', 'feature'])
        .ignore();
    });
  }

  async insertUserMetricsByIde(rows: V2UserMetricsByIdeRow[]): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricsByIdeRow>('copilot_user_metrics_by_ide')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id', 'ide'])
        .ignore();
    });
  }

  async insertUserMetricsByLanguageFeature(
    rows: V2UserMetricsByLanguageFeatureRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricsByLanguageFeatureRow>(
        'copilot_user_metrics_by_language_feature',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'user_id',
          'language',
          'feature',
        ])
        .ignore();
    });
  }

  async insertUserMetricsByModelFeature(
    rows: V2UserMetricsByModelFeatureRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricsByModelFeatureRow>(
        'copilot_user_metrics_by_model_feature',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'user_id',
          'model_id',
          'feature',
        ])
        .ignore();
    });
  }

  async insertUserMetricsByLanguageModel(
    rows: V2UserMetricsByLanguageModelRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserMetricsByLanguageModelRow>(
        'copilot_user_metrics_by_language_model',
      )
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'user_id',
          'language',
          'model_id',
        ])
        .ignore();
    });
  }

  async upsertIngestionLog(row: V2IngestionLogRow): Promise<void> {
    await this.db.transaction(async transaction => {
      const scope = {
        day: row.day,
        metrics_type: row.metrics_type,
        entity_id: row.entity_id,
      };
      await transaction<V2IngestionLogRow>('copilot_ingestion_log')
        .insert(row)
        .onConflict(['day', 'metrics_type', 'entity_id'])
        .ignore();

      const existing = await transaction<V2IngestionLogRow>(
        'copilot_ingestion_log',
      )
        .where(scope)
        .forUpdate()
        .first();
      if (!existing) {
        throw new Error('Ingestion log row could not be initialized.');
      }

      const componentsLoaded = [
        ...new Set([
          ...this.parseComponentsLoaded(existing.components_loaded),
          ...this.parseComponentsLoaded(row.components_loaded),
        ]),
      ];
      const preserveSuccessfulStatus =
        existing.status === 'success' && row.status !== 'success';
      let errorMessage = row.error_message;
      if (errorMessage === undefined) {
        errorMessage = preserveSuccessfulStatus ? existing.error_message : null;
      }

      await transaction('copilot_ingestion_log')
        .where(scope)
        .update({
          status: preserveSuccessfulStatus ? 'success' : row.status,
          components_loaded: JSON.stringify(componentsLoaded),
          error_message: errorMessage,
          source: row.source,
        });
    });
  }

  async getMissingDays(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    requiredComponents: string[] = ['totals'],
  ): Promise<string[]> {
    const ingestionRows = await this.db('copilot_ingestion_log')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to])
      .select('day', 'components_loaded');

    const completeDays = new Set(
      ingestionRows
        .filter(row =>
          this.hasRequiredComponents(row.components_loaded, requiredComponents),
        )
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    const start = DateTime.fromISO(from, { zone: 'utc' }).startOf('day');
    const end = DateTime.fromISO(to, { zone: 'utc' }).startOf('day');

    if (!start.isValid || !end.isValid || start > end) {
      return [];
    }

    const missingDays: string[] = [];
    for (let cursor = start; cursor <= end; cursor = cursor.plus({ days: 1 })) {
      const day = cursor.toISODate();
      if (day && !completeDays.has(day)) {
        missingDays.push(day);
      }
    }

    return missingDays;
  }

  async getLastIngestedDay(
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<string | null> {
    const row = await this.db('copilot_ingestion_log')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('status', 'success')
      .orderBy('day', 'desc')
      .first('day');

    return this.normalizeDay(row?.day);
  }

  async getIngestionLog(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
  ): Promise<V2IngestionLogRow[]> {
    const query = this.db<V2IngestionLogRow>('copilot_ingestion_log')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId);

    if (from && to) {
      query.whereBetween('day', [from, to]);
    } else if (from) {
      query.where('day', '>=', from);
    } else if (to) {
      query.where('day', '<=', to);
    }

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getDailyTotals(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2DailyTotal[]> {
    const query = this.db<V2DailyTotal>('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');

    const rows = (await query.orderBy('day', 'asc').select('*')).map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));

    // Org/enterprise-level rows already carry rolling weekly/monthly active
    // user counts straight from GitHub's report. Team-level rows are derived
    // from daily per-user data and have no rolling windows, so we compute them
    // here from the persisted per-user activity tables.
    if (teamSlug) {
      return this.enrichTeamRollingActiveUsers(
        rows,
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
      );
    }

    return rows;
  }

  /**
   * Enrich team-level daily totals with rolling weekly/monthly active-user
   * counts derived from the persisted per-user activity tables
   * (copilot_user_metrics joined with copilot_user_teams).
   *
   * Definitions follow GitHub's standard rolling windows:
   * - weekly_active_users: distinct active users over the trailing 7 days
   * - monthly_active_users: distinct active users over the trailing 28 days
   * - monthly_active_agent_users / monthly_active_chat_users: distinct users
   *   that used agent/chat over the trailing 28 days
   *
   * A user is "active" for the team on a given day when they have a per-user
   * metrics row that day and a team-membership row for the same day.
   */
  private async enrichTeamRollingActiveUsers(
    rows: V2DailyTotal[],
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug: string,
  ): Promise<V2DailyTotal[]> {
    if (rows.length === 0) {
      return rows;
    }

    const WEEKLY_WINDOW_DAYS = 7;
    const MONTHLY_WINDOW_DAYS = 28;
    const activityRows = await this.loadTeamActivityRows(
      metricsType,
      entityId,
      from,
      to,
      teamSlug,
      MONTHLY_WINDOW_DAYS,
    );

    const { activeByDay, agentByDay, chatByDay } =
      this.collectRollingUsersByDay(activityRows as TeamActivityRow[]);

    return rows.map(row =>
      this.enrichDailyTotalWithRollingUsers(row, {
        activeByDay,
        agentByDay,
        chatByDay,
        weeklyWindowDays: WEEKLY_WINDOW_DAYS,
        monthlyWindowDays: MONTHLY_WINDOW_DAYS,
      }),
    );
  }

  private async loadTeamActivityRows(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug: string,
    monthlyWindowDays: number,
  ) {
    // Each emitted row's rolling value is anchored at *that row's own day* and
    // looks backwards. `fetchLowerBound` only widens the SQL fetch so the
    // earliest days in [from, to] still have a full trailing window available.
    const fetchLowerBound = DateTime.fromISO(from)
      .minus({ days: monthlyWindowDays - 1 })
      .toFormat('yyyy-MM-dd');

    return this.db('copilot_user_metrics as m')
      .join('copilot_user_teams as t', function joinOn() {
        this.on('m.day', '=', 't.day')
          .andOn('m.user_id', '=', 't.user_id')
          .andOn('m.metrics_type', '=', 't.metrics_type')
          .andOn('m.entity_id', '=', 't.entity_id');
      })
      .where('m.metrics_type', metricsType)
      .where('m.entity_id', entityId)
      .where('t.team_slug', teamSlug)
      .whereBetween('m.day', [fetchLowerBound, to])
      .select(
        'm.day as day',
        'm.user_id as user_id',
        'm.used_agent as used_agent',
        'm.used_chat as used_chat',
      );
  }

  private enrichDailyTotalWithRollingUsers(
    row: V2DailyTotal,
    params: {
      activeByDay: Map<string, Set<number>>;
      agentByDay: Map<string, Set<number>>;
      chatByDay: Map<string, Set<number>>;
      weeklyWindowDays: number;
      monthlyWindowDays: number;
    },
  ): V2DailyTotal {
    const day = this.normalizeDay(row.day);
    if (!day) {
      return row;
    }

    return {
      ...row,
      weekly_active_users: this.countDistinctUsersOverWindow(
        params.activeByDay,
        day,
        params.weeklyWindowDays,
      ),
      monthly_active_users: this.countDistinctUsersOverWindow(
        params.activeByDay,
        day,
        params.monthlyWindowDays,
      ),
      monthly_active_agent_users: this.countDistinctUsersOverWindow(
        params.agentByDay,
        day,
        params.monthlyWindowDays,
      ),
      monthly_active_chat_users: this.countDistinctUsersOverWindow(
        params.chatByDay,
        day,
        params.monthlyWindowDays,
      ),
    };
  }

  private collectRollingUsersByDay(activityRows: TeamActivityRow[]): {
    activeByDay: Map<string, Set<number>>;
    agentByDay: Map<string, Set<number>>;
    chatByDay: Map<string, Set<number>>;
  } {
    const activeByDay = new Map<string, Set<number>>();
    const agentByDay = new Map<string, Set<number>>();
    const chatByDay = new Map<string, Set<number>>();

    for (const row of activityRows) {
      const day = this.normalizeDay(row.day);
      if (!day) {
        continue;
      }

      const userId = Number(row.user_id);
      this.addUserToDayMap(activeByDay, day, userId);

      if (row.used_agent) {
        this.addUserToDayMap(agentByDay, day, userId);
      }

      if (row.used_chat) {
        this.addUserToDayMap(chatByDay, day, userId);
      }
    }

    return { activeByDay, agentByDay, chatByDay };
  }

  private countDistinctUsersOverWindow(
    byDay: Map<string, Set<number>>,
    endDay: string,
    windowDays: number,
  ): number {
    const end = DateTime.fromISO(endDay, { zone: 'utc' });
    if (!end.isValid) {
      return 0;
    }

    const users = new Set<number>();
    for (let dayOffset = 0; dayOffset < windowDays; dayOffset++) {
      const dayKey = end.minus({ days: dayOffset }).toFormat('yyyy-MM-dd');
      const dayUsers = byDay.get(dayKey);
      if (!dayUsers) {
        continue;
      }

      for (const userId of dayUsers) {
        users.add(userId);
      }
    }

    return users.size;
  }

  private addUserToDayMap(
    byDay: Map<string, Set<number>>,
    day: string,
    userId: number,
  ): void {
    const users = byDay.get(day) ?? new Set<number>();
    users.add(userId);
    byDay.set(day, users);
  }

  async getPrMetrics(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2PrMetricsRow[]> {
    const query = this.db<V2PrMetricsRow>('copilot_pr_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getByFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2MetricsByFeatureRow[]> {
    const query = this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getByIde(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2MetricsByIdeRow[]> {
    const query = this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getByLanguageFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    feature?: string,
  ): Promise<V2MetricsByLanguageFeatureRow[]> {
    const query = this.db<V2MetricsByLanguageFeatureRow>(
      'copilot_metrics_by_language_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');

    if (feature !== undefined) {
      query.where('feature', feature);
    }

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getByModelFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2MetricsByModelFeatureRow[]> {
    const query = this.db<V2MetricsByModelFeatureRow>(
      'copilot_metrics_by_model_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getByLanguageModel(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2MetricsByLanguageModelRow[]> {
    const query = this.db<V2MetricsByLanguageModelRow>(
      'copilot_metrics_by_language_model',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getCustomizationMetrics(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2CustomizationMetricRow[]> {
    const query = this.db<V2CustomizationMetricRow>(
      'copilot_customization_metrics',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to])
      .where('scope', teamSlug ? 'team' : 'aggregate')
      .where('team_slug', teamSlug ?? '')
      .orderBy('day', 'asc');

    const rows = await query.select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getCustomizationSummaries(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2CustomizationSummaryRow[]> {
    if (teamSlug) {
      return [];
    }

    const rows = await this.db<V2CustomizationSummaryRow>(
      'copilot_customization_summaries',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to])
      .where('scope', 'aggregate')
      .where('team_slug', '')
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getDashboardData(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
  ): Promise<V2DashboardData> {
    const [
      daily,
      byFeature,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      prMetrics,
      customizationMetrics,
      customizationSummaries,
    ] = await Promise.all([
      this.getDailyTotals(metricsType, entityId, from, to, teamSlug),
      this.getByFeature(metricsType, entityId, from, to, teamSlug),
      this.getByLanguageFeature(metricsType, entityId, from, to, teamSlug),
      this.getByModelFeature(metricsType, entityId, from, to, teamSlug),
      this.getByLanguageModel(metricsType, entityId, from, to, teamSlug),
      this.getPrMetrics(metricsType, entityId, from, to, teamSlug),
      this.getCustomizationMetrics(metricsType, entityId, from, to, teamSlug),
      this.getCustomizationSummaries(metricsType, entityId, from, to, teamSlug),
    ]);

    return {
      daily,
      byFeature,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      prMetrics,
      customizationMetrics,
      customizationSummaries,
    };
  }

  /**
   * Fetch daily per-user metric rows for a single, specific user.
   *
   * `userLogin` is a required parameter (not optional) by design: there is
   * intentionally no variant of this method that returns rows for more than
   * one user, since this data backs privacy-scoped "my metrics" APIs that
   * must never be able to return another user's data.
   */
  async getUserMetrics(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricRow[]> {
    const rows = await this.db<V2UserMetricRow>('copilot_user_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getUserMetricsByFeature(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricsByFeatureRow[]> {
    const rows = await this.db<V2UserMetricsByFeatureRow>(
      'copilot_user_metrics_by_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getUserMetricsByIde(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricsByIdeRow[]> {
    const rows = await this.db<V2UserMetricsByIdeRow>(
      'copilot_user_metrics_by_ide',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getUserMetricsByLanguageFeature(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricsByLanguageFeatureRow[]> {
    const rows = await this.db<V2UserMetricsByLanguageFeatureRow>(
      'copilot_user_metrics_by_language_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getUserMetricsByModelFeature(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricsByModelFeatureRow[]> {
    const rows = await this.db<V2UserMetricsByModelFeatureRow>(
      'copilot_user_metrics_by_model_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  async getUserMetricsByLanguageModel(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserMetricsByLanguageModelRow[]> {
    const rows = await this.db<V2UserMetricsByLanguageModelRow>(
      'copilot_user_metrics_by_language_model',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  private async getUserCustomizationMetrics(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2CustomizationMetricRow[]> {
    const rows = await this.db<V2CustomizationMetricRow>(
      'copilot_customization_metrics',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('scope', 'user')
      .where('user_login', userLogin.toLowerCase())
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  private async getUserCustomizationSummaries(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2CustomizationSummaryRow[]> {
    const rows = await this.db<V2CustomizationSummaryRow>(
      'copilot_customization_summaries',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('scope', 'user')
      .where('user_login', userLogin.toLowerCase())
      .whereBetween('day', [from, to])
      .orderBy('day', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Fetch the full set of chart data needed to render an individual user's
   * "my metrics" dashboard, scoped strictly to `userLogin`. Backs the
   * `/v2/me/dashboard` endpoint, whose route handler resolves `userLogin`
   * itself from the caller's own Backstage identity — this method has no
   * knowledge of, and cannot be called with, an unauthenticated or
   * arbitrary user identifier from client input.
   */
  async getUserDashboardData(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
    from: string,
    to: string,
  ): Promise<V2UserDashboardData> {
    const [
      daily,
      byFeature,
      byIde,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      customizationMetrics,
      customizationSummaries,
    ] = await Promise.all([
      this.getUserMetrics(metricsType, entityId, userLogin, from, to),
      this.getUserMetricsByFeature(metricsType, entityId, userLogin, from, to),
      this.getUserMetricsByIde(metricsType, entityId, userLogin, from, to),
      this.getUserMetricsByLanguageFeature(
        metricsType,
        entityId,
        userLogin,
        from,
        to,
      ),
      this.getUserMetricsByModelFeature(
        metricsType,
        entityId,
        userLogin,
        from,
        to,
      ),
      this.getUserMetricsByLanguageModel(
        metricsType,
        entityId,
        userLogin,
        from,
        to,
      ),
      this.getUserCustomizationMetrics(
        metricsType,
        entityId,
        userLogin,
        from,
        to,
      ),
      this.getUserCustomizationSummaries(
        metricsType,
        entityId,
        userLogin,
        from,
        to,
      ),
    ]);

    return {
      userLogin,
      daily,
      byFeature,
      byIde,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      customizationMetrics,
      customizationSummaries,
    };
  }

  async getUserMetricsForDay(
    metricsType: MetricsScope,
    entityId: string,
    day: string,
  ): Promise<V2UserMetricRow[]> {
    const rows = await this.db<V2UserMetricRow>('copilot_user_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('day', day)
      .orderBy('user_id', 'asc')
      .select('*');

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
      user_id: Number(row.user_id),
    }));
  }

  async getUserBreakdownsForDay(
    metricsType: MetricsScope,
    entityId: string,
    day: string,
  ): Promise<UserBreakdownData[]> {
    const getRows = (tableName: string) =>
      this.db(tableName)
        .where('metrics_type', metricsType)
        .where('entity_id', entityId)
        .where('day', day)
        .orderBy('user_id', 'asc')
        .select('*');

    const [
      featureRows,
      ideRows,
      languageFeatureRows,
      modelFeatureRows,
      languageModelRows,
      customizationRows,
    ] = await Promise.all([
      getRows('copilot_user_metrics_by_feature'),
      getRows('copilot_user_metrics_by_ide'),
      getRows('copilot_user_metrics_by_language_feature'),
      getRows('copilot_user_metrics_by_model_feature'),
      getRows('copilot_user_metrics_by_language_model'),
      this.db<V2CustomizationMetricRow>('copilot_customization_metrics')
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          day,
          scope: 'user',
        })
        .orderBy('user_id', 'asc')
        .orderBy('category', 'asc')
        .orderBy('item_name', 'asc')
        .select('*'),
    ]);

    const breakdowns = new Map<number, UserBreakdownData>();
    const getBreakdown = (
      userIdValue: unknown,
      userLogin: string,
    ): UserBreakdownData => {
      const userId = Number(userIdValue);
      const existing = breakdowns.get(userId);
      if (existing) {
        return existing;
      }

      const breakdown: UserBreakdownData = {
        user_id: userId,
        user_login: userLogin,
        byFeature: [],
        byIde: [],
        byLanguageFeature: [],
        byModelFeature: [],
        byLanguageModel: [],
        customizations: [],
      };
      breakdowns.set(userId, breakdown);
      return breakdown;
    };

    for (const row of featureRows) {
      getBreakdown(row.user_id, row.user_login).byFeature.push({
        feature: row.feature,
        user_initiated_interaction_count: row.user_initiated_interaction_count,
        code_generation_activity_count: row.code_generation_activity_count,
        code_acceptance_activity_count: row.code_acceptance_activity_count,
        loc_added_sum: row.loc_added_sum,
        loc_deleted_sum: row.loc_deleted_sum,
        loc_suggested_to_add_sum: row.loc_suggested_to_add_sum,
        loc_suggested_to_delete_sum: row.loc_suggested_to_delete_sum,
      });
    }

    for (const row of ideRows) {
      getBreakdown(row.user_id, row.user_login).byIde.push({
        ide: row.ide,
        user_initiated_interaction_count: row.user_initiated_interaction_count,
        code_generation_activity_count: row.code_generation_activity_count,
        code_acceptance_activity_count: row.code_acceptance_activity_count,
        loc_added_sum: row.loc_added_sum,
        loc_deleted_sum: row.loc_deleted_sum,
        loc_suggested_to_add_sum: row.loc_suggested_to_add_sum,
        loc_suggested_to_delete_sum: row.loc_suggested_to_delete_sum,
      });
    }

    for (const row of languageFeatureRows) {
      getBreakdown(row.user_id, row.user_login).byLanguageFeature.push({
        language: row.language,
        feature: row.feature,
        code_generation_activity_count: row.code_generation_activity_count,
        code_acceptance_activity_count: row.code_acceptance_activity_count,
        loc_added_sum: row.loc_added_sum,
        loc_deleted_sum: row.loc_deleted_sum,
        loc_suggested_to_add_sum: row.loc_suggested_to_add_sum,
        loc_suggested_to_delete_sum: row.loc_suggested_to_delete_sum,
      });
    }

    for (const row of modelFeatureRows) {
      getBreakdown(row.user_id, row.user_login).byModelFeature.push({
        model_id: row.model_id,
        feature: row.feature,
        user_initiated_interaction_count: row.user_initiated_interaction_count,
        code_generation_activity_count: row.code_generation_activity_count,
        code_acceptance_activity_count: row.code_acceptance_activity_count,
        loc_added_sum: row.loc_added_sum,
        loc_deleted_sum: row.loc_deleted_sum,
        loc_suggested_to_add_sum: row.loc_suggested_to_add_sum,
        loc_suggested_to_delete_sum: row.loc_suggested_to_delete_sum,
      });
    }

    for (const row of languageModelRows) {
      getBreakdown(row.user_id, row.user_login).byLanguageModel.push({
        language: row.language,
        model_id: row.model_id,
        request_count: row.request_count,
        code_generation_activity_count: row.code_generation_activity_count,
        code_acceptance_activity_count: row.code_acceptance_activity_count,
        loc_added_sum: row.loc_added_sum,
        loc_deleted_sum: row.loc_deleted_sum,
        loc_suggested_to_add_sum: row.loc_suggested_to_add_sum,
        loc_suggested_to_delete_sum: row.loc_suggested_to_delete_sum,
      });
    }

    for (const row of customizationRows) {
      getBreakdown(row.user_id, row.user_login).customizations.push({
        category: row.category,
        item_name: row.item_name,
        interaction_count: row.interaction_count,
      });
    }

    return [...breakdowns.values()];
  }

  async hasUserMetrics(
    metricsType: MetricsScope,
    entityId: string,
    userLogin: string,
  ): Promise<boolean> {
    const row = await this.db('copilot_user_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereRaw('LOWER(user_login) = ?', [userLogin.toLowerCase()])
      .select('user_id')
      .first();

    return Boolean(row);
  }

  async getTeams(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<string[]> {
    const query = this.db('copilot_user_teams')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereNot('team_slug', '');

    if (from && to) {
      query.whereBetween('day', [from, to]);
    } else if (from) {
      query.where('day', '>=', from);
    } else if (to) {
      query.where('day', '<=', to);
    }

    const rows = await query
      .groupBy('team_slug')
      .havingRaw('COUNT(DISTINCT ??) >= ?', ['user_id', minMembers])
      .orderBy('team_slug', 'asc')
      .select('team_slug');

    return rows.map(row => row.team_slug);
  }

  async getPeriodRange(
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<PeriodRange | null> {
    const row = await this.db('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .select(
        this.db.raw('MIN(day) as min_day'),
        this.db.raw('MAX(day) as max_day'),
      )
      .first();

    const minDate = this.normalizeDay(row?.min_day);
    const maxDate = this.normalizeDay(row?.max_day);

    if (!minDate || !maxDate) {
      return null;
    }

    return { minDate, maxDate };
  }

  private normalizeDay(value: unknown): string | null {
    if (!value) {
      return null;
    }

    if (value instanceof Date) {
      return DateTime.fromJSDate(value, { zone: 'utc' }).toISODate();
    }

    if (typeof value === 'string') {
      const datePrefix = /^\d{4}-\d{2}-\d{2}/.exec(value)?.[0];
      if (datePrefix) {
        return datePrefix;
      }

      const parsed = DateTime.fromISO(value, { zone: 'utc' });
      if (parsed.isValid) {
        return parsed.toISODate();
      }
    }

    return null;
  }
  /**
   * Like normalizeDay, but throws if the value can't be normalized.
   * Use this for DB columns that are NOT NULL — if this ever throws,
   * it means the data violates an invariant we assume elsewhere
   * (e.g. the frontend requires a valid day string to render charts).
   */
  private normalizeRequiredDay(value: unknown): string {
    const normalized = this.normalizeDay(value);
    if (normalized === null) {
      throw new Error(
        `Expected a valid 'day' value from the database but got: ${JSON.stringify(
          value,
        )}`,
      );
    }
    return normalized;
  }

  private hasRequiredComponents(
    value: unknown,
    requiredComponents: string[],
  ): boolean {
    const loadedComponents = new Set(this.parseComponentsLoaded(value));
    return requiredComponents.every(component =>
      loadedComponents.has(component),
    );
  }

  private parseComponentsLoaded(value: unknown): string[] {
    if (!value) {
      return [];
    }

    if (Array.isArray(value)) {
      return value.filter(
        (entry): entry is string => typeof entry === 'string',
      );
    }

    if (typeof value !== 'string') {
      return [];
    }

    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed)
        ? parsed.filter((entry): entry is string => typeof entry === 'string')
        : [];
    } catch {
      return [];
    }
  }
}
