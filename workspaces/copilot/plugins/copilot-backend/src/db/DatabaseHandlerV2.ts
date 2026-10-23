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
  V2CostCenterRow,
  V2CostCenterListResponse,
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
  V2UserCostCenterRow,
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
import {
  bucketCostCenterBreakdown,
  CostCenterBreakdownKind,
} from './longTailBucketing';
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

type CostCenterScopedRow = {
  cost_center_id?: string;
  without_cost_center?: boolean;
};

export type CostCenterDashboardScope =
  | { kind: 'costCenter'; id: string }
  | { kind: 'noCostCenter' };

type CostCenterFilter = string | { kind: 'noCostCenter' };

export type CostCenterAggregateRows = {
  dailyTotals: V2DailyTotal[];
  prMetrics: V2PrMetricsRow[];
  byFeature: V2MetricsByFeatureRow[];
  byIde: V2MetricsByIdeRow[];
  byLanguageFeature: V2MetricsByLanguageFeatureRow[];
  byModelFeature: V2MetricsByModelFeatureRow[];
  byLanguageModel: V2MetricsByLanguageModelRow[];
  byCli: V2MetricsByCliRow[];
  byVscodeAgent: V2MetricsByVscodeAgentRow[];
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

export type TeamMembershipSnapshotState = {
  generation: string;
  cursor: string | null;
  complete: boolean;
};

const costCenterAggregateTables = [
  'copilot_daily_totals',
  'copilot_pr_metrics',
  'copilot_metrics_by_feature',
  'copilot_metrics_by_ide',
  'copilot_metrics_by_language_feature',
  'copilot_metrics_by_model_feature',
  'copilot_metrics_by_language_model',
  'copilot_metrics_by_cli',
  'copilot_metrics_by_vscode_agent',
] as const;

const WEEKLY_ACTIVE_USERS_WINDOW_DAYS = 7;
const MONTHLY_ACTIVE_USERS_WINDOW_DAYS = 28;

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
] as const;

const teamAggregationCopyBatchSize = 500;
const teamMembershipPageSize = 500;

const teamAggregateRowsByTable = (
  aggregates: TeamAggregates,
): Array<[string, object[]]> => [
  ['copilot_daily_totals', aggregates.dailyTotals],
  ['copilot_metrics_by_feature', aggregates.byFeature],
  ['copilot_metrics_by_ide', aggregates.byIde],
  ['copilot_metrics_by_language_feature', aggregates.byLanguageFeature],
  ['copilot_metrics_by_model_feature', aggregates.byModelFeature],
  ['copilot_metrics_by_language_model', aggregates.byLanguageModel],
  ['copilot_metrics_by_cli', aggregates.byCli ?? []],
  ['copilot_metrics_by_vscode_agent', aggregates.byVscodeAgent ?? []],
];

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

type SourcedUserTeamRow = V2UserTeamRow & {
  source: TeamAggregationSource;
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
  cursor: string | null;
  complete: boolean | number;
};

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
};

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

function assertExclusiveMetricFilters(
  teamSlug?: string,
  costCenter?: CostCenterFilter,
): void {
  if (teamSlug && costCenter) {
    throw new Error('Team and cost center filters cannot be combined.');
  }
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

  private addCostCenterFilter(
    query: Knex.QueryBuilder,
    costCenter?: CostCenterFilter,
    aggregateTable?: string,
  ): void {
    if (typeof costCenter === 'string') {
      query
        .where('cost_center_id', costCenter)
        .where('without_cost_center', false);
    } else if (costCenter?.kind === 'noCostCenter') {
      query.where('cost_center_id', '').where('without_cost_center', true);
      if (!aggregateTable) {
        throw new Error(
          'An aggregate table is required for the no-cost-center filter.',
        );
      }

      const database = this.db;
      query.whereExists(function hasCompleteUnassignedSnapshot() {
        this.select(database.raw('1'))
          .from('copilot_cost_center_aggregation_log as done')
          .whereRaw('?? = ??', ['done.day', `${aggregateTable}.day`])
          .whereRaw('?? = ??', [
            'done.metrics_type',
            `${aggregateTable}.metrics_type`,
          ])
          .whereRaw('?? = ??', [
            'done.entity_id',
            `${aggregateTable}.entity_id`,
          ])
          .where('done.unassigned_complete', true);
      });
    } else {
      query.where('cost_center_id', '').where('without_cost_center', false);
    }
  }

  private normalizeAggregateRow<T extends { day: unknown }>(
    row: T & { without_cost_center?: boolean },
  ): Omit<T, 'without_cost_center'> & { day: string } {
    const { without_cost_center: _withoutCostCenter, ...publicRow } = row;
    return {
      ...publicRow,
      day: this.normalizeRequiredDay(row.day),
    } as Omit<T, 'without_cost_center'> & { day: string };
  }

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
      let snapshotGeneration = generation;
      let row: TeamMembershipSnapshotStateRow | undefined;

      if (snapshotGeneration) {
        row = (await transaction('copilot_team_membership_snapshot_state')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            generation: snapshotGeneration,
          })
          .first()) as TeamMembershipSnapshotStateRow | undefined;
      } else {
        row = (await transaction('copilot_team_membership_snapshot_state')
          .where({
            metrics_type: metricsType,
            entity_id: entityId,
            complete: false,
          })
          .orderBy('updated_at', 'desc')
          .first()) as TeamMembershipSnapshotStateRow | undefined;
        if (row) {
          snapshotGeneration = row.generation;
        }
      }

      if (!snapshotGeneration) {
        snapshotGeneration = randomUUID();
      }

      if (!row) {
        await transaction('copilot_team_membership_snapshot_staging')
          .where({ metrics_type: metricsType, entity_id: entityId })
          .del();
        await transaction('copilot_team_membership_snapshot_state')
          .where({ metrics_type: metricsType, entity_id: entityId })
          .del();
        await transaction('copilot_team_membership_snapshot_state').insert({
          metrics_type: metricsType,
          entity_id: entityId,
          generation: snapshotGeneration,
          cursor: null,
          complete: false,
          updated_at: new Date(),
        });
        return {
          generation: snapshotGeneration,
          cursor: null,
          complete: false,
        };
      }

      return {
        generation: snapshotGeneration,
        cursor: row.cursor ?? null,
        complete: row.complete === true || Number(row.complete) === 1,
      };
    });
  }

  async getTeamMembershipSnapshot(
    metricsType: MetricsScope,
    entityId: string,
    generation: string,
  ): Promise<{
    state: TeamMembershipSnapshotState;
    memberships: Array<{ teamId: number; teamSlug: string; userId: number }>;
  }> {
    const [state, rows] = await Promise.all([
      this.db('copilot_team_membership_snapshot_state')
        .where({ metrics_type: metricsType, entity_id: entityId, generation })
        .first('generation', 'cursor', 'complete') as Promise<
        TeamMembershipSnapshotStateRow | undefined
      >,
      this.db<TeamMembershipSnapshotRow>(
        'copilot_team_membership_snapshot_staging',
      )
        .where({ metrics_type: metricsType, entity_id: entityId, generation })
        .orderBy('team_id', 'asc')
        .orderBy('user_id', 'asc')
        .select('team_id', 'team_slug', 'user_id'),
    ]);

    if (!state) {
      throw new Error('Organization team membership snapshot was not started.');
    }

    return {
      state: {
        generation,
        cursor: state.cursor ?? null,
        complete: state.complete === true || Number(state.complete) === 1,
      },
      memberships: rows.map(row => ({
        teamId: Number(row.team_id),
        teamSlug: row.team_slug,
        userId: Number(row.user_id),
      })),
    };
  }

  async getTeamMembershipSnapshotState(
    metricsType: MetricsScope,
    entityId: string,
    generation: string,
  ): Promise<TeamMembershipSnapshotState> {
    const row = (await this.db('copilot_team_membership_snapshot_state')
      .where({ metrics_type: metricsType, entity_id: entityId, generation })
      .first('generation', 'cursor', 'complete')) as
      | TeamMembershipSnapshotStateRow
      | undefined;

    if (!row) {
      throw new Error('Organization team membership snapshot was not started.');
    }

    return {
      generation,
      cursor: row.cursor ?? null,
      complete: row.complete === true || Number(row.complete) === 1,
    };
  }

  async getTeamMembershipSnapshotBatchForDay(
    metricsType: MetricsScope,
    entityId: string,
    generation: string,
    day: string,
    afterId: number = 0,
    pageSize: number = teamMembershipPageSize,
  ): Promise<{ memberships: V2UserTeamRow[]; nextCursor: number | null }> {
    const rows = await this.db<TeamMembershipSnapshotPageRow>(
      'copilot_team_membership_snapshot_staging as s',
    )
      .join('copilot_user_metrics as m', function joinDailyMetrics() {
        this.on('s.metrics_type', '=', 'm.metrics_type')
          .andOn('s.entity_id', '=', 'm.entity_id')
          .andOn('s.user_id', '=', 'm.user_id');
      })
      .where('s.metrics_type', metricsType)
      .where('s.entity_id', entityId)
      .where('s.generation', generation)
      .where('m.day', day)
      .where('s.id', '>', afterId)
      .orderBy('s.id', 'asc')
      .limit(pageSize)
      .select('s.id', 's.team_id', 's.team_slug', 's.user_id', 'm.user_login');

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
  }

  async saveTeamMembershipSnapshotBatch(
    metricsType: MetricsScope,
    entityId: string,
    generation: string,
    memberships: Array<{ teamId: number; teamSlug: string; userId: number }>,
    cursor: string | null,
    complete: boolean,
  ): Promise<void> {
    await this.db.transaction(async transaction => {
      const state = await transaction('copilot_team_membership_snapshot_state')
        .where({ metrics_type: metricsType, entity_id: entityId, generation })
        .forUpdate()
        .first();
      if (!state) {
        throw new Error(
          'Organization team membership snapshot was not started.',
        );
      }

      const rows: TeamMembershipSnapshotRow[] = memberships.map(
        ({ teamId, teamSlug, userId }) => ({
          generation,
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
        .where({ metrics_type: metricsType, entity_id: entityId, generation })
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
  ): Promise<void> {
    await this.db<TeamAggregationLogRow>('copilot_team_aggregation_log')
      .insert({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        source,
        generation,
        status: 'failed',
        error_message: errorMessage,
      })
      .onConflict(['day', 'metrics_type', 'entity_id', 'source', 'generation'])
      .merge({
        status: 'failed',
        error_message: errorMessage,
        completed_at: null,
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
          const rowData = {
            ...row,
            cost_center_id: row.cost_center_id ?? '',
            without_cost_center: false,
          };
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
      );
    });
  }

  async replaceStagedTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
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

        const sourcedMemberships: SourcedUserTeamRow[] = stagedMemberships.map(
          row => ({
            day: this.normalizeRequiredDay(row.day),
            metrics_type: row.metrics_type,
            entity_id: row.entity_id,
            user_id: Number(row.user_id),
            user_login: row.user_login,
            team_id: Number(row.team_id),
            team_slug: row.team_slug,
            source,
          }),
        );
        await batchInsertInChunks(sourcedMemberships, 100, async chunk => {
          await transaction<SourcedUserTeamRow>('copilot_user_teams').insert(
            chunk,
          );
        });
        lastMembershipId = Number(
          stagedMemberships[stagedMemberships.length - 1].id,
        );
      }

      for (const tableName of teamAggregateTables) {
        await transaction(tableName)
          .where({ day, metrics_type: metricsType, entity_id: entityId })
          .whereNot('team_slug', '')
          .where('cost_center_id', '')
          .where('without_cost_center', false)
          .del();
      }

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

  private withDefaultCostCenter<T extends CostCenterScopedRow>(
    rows: T[],
  ): Array<T & { cost_center_id: string; without_cost_center: boolean }> {
    return rows.map(row => ({
      ...row,
      cost_center_id: row.cost_center_id ?? '',
      without_cost_center: row.without_cost_center ?? false,
    }));
  }

  async insertDailyTotals(rows: V2DailyTotal[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2DailyTotal>('copilot_daily_totals')
          .insert(chunk)
          // Merge `total_ai_credits_used`/`daily_active_vscode_agent_users` on
          // conflict (rather than ignoring) so that re-ingesting a day that
          // was already stored before these columns existed backfills the new
          // metrics in place.
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
          ])
          .merge(['total_ai_credits_used', 'daily_active_vscode_agent_users']);
      },
    );
  }

  async insertPrMetrics(rows: V2PrMetricsRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2PrMetricsRow>('copilot_pr_metrics')
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
          ])
          .ignore();
      },
    );
  }

  async insertByFeature(rows: V2MetricsByFeatureRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
            'feature',
          ])
          .ignore();
      },
    );
  }

  async insertByIde(rows: V2MetricsByIdeRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
            'ide',
          ])
          .ignore();
      },
    );
  }

  async insertByLanguageFeature(
    rows: V2MetricsByLanguageFeatureRow[],
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByLanguageFeatureRow>(
          'copilot_metrics_by_language_feature',
        )
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
            'language',
            'feature',
          ])
          .ignore();
      },
    );
  }

  async insertByModelFeature(
    rows: V2MetricsByModelFeatureRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByModelFeatureRow>(
          'copilot_metrics_by_model_feature',
        )
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
            'model_id',
            'feature',
          ])
          .ignore();
      },
    );
  }

  async insertByLanguageModel(
    rows: V2MetricsByLanguageModelRow[],
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByLanguageModelRow>(
          'copilot_metrics_by_language_model',
        )
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
            'language',
            'model_id',
          ])
          .ignore();
      },
    );
  }

  async insertByCli(rows: V2MetricsByCliRow[]): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByCliRow>('copilot_metrics_by_cli')
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
          ])
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
      },
    );
  }

  async insertByVscodeAgent(rows: V2MetricsByVscodeAgentRow[]): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.db<V2MetricsByVscodeAgentRow>(
          'copilot_metrics_by_vscode_agent',
        )
          .insert(chunk)
          .onConflict([
            'day',
            'metrics_type',
            'entity_id',
            'team_slug',
            'cost_center_id',
            'without_cost_center',
          ])
          // Merge (rather than ignore) on conflict so that re-ingesting a day
          // updates any stale/zeroed row already stored for that key, mirroring
          // insertByCli above.
          .merge(['session_count', 'total_user_messages']);
      },
    );
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

  async insertUserTeams(
    rows: V2UserTeamRow[],
    source: TeamAggregationSource = 'native',
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    const rowsWithSource: SourcedUserTeamRow[] = rows.map(row => ({
      ...row,
      source,
    }));

    await batchInsertInChunks(rowsWithSource, 100, async chunk => {
      await this.db<SourcedUserTeamRow>('copilot_user_teams')
        .insert(chunk)
        .onConflict([
          'day',
          'metrics_type',
          'entity_id',
          'source',
          'user_id',
          'team_id',
        ])
        .ignore();
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
          cost_center_id?: string;
          without_cost_center?: boolean;
        };
        if (
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId ||
          !row.team_slug ||
          (row.cost_center_id !== undefined && row.cost_center_id !== '') ||
          row.without_cost_center === true
        ) {
          throw new Error(
            'Team replacement aggregates must match the requested day and scope and must not include a cost center.',
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

      const rowsWithCostCenter = rows.map(row => ({
        ...row,
        cost_center_id:
          (row as { cost_center_id?: string }).cost_center_id ?? '',
        without_cost_center: false,
      }));
      await batchInsertInChunks(rowsWithCostCenter, 100, async chunk => {
        await transaction(tableName).insert(chunk);
      });
    }
  }

  private async tryActivateTeamAggregationSource(
    transaction: Knex.Transaction,
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
  ): Promise<boolean> {
    const state = await this.lockTeamAggregationState(
      transaction,
      metricsType,
      entityId,
    );
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

      const sourcedMemberships: SourcedUserTeamRow[] = stagedMemberships.map(
        row => ({
          day: this.normalizeRequiredDay(row.day),
          metrics_type: row.metrics_type,
          entity_id: row.entity_id,
          user_id: Number(row.user_id),
          user_login: row.user_login,
          team_id: Number(row.team_id),
          team_slug: row.team_slug,
          source,
        }),
      );
      await batchInsertInChunks(sourcedMemberships, 100, async chunk => {
        await transaction<SourcedUserTeamRow>('copilot_user_teams').insert(
          chunk,
        );
      });
      lastMembershipId = Number(
        stagedMemberships[stagedMemberships.length - 1].id,
      );
    }

    for (const tableName of teamAggregateTables) {
      await transaction(tableName)
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereNot('team_slug', '')
        .where('cost_center_id', '')
        .where('without_cost_center', false)
        .del();
    }

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
          const rowData = {
            ...row,
            cost_center_id: row.cost_center_id ?? '',
            without_cost_center: false,
          };
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
      );
    });
  }

  async activateTeamAggregationSource(
    metricsType: MetricsScope,
    entityId: string,
    source: TeamAggregationSource,
    generation: string,
    requiredDays: string[],
  ): Promise<boolean> {
    return this.db.transaction(transaction =>
      this.tryActivateTeamAggregationSource(
        transaction,
        metricsType,
        entityId,
        source,
        generation,
        requiredDays,
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

      await transaction('copilot_user_teams')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();
      const sourcedMemberships: SourcedUserTeamRow[] = userTeams.map(row => ({
        ...row,
        source,
      }));
      await batchInsertInChunks(sourcedMemberships, 100, async chunk => {
        await transaction<SourcedUserTeamRow>('copilot_user_teams').insert(
          chunk,
        );
      });

      for (const tableName of teamAggregateTables) {
        await transaction(tableName)
          .where({ day, metrics_type: metricsType, entity_id: entityId })
          .whereNot('team_slug', '')
          .where('cost_center_id', '')
          .where('without_cost_center', false)
          .del();
      }
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

  async upsertCostCenters(rows: V2CostCenterRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    const updatedAt = new Date();
    const rowsWithUpdatedAt = rows.map(row => ({
      ...row,
      updated_at: updatedAt,
    }));

    await batchInsertInChunks(rowsWithUpdatedAt, 100, async chunk => {
      await this.db<V2CostCenterRow>('copilot_cost_centers')
        .insert(chunk)
        .onConflict(['metrics_type', 'entity_id', 'cost_center_id'])
        .merge(['name', 'updated_at']);
    });
  }

  async upsertUserCostCenters(rows: V2UserCostCenterRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserCostCenterRow>('copilot_user_cost_centers')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id'])
        .merge(['user_login', 'cost_center_id']);
    });
  }

  /**
   * Replaces all cost-center-derived state for a day atomically.
   */
  async replaceCostCenterAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userCostCenters: V2UserCostCenterRow[],
    aggregates: CostCenterAggregateRows,
    unassignedComplete: boolean = true,
  ): Promise<void> {
    const aggregateRows = [
      ...aggregates.dailyTotals,
      ...aggregates.prMetrics,
      ...aggregates.byFeature,
      ...aggregates.byIde,
      ...aggregates.byLanguageFeature,
      ...aggregates.byModelFeature,
      ...aggregates.byLanguageModel,
      ...aggregates.byCli,
      ...aggregates.byVscodeAgent,
    ];
    const rowsWithCostCenter = aggregateRows.map(row => ({
      ...row,
      cost_center_id: row.cost_center_id ?? '',
      without_cost_center:
        (row as CostCenterScopedRow).without_cost_center ?? false,
    }));

    if (
      userCostCenters.some(
        row =>
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId,
      ) ||
      rowsWithCostCenter.some(
        row =>
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId ||
          (row.without_cost_center
            ? row.cost_center_id !== ''
            : row.cost_center_id === '') ||
          ('team_slug' in row && row.team_slug !== ''),
      )
    ) {
      throw new Error(
        'Cost-center replacement rows must match the requested day and scope and must not include a team scope.',
      );
    }

    await this.db.transaction(async transaction => {
      await transaction('copilot_user_cost_centers')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();

      await batchInsertInChunks(userCostCenters, 100, async chunk => {
        await transaction<V2UserCostCenterRow>(
          'copilot_user_cost_centers',
        ).insert(chunk);
      });

      for (const tableName of costCenterAggregateTables) {
        await transaction(tableName)
          .where({ day, metrics_type: metricsType, entity_id: entityId })
          .andWhere(function removeCostCenterScopes() {
            this.whereNot('cost_center_id', '').orWhere(
              'without_cost_center',
              true,
            );
          })
          .del();
      }

      const insertCostCenterRows = async <T extends CostCenterScopedRow>(
        tableName: string,
        rows: T[],
      ): Promise<void> => {
        if (!rows.length) {
          return;
        }

        await batchInsertInChunks(
          this.withDefaultCostCenter(rows),
          100,
          async chunk => {
            await transaction(tableName).insert(chunk);
          },
        );
      };

      await insertCostCenterRows(
        'copilot_daily_totals',
        aggregates.dailyTotals,
      );
      await insertCostCenterRows('copilot_pr_metrics', aggregates.prMetrics);
      await insertCostCenterRows(
        'copilot_metrics_by_feature',
        aggregates.byFeature,
      );
      await insertCostCenterRows('copilot_metrics_by_ide', aggregates.byIde);
      await insertCostCenterRows(
        'copilot_metrics_by_language_feature',
        aggregates.byLanguageFeature,
      );
      await insertCostCenterRows(
        'copilot_metrics_by_model_feature',
        aggregates.byModelFeature,
      );
      await insertCostCenterRows(
        'copilot_metrics_by_language_model',
        aggregates.byLanguageModel,
      );
      await insertCostCenterRows('copilot_metrics_by_cli', aggregates.byCli);
      await insertCostCenterRows(
        'copilot_metrics_by_vscode_agent',
        aggregates.byVscodeAgent,
      );

      await transaction('copilot_cost_center_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          unassigned_complete: unassignedComplete,
          aggregated_at: this.db.fn.now(),
        })
        .onConflict(['day', 'metrics_type', 'entity_id'])
        .merge(['unassigned_complete', 'aggregated_at']);
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

  async getDaysMissingCostCenterAggregation(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
  ): Promise<string[]> {
    const [metricRows, ingestionRows, completedRows] = await Promise.all([
      this.db('copilot_user_metrics')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [from, to])
        .distinct('day')
        .select('day'),
      this.db('copilot_ingestion_log')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [from, to])
        .select('day', 'components_loaded'),
      this.db('copilot_cost_center_aggregation_log')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [from, to])
        .select('day'),
    ]);

    const daysToAggregate = new Set<string>();
    for (const row of metricRows) {
      const day = this.normalizeDay(row.day);
      if (day) {
        daysToAggregate.add(day);
      }
    }
    for (const row of ingestionRows) {
      if (!this.hasRequiredComponents(row.components_loaded, ['users'])) {
        continue;
      }

      const day = this.normalizeDay(row.day);
      if (day) {
        daysToAggregate.add(day);
      }
    }

    const completedDays = new Set(
      completedRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    return [...daysToAggregate].filter(day => !completedDays.has(day)).sort();
  }

  async getDaysWithIncompleteCostCenterAggregation(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
  ): Promise<string[]> {
    const rows = await this.db('copilot_cost_center_aggregation_log')
      .where({
        metrics_type: metricsType,
        entity_id: entityId,
        unassigned_complete: false,
      })
      .whereBetween('day', [from, to])
      .select('day');

    return rows
      .map(row => this.normalizeDay(row.day))
      .filter((day): day is string => Boolean(day))
      .sort();
  }

  async markCostCenterAggregationDone(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    unassignedComplete: boolean = true,
  ): Promise<void> {
    await this.db('copilot_cost_center_aggregation_log')
      .insert({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        unassigned_complete: unassignedComplete,
        aggregated_at: this.db.fn.now(),
      })
      .onConflict(['day', 'metrics_type', 'entity_id'])
      .merge(['unassigned_complete', 'aggregated_at']);
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getDailyTotals(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2DailyTotal[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2DailyTotal>('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_daily_totals');

    const rows = (await query.orderBy('day', 'asc').select('*')).map(row =>
      this.normalizeAggregateRow(
        row as V2DailyTotal & { without_cost_center?: boolean },
      ),
    );

    // Unscoped org/enterprise rows already carry rolling counts from GitHub.
    // Team and cost-center aggregates are derived from daily per-user data,
    // so their rolling counts are computed from the membership snapshots.
    if (teamSlug || costCenter) {
      return this.enrichScopedRollingActiveUsers(
        rows,
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
    }

    return rows;
  }

  /**
   * Enrich scoped daily totals with rolling weekly/monthly active-user counts
   * derived from per-user activity and the selected membership snapshots.
   *
   * Definitions follow GitHub's standard rolling windows:
   * - weekly_active_users: distinct active users over the trailing 7 days
   * - monthly_active_users: distinct active users over the trailing 28 days
   * - monthly_active_agent_users / monthly_active_chat_users: distinct users
   *   that used agent/chat over the trailing 28 days
   *
   * A user is active for a scope on a day when they have a per-user metrics
   * row and matching team or cost-center membership for that day.
   */
  private async enrichScopedRollingActiveUsers(
    rows: V2DailyTotal[],
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2DailyTotal[]> {
    if (rows.length === 0) {
      return rows;
    }

    const activityRows = await this.loadScopedActivityRows(
      metricsType,
      entityId,
      from,
      to,
      teamSlug,
      costCenter,
      MONTHLY_ACTIVE_USERS_WINDOW_DAYS,
    );

    const { activeByDay, agentByDay, chatByDay } =
      this.collectRollingUsersByDay(activityRows as TeamActivityRow[]);
    let completeUnassignedDays: Set<string> | undefined;
    if (typeof costCenter !== 'string' && costCenter?.kind === 'noCostCenter') {
      const lowerBound = DateTime.fromISO(from)
        .minus({ days: MONTHLY_ACTIVE_USERS_WINDOW_DAYS - 1 })
        .toFormat('yyyy-MM-dd');
      const completionRows = await this.db(
        'copilot_cost_center_aggregation_log',
      )
        .where({
          metrics_type: metricsType,
          entity_id: entityId,
          unassigned_complete: true,
        })
        .whereBetween('day', [lowerBound, to])
        .select('day');
      completeUnassignedDays = new Set(
        completionRows
          .map(completion => this.normalizeDay(completion.day))
          .filter((day): day is string => Boolean(day)),
      );
    }

    return rows.map(row =>
      this.enrichDailyTotalWithRollingUsers(row, {
        activeByDay,
        agentByDay,
        chatByDay,
        weeklyWindowDays: WEEKLY_ACTIVE_USERS_WINDOW_DAYS,
        monthlyWindowDays: MONTHLY_ACTIVE_USERS_WINDOW_DAYS,
        completeUnassignedDays,
      }),
    );
  }

  private async loadScopedActivityRows(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug: string | undefined,
    costCenter: CostCenterFilter | undefined,
    monthlyWindowDays: number,
  ) {
    // Each emitted row's rolling value is anchored at *that row's own day* and
    // looks backwards. `fetchLowerBound` only widens the SQL fetch so the
    // earliest days in [from, to] still have a full trailing window available.
    const fetchLowerBound = DateTime.fromISO(from)
      .minus({ days: monthlyWindowDays - 1 })
      .toFormat('yyyy-MM-dd');

    const database = this.db;
    const query = database('copilot_user_metrics as m');

    if (teamSlug) {
      query.join('copilot_user_teams as t', function joinTeam() {
        this.on('m.day', '=', 't.day')
          .andOn('m.user_id', '=', 't.user_id')
          .andOn('m.metrics_type', '=', 't.metrics_type')
          .andOn('m.entity_id', '=', 't.entity_id');
      });
      query.leftJoin(
        'copilot_team_aggregation_state as team_state',
        function joinTeamState() {
          this.on('m.metrics_type', '=', 'team_state.metrics_type').andOn(
            'm.entity_id',
            '=',
            'team_state.entity_id',
          );
        },
      );
    }

    if (typeof costCenter === 'string') {
      query.join('copilot_user_cost_centers as c', function joinCostCenter() {
        this.on('m.day', '=', 'c.day')
          .andOn('m.user_id', '=', 'c.user_id')
          .andOn('m.metrics_type', '=', 'c.metrics_type')
          .andOn('m.entity_id', '=', 'c.entity_id');
      });
    } else if (costCenter?.kind === 'noCostCenter') {
      // Only treat a day as unassigned when its cost-center snapshot was
      // transactionally completed; an absent snapshot is not evidence that a
      // user had no cost center.
      query.join(
        'copilot_cost_center_aggregation_log as cost_center_done',
        function joinCompletedCostCenterDay() {
          this.on('m.day', '=', 'cost_center_done.day')
            .andOn('m.metrics_type', '=', 'cost_center_done.metrics_type')
            .andOn('m.entity_id', '=', 'cost_center_done.entity_id');
        },
      );
      query.where('cost_center_done.unassigned_complete', true);
    }

    query
      .where('m.metrics_type', metricsType)
      .where('m.entity_id', entityId)
      .whereBetween('m.day', [fetchLowerBound, to]);

    if (teamSlug) {
      query.whereRaw('?? = COALESCE(??, ?)', [
        't.source',
        'team_state.active_source',
        'native',
      ]);
      query.where('t.team_slug', teamSlug);
    }

    if (typeof costCenter === 'string') {
      query.where('c.cost_center_id', costCenter);
    } else if (costCenter?.kind === 'noCostCenter') {
      query.whereNotExists(function userHasCostCenter() {
        this.select(database.raw('1'))
          .from('copilot_user_cost_centers as c')
          .whereRaw('?? = ??', ['c.day', 'm.day'])
          .whereRaw('?? = ??', ['c.metrics_type', 'm.metrics_type'])
          .whereRaw('?? = ??', ['c.entity_id', 'm.entity_id'])
          .whereRaw('?? = ??', ['c.user_id', 'm.user_id']);
      });
    }

    return query.select(
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
      completeUnassignedDays?: Set<string>;
    },
  ): V2DailyTotal {
    const day = this.normalizeDay(row.day);
    if (!day) {
      return row;
    }

    const weeklyWindowComplete =
      !params.completeUnassignedDays ||
      this.isRollingWindowComplete(
        params.completeUnassignedDays,
        day,
        params.weeklyWindowDays,
      );
    const monthlyWindowComplete =
      !params.completeUnassignedDays ||
      this.isRollingWindowComplete(
        params.completeUnassignedDays,
        day,
        params.monthlyWindowDays,
      );

    return {
      ...row,
      weekly_active_users: weeklyWindowComplete
        ? this.countDistinctUsersOverWindow(
            params.activeByDay,
            day,
            params.weeklyWindowDays,
          )
        : undefined,
      monthly_active_users: monthlyWindowComplete
        ? this.countDistinctUsersOverWindow(
            params.activeByDay,
            day,
            params.monthlyWindowDays,
          )
        : undefined,
      monthly_active_agent_users: monthlyWindowComplete
        ? this.countDistinctUsersOverWindow(
            params.agentByDay,
            day,
            params.monthlyWindowDays,
          )
        : undefined,
      monthly_active_chat_users: monthlyWindowComplete
        ? this.countDistinctUsersOverWindow(
            params.chatByDay,
            day,
            params.monthlyWindowDays,
          )
        : undefined,
    };
  }

  private isRollingWindowComplete(
    completeDays: Set<string>,
    endDay: string,
    windowDays: number,
  ): boolean {
    const end = DateTime.fromISO(endDay, { zone: 'utc' });
    if (!end.isValid) {
      return false;
    }

    for (let dayOffset = 0; dayOffset < windowDays; dayOffset++) {
      const dayKey = end.minus({ days: dayOffset }).toFormat('yyyy-MM-dd');
      if (!completeDays.has(dayKey)) {
        return false;
      }
    }

    return true;
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

  private applyCostCenterBreakdownBucketing<T extends { day: string }>(
    rows: T[],
    costCenter: CostCenterFilter | undefined,
    kind: CostCenterBreakdownKind,
  ): T[] {
    return costCenter ? bucketCostCenterBreakdown(rows, kind) : rows;
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getPrMetrics(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2PrMetricsRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2PrMetricsRow>('copilot_pr_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_pr_metrics');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row =>
      this.normalizeAggregateRow(
        row as V2PrMetricsRow & { without_cost_center?: boolean },
      ),
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2MetricsByFeatureRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_metrics_by_feature');

    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row =>
        this.normalizeAggregateRow(
          row as V2MetricsByFeatureRow & { without_cost_center?: boolean },
        ),
      ),
      costCenter,
      'feature',
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByIde(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2MetricsByIdeRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_metrics_by_ide');

    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row =>
        this.normalizeAggregateRow(
          row as V2MetricsByIdeRow & { without_cost_center?: boolean },
        ),
      ),
      costCenter,
      'ide',
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByLanguageFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    feature?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2MetricsByLanguageFeatureRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2MetricsByLanguageFeatureRow>(
      'copilot_metrics_by_language_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(
      query,
      costCenter,
      'copilot_metrics_by_language_feature',
    );

    if (feature !== undefined) {
      query.where('feature', feature);
    }

    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row =>
        this.normalizeAggregateRow(
          row as V2MetricsByLanguageFeatureRow & {
            without_cost_center?: boolean;
          },
        ),
      ),
      costCenter,
      'language-feature',
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByModelFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2MetricsByModelFeatureRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2MetricsByModelFeatureRow>(
      'copilot_metrics_by_model_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(
      query,
      costCenter,
      'copilot_metrics_by_model_feature',
    );
    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row =>
        this.normalizeAggregateRow(
          row as V2MetricsByModelFeatureRow & { without_cost_center?: boolean },
        ),
      ),
      costCenter,
      'model-feature',
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByLanguageModel(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2MetricsByLanguageModelRow[]> {
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const query = this.db<V2MetricsByLanguageModelRow>(
      'copilot_metrics_by_language_model',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(
      query,
      costCenter,
      'copilot_metrics_by_language_model',
    );
    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row =>
        this.normalizeAggregateRow(
          row as V2MetricsByLanguageModelRow & {
            without_cost_center?: boolean;
          },
        ),
      ),
      costCenter,
      'language-model',
    );
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team or cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getDashboardData(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenterScope?: CostCenterDashboardScope,
  ): Promise<V2DashboardData> {
    const costCenter: CostCenterFilter | undefined =
      costCenterScope?.kind === 'costCenter'
        ? costCenterScope.id
        : costCenterScope;
    assertExclusiveMetricFilters(teamSlug, costCenter);

    const [
      daily,
      byFeature,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      prMetrics,
    ] = await Promise.all([
      this.getDailyTotals(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      ),
      this.getByFeature(metricsType, entityId, from, to, teamSlug, costCenter),
      this.getByLanguageFeature(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        undefined,
        costCenter,
      ),
      this.getByModelFeature(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      ),
      this.getByLanguageModel(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      ),
      this.getPrMetrics(metricsType, entityId, from, to, teamSlug, costCenter),
    ]);

    return {
      daily,
      byFeature,
      byLanguage,
      byModelFeature,
      byLanguageModel,
      prMetrics,
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
    ]);

    return {
      userLogin,
      daily,
      byFeature,
      byIde,
      byLanguage,
      byModelFeature,
      byLanguageModel,
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
    ] = await Promise.all([
      getRows('copilot_user_metrics_by_feature'),
      getRows('copilot_user_metrics_by_ide'),
      getRows('copilot_user_metrics_by_language_feature'),
      getRows('copilot_user_metrics_by_model_feature'),
      getRows('copilot_user_metrics_by_language_model'),
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

    return [...breakdowns.values()];
  }

  async getUserTeamsForDay(
    metricsType: MetricsScope,
    entityId: string,
    day: string,
  ): Promise<V2UserTeamRow[]> {
    const rows = await this.db<V2UserTeamRow>('copilot_user_teams as t')
      .leftJoin(
        'copilot_team_aggregation_state as team_state',
        function joinTeamState() {
          this.on('t.metrics_type', '=', 'team_state.metrics_type').andOn(
            't.entity_id',
            '=',
            'team_state.entity_id',
          );
        },
      )
      .where('t.metrics_type', metricsType)
      .where('t.entity_id', entityId)
      .where('t.day', day)
      .whereRaw('?? = COALESCE(??, ?)', [
        't.source',
        'team_state.active_source',
        'native',
      ])
      .orderBy('user_id', 'asc')
      .select(
        't.id',
        't.day',
        't.metrics_type',
        't.entity_id',
        't.user_id',
        't.user_login',
        't.team_id',
        't.team_slug',
      );

    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
      user_id: Number(row.user_id),
      team_id: Number(row.team_id),
    }));
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

  async getCostCenters(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<V2CostCenterListResponse> {
    const costCenterQuery = this.db('copilot_user_cost_centers as ucc')
      .join('copilot_user_metrics as m', function joinMetrics() {
        this.on('ucc.day', '=', 'm.day')
          .andOn('ucc.metrics_type', '=', 'm.metrics_type')
          .andOn('ucc.entity_id', '=', 'm.entity_id')
          .andOn('ucc.user_id', '=', 'm.user_id');
      })
      .leftJoin('copilot_cost_centers as cc', function joinOn() {
        this.on('ucc.metrics_type', '=', 'cc.metrics_type')
          .andOn('ucc.entity_id', '=', 'cc.entity_id')
          .andOn('ucc.cost_center_id', '=', 'cc.cost_center_id');
      })
      .where('ucc.metrics_type', metricsType)
      .where('ucc.entity_id', entityId);

    if (from && to) {
      costCenterQuery.whereBetween('ucc.day', [from, to]);
    } else if (from) {
      costCenterQuery.where('ucc.day', '>=', from);
    } else if (to) {
      costCenterQuery.where('ucc.day', '<=', to);
    }

    const costCenterRows = await costCenterQuery
      .groupBy('ucc.cost_center_id', 'cc.name')
      .havingRaw('COUNT(DISTINCT ??) >= ?', ['ucc.user_id', minMembers])
      .orderByRaw('COALESCE(??, ??) ASC', ['cc.name', 'ucc.cost_center_id'])
      .select('ucc.cost_center_id as id', 'cc.name as name');

    const costCenters = costCenterRows.map(row => {
      const id = String(row.id);
      return {
        id,
        name:
          typeof row.name === 'string' && row.name.length > 0 ? row.name : id,
      };
    });

    const database = this.db;
    const unassignedQuery = database(
      'copilot_cost_center_aggregation_log as done',
    )
      .join('copilot_user_metrics as m', function joinCompletedMetrics() {
        this.on('done.day', '=', 'm.day')
          .andOn('done.metrics_type', '=', 'm.metrics_type')
          .andOn('done.entity_id', '=', 'm.entity_id');
      })
      .where('done.metrics_type', metricsType)
      .where('done.entity_id', entityId)
      .where('done.unassigned_complete', true)
      .whereNotExists(function userHasCostCenter() {
        this.select(database.raw('1'))
          .from('copilot_user_cost_centers as ucc')
          .whereRaw('?? = ??', ['ucc.day', 'm.day'])
          .whereRaw('?? = ??', ['ucc.metrics_type', 'm.metrics_type'])
          .whereRaw('?? = ??', ['ucc.entity_id', 'm.entity_id'])
          .whereRaw('?? = ??', ['ucc.user_id', 'm.user_id']);
      });

    if (from && to) {
      unassignedQuery.whereBetween('m.day', [from, to]);
    } else if (from) {
      unassignedQuery.where('m.day', '>=', from);
    } else if (to) {
      unassignedQuery.where('m.day', '<=', to);
    }

    const unassignedCount = await unassignedQuery
      .countDistinct({ member_count: 'm.user_id' })
      .first();

    return {
      costCenters,
      hasUsersWithoutCostCenter:
        Number(unassignedCount?.member_count ?? 0) >= minMembers,
    };
  }

  async hasUsersWithoutCostCenterOnDashboard(
    metricsType: MetricsScope,
    entityId: string,
    dailyRows: Pick<V2DailyTotal, 'day'>[],
    minMembers: number = 5,
  ): Promise<boolean> {
    const returnedDays = new Set(
      dailyRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );
    if (returnedDays.size === 0) {
      return false;
    }

    const database = this.db;
    const unassignedCount = await database(
      'copilot_cost_center_aggregation_log as done',
    )
      .join('copilot_user_metrics as m', function joinCompletedMetrics() {
        this.on('done.day', '=', 'm.day')
          .andOn('done.metrics_type', '=', 'm.metrics_type')
          .andOn('done.entity_id', '=', 'm.entity_id');
      })
      .where('done.metrics_type', metricsType)
      .where('done.entity_id', entityId)
      .where('done.unassigned_complete', true)
      .whereIn('done.day', [...returnedDays])
      .whereNotExists(function userHasCostCenter() {
        this.select(database.raw('1'))
          .from('copilot_user_cost_centers as ucc')
          .whereRaw('?? = ??', ['ucc.day', 'm.day'])
          .whereRaw('?? = ??', ['ucc.metrics_type', 'm.metrics_type'])
          .whereRaw('?? = ??', ['ucc.entity_id', 'm.entity_id'])
          .whereRaw('?? = ??', ['ucc.user_id', 'm.user_id']);
      })
      .countDistinct({ member_count: 'm.user_id' })
      .first();

    return Number(unassignedCount?.member_count ?? 0) >= minMembers;
  }

  async getTeams(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<string[]> {
    const query = this.db('copilot_user_teams as t')
      .leftJoin(
        'copilot_team_aggregation_state as team_state',
        function joinTeamState() {
          this.on('t.metrics_type', '=', 'team_state.metrics_type').andOn(
            't.entity_id',
            '=',
            'team_state.entity_id',
          );
        },
      )
      .where('t.metrics_type', metricsType)
      .where('t.entity_id', entityId)
      .whereRaw('?? = COALESCE(??, ?)', [
        't.source',
        'team_state.active_source',
        'native',
      ])
      .whereNot('t.team_slug', '');

    if (from && to) {
      query.whereBetween('t.day', [from, to]);
    } else if (from) {
      query.where('t.day', '>=', from);
    } else if (to) {
      query.where('t.day', '<=', to);
    }

    const rows = await query
      .groupBy('t.team_slug')
      .havingRaw('COUNT(DISTINCT ??) >= ?', ['t.user_id', minMembers])
      .orderBy('t.team_slug', 'asc')
      .select('t.team_slug');

    return rows.map(row => row.team_slug);
  }

  /**
   * Callers MUST enforce the minimum-member threshold for cost-center scopes
   * before exposing this range; the router enforces this.
   */
  async getPeriodRange(
    metricsType: MetricsScope,
    entityId: string,
    costCenter?: CostCenterFilter,
  ): Promise<PeriodRange | null> {
    const query = this.db('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId);
    this.addCostCenterFilter(query, costCenter, 'copilot_daily_totals');

    const row = await query
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
