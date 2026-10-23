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
  NO_COST_CENTER_ID,
} from '@backstage-community/plugin-copilot-common';
import { DateTime } from 'luxon';
import { Knex } from 'knex';
import { migrationsDir } from './DatabaseHandler';
import { batchInsertInChunks } from '../utils/batchInsert';
import { UserBreakdownData } from '../utils/reportParser';

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
};

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

  private withDefaultCostCenter<T extends CostCenterScopedRow>(
    rows: T[],
  ): Array<T & { cost_center_id: string }> {
    return rows.map(row => ({
      ...row,
      cost_center_id: row.cost_center_id ?? '',
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

  async insertUserTeams(rows: V2UserTeamRow[]): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.db<V2UserTeamRow>('copilot_user_teams')
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id', 'team_id'])
        .ignore();
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
          row.cost_center_id === '',
      )
    ) {
      throw new Error(
        'Cost-center replacement rows must match the requested day and scope and include a cost-center ID.',
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
          .whereNot('cost_center_id', '')
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
          aggregated_at: this.db.fn.now(),
        })
        .onConflict(['day', 'metrics_type', 'entity_id'])
        .merge(['aggregated_at']);
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
    await this.db<V2IngestionLogRow>('copilot_ingestion_log')
      .insert(row)
      .onConflict(['day', 'metrics_type', 'entity_id'])
      .merge();
  }

  async getMissingDays(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    requiredComponents: string[] = ['totals'],
  ): Promise<string[]> {
    const successfulRows = await this.db('copilot_ingestion_log')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('status', 'success')
      .whereBetween('day', [from, to])
      .select('day', 'components_loaded');

    const successfulDays = new Set(
      successfulRows
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
      if (day && !successfulDays.has(day)) {
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

  async markCostCenterAggregationDone(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    await this.db('copilot_cost_center_aggregation_log')
      .insert({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        aggregated_at: this.db.fn.now(),
      })
      .onConflict(['day', 'metrics_type', 'entity_id'])
      .merge(['aggregated_at']);
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getDailyTotals(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2DailyTotal[]> {
    const query = this.db<V2DailyTotal>('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');

    const rows = (await query.orderBy('day', 'asc').select('*')).map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));

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
   * row and matching team/cost-center membership rows for that day.
   */
  private async enrichScopedRollingActiveUsers(
    rows: V2DailyTotal[],
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2DailyTotal[]> {
    if (rows.length === 0) {
      return rows;
    }

    const WEEKLY_WINDOW_DAYS = 7;
    const MONTHLY_WINDOW_DAYS = 28;
    const activityRows = await this.loadScopedActivityRows(
      metricsType,
      entityId,
      from,
      to,
      teamSlug,
      costCenter,
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

  private async loadScopedActivityRows(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug: string | undefined,
    costCenter: string | undefined,
    monthlyWindowDays: number,
  ) {
    // Each emitted row's rolling value is anchored at *that row's own day* and
    // looks backwards. `fetchLowerBound` only widens the SQL fetch so the
    // earliest days in [from, to] still have a full trailing window available.
    const fetchLowerBound = DateTime.fromISO(from)
      .minus({ days: monthlyWindowDays - 1 })
      .toFormat('yyyy-MM-dd');

    const query = this.db('copilot_user_metrics as m');

    if (teamSlug) {
      query.join('copilot_user_teams as t', function joinTeam() {
        this.on('m.day', '=', 't.day')
          .andOn('m.user_id', '=', 't.user_id')
          .andOn('m.metrics_type', '=', 't.metrics_type')
          .andOn('m.entity_id', '=', 't.entity_id');
      });
    }

    if (costCenter) {
      query.join('copilot_user_cost_centers as c', function joinCostCenter() {
        this.on('m.day', '=', 'c.day')
          .andOn('m.user_id', '=', 'c.user_id')
          .andOn('m.metrics_type', '=', 'c.metrics_type')
          .andOn('m.entity_id', '=', 'c.entity_id');
      });
    }

    query
      .where('m.metrics_type', metricsType)
      .where('m.entity_id', entityId)
      .whereBetween('m.day', [fetchLowerBound, to]);

    if (teamSlug) {
      query.where('t.team_slug', teamSlug);
    }

    if (costCenter) {
      query.where('c.cost_center_id', costCenter);
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

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getPrMetrics(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2PrMetricsRow[]> {
    const query = this.db<V2PrMetricsRow>('copilot_pr_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2MetricsByFeatureRow[]> {
    const query = this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByIde(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2MetricsByIdeRow[]> {
    const query = this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByLanguageFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    feature?: string,
    costCenter?: string,
  ): Promise<V2MetricsByLanguageFeatureRow[]> {
    const query = this.db<V2MetricsByLanguageFeatureRow>(
      'copilot_metrics_by_language_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');

    if (feature !== undefined) {
      query.where('feature', feature);
    }

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByModelFeature(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2MetricsByModelFeatureRow[]> {
    const query = this.db<V2MetricsByModelFeatureRow>(
      'copilot_metrics_by_model_feature',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');
    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getByLanguageModel(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2MetricsByLanguageModelRow[]> {
    const query = this.db<V2MetricsByLanguageModelRow>(
      'copilot_metrics_by_language_model',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);
    query.where('team_slug', teamSlug ?? '');
    query.where('cost_center_id', costCenter ?? '');
    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => ({
      ...row,
      day: this.normalizeRequiredDay(row.day),
    }));
  }

  /**
   * Callers MUST enforce the minimum-member threshold for team/cost-center
   * scopes before exposing these results; the router enforces this.
   */
  async getDashboardData(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: string,
  ): Promise<V2DashboardData> {
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
    const rows = await this.db<V2UserTeamRow>('copilot_user_teams')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('day', day)
      .orderBy('user_id', 'asc')
      .select('*');

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
  ): Promise<Array<{ id: string; name: string | null }>> {
    const query = this.db('copilot_user_cost_centers as ucc')
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
      query.whereBetween('ucc.day', [from, to]);
    } else if (from) {
      query.where('ucc.day', '>=', from);
    } else if (to) {
      query.where('ucc.day', '<=', to);
    }

    const rows = await query
      .groupBy('ucc.cost_center_id', 'cc.name')
      .havingRaw('COUNT(DISTINCT ??) >= ?', ['ucc.user_id', minMembers])
      .orderByRaw('COALESCE(??, ??) ASC', ['cc.name', 'ucc.cost_center_id'])
      .select('ucc.cost_center_id as id', 'cc.name as name');

    return rows.map(row => {
      const id = String(row.id);
      if (id === NO_COST_CENTER_ID) {
        return { id, name: null };
      }

      return {
        id,
        name:
          typeof row.name === 'string' && row.name.length > 0 ? row.name : id,
      };
    });
  }

  async getTeams(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
    minMembers: number = 5,
    costCenter?: string,
  ): Promise<string[]> {
    const query = this.db('copilot_user_teams as t')
      .where('t.metrics_type', metricsType)
      .where('t.entity_id', entityId)
      .whereNot('t.team_slug', '');

    if (costCenter) {
      query.join('copilot_user_metrics as m', function joinMetrics() {
        this.on('t.day', '=', 'm.day')
          .andOn('t.metrics_type', '=', 'm.metrics_type')
          .andOn('t.entity_id', '=', 'm.entity_id')
          .andOn('t.user_id', '=', 'm.user_id');
      });
      query.join('copilot_user_cost_centers as c', function joinOn() {
        this.on('t.day', '=', 'c.day')
          .andOn('t.user_id', '=', 'c.user_id')
          .andOn('t.metrics_type', '=', 'c.metrics_type')
          .andOn('t.entity_id', '=', 'c.entity_id');
      });
      query.where('c.cost_center_id', costCenter);
    }

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
    costCenter?: string,
  ): Promise<PeriodRange | null> {
    const query = this.db('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .where('cost_center_id', costCenter ?? '');

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
