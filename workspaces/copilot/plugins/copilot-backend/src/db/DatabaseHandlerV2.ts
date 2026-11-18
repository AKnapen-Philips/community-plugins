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
import { migrationsDir } from './DatabaseHandler';
import { batchInsertInChunks } from '../utils/batchInsert';
import {
  bucketCostCenterBreakdown,
  CostCenterBreakdownKind,
} from './longTailBucketing';
import { UserBreakdownData } from '../utils/reportParser';
import { filterScopedUsers, readScopedMetrics } from './scopedMetrics';

type Options = {
  database: DatabaseService;
};

type DayValue = string | Date;

type TeamActivityRow = {
  day: DayValue;
  user_id: number;
  used_agent: boolean | number | null;
  used_chat: boolean | number | null;
  used_cli?: boolean | number | null;
  used_vscode_agent?: boolean | number | null;
};

export function parseComponentsLoaded(value: unknown): string[] {
  if (!value) {
    return [];
  }

  if (Array.isArray(value)) {
    return value.filter((entry): entry is string => typeof entry === 'string');
  }

  if (typeof value !== 'string') {
    return [];
  }

  try {
    const parsed: unknown = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed.filter((entry): entry is string => typeof entry === 'string')
      : [];
  } catch {
    return [];
  }
}

function isDatabaseBoolean(value: unknown): boolean {
  return value === true || value === 1 || value === '1' || value === 'true';
}

type CostCenterScopedRow = {
  cost_center_id?: string;
  without_cost_center?: boolean;
};

export type CostCenterDashboardScope =
  | { kind: 'costCenter'; id: string }
  | { kind: 'noCostCenter' };

type CostCenterFilter = string | { kind: 'noCostCenter' };

export type EnterpriseOrganizationAccessState =
  | 'pending'
  | 'accessible'
  | 'inaccessible';

export type EnterpriseOrganizationDirectory = {
  complete: boolean;
  hasPendingOrganizations: boolean;
  organizations: string[];
};

const WEEKLY_ACTIVE_USERS_WINDOW_DAYS = 7;
const MONTHLY_ACTIVE_USERS_WINDOW_DAYS = 28;

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

  private connection(transaction?: Knex.Transaction): Knex | Knex.Transaction {
    return transaction ?? this.db;
  }

  async syncEnterpriseOrganizations(
    enterpriseId: string,
    organizations: string[],
    now: Date,
  ): Promise<void> {
    const uniqueOrganizations = new Map<string, string>();
    for (const login of organizations) {
      const trimmed = login.trim();
      if (trimmed && !uniqueOrganizations.has(trimmed.toLowerCase())) {
        uniqueOrganizations.set(trimmed.toLowerCase(), trimmed);
      }
    }

    await this.db.transaction(async transaction => {
      const currentRows = await transaction('copilot_enterprise_organizations')
        .where({ enterprise_id: enterpriseId })
        .select('organization_key', 'organization_login', 'is_current');
      const previouslyCurrent = new Set(
        currentRows
          .filter(row => isDatabaseBoolean(row.is_current))
          .map(row => row.organization_key),
      );
      const knownLogins = new Map(
        currentRows.map(row => [row.organization_key, row.organization_login]),
      );

      await transaction('copilot_enterprise_organizations')
        .where({ enterprise_id: enterpriseId })
        .update({ is_current: false, updated_at: now });

      for (const [organizationKey, organizationLogin] of uniqueOrganizations) {
        const wasCurrent = previouslyCurrent.has(organizationKey);
        const canonicalLogin =
          knownLogins.get(organizationKey) ?? organizationLogin;
        await transaction('copilot_enterprise_organizations')
          .insert({
            enterprise_id: enterpriseId,
            organization_key: organizationKey,
            organization_login: canonicalLogin,
            is_current: true,
            access_state: 'pending',
            reports_ready: false,
            updated_at: now,
          })
          .onConflict(['enterprise_id', 'organization_key'])
          .merge({
            organization_login: canonicalLogin,
            is_current: true,
            updated_at: now,
            ...(wasCurrent
              ? {}
              : {
                  access_state: 'pending',
                  reports_ready: false,
                  last_attempted_at: null,
                  next_attempt_at: null,
                }),
          });
      }

      await transaction('copilot_enterprise_organization_directory')
        .insert({
          enterprise_id: enterpriseId,
          is_complete: true,
          last_synced_at: now,
          updated_at: now,
        })
        .onConflict('enterprise_id')
        .merge({
          is_complete: true,
          last_synced_at: now,
          updated_at: now,
        });
    });
  }

  async markEnterpriseOrganizationDirectoryFailed(
    enterpriseId: string,
    now: Date,
  ): Promise<void> {
    await this.db('copilot_enterprise_organization_directory')
      .insert({
        enterprise_id: enterpriseId,
        is_complete: false,
        updated_at: now,
      })
      .onConflict('enterprise_id')
      .merge({ is_complete: false, updated_at: now });
  }

  async markLegacyConfiguredOrganization(
    enterpriseId: string,
    organization: string,
    now: Date,
    nextAttemptAt?: Date,
    observedAt: Date = now,
  ): Promise<void> {
    const key = organization.trim().toLowerCase();
    const existingReport = await this.db('copilot_daily_totals')
      .where({
        metrics_type: 'organization',
        entity_id: organization,
        team_slug: '',
        cost_center_id: '',
        without_cost_center: false,
      })
      .first('day');

    const update: Record<string, unknown> = {
      organization_login: organization,
      access_state: 'accessible',
      reports_ready: Boolean(existingReport),
      last_attempted_at: now,
      updated_at: observedAt,
    };
    if (nextAttemptAt) {
      update.next_attempt_at = nextAttemptAt;
    }

    await this.db('copilot_enterprise_organizations')
      .where({
        enterprise_id: enterpriseId,
        organization_key: key,
        is_current: true,
      })
      .andWhere('updated_at', '<=', observedAt)
      .update(update);
  }

  async markEnterpriseOrganizationInaccessible(
    enterpriseId: string,
    organization: string,
    now: Date,
    nextAttemptAt: Date,
    observedAt: Date = now,
  ): Promise<void> {
    await this.updateEnterpriseOrganizationState(
      enterpriseId,
      organization,
      {
        accessState: 'inaccessible',
        reportsReady: false,
        lastAttemptedAt: now,
        nextAttemptAt,
      },
      observedAt,
    );
  }

  async getEnterpriseOrganizationDirectory(
    enterpriseId: string,
  ): Promise<EnterpriseOrganizationDirectory> {
    const [directory, rows] = await Promise.all([
      this.db('copilot_enterprise_organization_directory')
        .where({ enterprise_id: enterpriseId })
        .first('is_complete'),
      this.db('copilot_enterprise_organizations')
        .where({ enterprise_id: enterpriseId, is_current: true })
        .orderBy('organization_login', 'asc')
        .select('organization_login', 'access_state', 'reports_ready'),
    ]);

    const complete = isDatabaseBoolean(directory?.is_complete);
    if (!complete) {
      return {
        complete: false,
        hasPendingOrganizations: false,
        organizations: [],
      };
    }

    return {
      complete: true,
      hasPendingOrganizations: rows.some(
        row =>
          row.access_state === 'pending' ||
          (row.access_state === 'accessible' &&
            !isDatabaseBoolean(row.reports_ready)),
      ),
      organizations: rows
        .filter(
          row =>
            row.access_state === 'accessible' &&
            isDatabaseBoolean(row.reports_ready),
        )
        .map(row => row.organization_login),
    };
  }

  async getCurrentAccessibleEnterpriseOrganizationLogin(
    enterpriseId: string,
    organization: string,
  ): Promise<string | undefined> {
    const row = await this.db('copilot_enterprise_organizations as org')
      .join(
        'copilot_enterprise_organization_directory as directory',
        'directory.enterprise_id',
        'org.enterprise_id',
      )
      .where({
        'org.enterprise_id': enterpriseId,
        'org.organization_key': organization.trim().toLowerCase(),
        'org.is_current': true,
        'org.access_state': 'accessible',
        'directory.is_complete': true,
      })
      .first('org.organization_login');
    return typeof row?.organization_login === 'string'
      ? row.organization_login
      : undefined;
  }

  async getEnterpriseOrganizationsToIngest(
    enterpriseId: string,
    now: Date,
    limit: number,
    inaccessibleLimit: number,
    excludedOrganization?: string,
  ): Promise<string[]> {
    const dueRows = () => {
      let query = this.db('copilot_enterprise_organizations').where({
        enterprise_id: enterpriseId,
        is_current: true,
      });
      const excludedKey = excludedOrganization?.trim().toLowerCase();
      if (excludedKey) {
        query = query.whereNot('organization_key', excludedKey);
      }
      return query
        .where(function dueForAttempt() {
          this.whereNull('next_attempt_at').orWhere(
            'next_attempt_at',
            '<=',
            now,
          );
        })
        .orderByRaw('CASE WHEN last_attempted_at IS NULL THEN 0 ELSE 1 END ASC')
        .orderBy('last_attempted_at', 'asc')
        .orderBy('organization_key', 'asc');
    };

    const recheckCount = Math.min(limit, Math.max(0, inaccessibleLimit));
    const inaccessibleRows =
      recheckCount > 0
        ? await dueRows()
            .where('access_state', 'inaccessible')
            .limit(recheckCount)
            .select('organization_login')
        : [];
    const activeLimit = Math.max(0, limit - inaccessibleRows.length);
    const activeRows =
      activeLimit > 0
        ? await dueRows()
            .whereNot('access_state', 'inaccessible')
            .limit(activeLimit)
            .select('organization_login')
        : [];

    return [...activeRows, ...inaccessibleRows].map(
      row => row.organization_login,
    );
  }

  async withIngestionDay<T>(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    operation: (
      transaction: Knex.Transaction,
      existingLog: V2IngestionLogRow,
    ) => Promise<T>,
  ): Promise<T> {
    return this.db.transaction(async transaction => {
      const scope = {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
      };
      // Initialize before locking so first-time imports serialize as well.
      await transaction<V2IngestionLogRow>('copilot_ingestion_log')
        .insert({
          ...scope,
          status: 'partial',
          components_loaded: '[]',
          source: 'scheduled',
        })
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
      return operation(transaction, existing);
    });
  }

  async updateEnterpriseOrganizationState(
    enterpriseId: string,
    organization: string,
    state: {
      accessState?: EnterpriseOrganizationAccessState;
      reportsReady?: boolean;
      lastAttemptedAt: Date;
      nextAttemptAt?: Date | null;
    },
    observedAt: Date = state.lastAttemptedAt,
  ): Promise<void> {
    // Order access observations by request start, not by response arrival.
    const update: Record<string, unknown> = {
      last_attempted_at: state.lastAttemptedAt,
      updated_at: observedAt,
    };
    if (state.accessState !== undefined) {
      update.access_state = state.accessState;
    }
    if (state.reportsReady !== undefined) {
      update.reports_ready = state.reportsReady;
    }
    if (state.nextAttemptAt !== undefined) {
      update.next_attempt_at = state.nextAttemptAt;
    }

    await this.db('copilot_enterprise_organizations')
      .where({
        enterprise_id: enterpriseId,
        organization_key: organization.trim().toLowerCase(),
        is_current: true,
      })
      .andWhere('updated_at', '<=', observedAt)
      .update(update);
  }

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
    };
  }

  private validateTeamReplacementRows(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userTeams: V2UserTeamRow[],
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
  }

  async replaceTeamMemberships(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userTeams: V2UserTeamRow[],
    outerTransaction?: Knex.Transaction,
  ): Promise<void> {
    this.validateTeamReplacementRows(day, metricsType, entityId, userTeams);

    const replace = async (transaction: Knex.Transaction) => {
      await transaction('copilot_user_teams')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();
      await batchInsertInChunks(userTeams, 100, async chunk => {
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
    };

    if (outerTransaction) {
      await replace(outerTransaction);
    } else {
      await this.db.transaction(replace);
    }
  }

  async getBackfillStatus(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
  ): Promise<V2IngestionLogRow[]> {
    return this.getIngestionLog(metricsType, entityId, from, to);
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

  async insertDailyTotals(
    rows: V2DailyTotal[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2DailyTotal>('copilot_daily_totals')
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

  async insertPrMetrics(
    rows: V2PrMetricsRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2PrMetricsRow>('copilot_pr_metrics')
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

  async insertByFeature(
    rows: V2MetricsByFeatureRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByFeatureRow>(
          'copilot_metrics_by_feature',
        )
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

  async insertByIde(
    rows: V2MetricsByIdeRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByIdeRow>(
          'copilot_metrics_by_ide',
        )
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
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByLanguageFeatureRow>(
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
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByModelFeatureRow>(
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
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByLanguageModelRow>(
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

  async insertByCli(
    rows: V2MetricsByCliRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByCliRow>(
          'copilot_metrics_by_cli',
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

  async insertByVscodeAgent(
    rows: V2MetricsByVscodeAgentRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(
      this.withDefaultCostCenter(rows),
      100,
      async chunk => {
        await this.connection(transaction)<V2MetricsByVscodeAgentRow>(
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

  async insertCustomizationMetrics(
    rows: V2CustomizationMetricRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;

    const normalizedRows = rows.map(row => ({
      ...row,
      user_login: row.user_login.toLowerCase(),
    }));

    await batchInsertInChunks(normalizedRows, 100, async chunk => {
      await this.connection(transaction)<V2CustomizationMetricRow>(
        'copilot_customization_metrics',
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
          'item_name',
        ])
        .merge(['interaction_count', 'user_login']);
    });
  }

  async insertCustomizationSummaries(
    rows: V2CustomizationSummaryRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;

    const normalizedRows = rows.map(row => ({
      ...row,
      user_login: row.user_login.toLowerCase(),
    }));

    await batchInsertInChunks(normalizedRows, 100, async chunk => {
      await this.connection(transaction)<V2CustomizationSummaryRow>(
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

  async insertUserMetrics(
    rows: V2UserMetricRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) {
      return;
    }

    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricRow>(
        'copilot_user_metrics',
      )
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
   * Replaces a membership snapshot and its completion evidence atomically.
   */
  async replaceCostCenterMemberships(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userCostCenters: V2UserCostCenterRow[],
    unassignedComplete: boolean = true,
    outerTransaction?: Knex.Transaction,
  ): Promise<void> {
    if (
      userCostCenters.some(
        row =>
          row.day !== day ||
          row.metrics_type !== metricsType ||
          row.entity_id !== entityId ||
          !row.cost_center_id,
      )
    ) {
      throw new Error(
        'Cost-center replacement memberships must match the requested day and scope and have a cost center.',
      );
    }

    const replace = async (transaction: Knex.Transaction) => {
      await transaction('copilot_cost_center_aggregation_log')
        .insert({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          unassigned_complete: false,
          needs_reaggregation: false,
        })
        .onConflict(['day', 'metrics_type', 'entity_id'])
        .ignore();
      const previous = await transaction('copilot_cost_center_aggregation_log')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .forUpdate()
        .first();
      if (
        !unassignedComplete &&
        isDatabaseBoolean(previous?.unassigned_complete)
      ) {
        return;
      }
      await transaction('copilot_user_cost_centers')
        .where({ day, metrics_type: metricsType, entity_id: entityId })
        .del();

      await batchInsertInChunks(userCostCenters, 100, async chunk => {
        await transaction<V2UserCostCenterRow>(
          'copilot_user_cost_centers',
        ).insert(chunk);
      });

      await this.markCostCenterMembershipsDone(
        day,
        metricsType,
        entityId,
        unassignedComplete,
        transaction,
      );
    };

    if (outerTransaction) {
      await replace(outerTransaction);
    } else {
      await this.db.transaction(replace);
    }
  }

  async insertUserMetricsByFeature(
    rows: V2UserMetricsByFeatureRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricsByFeatureRow>(
        'copilot_user_metrics_by_feature',
      )
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id', 'feature'])
        .ignore();
    });
  }

  async insertUserMetricsByIde(
    rows: V2UserMetricsByIdeRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricsByIdeRow>(
        'copilot_user_metrics_by_ide',
      )
        .insert(chunk)
        .onConflict(['day', 'metrics_type', 'entity_id', 'user_id', 'ide'])
        .ignore();
    });
  }

  async insertUserMetricsByLanguageFeature(
    rows: V2UserMetricsByLanguageFeatureRow[],
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricsByLanguageFeatureRow>(
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
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricsByModelFeatureRow>(
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
    transaction?: Knex.Transaction,
  ): Promise<void> {
    if (!rows.length) return;
    await batchInsertInChunks(rows, 100, async chunk => {
      await this.connection(transaction)<V2UserMetricsByLanguageModelRow>(
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

  async upsertIngestionLog(
    row: V2IngestionLogRow,
    outerTransaction?: Knex.Transaction,
  ): Promise<void> {
    const upsert = async (transaction: Knex.Transaction) => {
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
          ...parseComponentsLoaded(existing.components_loaded),
          ...parseComponentsLoaded(row.components_loaded),
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
    };

    if (outerTransaction) {
      await upsert(outerTransaction);
    } else {
      await this.db.transaction(upsert);
    }
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

  async getDaysMissingCostCenterMemberships(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    transaction?: Knex.Transaction,
  ): Promise<string[]> {
    const connection = this.connection(transaction);
    const [metricRows, ingestionRows, completedRows] = await Promise.all([
      connection('copilot_user_metrics')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [from, to])
        .distinct('day')
        .select('day'),
      connection('copilot_ingestion_log')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [from, to])
        .select('day', 'components_loaded'),
      connection('copilot_cost_center_aggregation_log')
        .where({ metrics_type: metricsType, entity_id: entityId })
        // Consume legacy dirty snapshots once; new ingestion never sets this flag.
        .where('needs_reaggregation', false)
        .whereBetween('day', [from, to])
        .select('day'),
    ]);

    const daysWithUsers = new Set<string>();
    for (const row of metricRows) {
      const day = this.normalizeDay(row.day);
      if (day) {
        daysWithUsers.add(day);
      }
    }
    for (const row of ingestionRows) {
      if (!this.hasRequiredComponents(row.components_loaded, ['users'])) {
        continue;
      }

      const day = this.normalizeDay(row.day);
      if (day) {
        daysWithUsers.add(day);
      }
    }

    const completedDays = new Set(
      completedRows
        .map(row => this.normalizeDay(row.day))
        .filter((day): day is string => Boolean(day)),
    );

    return [...daysWithUsers].filter(day => !completedDays.has(day)).sort();
  }

  async getDaysWithIncompleteCostCenterMemberships(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    transaction?: Knex.Transaction,
  ): Promise<string[]> {
    const rows = await this.connection(transaction)(
      'copilot_cost_center_aggregation_log',
    )
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

  async markCostCenterMembershipsDone(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    unassignedComplete: boolean = true,
    transaction?: Knex.Transaction,
  ): Promise<void> {
    await this.connection(transaction)('copilot_cost_center_aggregation_log')
      .insert({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        unassigned_complete: unassignedComplete,
        needs_reaggregation: false,
        aggregated_at: this.db.fn.now(),
      })
      .onConflict(['day', 'metrics_type', 'entity_id'])
      .merge({
        unassigned_complete: this.db.raw('?? OR ?', [
          'copilot_cost_center_aggregation_log.unassigned_complete',
          unassignedComplete,
        ]),
        needs_reaggregation: false,
        aggregated_at: this.db.fn.now(),
      });
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2DailyTotal>(
        this.db,
        'daily',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.enrichScopedRollingActiveUsers(
        rows.map(row => this.normalizeAggregateRow(row)),
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
    }
    const query = this.db<V2DailyTotal>('copilot_daily_totals')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_daily_totals');

    const rows = (await query.orderBy('day', 'asc').select('*')).map(row =>
      this.normalizeAggregateRow(row),
    );

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
    let completeTeamDays: Set<string> | undefined;
    if (teamSlug) {
      const lowerBound = DateTime.fromISO(from, { zone: 'utc' })
        .minus({ days: MONTHLY_ACTIVE_USERS_WINDOW_DAYS - 1 })
        .toFormat('yyyy-MM-dd');
      const coverageRows = await this.db('copilot_ingestion_log')
        .where({ metrics_type: metricsType, entity_id: entityId })
        .whereBetween('day', [lowerBound, to])
        .select('day', 'components_loaded');
      completeTeamDays = new Set(
        coverageRows
          .filter(row =>
            this.hasRequiredComponents(row.components_loaded, [
              'users',
              'teams',
            ]),
          )
          .map(row => this.normalizeDay(row.day))
          .filter((day): day is string => Boolean(day)),
      );
    }

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
        completeTeamDays,
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

    const query = this.db('copilot_user_metrics as m');
    filterScopedUsers(this.db, query, 'm', teamSlug, costCenter);

    query
      .where('m.metrics_type', metricsType)
      .where('m.entity_id', entityId)
      .whereBetween('m.day', [fetchLowerBound, to]);

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
      completeTeamDays?: Set<string>;
      completeUnassignedDays?: Set<string>;
    },
  ): V2DailyTotal {
    const day = this.normalizeDay(row.day);
    if (!day) {
      return row;
    }

    const weeklyWindowComplete =
      (!params.completeTeamDays ||
        this.isRollingWindowComplete(
          params.completeTeamDays,
          day,
          params.weeklyWindowDays,
        )) &&
      (!params.completeUnassignedDays ||
        this.isRollingWindowComplete(
          params.completeUnassignedDays,
          day,
          params.weeklyWindowDays,
        ));
    const monthlyWindowComplete =
      (!params.completeTeamDays ||
        this.isRollingWindowComplete(
          params.completeTeamDays,
          day,
          params.monthlyWindowDays,
        )) &&
      (!params.completeUnassignedDays ||
        this.isRollingWindowComplete(
          params.completeUnassignedDays,
          day,
          params.monthlyWindowDays,
        ));

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
    cliByDay: Map<string, Set<number>>;
    vscodeAgentByDay: Map<string, Set<number>>;
  } {
    const activeByDay = new Map<string, Set<number>>();
    const agentByDay = new Map<string, Set<number>>();
    const chatByDay = new Map<string, Set<number>>();
    const cliByDay = new Map<string, Set<number>>();
    const vscodeAgentByDay = new Map<string, Set<number>>();

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
      if (row.used_cli) {
        this.addUserToDayMap(cliByDay, day, userId);
      }
      if (row.used_vscode_agent) {
        this.addUserToDayMap(vscodeAgentByDay, day, userId);
      }
    }

    return {
      activeByDay,
      agentByDay,
      chatByDay,
      cliByDay,
      vscodeAgentByDay,
    };
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
    if (teamSlug || costCenter) {
      return [];
    }
    const query = this.db<V2PrMetricsRow>('copilot_pr_metrics')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_pr_metrics');

    const rows = await query.orderBy('day', 'asc').select('*');
    return rows.map(row => this.normalizeAggregateRow(row));
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2MetricsByFeatureRow>(
        this.db,
        'feature',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'feature',
      );
    }
    const query = this.db<V2MetricsByFeatureRow>('copilot_metrics_by_feature')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_metrics_by_feature');

    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row => this.normalizeAggregateRow(row)),
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2MetricsByIdeRow>(
        this.db,
        'ide',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'ide',
      );
    }
    const query = this.db<V2MetricsByIdeRow>('copilot_metrics_by_ide')
      .where('metrics_type', metricsType)
      .where('entity_id', entityId)
      .whereBetween('day', [from, to]);

    query.where('team_slug', teamSlug ?? '');
    this.addCostCenterFilter(query, costCenter, 'copilot_metrics_by_ide');

    const rows = await query.orderBy('day', 'asc').select('*');
    return this.applyCostCenterBreakdownBucketing(
      rows.map(row => this.normalizeAggregateRow(row)),
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2MetricsByLanguageFeatureRow>(
        this.db,
        'language_feature',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
        feature,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'language-feature',
      );
    }
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
      rows.map(row => this.normalizeAggregateRow(row)),
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2MetricsByModelFeatureRow>(
        this.db,
        'model_feature',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'model-feature',
      );
    }
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
      rows.map(row => this.normalizeAggregateRow(row)),
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
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2MetricsByLanguageModelRow>(
        this.db,
        'language_model',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'language-model',
      );
    }
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
      rows.map(row => this.normalizeAggregateRow(row)),
      costCenter,
      'language-model',
    );
  }

  async getCustomizationMetrics(
    metricsType: MetricsScope,
    entityId: string,
    from: string,
    to: string,
    teamSlug?: string,
    costCenter?: CostCenterFilter,
  ): Promise<V2CustomizationMetricRow[]> {
    if (teamSlug || costCenter) {
      const rows = await readScopedMetrics<V2CustomizationMetricRow>(
        this.db,
        'customization',
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      );
      return this.applyCostCenterBreakdownBucketing(
        rows.map(row => this.normalizeAggregateRow(row)),
        costCenter,
        'customization',
      );
    }
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
    costCenter?: CostCenterFilter,
  ): Promise<V2CustomizationSummaryRow[]> {
    if (teamSlug || costCenter) {
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
      this.getCustomizationMetrics(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      ),
      this.getCustomizationSummaries(
        metricsType,
        entityId,
        from,
        to,
        teamSlug,
        costCenter,
      ),
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
    transaction?: Knex.Transaction,
  ): Promise<V2UserMetricRow[]> {
    const rows = await this.connection(transaction)<V2UserMetricRow>(
      'copilot_user_metrics',
    )
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
    transaction?: Knex.Transaction,
  ): Promise<UserBreakdownData[]> {
    const getRows = (tableName: string) =>
      this.connection(transaction)(tableName)
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
      this.connection(transaction)<V2CustomizationMetricRow>(
        'copilot_customization_metrics',
      )
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
    const returnedDays = new Set(dailyRows.map(row => row.day));
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

  private async getTeamsForEntities(
    metricsType: MetricsScope,
    entityIds: string[],
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<Array<{ entityId: string; team: string }>> {
    if (entityIds.length === 0) {
      return [];
    }

    const query = this.db('copilot_user_teams')
      .where('metrics_type', metricsType)
      .whereIn('entity_id', entityIds)
      .whereNot('team_slug', '');
    const database = this.db;
    query.whereExists(function activeTeamMember() {
      this.select(database.raw('1')).from('copilot_user_metrics as active');
      for (const column of ['day', 'metrics_type', 'entity_id', 'user_id']) {
        this.whereRaw('?? = ??', [
          `active.${column}`,
          `copilot_user_teams.${column}`,
        ]);
      }
    });
    if (from && to) {
      query.whereBetween('day', [from, to]);
    } else if (from) {
      query.where('day', '>=', from);
    } else if (to) {
      query.where('day', '<=', to);
    }

    const rows = await query
      .groupBy('entity_id', 'team_slug')
      .havingRaw('COUNT(DISTINCT ??) >= ?', ['user_id', minMembers])
      .orderBy('entity_id', 'asc')
      .orderBy('team_slug', 'asc')
      .select('entity_id', 'team_slug');
    return rows.map(row => ({
      entityId: row.entity_id,
      team: row.team_slug,
    }));
  }

  async getAccessibleOrganizationTeams(
    organizations: string[],
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<Array<{ organization: string; team: string }>> {
    const teams = await this.getTeamsForEntities(
      'organization',
      organizations,
      from,
      to,
      minMembers,
    );
    return teams.map(({ entityId, team }) => ({
      organization: entityId,
      team,
    }));
  }

  async getTeams(
    metricsType: MetricsScope,
    entityId: string,
    from?: string,
    to?: string,
    minMembers: number = 5,
  ): Promise<string[]> {
    const teams = await this.getTeamsForEntities(
      metricsType,
      [entityId],
      from,
      to,
      minMembers,
    );
    return teams.map(({ team }) => team);
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
    const query = this.db(
      costCenter ? 'copilot_user_metrics' : 'copilot_daily_totals',
    )
      .where('metrics_type', metricsType)
      .where('entity_id', entityId);
    if (costCenter) {
      filterScopedUsers(
        this.db,
        query,
        'copilot_user_metrics',
        undefined,
        costCenter,
      );
    } else {
      this.addCostCenterFilter(query, undefined, 'copilot_daily_totals');
    }

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
    const loadedComponents = new Set(parseComponentsLoaded(value));
    return requiredComponents.every(component =>
      loadedComponents.has(component),
    );
  }
}
