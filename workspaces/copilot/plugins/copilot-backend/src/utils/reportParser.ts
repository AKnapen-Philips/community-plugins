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

import {
  V2DailyTotal,
  V2CustomizationMetricRow,
  V2CustomizationSummaryRow,
  V2MetricsByCliRow,
  V2MetricsByFeatureRow,
  V2MetricsByIdeRow,
  V2MetricsByLanguageFeatureRow,
  V2MetricsByModelFeatureRow,
  V2MetricsByLanguageModelRow,
  V2MetricsByVscodeAgentRow,
  V2PrMetricsRow,
  V2UserMetric,
  V2UserMetricRow,
  V2UserTeam,
  V2UserTeamRow,
  MetricsScope,
} from '@backstage-community/plugin-copilot-common';

export interface ParsedEnterpriseDocument {
  dailyTotals: V2DailyTotal[];
  prMetrics: V2PrMetricsRow[];
  byFeature: V2MetricsByFeatureRow[];
  byIde: V2MetricsByIdeRow[];
  byLanguageFeature: V2MetricsByLanguageFeatureRow[];
  byModelFeature: V2MetricsByModelFeatureRow[];
  byLanguageModel: V2MetricsByLanguageModelRow[];
  byCli: V2MetricsByCliRow[];
  byVscodeAgent: V2MetricsByVscodeAgentRow[];
  customizationMetrics: V2CustomizationMetricRow[];
  customizationSummaries: V2CustomizationSummaryRow[];
}

/**
 * Per-user breakdown data (in-memory only; aggregated into team rows before DB storage).
 */
export interface UserBreakdownData {
  user_id: number;
  user_login: string;
  byFeature: Array<{
    feature: string;
    user_initiated_interaction_count: number;
    code_generation_activity_count: number;
    code_acceptance_activity_count: number;
    loc_added_sum: number;
    loc_deleted_sum: number;
    loc_suggested_to_add_sum: number;
    loc_suggested_to_delete_sum: number;
  }>;
  byIde: Array<{
    ide: string;
    user_initiated_interaction_count: number;
    code_generation_activity_count: number;
    code_acceptance_activity_count: number;
    loc_added_sum: number;
    loc_deleted_sum: number;
    loc_suggested_to_add_sum: number;
    loc_suggested_to_delete_sum: number;
  }>;
  byLanguageFeature: Array<{
    language: string;
    feature: string;
    code_generation_activity_count: number;
    code_acceptance_activity_count: number;
    loc_added_sum: number;
    loc_deleted_sum: number;
    loc_suggested_to_add_sum: number;
    loc_suggested_to_delete_sum: number;
  }>;
  byModelFeature: Array<{
    model_id: string;
    feature: string;
    user_initiated_interaction_count: number;
    code_generation_activity_count: number;
    code_acceptance_activity_count: number;
    loc_added_sum: number;
    loc_deleted_sum: number;
    loc_suggested_to_add_sum: number;
    loc_suggested_to_delete_sum: number;
  }>;
  byLanguageModel: Array<{
    language: string;
    model_id: string;
    request_count: number;
    code_generation_activity_count: number;
    code_acceptance_activity_count: number;
    loc_added_sum: number;
    loc_deleted_sum: number;
    loc_suggested_to_add_sum: number;
    loc_suggested_to_delete_sum: number;
  }>;
  customizations: Array<{
    category: string;
    item_name: string;
    interaction_count: number;
  }>;
}

export interface ParsedUserDocument {
  userMetrics: V2UserMetricRow[];
  userBreakdowns: UserBreakdownData[];
  customizationMetrics: V2CustomizationMetricRow[];
  customizationSummaries: V2CustomizationSummaryRow[];
}

function logWarn(message: string): void {
  // eslint-disable-next-line no-console
  console.warn(`[reportParser] ${message}`);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toNumber(value: unknown, strict = false, context = 'report'): number {
  if (value === undefined || value === null) {
    return 0;
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (strict) {
    throw new Error(`Invalid numeric ${context} value`);
  }
  return 0;
}

function toStringValue(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

type UserReportParseOptions = {
  strict?: boolean;
  expectedDay?: string;
};

function toBoolean(value: unknown, strict: boolean, context: string): boolean {
  if (value === undefined) {
    return false;
  }
  if (typeof value === 'boolean') {
    return value;
  }
  if (strict) {
    throw new Error(`Invalid boolean ${context} value`);
  }
  return Boolean(value);
}

function reportRows(
  value: unknown,
  strict: boolean,
  kind: string,
  day?: string,
): unknown[] {
  if (value === undefined || value === null) {
    return [];
  }
  if (Array.isArray(value)) {
    return value;
  }
  if (strict) {
    throw new Error(`Invalid ${kind} breakdown for ${day ?? 'report day'}`);
  }
  return [];
}

type CustomizationContext = {
  day: string;
  metrics_type: MetricsScope;
  entity_id: string;
  scope: 'aggregate' | 'user';
  user_id?: number;
  user_login?: string;
};

const TYPED_BREAKDOWN_CATEGORIES = new Set([
  'feature',
  'ide',
  'language_feature',
  'model_feature',
  'language_model',
]);

function isCustomizationCategory(category: string): boolean {
  return /^[a-z][a-z0-9_]{0,63}$/.test(category);
}

/**
 * Parses GitHub's extensible named-customization arrays. A category is
 * recognized only when entries follow the `totals_by_<category>` convention
 * and provide both the matching name field and interaction_count. This keeps
 * the generic path separate from existing typed report dimensions.
 */
function parseCustomizationData(
  record: Record<string, unknown>,
  context: CustomizationContext,
  options: UserReportParseOptions = {},
): {
  metrics: V2CustomizationMetricRow[];
  summaries: V2CustomizationSummaryRow[];
} {
  const strict = options.strict ?? false;
  const day = options.expectedDay ?? context.day;
  const metrics: V2CustomizationMetricRow[] = [];
  const summaries: V2CustomizationSummaryRow[] = [];
  const categories = new Set<string>();

  for (const [field, value] of Object.entries(record)) {
    if (!field.startsWith('totals_by_') || !Array.isArray(value)) {
      continue;
    }

    const category = field.slice('totals_by_'.length);
    if (
      !isCustomizationCategory(category) ||
      TYPED_BREAKDOWN_CATEGORIES.has(category)
    ) {
      continue;
    }

    for (const [index, row] of value.entries()) {
      if (!isRecord(row)) {
        if (strict) {
          throw new Error(`Invalid customization row ${index + 1} for ${day}`);
        }
        continue;
      }

      const itemName = toStringValue(row[category])?.trim();
      const interactionCount = row.interaction_count;
      if (
        !itemName ||
        itemName.length > 128 ||
        typeof interactionCount !== 'number' ||
        !Number.isFinite(interactionCount)
      ) {
        if (strict) {
          throw new Error(
            `Invalid ${category} customization row ${index + 1} for ${day}`,
          );
        }
        continue;
      }

      categories.add(category);
      metrics.push({
        day: context.day,
        metrics_type: context.metrics_type,
        entity_id: context.entity_id,
        scope: context.scope,
        team_slug: '',
        user_id: context.user_id ?? 0,
        user_login: context.user_login ?? '',
        category,
        item_name: itemName,
        interaction_count: interactionCount,
      });
    }
  }

  for (const [field, value] of Object.entries(record)) {
    const match = /^distinct_([a-z][a-z0-9_]*)_use_count$/.exec(field);
    if (!match || !isCustomizationCategory(match[1])) {
      continue;
    }

    // Keep summary and item data aligned. An absent item list means GitHub
    // did not make that customization category available for this record.
    if (!categories.has(match[1])) {
      continue;
    }

    if (typeof value !== 'number' || !Number.isFinite(value)) {
      if (strict) {
        throw new Error(`Invalid ${match[1]} customization summary for ${day}`);
      }
      continue;
    }

    summaries.push({
      day: context.day,
      metrics_type: context.metrics_type,
      entity_id: context.entity_id,
      scope: context.scope,
      team_slug: '',
      user_id: context.user_id ?? 0,
      user_login: context.user_login ?? '',
      category: match[1],
      distinct_item_count: value,
    });
  }

  return { metrics, summaries };
}

/**
 * Parse an enterprise report document downloaded from a signed enterprise-1-day URL.
 * The GitHub report API (2026-03-10) returns a single flat V2EnterpriseDayTotal object
 * per file. Both flat objects and legacy V2EnterpriseDocument arrays (with a day_totals
 * wrapper) are accepted for robustness.
 */
export function parseEnterpriseDocument(
  doc: unknown,
  metricsType: MetricsScope,
  entityId: string,
): ParsedEnterpriseDocument {
  const parsed: ParsedEnterpriseDocument = {
    dailyTotals: [],
    prMetrics: [],
    byFeature: [],
    byIde: [],
    byLanguageFeature: [],
    byModelFeature: [],
    byLanguageModel: [],
    byCli: [],
    byVscodeAgent: [],
    customizationMetrics: [],
    customizationSummaries: [],
  };

  // Normalize: the GitHub report API (2026-03-10) downloads a single flat
  // V2EnterpriseDayTotal object per file — not a V2EnterpriseDocument with a
  // day_totals wrapper. Accept both shapes for robustness.
  let rawDocs: unknown[];
  if (Array.isArray(doc)) {
    rawDocs = doc;
  } else if (isRecord(doc)) {
    rawDocs = [doc];
  } else {
    rawDocs = [];
  }

  if (rawDocs.length === 0) {
    logWarn(
      'Expected enterprise document object or array, received invalid or empty payload.',
    );
    return parsed;
  }

  for (const rawDoc of rawDocs) {
    // Unwrap V2EnterpriseDocument (day_totals wrapper) if present;
    // otherwise treat the item itself as the day's metrics object.
    if (!isRecord(rawDoc)) {
      logWarn('Skipping enterprise document item because it is not an object.');
      continue;
    }

    const dayTotals: unknown[] = Array.isArray(rawDoc.day_totals)
      ? (rawDoc.day_totals as unknown[])
      : [rawDoc];

    for (const dayTotal of dayTotals) {
      if (!isRecord(dayTotal)) {
        logWarn(
          'Skipping enterprise document item because it is not an object.',
        );
        continue;
      }

      const day = toStringValue(dayTotal.day);
      if (!day) {
        logWarn('Skipping enterprise document item with missing day field.');
        continue;
      }

      parsed.dailyTotals.push({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        team_slug: '',
        daily_active_users: toNumber(dayTotal.daily_active_users),
        weekly_active_users: toNumber(dayTotal.weekly_active_users),
        monthly_active_users: toNumber(dayTotal.monthly_active_users),
        daily_active_cli_users: toNumber(dayTotal.daily_active_cli_users),
        daily_active_vscode_agent_users: toNumber(
          dayTotal.daily_active_vscode_agent_users,
        ),
        monthly_active_agent_users: toNumber(
          dayTotal.monthly_active_agent_users,
        ),
        monthly_active_chat_users: toNumber(dayTotal.monthly_active_chat_users),
        code_acceptance_activity_count: toNumber(
          dayTotal.code_acceptance_activity_count,
        ),
        code_generation_activity_count: toNumber(
          dayTotal.code_generation_activity_count,
        ),
        loc_added_sum: toNumber(dayTotal.loc_added_sum),
        loc_deleted_sum: toNumber(dayTotal.loc_deleted_sum),
        loc_suggested_to_add_sum: toNumber(dayTotal.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: toNumber(
          dayTotal.loc_suggested_to_delete_sum,
        ),
        user_initiated_interaction_count: toNumber(
          dayTotal.user_initiated_interaction_count,
        ),
      });

      const customizationData = parseCustomizationData(dayTotal, {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        scope: 'aggregate',
      });
      parsed.customizationMetrics.push(...customizationData.metrics);
      parsed.customizationSummaries.push(...customizationData.summaries);

      if (isRecord(dayTotal.pull_requests)) {
        parsed.prMetrics.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          total_created: toNumber(dayTotal.pull_requests.total_created),
          total_merged: toNumber(dayTotal.pull_requests.total_merged),
          total_reviewed: toNumber(dayTotal.pull_requests.total_reviewed),
          total_created_by_copilot: toNumber(
            dayTotal.pull_requests.total_created_by_copilot,
          ),
          total_merged_created_by_copilot: toNumber(
            dayTotal.pull_requests.total_merged_created_by_copilot,
          ),
          total_merged_reviewed_by_copilot: toNumber(
            dayTotal.pull_requests.total_merged_reviewed_by_copilot,
          ),
          total_reviewed_by_copilot: toNumber(
            dayTotal.pull_requests.total_reviewed_by_copilot,
          ),
          total_suggestions: toNumber(dayTotal.pull_requests.total_suggestions),
          total_applied_suggestions: toNumber(
            dayTotal.pull_requests.total_applied_suggestions,
          ),
          total_copilot_suggestions: toNumber(
            dayTotal.pull_requests.total_copilot_suggestions,
          ),
          total_copilot_applied_suggestions: toNumber(
            dayTotal.pull_requests.total_copilot_applied_suggestions,
          ),
          median_minutes_to_merge: toNumber(
            dayTotal.pull_requests.median_minutes_to_merge,
          ),
          median_minutes_to_merge_copilot_authored: toNumber(
            dayTotal.pull_requests.median_minutes_to_merge_copilot_authored,
          ),
          median_minutes_to_merge_copilot_reviewed: toNumber(
            dayTotal.pull_requests.median_minutes_to_merge_copilot_reviewed,
          ),
        });
      }

      if (Array.isArray(dayTotal.totals_by_feature)) {
        for (const row of dayTotal.totals_by_feature) {
          if (!isRecord(row) || !toStringValue(row.feature)) {
            logWarn('Skipping totals_by_feature row with missing feature.');
            continue;
          }

          parsed.byFeature.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            team_slug: '',
            feature: toStringValue(row.feature)!,
            code_acceptance_activity_count: toNumber(
              row.code_acceptance_activity_count,
            ),
            code_generation_activity_count: toNumber(
              row.code_generation_activity_count,
            ),
            loc_added_sum: toNumber(row.loc_added_sum),
            loc_deleted_sum: toNumber(row.loc_deleted_sum),
            loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
            loc_suggested_to_delete_sum: toNumber(
              row.loc_suggested_to_delete_sum,
            ),
            user_initiated_interaction_count: toNumber(
              row.user_initiated_interaction_count,
            ),
          });
        }
      }

      if (Array.isArray(dayTotal.totals_by_ide)) {
        for (const row of dayTotal.totals_by_ide) {
          if (!isRecord(row) || !toStringValue(row.ide)) {
            logWarn('Skipping totals_by_ide row with missing ide.');
            continue;
          }

          parsed.byIde.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            team_slug: '',
            ide: toStringValue(row.ide)!,
            code_acceptance_activity_count: toNumber(
              row.code_acceptance_activity_count,
            ),
            code_generation_activity_count: toNumber(
              row.code_generation_activity_count,
            ),
            loc_added_sum: toNumber(row.loc_added_sum),
            loc_deleted_sum: toNumber(row.loc_deleted_sum),
            loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
            loc_suggested_to_delete_sum: toNumber(
              row.loc_suggested_to_delete_sum,
            ),
            user_initiated_interaction_count: toNumber(
              row.user_initiated_interaction_count,
            ),
          });
        }
      }

      if (Array.isArray(dayTotal.totals_by_language_feature)) {
        for (const row of dayTotal.totals_by_language_feature) {
          if (
            !isRecord(row) ||
            !toStringValue(row.language) ||
            !toStringValue(row.feature)
          ) {
            logWarn(
              'Skipping totals_by_language_feature row with missing language or feature.',
            );
            continue;
          }

          parsed.byLanguageFeature.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            team_slug: '',
            language: toStringValue(row.language)!,
            feature: toStringValue(row.feature)!,
            code_acceptance_activity_count: toNumber(
              row.code_acceptance_activity_count,
            ),
            code_generation_activity_count: toNumber(
              row.code_generation_activity_count,
            ),
            loc_added_sum: toNumber(row.loc_added_sum),
            loc_deleted_sum: toNumber(row.loc_deleted_sum),
            loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
            loc_suggested_to_delete_sum: toNumber(
              row.loc_suggested_to_delete_sum,
            ),
          });
        }
      }

      if (Array.isArray(dayTotal.totals_by_model_feature)) {
        for (const row of dayTotal.totals_by_model_feature) {
          // API uses "model" field name, not "model_id"
          if (
            !isRecord(row) ||
            !toStringValue(row.model) ||
            !toStringValue(row.feature)
          ) {
            logWarn(
              'Skipping totals_by_model_feature row with missing model or feature.',
            );
            continue;
          }
          parsed.byModelFeature.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            team_slug: '',
            model_id: toStringValue(row.model) as string,
            feature: toStringValue(row.feature) as string,
            user_initiated_interaction_count: toNumber(
              row.user_initiated_interaction_count,
            ),
            code_generation_activity_count: toNumber(
              row.code_generation_activity_count,
            ),
            code_acceptance_activity_count: toNumber(
              row.code_acceptance_activity_count,
            ),
            loc_added_sum: toNumber(row.loc_added_sum),
            loc_deleted_sum: toNumber(row.loc_deleted_sum),
            loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
            loc_suggested_to_delete_sum: toNumber(
              row.loc_suggested_to_delete_sum,
            ),
          });
        }
      }

      if (Array.isArray(dayTotal.totals_by_language_model)) {
        for (const row of dayTotal.totals_by_language_model) {
          // API uses "model" field name, not "model_id"; no "request_count" — use code_generation_activity_count
          if (
            !isRecord(row) ||
            !toStringValue(row.language) ||
            !toStringValue(row.model)
          ) {
            logWarn(
              'Skipping totals_by_language_model row with missing language or model.',
            );
            continue;
          }
          parsed.byLanguageModel.push({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            team_slug: '',
            language: toStringValue(row.language) as string,
            model_id: toStringValue(row.model) as string,
            request_count: toNumber(row.code_generation_activity_count),
            code_generation_activity_count: toNumber(
              row.code_generation_activity_count,
            ),
            code_acceptance_activity_count: toNumber(
              row.code_acceptance_activity_count,
            ),
            loc_added_sum: toNumber(row.loc_added_sum),
            loc_deleted_sum: toNumber(row.loc_deleted_sum),
            loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
            loc_suggested_to_delete_sum: toNumber(
              row.loc_suggested_to_delete_sum,
            ),
          });
        }
      }

      if (isRecord(dayTotal.totals_by_cli)) {
        const cli = dayTotal.totals_by_cli;
        parsed.byCli.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          prompt_count: toNumber(cli.prompt_count),
          request_count: toNumber(cli.request_count),
          session_count: toNumber(cli.session_count),
          avg_tokens_per_request: toNumber(
            isRecord(cli.token_usage)
              ? cli.token_usage.avg_tokens_per_request
              : undefined,
          ),
          output_tokens_sum: toNumber(
            isRecord(cli.token_usage)
              ? cli.token_usage.output_tokens_sum
              : undefined,
          ),
          prompt_tokens_sum: toNumber(
            isRecord(cli.token_usage)
              ? cli.token_usage.prompt_tokens_sum
              : undefined,
          ),
        });
      }

      if (isRecord(dayTotal.totals_by_vscode_agent)) {
        const vscodeAgent = dayTotal.totals_by_vscode_agent;
        parsed.byVscodeAgent.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          session_count: toNumber(vscodeAgent.session_count),
          total_user_messages: toNumber(vscodeAgent.total_user_messages),
        });
      }
    }
  }

  return parsed;
}

/**
 * Parse an organization report document (single flat object per downloaded file).
 * Unlike enterprise documents, org documents are not wrapped in an array with day_totals;
 * the downloaded file IS the day's metrics object. LOC totals are not present at the top
 * level and must be summed from totals_by_ide when absent.
 */
export function parseOrganizationDocument(
  doc: unknown,
  metricsType: MetricsScope,
  entityId: string,
): ParsedEnterpriseDocument {
  const parsed: ParsedEnterpriseDocument = {
    dailyTotals: [],
    prMetrics: [],
    byFeature: [],
    byIde: [],
    byLanguageFeature: [],
    byModelFeature: [],
    byLanguageModel: [],
    byCli: [],
    byVscodeAgent: [],
    customizationMetrics: [],
    customizationSummaries: [],
  };

  // Normalize: the API downloads a single flat object per file, but accept arrays too.
  let orgDocs: unknown[];
  if (Array.isArray(doc)) {
    orgDocs = doc;
  } else if (isRecord(doc)) {
    orgDocs = [doc];
  } else {
    orgDocs = [];
  }

  if (orgDocs.length === 0) {
    logWarn('Expected organization document object, received invalid payload.');
    return parsed;
  }

  for (const orgDoc of orgDocs) {
    if (!isRecord(orgDoc)) {
      logWarn(
        'Skipping organization document item because it is not an object.',
      );
      continue;
    }

    const day = toStringValue(orgDoc.day);
    if (!day) {
      logWarn('Skipping organization document item with missing day field.');
      continue;
    }

    const ideRows = Array.isArray(orgDoc.totals_by_ide)
      ? orgDoc.totals_by_ide
      : [];

    // LOC fields are not present at the top level in org documents — sum from IDE rows.
    const hasTopLevelLoc = orgDoc.loc_added_sum !== undefined;
    let locAddedSum = hasTopLevelLoc ? toNumber(orgDoc.loc_added_sum) : 0;
    let locDeletedSum = hasTopLevelLoc ? toNumber(orgDoc.loc_deleted_sum) : 0;
    let locSuggestedToAddSum = hasTopLevelLoc
      ? toNumber(orgDoc.loc_suggested_to_add_sum)
      : 0;
    let locSuggestedToDeleteSum = hasTopLevelLoc
      ? toNumber(orgDoc.loc_suggested_to_delete_sum)
      : 0;

    if (!hasTopLevelLoc) {
      for (const ideRow of ideRows) {
        if (isRecord(ideRow)) {
          locAddedSum += toNumber(ideRow.loc_added_sum);
          locDeletedSum += toNumber(ideRow.loc_deleted_sum);
          locSuggestedToAddSum += toNumber(ideRow.loc_suggested_to_add_sum);
          locSuggestedToDeleteSum += toNumber(
            ideRow.loc_suggested_to_delete_sum,
          );
        }
      }
    }

    parsed.dailyTotals.push({
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      team_slug: '',
      daily_active_users: toNumber(orgDoc.daily_active_users),
      weekly_active_users: toNumber(orgDoc.weekly_active_users),
      monthly_active_users: toNumber(orgDoc.monthly_active_users),
      daily_active_cli_users: toNumber(orgDoc.daily_active_cli_users),
      daily_active_vscode_agent_users: toNumber(
        orgDoc.daily_active_vscode_agent_users,
      ),
      monthly_active_agent_users: toNumber(orgDoc.monthly_active_agent_users),
      monthly_active_chat_users: toNumber(orgDoc.monthly_active_chat_users),
      code_acceptance_activity_count: toNumber(
        orgDoc.code_acceptance_activity_count,
      ),
      code_generation_activity_count: toNumber(
        orgDoc.code_generation_activity_count,
      ),
      loc_added_sum: locAddedSum,
      loc_deleted_sum: locDeletedSum,
      loc_suggested_to_add_sum: locSuggestedToAddSum,
      loc_suggested_to_delete_sum: locSuggestedToDeleteSum,
      user_initiated_interaction_count: toNumber(
        orgDoc.user_initiated_interaction_count,
      ),
    });

    const customizationData = parseCustomizationData(orgDoc, {
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      scope: 'aggregate',
    });
    parsed.customizationMetrics.push(...customizationData.metrics);
    parsed.customizationSummaries.push(...customizationData.summaries);

    // No pull_requests field at the org level.

    if (Array.isArray(orgDoc.totals_by_feature)) {
      for (const row of orgDoc.totals_by_feature) {
        if (!isRecord(row) || !toStringValue(row.feature)) {
          logWarn('Skipping totals_by_feature row with missing feature.');
          continue;
        }

        parsed.byFeature.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          feature: toStringValue(row.feature) as string,
          code_acceptance_activity_count: toNumber(
            row.code_acceptance_activity_count,
          ),
          code_generation_activity_count: toNumber(
            row.code_generation_activity_count,
          ),
          loc_added_sum: toNumber(row.loc_added_sum),
          loc_deleted_sum: toNumber(row.loc_deleted_sum),
          loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
          loc_suggested_to_delete_sum: toNumber(
            row.loc_suggested_to_delete_sum,
          ),
          user_initiated_interaction_count: toNumber(
            row.user_initiated_interaction_count,
          ),
        });
      }
    }

    for (const row of ideRows) {
      if (!isRecord(row) || !toStringValue(row.ide)) {
        logWarn('Skipping totals_by_ide row with missing ide.');
        continue;
      }

      parsed.byIde.push({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        team_slug: '',
        ide: toStringValue(row.ide) as string,
        code_acceptance_activity_count: toNumber(
          row.code_acceptance_activity_count,
        ),
        code_generation_activity_count: toNumber(
          row.code_generation_activity_count,
        ),
        loc_added_sum: toNumber(row.loc_added_sum),
        loc_deleted_sum: toNumber(row.loc_deleted_sum),
        loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: toNumber(row.loc_suggested_to_delete_sum),
        user_initiated_interaction_count: toNumber(
          row.user_initiated_interaction_count,
        ),
      });
    }

    if (Array.isArray(orgDoc.totals_by_language_feature)) {
      for (const row of orgDoc.totals_by_language_feature) {
        if (
          !isRecord(row) ||
          !toStringValue(row.language) ||
          !toStringValue(row.feature)
        ) {
          logWarn(
            'Skipping totals_by_language_feature row with missing language or feature.',
          );
          continue;
        }

        parsed.byLanguageFeature.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          language: toStringValue(row.language) as string,
          feature: toStringValue(row.feature) as string,
          code_acceptance_activity_count: toNumber(
            row.code_acceptance_activity_count,
          ),
          code_generation_activity_count: toNumber(
            row.code_generation_activity_count,
          ),
          loc_added_sum: toNumber(row.loc_added_sum),
          loc_deleted_sum: toNumber(row.loc_deleted_sum),
          loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
          loc_suggested_to_delete_sum: toNumber(
            row.loc_suggested_to_delete_sum,
          ),
        });
      }
    }

    if (Array.isArray(orgDoc.totals_by_model_feature)) {
      for (const row of orgDoc.totals_by_model_feature) {
        // API uses "model" field name, not "model_id"
        if (
          !isRecord(row) ||
          !toStringValue(row.model) ||
          !toStringValue(row.feature)
        ) {
          logWarn(
            'Skipping totals_by_model_feature row with missing model or feature.',
          );
          continue;
        }
        parsed.byModelFeature.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          model_id: toStringValue(row.model) as string,
          feature: toStringValue(row.feature) as string,
          user_initiated_interaction_count: toNumber(
            row.user_initiated_interaction_count,
          ),
          code_generation_activity_count: toNumber(
            row.code_generation_activity_count,
          ),
          code_acceptance_activity_count: toNumber(
            row.code_acceptance_activity_count,
          ),
          loc_added_sum: toNumber(row.loc_added_sum),
          loc_deleted_sum: toNumber(row.loc_deleted_sum),
          loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
          loc_suggested_to_delete_sum: toNumber(
            row.loc_suggested_to_delete_sum,
          ),
        });
      }
    }

    if (Array.isArray(orgDoc.totals_by_language_model)) {
      for (const row of orgDoc.totals_by_language_model) {
        // API uses "model" field name, not "model_id"; no "request_count" — use code_generation_activity_count
        if (
          !isRecord(row) ||
          !toStringValue(row.language) ||
          !toStringValue(row.model)
        ) {
          logWarn(
            'Skipping totals_by_language_model row with missing language or model.',
          );
          continue;
        }
        parsed.byLanguageModel.push({
          day,
          metrics_type: metricsType,
          entity_id: entityId,
          team_slug: '',
          language: toStringValue(row.language) as string,
          model_id: toStringValue(row.model) as string,
          request_count: toNumber(row.code_generation_activity_count),
          code_generation_activity_count: toNumber(
            row.code_generation_activity_count,
          ),
          code_acceptance_activity_count: toNumber(
            row.code_acceptance_activity_count,
          ),
          loc_added_sum: toNumber(row.loc_added_sum),
          loc_deleted_sum: toNumber(row.loc_deleted_sum),
          loc_suggested_to_add_sum: toNumber(row.loc_suggested_to_add_sum),
          loc_suggested_to_delete_sum: toNumber(
            row.loc_suggested_to_delete_sum,
          ),
        });
      }
    }

    if (isRecord(orgDoc.totals_by_cli)) {
      const cli = orgDoc.totals_by_cli;
      parsed.byCli.push({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        team_slug: '',
        prompt_count: toNumber(cli.prompt_count),
        request_count: toNumber(cli.request_count),
        session_count: toNumber(cli.session_count),
        avg_tokens_per_request: toNumber(
          isRecord(cli.token_usage)
            ? cli.token_usage.avg_tokens_per_request
            : undefined,
        ),
        output_tokens_sum: toNumber(
          isRecord(cli.token_usage)
            ? cli.token_usage.output_tokens_sum
            : undefined,
        ),
        prompt_tokens_sum: toNumber(
          isRecord(cli.token_usage)
            ? cli.token_usage.prompt_tokens_sum
            : undefined,
        ),
      });
    }

    if (isRecord(orgDoc.totals_by_vscode_agent)) {
      const vscodeAgent = orgDoc.totals_by_vscode_agent;
      parsed.byVscodeAgent.push({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        team_slug: '',
        session_count: toNumber(vscodeAgent.session_count),
        total_user_messages: toNumber(vscodeAgent.total_user_messages),
      });
    }
  }

  return parsed;
}

/**
 * Parse a users-1-day document (flat array of user metric objects).
 */
export function parseUserDocument(
  doc: unknown,
  metricsType: MetricsScope,
  entityId: string,
  options: UserReportParseOptions = {},
): ParsedUserDocument {
  const strict = options.strict ?? false;
  const expectedDay = options.expectedDay;
  if (!Array.isArray(doc)) {
    if (strict) {
      throw new Error(
        `Invalid user report document for ${expectedDay ?? 'report day'}`,
      );
    }
    logWarn(
      'Expected user metrics document array, received non-array payload.',
    );
    return {
      userMetrics: [],
      userBreakdowns: [],
      customizationMetrics: [],
      customizationSummaries: [],
    };
  }

  const userMetrics: V2UserMetricRow[] = [];
  const userBreakdowns: UserBreakdownData[] = [];
  const customizationMetrics: V2CustomizationMetricRow[] = [];
  const customizationSummaries: V2CustomizationSummaryRow[] = [];

  for (const [rowIndex, metric] of (doc as V2UserMetric[]).entries()) {
    const rowContext = `user report row ${rowIndex + 1} for ${
      expectedDay ?? 'report day'
    }`;
    if (!isRecord(metric)) {
      if (strict) {
        throw new Error(`Invalid ${rowContext}`);
      }
      logWarn('Skipping user metrics row because it is not an object.');
      continue;
    }

    const day = toStringValue(metric.day);
    const userLogin = toStringValue(metric.user_login);
    const userId = toNumber(metric.user_id, strict, rowContext);

    if (
      !day ||
      !userLogin ||
      userId <= 0 ||
      (strict && expectedDay !== undefined && day !== expectedDay)
    ) {
      if (strict) {
        throw new Error(`Invalid ${rowContext}`);
      }
      logWarn('Skipping user metrics row with missing required fields.');
      continue;
    }

    const userReportNumber = (value: unknown) =>
      toNumber(value, strict, rowContext);
    const userReportBoolean = (value: unknown) =>
      toBoolean(value, strict, rowContext);
    userMetrics.push({
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      user_id: userId,
      user_login: userLogin,
      used_agent: userReportBoolean(metric.used_agent),
      used_chat: userReportBoolean(metric.used_chat),
      used_cli: userReportBoolean(metric.used_cli),
      used_vscode_agent: userReportBoolean(metric.used_vscode_agent),
      code_acceptance_activity_count: userReportNumber(
        metric.code_acceptance_activity_count,
      ),
      code_generation_activity_count: userReportNumber(
        metric.code_generation_activity_count,
      ),
      loc_added_sum: userReportNumber(metric.loc_added_sum),
      loc_deleted_sum: userReportNumber(metric.loc_deleted_sum),
      loc_suggested_to_add_sum: userReportNumber(
        metric.loc_suggested_to_add_sum,
      ),
      loc_suggested_to_delete_sum: userReportNumber(
        metric.loc_suggested_to_delete_sum,
      ),
      user_initiated_interaction_count: userReportNumber(
        metric.user_initiated_interaction_count,
      ),
      ai_credits_used: userReportNumber(metric.ai_credits_used),
    });

    const customizationData = parseCustomizationData(
      metric,
      {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        scope: 'user',
        user_id: userId,
        user_login: userLogin,
      },
      { strict, expectedDay: expectedDay ?? day },
    );
    customizationMetrics.push(...customizationData.metrics);
    customizationSummaries.push(...customizationData.summaries);

    const breakdown: UserBreakdownData = {
      user_id: userId,
      user_login: userLogin,
      byFeature: [],
      byIde: [],
      byLanguageFeature: [],
      byModelFeature: [],
      byLanguageModel: [],
      customizations: customizationData.metrics.map(row => ({
        category: row.category,
        item_name: row.item_name,
        interaction_count: row.interaction_count,
      })),
    };

    for (const [index, row] of reportRows(
      metric.totals_by_feature,
      strict,
      'feature',
      day,
    ).entries()) {
      if (!isRecord(row) || !toStringValue(row.feature)) {
        if (strict) {
          throw new Error(
            `Invalid feature breakdown row ${index + 1} for ${day}`,
          );
        }
        continue;
      }
      const number = (value: unknown) =>
        toNumber(
          value,
          strict,
          `feature breakdown row ${index + 1} for ${day}`,
        );
      breakdown.byFeature.push({
        feature: toStringValue(row.feature) as string,
        user_initiated_interaction_count: number(
          row.user_initiated_interaction_count,
        ),
        code_generation_activity_count: number(
          row.code_generation_activity_count,
        ),
        code_acceptance_activity_count: number(
          row.code_acceptance_activity_count,
        ),
        loc_added_sum: number(row.loc_added_sum),
        loc_deleted_sum: number(row.loc_deleted_sum),
        loc_suggested_to_add_sum: number(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: number(row.loc_suggested_to_delete_sum),
      });
    }

    for (const [index, row] of reportRows(
      metric.totals_by_ide,
      strict,
      'IDE',
      day,
    ).entries()) {
      if (!isRecord(row) || !toStringValue(row.ide)) {
        if (strict) {
          throw new Error(`Invalid IDE breakdown row ${index + 1} for ${day}`);
        }
        continue;
      }
      const number = (value: unknown) =>
        toNumber(value, strict, `IDE breakdown row ${index + 1} for ${day}`);
      breakdown.byIde.push({
        ide: toStringValue(row.ide) as string,
        user_initiated_interaction_count: number(
          row.user_initiated_interaction_count,
        ),
        code_generation_activity_count: number(
          row.code_generation_activity_count,
        ),
        code_acceptance_activity_count: number(
          row.code_acceptance_activity_count,
        ),
        loc_added_sum: number(row.loc_added_sum),
        loc_deleted_sum: number(row.loc_deleted_sum),
        loc_suggested_to_add_sum: number(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: number(row.loc_suggested_to_delete_sum),
      });
    }

    for (const [index, row] of reportRows(
      metric.totals_by_language_feature,
      strict,
      'language-feature',
      day,
    ).entries()) {
      if (
        !isRecord(row) ||
        !toStringValue(row.language) ||
        !toStringValue(row.feature)
      ) {
        if (strict) {
          throw new Error(
            `Invalid language-feature breakdown row ${index + 1} for ${day}`,
          );
        }
        continue;
      }
      const number = (value: unknown) =>
        toNumber(
          value,
          strict,
          `language-feature breakdown row ${index + 1} for ${day}`,
        );
      breakdown.byLanguageFeature.push({
        language: toStringValue(row.language) as string,
        feature: toStringValue(row.feature) as string,
        code_generation_activity_count: number(
          row.code_generation_activity_count,
        ),
        code_acceptance_activity_count: number(
          row.code_acceptance_activity_count,
        ),
        loc_added_sum: number(row.loc_added_sum),
        loc_deleted_sum: number(row.loc_deleted_sum),
        loc_suggested_to_add_sum: number(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: number(row.loc_suggested_to_delete_sum),
      });
    }

    for (const [index, row] of reportRows(
      metric.totals_by_model_feature,
      strict,
      'model-feature',
      day,
    ).entries()) {
      // API uses "model" field name, not "model_id"
      if (
        !isRecord(row) ||
        !toStringValue(row.model) ||
        !toStringValue(row.feature)
      ) {
        if (strict) {
          throw new Error(
            `Invalid model-feature breakdown row ${index + 1} for ${day}`,
          );
        }
        continue;
      }
      const number = (value: unknown) =>
        toNumber(
          value,
          strict,
          `model-feature breakdown row ${index + 1} for ${day}`,
        );
      breakdown.byModelFeature.push({
        model_id: toStringValue(row.model) as string,
        feature: toStringValue(row.feature) as string,
        user_initiated_interaction_count: number(
          row.user_initiated_interaction_count,
        ),
        code_generation_activity_count: number(
          row.code_generation_activity_count,
        ),
        code_acceptance_activity_count: number(
          row.code_acceptance_activity_count,
        ),
        loc_added_sum: number(row.loc_added_sum),
        loc_deleted_sum: number(row.loc_deleted_sum),
        loc_suggested_to_add_sum: number(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: number(row.loc_suggested_to_delete_sum),
      });
    }

    for (const [index, row] of reportRows(
      metric.totals_by_language_model,
      strict,
      'language-model',
      day,
    ).entries()) {
      // API uses "model" field name, not "model_id"; no "request_count" — use code_generation_activity_count
      if (
        !isRecord(row) ||
        !toStringValue(row.language) ||
        !toStringValue(row.model)
      ) {
        if (strict) {
          throw new Error(
            `Invalid language-model breakdown row ${index + 1} for ${day}`,
          );
        }
        continue;
      }
      const number = (value: unknown) =>
        toNumber(
          value,
          strict,
          `language-model breakdown row ${index + 1} for ${day}`,
        );
      breakdown.byLanguageModel.push({
        language: toStringValue(row.language) as string,
        model_id: toStringValue(row.model) as string,
        request_count: number(row.code_generation_activity_count),
        code_generation_activity_count: number(
          row.code_generation_activity_count,
        ),
        code_acceptance_activity_count: number(
          row.code_acceptance_activity_count,
        ),
        loc_added_sum: number(row.loc_added_sum),
        loc_deleted_sum: number(row.loc_deleted_sum),
        loc_suggested_to_add_sum: number(row.loc_suggested_to_add_sum),
        loc_suggested_to_delete_sum: number(row.loc_suggested_to_delete_sum),
      });
    }

    userBreakdowns.push(breakdown);
  }

  return {
    userMetrics,
    userBreakdowns,
    customizationMetrics,
    customizationSummaries,
  };
}

/**
 * Parse a user-teams-1-day document (flat array of user-team membership rows).
 */
export function parseUserTeamsDocument(
  doc: unknown,
  metricsType: MetricsScope,
  entityId: string,
  options: UserReportParseOptions = {},
): V2UserTeamRow[] {
  const strict = options.strict ?? false;
  const expectedDay = options.expectedDay;
  if (!Array.isArray(doc)) {
    if (strict) {
      throw new Error(
        `Invalid user-team report document for ${expectedDay ?? 'report day'}`,
      );
    }
    logWarn('Expected user teams document array, received non-array payload.');
    return [];
  }

  const parsed: V2UserTeamRow[] = [];

  for (const [rowIndex, teamMembership] of (doc as V2UserTeam[]).entries()) {
    const rowContext = `user-team row ${rowIndex + 1} for ${
      expectedDay ?? 'report day'
    }`;
    if (!isRecord(teamMembership)) {
      if (strict) {
        throw new Error(`Invalid ${rowContext}`);
      }
      logWarn('Skipping user teams row because it is not an object.');
      continue;
    }

    const day = toStringValue(teamMembership.day);
    const userLogin = toStringValue(teamMembership.user_login);
    const teamSlug = toStringValue(teamMembership.slug);
    const userId = toNumber(teamMembership.user_id, strict, rowContext);
    const teamId = toNumber(teamMembership.team_id, strict, rowContext);

    if (
      !day ||
      !userLogin ||
      !teamSlug ||
      userId <= 0 ||
      teamId <= 0 ||
      (strict && expectedDay !== undefined && day !== expectedDay)
    ) {
      if (strict) {
        throw new Error(`Invalid ${rowContext}`);
      }
      logWarn('Skipping user teams row with missing required fields.');
      continue;
    }

    parsed.push({
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      user_id: userId,
      user_login: userLogin,
      team_id: teamId,
      team_slug: teamSlug,
    });
  }

  return parsed;
}
