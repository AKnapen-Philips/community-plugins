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

import { Knex } from 'knex';
import { MetricsScope } from '@backstage-community/plugin-copilot-common';

type CostCenter = string | { kind: 'noCostCenter' };

const activityColumns = [
  'code_acceptance_activity_count',
  'code_generation_activity_count',
  'loc_added_sum',
  'loc_deleted_sum',
  'loc_suggested_to_add_sum',
  'loc_suggested_to_delete_sum',
];

const breakdownDimensions = {
  feature: ['feature'],
  ide: ['ide'],
  language_feature: ['language', 'feature'],
  model_feature: ['model_id', 'feature'],
  language_model: ['language', 'model_id'],
} satisfies Record<string, string[]>;

function correlate(
  query: Knex.QueryBuilder,
  left: string,
  right: string,
  user = true,
) {
  for (const column of [
    'day',
    'metrics_type',
    'entity_id',
    ...(user ? ['user_id'] : []),
  ]) {
    query.whereRaw('?? = ??', [`${left}.${column}`, `${right}.${column}`]);
  }
}

export function filterScopedUsers(
  db: Knex,
  query: Knex.QueryBuilder,
  alias: string,
  teamSlug?: string,
  costCenter?: CostCenter,
) {
  if (teamSlug) {
    query.whereExists(function teamMembership() {
      this.select(db.raw('1')).from('copilot_user_teams as membership');
      correlate(this, 'membership', alias);
      this.where('membership.team_slug', teamSlug);
    });
  }
  if (typeof costCenter === 'string') {
    query.whereExists(function costCenterMembership() {
      this.select(db.raw('1')).from('copilot_user_cost_centers as membership');
      correlate(this, 'membership', alias);
      this.where('membership.cost_center_id', costCenter);
    });
  } else if (costCenter) {
    query
      .whereExists(function completeSnapshot() {
        this.select(db.raw('1')).from(
          'copilot_cost_center_aggregation_log as done',
        );
        correlate(this, 'done', alias, false);
        this.where('done.unassigned_complete', true);
      })
      .whereNotExists(function assignedMembership() {
        this.select(db.raw('1')).from(
          'copilot_user_cost_centers as membership',
        );
        correlate(this, 'membership', alias);
      });
  }
}

/**
 * Legacy team detail remains a fallback until its matching raw dimension is
 * present. A completed userBreakdowns log makes the whole day authoritative.
 */
function onlyLegacyDays(
  db: Knex,
  query: Knex.QueryBuilder,
  rawTable: string,
  customization: boolean,
) {
  query.whereNotExists(function rawBreakdowns() {
    this.select(db.raw('1')).from(`${rawTable} as raw`);
    correlate(this, 'raw', 'legacy', false);
    if (customization) {
      this.where('raw.scope', 'user');
    }
  });
  const textType = String(db.client.config.client).includes('mysql')
    ? 'CHAR'
    : 'TEXT';
  query.whereNotExists(function completedRawBreakdowns() {
    this.select(db.raw('1')).from('copilot_ingestion_log as ingestion');
    correlate(this, 'ingestion', 'legacy', false);
    this.whereRaw(`CAST(?? AS ${textType}) LIKE ?`, [
      'ingestion.components_loaded',
      '%"userBreakdowns"%',
    ]);
  });
}

export async function readScopedMetrics<T>(
  db: Knex,
  kind: 'daily' | 'customization' | keyof typeof breakdownDimensions,
  metricsType: MetricsScope,
  entityId: string,
  from: string,
  to: string,
  teamSlug?: string,
  costCenter?: CostCenter,
  feature?: string,
): Promise<T[]> {
  const daily = kind === 'daily';
  const customization = kind === 'customization';
  let table: string;
  let dimensions: string[];
  if (daily) {
    table = 'copilot_user_metrics';
    dimensions = [];
  } else if (customization) {
    table = 'copilot_customization_metrics';
    dimensions = ['category', 'item_name'];
  } else {
    table = `copilot_user_metrics_by_${kind}`;
    dimensions = breakdownDimensions[kind];
  }
  const sums = customization
    ? ['interaction_count']
    : [
        ...activityColumns,
        ...(kind === 'language_feature' || kind === 'language_model'
          ? []
          : ['user_initiated_interaction_count']),
        ...(kind === 'language_model' ? ['request_count'] : []),
      ];
  const query = db(`${table} as raw`)
    .where('raw.metrics_type', metricsType)
    .where('raw.entity_id', entityId)
    .whereBetween('raw.day', [from, to]);
  if (!daily) {
    query.whereExists(function activeUser() {
      this.select(db.raw('1')).from('copilot_user_metrics as base');
      correlate(this, 'base', 'raw');
    });
  }
  if (customization) {
    query.where('raw.scope', 'user');
  }
  if (feature !== undefined) {
    query.where('raw.feature', feature);
  }
  filterScopedUsers(db, query, 'raw', teamSlug, costCenter);
  const groups = ['day', 'metrics_type', 'entity_id', ...dimensions].map(
    column => `raw.${column}`,
  );
  query.select(groups).groupBy(groups).orderBy('raw.day', 'asc');
  for (const column of sums) {
    query.select(
      db.raw('COALESCE(SUM(??), 0) as ??', [`raw.${column}`, column]),
    );
  }
  const counts = daily
    ? {
        monthly_active_agent_users: 'used_agent',
        monthly_active_chat_users: 'used_chat',
        daily_active_vscode_agent_users: 'used_vscode_agent',
        ...(costCenter ? { daily_active_cli_users: 'used_cli' } : {}),
      }
    : {};
  if (daily) {
    query
      .count({ daily_active_users: 'raw.user_id' })
      .select(
        db.raw('COALESCE(SUM(??), 0) as ??', [
          'raw.ai_credits_used',
          'total_ai_credits_used',
        ]),
      );
    for (const [output, column] of Object.entries(counts)) {
      query.select(
        db.raw('SUM(CASE WHEN ?? THEN 1 ELSE 0 END) as ??', [
          `raw.${column}`,
          output,
        ]),
      );
    }
  }
  const numeric = [
    ...sums,
    ...Object.keys(counts),
    ...(daily ? ['daily_active_users', 'total_ai_credits_used'] : []),
  ];
  const rows = (await query).map(row => {
    for (const column of numeric) {
      row[column] = Number(row[column]);
    }
    return {
      ...row,
      team_slug: teamSlug ?? '',
      ...(!customization
        ? { cost_center_id: typeof costCenter === 'string' ? costCenter : '' }
        : {
            scope: teamSlug ? 'team' : 'aggregate',
            user_id: 0,
            user_login: '',
          }),
    };
  });
  if (teamSlug && !costCenter && !daily) {
    const legacy = db(
      `${
        customization
          ? 'copilot_customization_metrics'
          : `copilot_metrics_by_${kind}`
      } as legacy`,
    )
      .where({
        'legacy.metrics_type': metricsType,
        'legacy.entity_id': entityId,
        'legacy.team_slug': teamSlug,
      })
      .whereBetween('legacy.day', [from, to]);
    if (customization) {
      legacy.where('legacy.scope', 'team');
    } else {
      legacy
        .where('legacy.cost_center_id', '')
        .where('legacy.without_cost_center', false);
    }
    if (feature !== undefined) {
      legacy.where('legacy.feature', feature);
    }
    onlyLegacyDays(db, legacy, table, customization);
    rows.push(...(await legacy.select('legacy.*')));
  }
  return rows.sort(
    (left, right) =>
      new Date(left.day).getTime() - new Date(right.day).getTime(),
  ) as T[];
}
