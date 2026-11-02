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
];

/**
 * @param {import('knex').Knex} knex
 */
exports.up = async function up(knex) {
  await knex.schema.createTable('copilot_team_aggregation_state', table => {
    table.increments('id').primary();
    table.string('metrics_type').notNullable();
    table.string('entity_id').notNullable();
    table.string('active_source').notNullable().defaultTo('native');
    table.string('active_generation').notNullable().defaultTo('native-default');
    table.string('pending_source').nullable();
    table.string('pending_generation').nullable();
    table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());
    table.unique(
      ['metrics_type', 'entity_id'],
      'uq_team_aggregation_state_scope',
    );
  });

  await knex.schema.createTable('copilot_team_aggregation_log', table => {
    table.increments('id').primary();
    table.date('day').notNullable();
    table.string('metrics_type').notNullable();
    table.string('entity_id').notNullable();
    table.string('source').notNullable();
    table.string('generation').notNullable();
    table.string('status').notNullable();
    table.text('error_message').nullable();
    table.timestamp('completed_at').nullable();
    table.unique(
      ['day', 'metrics_type', 'entity_id', 'source', 'generation'],
      'uq_team_aggregation_log_generation',
    );
    table.index(
      ['metrics_type', 'entity_id', 'source', 'generation', 'status'],
      'idx_team_aggregation_log_state',
    );
  });

  await knex.schema.createTable('copilot_user_teams_staging', table => {
    table.increments('id').primary();
    table.date('day').notNullable();
    table.string('metrics_type').notNullable();
    table.string('entity_id').notNullable();
    table.string('generation').notNullable();
    table.bigInteger('user_id').notNullable();
    table.string('user_login').notNullable();
    table.bigInteger('team_id').notNullable();
    table.string('team_slug').notNullable();
    table.unique(
      ['day', 'metrics_type', 'entity_id', 'generation', 'user_id', 'team_id'],
      'uq_user_teams_staging_generation',
    );
    table.index(
      ['metrics_type', 'entity_id', 'generation', 'day'],
      'idx_user_teams_staging_generation_day',
    );
  });

  await knex.schema.createTable('copilot_team_aggregates_staging', table => {
    table.increments('id').primary();
    table.date('day').notNullable();
    table.string('metrics_type').notNullable();
    table.string('entity_id').notNullable();
    table.string('generation').notNullable();
    table.string('table_name').notNullable();
    table.string('row_key').notNullable().defaultTo('');
    table.text('row_data').notNullable();
    table.index(
      ['metrics_type', 'entity_id', 'generation', 'day'],
      'idx_team_aggregates_staging_generation_day',
    );
  });

  await knex.schema.createTable(
    'copilot_team_membership_snapshot_state',
    table => {
      table.increments('id').primary();
      table.string('metrics_type').notNullable();
      table.string('entity_id').notNullable();
      table.string('generation').notNullable();
      table.string('attempt_token').notNullable();
      table.string('cursor').nullable();
      table.boolean('complete').notNullable().defaultTo(false);
      table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
      table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());
      table.unique(
        ['metrics_type', 'entity_id'],
        'uq_team_membership_snapshot_state',
      );
      table.index(
        ['metrics_type', 'entity_id', 'complete', 'updated_at'],
        'idx_team_membership_snapshot_incomplete',
      );
    },
  );

  await knex.schema.createTable(
    'copilot_team_membership_snapshot_staging',
    table => {
      table.increments('id').primary();
      table.string('metrics_type').notNullable();
      table.string('entity_id').notNullable();
      table.string('generation').notNullable();
      table.bigInteger('team_id').notNullable();
      table.string('team_slug').notNullable();
      table.bigInteger('user_id').notNullable();
      table.unique(
        ['metrics_type', 'entity_id', 'generation', 'team_id', 'user_id'],
        'uq_team_membership_snapshot_membership',
      );
      table.index(
        ['metrics_type', 'entity_id', 'generation', 'team_slug', 'user_id'],
        'idx_team_membership_snapshot_team',
      );
      table.index(
        ['metrics_type', 'entity_id', 'generation', 'user_id', 'id'],
        'idx_team_membership_snapshot_user',
      );
    },
  );
};

/**
 * @param {import('knex').Knex} knex
 */
exports.down = async function down(knex) {
  for (const tableName of teamAggregateTables) {
    const query = knex(tableName).whereNot('team_slug', '');
    if (tableName === 'copilot_customization_metrics') {
      query.where('scope', 'team');
    }

    await query
      .whereExists(function activeFallbackScope() {
        this.select(knex.raw('1'))
          .from('copilot_team_aggregation_state as active_state')
          .whereColumn('active_state.metrics_type', `${tableName}.metrics_type`)
          .whereColumn('active_state.entity_id', `${tableName}.entity_id`)
          .where('active_state.active_source', 'organization-fallback');
      })
      .whereExists(function fallbackAggregationExists() {
        this.select(knex.raw('1'))
          .from('copilot_team_aggregation_log as fallback_log')
          .whereColumn('fallback_log.day', `${tableName}.day`)
          .whereColumn('fallback_log.metrics_type', `${tableName}.metrics_type`)
          .whereColumn('fallback_log.entity_id', `${tableName}.entity_id`)
          .where('fallback_log.source', 'organization-fallback')
          .where('fallback_log.status', 'complete');
      })
      .del();
  }

  await knex('copilot_ingestion_log')
    .whereExists(function fallbackAggregationExists() {
      this.select(knex.raw('1'))
        .from('copilot_team_aggregation_log as fallback_log')
        .join(
          'copilot_team_aggregation_state as active_state',
          function joinActiveFallbackState() {
            this.on(
              'active_state.metrics_type',
              '=',
              'fallback_log.metrics_type',
            ).andOn('active_state.entity_id', '=', 'fallback_log.entity_id');
          },
        )
        .whereColumn('fallback_log.day', 'copilot_ingestion_log.day')
        .whereColumn(
          'fallback_log.metrics_type',
          'copilot_ingestion_log.metrics_type',
        )
        .whereColumn(
          'fallback_log.entity_id',
          'copilot_ingestion_log.entity_id',
        )
        .where('fallback_log.source', 'organization-fallback')
        .where('active_state.active_source', 'organization-fallback')
        .where('fallback_log.status', 'complete');
    })
    .del();

  await knex('copilot_user_teams')
    .whereExists(function activeFallbackMemberships() {
      this.select(knex.raw('1'))
        .from('copilot_team_aggregation_state as active_state')
        .whereColumn(
          'active_state.metrics_type',
          'copilot_user_teams.metrics_type',
        )
        .whereColumn('active_state.entity_id', 'copilot_user_teams.entity_id')
        .where('active_state.active_source', 'organization-fallback');
    })
    .whereExists(function completedFallbackDay() {
      this.select(knex.raw('1'))
        .from('copilot_team_aggregation_log as fallback_log')
        .whereColumn('fallback_log.day', 'copilot_user_teams.day')
        .whereColumn(
          'fallback_log.metrics_type',
          'copilot_user_teams.metrics_type',
        )
        .whereColumn('fallback_log.entity_id', 'copilot_user_teams.entity_id')
        .where('fallback_log.source', 'organization-fallback')
        .where('fallback_log.status', 'complete');
    })
    .del();

  await knex.schema.dropTableIfExists(
    'copilot_team_membership_snapshot_staging',
  );
  await knex.schema.dropTableIfExists('copilot_team_membership_snapshot_state');
  await knex.schema.dropTableIfExists('copilot_team_aggregates_staging');
  await knex.schema.dropTableIfExists('copilot_user_teams_staging');
  await knex.schema.dropTableIfExists('copilot_team_aggregation_log');
  await knex.schema.dropTableIfExists('copilot_team_aggregation_state');
};
