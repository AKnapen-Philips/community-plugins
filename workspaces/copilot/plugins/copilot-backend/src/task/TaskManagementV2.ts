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
  MetricsScope,
  NO_COST_CENTER_ID,
  V2UserCostCenterRow,
  V2UserMetricRow,
} from '@backstage-community/plugin-copilot-common';
import { DatabaseHandlerV2 } from '../db/DatabaseHandlerV2';
import { GithubClientV2 } from '../client/GithubClientV2';
import type { CostCenterMembershipSnapshot } from '../client/GithubClientV2';
import {
  parseEnterpriseDocument,
  parseOrganizationDocument,
  parseUserDocument,
  parseUserTeamsDocument,
  UserBreakdownData,
} from '../utils/reportParser';
import {
  aggregateCostCenterMetrics,
  aggregateTeamMetrics,
} from '../utils/teamAggregator';
import type { TeamAggregates } from '../utils/teamAggregator';
import { buildUserBreakdownRows } from '../utils/userBreakdownRows';

type IngestSource = 'scheduled' | 'backfill';

type IngestContext = {
  ingestTeams: boolean;
  ingestUsers: boolean;
  source: IngestSource;
};

type IngestPlan = {
  entity: { metricsType: MetricsScope; entityId: string };
  missingDays: string[];
  missingCostCenterDays: string[];
  ingestUsers: boolean;
  ingestCostCenters: boolean;
};

export type TaskManagementV2Options = {
  db: DatabaseHandlerV2;
  api: GithubClientV2;
  config: Config;
  logger: LoggerService;
};

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function getRequiredComponents(
  ingestUsers: boolean,
  ingestTeams: boolean,
): string[] {
  const components = ['totals'];
  if (ingestUsers) {
    components.push('users');
  }
  if (ingestTeams) {
    components.push('teams');
  }
  return components;
}

function uniqueDays(...dayLists: string[][]): string[] {
  return [...new Set(dayLists.flat())].sort();
}

function isSupportedCostCenterScope(
  metricsType: MetricsScope,
  entityId: string,
  enterprise?: string,
  organization?: string,
): boolean {
  if (metricsType === 'enterprise') {
    return Boolean(enterprise && entityId === enterprise);
  }
  return Boolean(enterprise && organization && entityId === organization);
}

export async function runBackfill(
  options: TaskManagementV2Options,
  fromDate: string,
  toDate: string,
  metricsType: MetricsScope,
  entityId: string,
): Promise<void> {
  // eslint-disable-next-line @typescript-eslint/no-use-before-define
  const task = TaskManagementV2.create(options);
  await task.runBackfill(fromDate, toDate, metricsType, entityId);
}

export class TaskManagementV2 {
  static create(options: TaskManagementV2Options): TaskManagementV2 {
    return new TaskManagementV2(options);
  }

  private constructor(private readonly options: TaskManagementV2Options) {}

  async runAsync(): Promise<void> {
    const { config, db, logger } = this.options;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    const backfillFromDate =
      config.getOptionalString('copilot.backfillFromDate') ?? '2025-10-10';
    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    const ingestTeams =
      config.getOptionalBoolean('copilot.ingestTeams') ?? false;
    const ingestCostCenters =
      config.getOptionalBoolean('copilot.ingestCostCenters') ?? false;

    if (ingestCostCenters && !enterprise) {
      logger.warn(
        '[TaskManagementV2] copilot.ingestCostCenters is enabled, but copilot.enterprise is not configured; skipping cost-center ingestion.',
      );
    }

    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate();
    if (!yesterday) {
      logger.error('[TaskManagementV2] Unable to compute yesterday date.');
      return;
    }

    const entities: Array<{ metricsType: MetricsScope; entityId: string }> = [];

    if (enterprise) {
      entities.push({ metricsType: 'enterprise', entityId: enterprise });
    }
    if (organization) {
      entities.push({ metricsType: 'organization', entityId: organization });
    }

    if (!entities.length) {
      logger.info(
        '[TaskManagementV2] No enterprise or organization configured, skipping run.',
      );
      return;
    }

    logger.info(
      `[TaskManagementV2] Starting gap-fill ingestion from ${backfillFromDate} to ${yesterday} for ${entities.length} entities.`,
    );

    const plans: IngestPlan[] = [];
    for (const entity of entities) {
      const costCenterEnabledForEntity =
        ingestCostCenters &&
        isSupportedCostCenterScope(
          entity.metricsType,
          entity.entityId,
          enterprise,
          organization,
        );
      const ingestUsers = ingestTeams || costCenterEnabledForEntity;
      const requiredComponents = getRequiredComponents(
        ingestUsers,
        ingestTeams,
      );
      const missingDays = await this.options.db.getMissingDays(
        entity.metricsType,
        entity.entityId,
        backfillFromDate,
        yesterday,
        requiredComponents,
      );
      const missingCostCenterDays = costCenterEnabledForEntity
        ? await db.getDaysMissingCostCenterAggregation(
            entity.metricsType,
            entity.entityId,
            backfillFromDate,
            yesterday,
          )
        : [];

      if (!missingDays.length) {
        logger.info(
          `[TaskManagementV2] No missing ingestion days for ${entity.metricsType}:${entity.entityId}.`,
        );
      } else {
        logger.info(
          `[TaskManagementV2] Found ${missingDays.length} missing days for ${entity.metricsType}:${entity.entityId}.`,
        );
      }

      plans.push({
        entity,
        missingDays,
        missingCostCenterDays,
        ingestUsers,
        ingestCostCenters: costCenterEnabledForEntity,
      });
    }

    const costCenterPlans = plans.filter(
      plan =>
        plan.ingestCostCenters &&
        (plan.missingDays.length > 0 || plan.missingCostCenterDays.length > 0),
    );
    const costCenterDays = uniqueDays(
      ...costCenterPlans.map(plan => [
        ...plan.missingDays,
        ...plan.missingCostCenterDays,
      ]),
    );
    let costCenterSnapshot: CostCenterMembershipSnapshot | undefined;
    if (enterprise && costCenterDays.length) {
      try {
        costCenterSnapshot = await this.loadCostCenterSnapshot(
          costCenterPlans.map(plan => plan.entity),
        );
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed to load cost-center membership for enterprise:${enterprise}; cost-center days will be retried.`,
          error,
        );
      }
    }

    for (const plan of plans) {
      const loadedUserReportDays = new Set<string>();
      for (const day of plan.missingDays) {
        const usersLoaded = await this.ingestDay(
          day,
          plan.entity.metricsType,
          plan.entity.entityId,
          {
            ingestTeams,
            ingestUsers: plan.ingestUsers,
            source: 'scheduled',
          },
        );
        if (usersLoaded) {
          loadedUserReportDays.add(day);
        }

        if (backfillDelayMs > 0) {
          await delay(backfillDelayMs);
        }
      }

      if (!costCenterSnapshot || !plan.ingestCostCenters) {
        continue;
      }

      const daysToAggregate = uniqueDays(
        plan.missingDays,
        plan.missingCostCenterDays,
      );
      for (const day of daysToAggregate) {
        try {
          await this.ingestCostCenterAggregates(
            day,
            plan.entity.metricsType,
            plan.entity.entityId,
            costCenterSnapshot,
            ingestTeams,
            plan.missingCostCenterDays.includes(day) ||
              loadedUserReportDays.has(day),
          );
        } catch (error) {
          logger.error(
            `[TaskManagementV2] Failed cost-center aggregation for ${plan.entity.metricsType}:${plan.entity.entityId} on ${day}; the day will be retried.`,
            error,
          );
        }
      }
    }

    logger.info('[TaskManagementV2] Completed gap-fill ingestion run.');
  }

  async runBackfill(
    fromDate: string,
    toDate: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    const { config, db, logger } = this.options;
    const ingestTeams =
      config.getOptionalBoolean('copilot.ingestTeams') ?? false;
    const ingestCostCenters =
      config.getOptionalBoolean('copilot.ingestCostCenters') ?? false;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    const costCenterIngestionAvailable =
      ingestCostCenters &&
      isSupportedCostCenterScope(
        metricsType,
        entityId,
        enterprise,
        organization,
      );

    if (ingestCostCenters && !enterprise) {
      logger.warn(
        '[TaskManagementV2] copilot.ingestCostCenters is enabled, but copilot.enterprise is not configured; skipping cost-center ingestion.',
      );
    } else if (
      ingestCostCenters &&
      metricsType === 'organization' &&
      !costCenterIngestionAvailable
    ) {
      logger.warn(
        `[TaskManagementV2] Organization cost-center ingestion requires copilot.enterprise and the configured copilot.organization; skipping cost-center ingestion for organization:${entityId}.`,
      );
    }

    const ingestUsers = ingestTeams || costCenterIngestionAvailable;
    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    const requiredComponents = getRequiredComponents(ingestUsers, ingestTeams);

    const missingDays = await this.options.db.getMissingDays(
      metricsType,
      entityId,
      fromDate,
      toDate,
      requiredComponents,
    );
    const missingCostCenterDays = costCenterIngestionAvailable
      ? await db.getDaysMissingCostCenterAggregation(
          metricsType,
          entityId,
          fromDate,
          toDate,
        )
      : [];
    const costCenterDays = uniqueDays(missingDays, missingCostCenterDays);

    if (!missingDays.length && !missingCostCenterDays.length) {
      logger.info(
        `[TaskManagementV2] No missing days for backfill ${metricsType}:${entityId} in range ${fromDate}..${toDate}.`,
      );
      return;
    }

    if (missingDays.length) {
      logger.info(
        `[TaskManagementV2] Backfill processing ${missingDays.length} days for ${metricsType}:${entityId}.`,
      );
    }

    let costCenterSnapshot: CostCenterMembershipSnapshot | undefined;
    if (costCenterIngestionAvailable && enterprise && costCenterDays.length) {
      try {
        costCenterSnapshot = await this.loadCostCenterSnapshot([
          { metricsType, entityId },
        ]);
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed to load cost-center membership for enterprise:${enterprise}; cost-center days will be retried.`,
          error,
        );
      }
    }

    const loadedUserReportDays = new Set<string>();
    for (const day of missingDays) {
      if (
        await this.ingestDay(day, metricsType, entityId, {
          ingestTeams,
          ingestUsers,
          source: 'backfill',
        })
      ) {
        loadedUserReportDays.add(day);
      }

      if (backfillDelayMs > 0) {
        await delay(backfillDelayMs);
      }
    }

    if (!costCenterSnapshot) {
      return;
    }

    for (const day of costCenterDays) {
      try {
        await this.ingestCostCenterAggregates(
          day,
          metricsType,
          entityId,
          costCenterSnapshot,
          ingestTeams,
          missingCostCenterDays.includes(day) || loadedUserReportDays.has(day),
        );
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed cost-center aggregation for ${metricsType}:${entityId} on ${day}; the day will be retried.`,
          error,
        );
      }
    }
  }

  private async loadCostCenterSnapshot(
    entities: Array<{ metricsType: MetricsScope; entityId: string }>,
  ): Promise<CostCenterMembershipSnapshot> {
    const { api, db } = this.options;
    // GithubClientV2 logs the warnings as it builds the snapshot.
    const snapshot = await api.fetchCostCenterMemberships();

    for (const entity of entities) {
      await db.upsertCostCenters(
        snapshot.costCenters.map(costCenter => ({
          metrics_type: entity.metricsType,
          entity_id: entity.entityId,
          cost_center_id: costCenter.id,
          name: costCenter.name,
        })),
      );
    }

    return snapshot;
  }

  private async insertAggregates(aggregates: TeamAggregates): Promise<void> {
    const { db } = this.options;
    await db.insertDailyTotals(aggregates.dailyTotals);
    await db.insertByFeature(aggregates.byFeature);
    await db.insertByIde(aggregates.byIde);
    await db.insertByLanguageFeature(aggregates.byLanguageFeature);
    await db.insertByModelFeature(aggregates.byModelFeature);
    await db.insertByLanguageModel(aggregates.byLanguageModel);
    if (aggregates.byCli) {
      await db.insertByCli(aggregates.byCli);
    }
    if (aggregates.byVscodeAgent) {
      await db.insertByVscodeAgent(aggregates.byVscodeAgent);
    }
  }

  private async ingestTotals(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<boolean> {
    const { api, db, logger } = this.options;
    const reportEnvelope =
      metricsType === 'enterprise'
        ? await api.fetchEnterpriseReportLinks(day)
        : await api.fetchOrganizationReportLinks(day);

    const totalLinks = reportEnvelope.download_links ?? [];
    if (totalLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} totals on ${day} — report data may not yet be available for this period.`,
      );
      return false;
    }

    let totalRowsParsed = 0;

    for (const url of totalLinks) {
      const document = await api.downloadDocument(url);
      const parsed =
        metricsType === 'organization'
          ? parseOrganizationDocument(document, metricsType, entityId)
          : parseEnterpriseDocument(document, metricsType, entityId);

      await db.insertDailyTotals(parsed.dailyTotals);
      await db.insertPrMetrics(parsed.prMetrics);
      await db.insertByFeature(parsed.byFeature);
      await db.insertByIde(parsed.byIde);
      await db.insertByLanguageFeature(parsed.byLanguageFeature);
      await db.insertByModelFeature(parsed.byModelFeature);
      await db.insertByLanguageModel(parsed.byLanguageModel);
      await db.insertByCli(parsed.byCli);
      await db.insertByVscodeAgent(parsed.byVscodeAgent);

      totalRowsParsed += parsed.dailyTotals.length;
    }

    if (totalRowsParsed === 0) {
      logger.warn(
        `[TaskManagementV2] Parsed 0 daily rows for ${metricsType} totals on ${day} — not marking as success.`,
      );
      return false;
    }

    return true;
  }

  private async ingestUserReports(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<{
    loaded: boolean;
    userMetrics: V2UserMetricRow[];
    userBreakdowns: UserBreakdownData[];
  }> {
    const { api, db, logger } = this.options;
    const userReportEnvelope =
      metricsType === 'enterprise'
        ? await api.fetchEnterpriseUserReportLinks(day)
        : await api.fetchOrganizationUserReportLinks(day);

    const userLinks = userReportEnvelope.download_links ?? [];
    if (userLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} users on ${day} — user metrics will be skipped.`,
      );
      return { loaded: false, userMetrics: [], userBreakdowns: [] };
    }

    const userMetrics: V2UserMetricRow[] = [];
    const userBreakdowns: UserBreakdownData[] = [];

    for (const url of userLinks) {
      const rows = await api.downloadNdjsonDocument(url);
      const parsed = parseUserDocument(rows, metricsType, entityId);
      userMetrics.push(...parsed.userMetrics);
      userBreakdowns.push(...parsed.userBreakdowns);
    }

    await db.insertUserMetrics(userMetrics);

    const breakdownRows = buildUserBreakdownRows(
      userBreakdowns,
      day,
      metricsType,
      entityId,
    );
    await db.insertUserMetricsByFeature(breakdownRows.byFeature);
    await db.insertUserMetricsByIde(breakdownRows.byIde);
    await db.insertUserMetricsByLanguageFeature(
      breakdownRows.byLanguageFeature,
    );
    await db.insertUserMetricsByModelFeature(breakdownRows.byModelFeature);
    await db.insertUserMetricsByLanguageModel(breakdownRows.byLanguageModel);

    return { loaded: true, userMetrics, userBreakdowns };
  }

  private async ingestTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userMetrics: V2UserMetricRow[],
    userBreakdowns: UserBreakdownData[],
  ): Promise<boolean> {
    const { api, db, logger } = this.options;
    const userTeamsEnvelope =
      metricsType === 'enterprise'
        ? await api.fetchEnterpriseUserTeamsLinks(day)
        : await api.fetchOrganizationUserTeamsLinks(day);

    const userTeamLinks = userTeamsEnvelope.download_links ?? [];
    if (userTeamLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} user-teams on ${day} — team aggregation will be skipped.`,
      );
      return false;
    }

    const userTeams: ReturnType<typeof parseUserTeamsDocument> = [];
    for (const url of userTeamLinks) {
      const rows = await api.downloadNdjsonDocument(url);
      userTeams.push(...parseUserTeamsDocument(rows, metricsType, entityId));
    }

    await db.insertUserTeams(userTeams);

    if (userMetrics.length === 0) {
      return false;
    }

    const teamAggregates = aggregateTeamMetrics(
      userMetrics,
      userTeams,
      userBreakdowns,
      day,
      metricsType,
      entityId,
    );

    await this.insertAggregates(teamAggregates);

    return true;
  }

  private async ingestCostCenterAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    snapshot: CostCenterMembershipSnapshot,
    ingestTeams: boolean,
    markEmptyDay: boolean,
  ): Promise<boolean> {
    const { db } = this.options;
    const userMetrics = await db.getUserMetricsForDay(
      metricsType,
      entityId,
      day,
    );

    if (!userMetrics.length && !markEmptyDay) {
      return false;
    }

    const memberships = new Map<number, string>();
    const userCostCentersByUserId = new Map<number, V2UserCostCenterRow>();
    for (const metric of userMetrics) {
      const membership = snapshot.members.get(metric.user_id);
      const costCenterId = membership?.costCenterId ?? NO_COST_CENTER_ID;
      memberships.set(metric.user_id, costCenterId);

      userCostCentersByUserId.set(metric.user_id, {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        user_id: metric.user_id,
        user_login: membership?.login ?? metric.user_login,
        cost_center_id: costCenterId,
      });
    }
    const userCostCenters = [...userCostCentersByUserId.values()];

    const [userBreakdowns, userTeams] = await Promise.all([
      userMetrics.length
        ? db.getUserBreakdownsForDay(metricsType, entityId, day)
        : Promise.resolve([]),
      ingestTeams && userMetrics.length
        ? db.getUserTeamsForDay(metricsType, entityId, day)
        : Promise.resolve([]),
    ]);
    const aggregates = aggregateCostCenterMetrics(
      userMetrics,
      userTeams,
      userBreakdowns,
      memberships,
      day,
      metricsType,
      entityId,
      ingestTeams,
    );

    await db.replaceCostCenterAggregates(
      day,
      metricsType,
      entityId,
      userCostCenters,
      {
        dailyTotals: aggregates.dailyTotals,
        prMetrics: [],
        byFeature: aggregates.byFeature,
        byIde: aggregates.byIde,
        byLanguageFeature: aggregates.byLanguageFeature,
        byModelFeature: aggregates.byModelFeature,
        byLanguageModel: aggregates.byLanguageModel,
        byCli: aggregates.byCli ?? [],
        byVscodeAgent: aggregates.byVscodeAgent ?? [],
      },
    );
    return true;
  }

  private async logIngestionOutcome(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
    componentsLoaded: string[],
    requiredComponents: string[],
  ): Promise<void> {
    const { db, logger } = this.options;
    const missingComponents = requiredComponents.filter(
      component => !componentsLoaded.includes(component),
    );
    const status: 'success' | 'partial' =
      missingComponents.length === 0 ? 'success' : 'partial';
    const errorMessage =
      missingComponents.length > 0
        ? `Missing components: ${missingComponents.join(', ')}`
        : undefined;

    await db.upsertIngestionLog({
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      status,
      components_loaded: JSON.stringify(componentsLoaded),
      error_message: errorMessage,
      source: context.source,
    });

    if (status === 'success') {
      logger.info(
        `[TaskManagementV2] Ingested ${metricsType}:${entityId} for ${day} (source=${
          context.source
        }, components=${componentsLoaded.join(',')}).`,
      );
      return;
    }

    logger.warn(
      `[TaskManagementV2] Partially ingested ${metricsType}:${entityId} for ${day} (source=${
        context.source
      }, components=${componentsLoaded.join(
        ',',
      )}, missing=${missingComponents.join(',')}).`,
    );
  }

  private async ingestDay(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
  ): Promise<boolean> {
    const { db, logger } = this.options;
    const requiredComponents = getRequiredComponents(
      context.ingestUsers,
      context.ingestTeams,
    );
    const componentsLoaded: string[] = [];
    let userReportsLoaded = false;

    try {
      if (await this.ingestTotals(day, metricsType, entityId)) {
        componentsLoaded.push('totals');
      }

      if (context.ingestUsers) {
        const { loaded, userMetrics, userBreakdowns } =
          await this.ingestUserReports(day, metricsType, entityId);
        userReportsLoaded = loaded;
        if (loaded) {
          componentsLoaded.push('users');
        }

        if (
          context.ingestTeams &&
          (await this.ingestTeamAggregates(
            day,
            metricsType,
            entityId,
            userMetrics,
            userBreakdowns,
          ))
        ) {
          componentsLoaded.push('teams');
        }
      }

      await this.logIngestionOutcome(
        day,
        metricsType,
        entityId,
        context,
        componentsLoaded,
        requiredComponents,
      );
      return userReportsLoaded;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error(
        `[TaskManagementV2] Failed ingest for ${metricsType}:${entityId} on ${day}.`,
        error,
      );

      await db.upsertIngestionLog({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        status: 'error',
        components_loaded: JSON.stringify(componentsLoaded),
        error_message: message,
        source: context.source,
      });
      return userReportsLoaded;
    }
  }
}
