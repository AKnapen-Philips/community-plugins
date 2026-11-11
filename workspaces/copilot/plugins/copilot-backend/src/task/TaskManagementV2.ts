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
  V2UserTeamRow,
  V2UserCostCenterRow,
  V2UserMetricRow,
  V2IngestionLogRow,
} from '@backstage-community/plugin-copilot-common';
import {
  DatabaseHandlerV2,
  parseComponentsLoaded,
} from '../db/DatabaseHandlerV2';
import type { V2RawReportDayData } from '../db/DatabaseHandlerV2';
import { GithubClientV2 } from '../client/GithubClientV2';
import type { CostCenterMembershipSnapshot } from '../client/GithubClientV2';
import {
  parseEnterpriseDocument,
  parseOrganizationDocument,
  parseUserDocument,
  parseUserTeamsDocument,
  ParsedEnterpriseDocument,
  ParsedUserDocument,
  UserBreakdownData,
} from '../utils/reportParser';
import {
  aggregateCostCenterMetrics,
  aggregateTeamMetrics,
} from '../utils/teamAggregator';
import { buildUserBreakdownRows } from '../utils/userBreakdownRows';

type IngestSource = 'scheduled' | 'backfill';

type IngestContext = {
  ingestTeams: boolean;
  ingestUsers: boolean;
  ingestCostCenters: boolean;
  refetch: boolean;
  source: IngestSource;
};

type IngestPlan = {
  entity: { metricsType: MetricsScope; entityId: string };
  missingDays: string[];
  ingestionDays: string[];
  refetchDays: string[];
  missingCostCenterDays: string[];
  incompleteCostCenterDays: string[];
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
const GITHUB_REPORTS_EARLIEST_DATE = '2025-10-10';
const SCHEDULED_RECENT_DAYS = 3;
const ORGANIZATION_PERMISSIONS_DOCS_URL =
  'https://github.com/backstage/community-plugins/blob/main/workspaces/copilot/plugins/copilot-backend/README.md#required-github-permissions';

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

function getReportWindowStartDate(now = DateTime.utc()): string {
  const rollingStart = now
    .toUTC()
    .startOf('day')
    .minus({ years: 1 })
    .toISODate();
  return rollingStart > GITHUB_REPORTS_EARLIEST_DATE
    ? rollingStart
    : GITHUB_REPORTS_EARLIEST_DATE;
}

function getHttpStatus(error: unknown): number | undefined {
  if (
    typeof error === 'object' &&
    error !== null &&
    'status' in error &&
    typeof error.status === 'number'
  ) {
    return error.status;
  }
  return undefined;
}

function logOrganizationMetricsPermissionError(
  logger: LoggerService,
  report: 'per-user' | 'user-teams',
  day: string,
  error: unknown,
): boolean {
  const status = getHttpStatus(error);
  if (status !== 403 && status !== 404) {
    return false;
  }

  const githubMessage = error instanceof Error ? error.message : String(error);
  logger.error(
    `[TaskManagementV2] GitHub returned HTTP ${status} for the organization ${report} report on ${day}: ${githubMessage}. This is likely due to missing permissions. The GitHub App needs Organization Copilot metrics: read on the organization. Classic PATs need read:org for organization reports; enterprise reports use manage_billing:copilot or read:enterprise. See ${ORGANIZATION_PERMISSIONS_DOCS_URL}`,
    error instanceof Error ? error : { status, message: githubMessage },
  );
  return true;
}

function uniqueDays(...dayLists: string[][]): string[] {
  return [...new Set(dayLists.flat())].sort();
}

function getDaysInRange(from: string, to: string): string[] {
  const start = DateTime.fromISO(from, { zone: 'utc' }).startOf('day');
  const end = DateTime.fromISO(to, { zone: 'utc' }).startOf('day');
  if (!start.isValid || !end.isValid || start > end) {
    return [];
  }

  const days: string[] = [];
  for (let cursor = start; cursor <= end; cursor = cursor.plus({ days: 1 })) {
    const day = cursor.toISODate();
    if (day) {
      days.push(day);
    }
  }

  return days;
}

function getScheduledRefetchDays(
  yesterday: string,
  startDate: string,
): string[] {
  const recentFromDate = DateTime.fromISO(yesterday, { zone: 'utc' })
    .minus({ days: SCHEDULED_RECENT_DAYS - 1 })
    .toISODate();

  return recentFromDate
    ? getDaysInRange(recentFromDate, yesterday).filter(day => day >= startDate)
    : [];
}

function mergeTotalsReports(
  reports: ParsedEnterpriseDocument[],
): ParsedEnterpriseDocument {
  const merged: ParsedEnterpriseDocument = {
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

  for (const report of reports) {
    merged.dailyTotals.push(...report.dailyTotals);
    merged.prMetrics.push(...report.prMetrics);
    merged.byFeature.push(...report.byFeature);
    merged.byIde.push(...report.byIde);
    merged.byLanguageFeature.push(...report.byLanguageFeature);
    merged.byModelFeature.push(...report.byModelFeature);
    merged.byLanguageModel.push(...report.byLanguageModel);
    merged.byCli.push(...report.byCli);
    merged.byVscodeAgent.push(...report.byVscodeAgent);
    merged.customizationMetrics.push(...report.customizationMetrics);
    merged.customizationSummaries.push(...report.customizationSummaries);
  }

  return merged;
}

function validateReportScope(
  rows: Array<{ day: string; metrics_type: MetricsScope; entity_id: string }>,
  day: string,
  metricsType: MetricsScope,
  entityId: string,
  reportName: string,
): void {
  if (
    rows.some(
      row =>
        row.day !== day ||
        row.metrics_type !== metricsType ||
        row.entity_id !== entityId,
    )
  ) {
    throw new Error(
      `${reportName} report rows do not match the requested day and scope.`,
    );
  }
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
  // Class-wide so scheduled and manual work serialize across task instances.
  private static ingestionQueue: Promise<void> = Promise.resolve();

  private static runSerialized<T>(work: () => Promise<T>): Promise<T> {
    const result = TaskManagementV2.ingestionQueue.then(work);
    TaskManagementV2.ingestionQueue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  static create(options: TaskManagementV2Options): TaskManagementV2 {
    return new TaskManagementV2(options);
  }

  private constructor(private readonly options: TaskManagementV2Options) {}

  runAsync(): Promise<void> {
    return TaskManagementV2.runSerialized(() => this.runAsyncUnlocked());
  }

  private async runAsyncUnlocked(): Promise<void> {
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
    const reportWindowStartDate = getReportWindowStartDate();
    const refetchStartDate =
      backfillFromDate > reportWindowStartDate
        ? backfillFromDate
        : reportWindowStartDate;
    const scheduledRefetchDays = getScheduledRefetchDays(
      yesterday,
      refetchStartDate,
    );
    for (const entity of entities) {
      const costCenterEnabledForEntity =
        ingestCostCenters && Boolean(enterprise);
      const ingestUsers = ingestTeams || costCenterEnabledForEntity;
      const requiredComponents = getRequiredComponents(
        ingestUsers,
        ingestTeams,
      );
      const missingFromDate =
        ingestUsers && backfillFromDate < reportWindowStartDate
          ? reportWindowStartDate
          : backfillFromDate;
      if (missingFromDate !== backfillFromDate) {
        logger.info(
          `[TaskManagementV2] Limiting ${entity.metricsType} user, team, and cost-center backfill to GitHub's report window starting ${missingFromDate}.`,
        );
      }
      const { missingDays } = await this.getMissingIngestionDays(
        entity.metricsType,
        entity.entityId,
        backfillFromDate,
        missingFromDate,
        yesterday,
        requiredComponents,
        ingestUsers,
      );
      const ingestionDays = uniqueDays(missingDays, scheduledRefetchDays);
      const missingCostCenterDays = costCenterEnabledForEntity
        ? await db.getDaysMissingCostCenterAggregation(
            entity.metricsType,
            entity.entityId,
            missingFromDate,
            yesterday,
          )
        : [];
      const incompleteCostCenterDays = costCenterEnabledForEntity
        ? await db.getDaysWithIncompleteCostCenterAggregation(
            entity.metricsType,
            entity.entityId,
            missingFromDate,
            yesterday,
          )
        : [];

      if (!ingestionDays.length) {
        logger.info(
          `[TaskManagementV2] No ingestion days to process for ${entity.metricsType}:${entity.entityId}.`,
        );
      } else {
        logger.info(
          `[TaskManagementV2] Processing ${ingestionDays.length} ingestion days for ${entity.metricsType}:${entity.entityId}, including ${scheduledRefetchDays.length} recent refresh days.`,
        );
      }

      plans.push({
        entity,
        missingDays,
        ingestionDays,
        refetchDays: scheduledRefetchDays,
        missingCostCenterDays,
        incompleteCostCenterDays,
        ingestUsers,
        ingestCostCenters: costCenterEnabledForEntity,
      });
    }

    const costCenterPlans = plans.filter(
      plan =>
        plan.ingestCostCenters &&
        (plan.ingestionDays.some(day => day >= reportWindowStartDate) ||
          plan.missingCostCenterDays.some(
            day => day >= reportWindowStartDate,
          ) ||
          plan.incompleteCostCenterDays.some(
            day => day >= reportWindowStartDate,
          )),
    );
    const costCenterDays = uniqueDays(
      ...costCenterPlans.map(plan => [
        ...plan.ingestionDays.filter(day => day >= reportWindowStartDate),
        ...plan.missingCostCenterDays.filter(
          day => day >= reportWindowStartDate,
        ),
        ...plan.incompleteCostCenterDays.filter(
          day => day >= reportWindowStartDate,
        ),
      ]),
    );
    let costCenterSnapshot: CostCenterMembershipSnapshot | undefined;
    if (costCenterDays.length) {
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
      const refreshedUserReportDays = new Set<string>();
      const failedRefetchDays = new Set<string>();
      for (const day of plan.ingestionDays) {
        const refetch = plan.refetchDays.includes(day);
        const userReport = await this.ingestDay(
          day,
          plan.entity.metricsType,
          plan.entity.entityId,
          {
            ingestTeams: ingestTeams && day >= reportWindowStartDate,
            ingestUsers: plan.ingestUsers && day >= reportWindowStartDate,
            ingestCostCenters: plan.ingestCostCenters,
            refetch,
            source: 'scheduled',
          },
        );
        if (userReport.loaded) {
          loadedUserReportDays.add(day);
        }
        if (userReport.refreshed) {
          refreshedUserReportDays.add(day);
        }
        if (refetch && !userReport.succeeded) {
          failedRefetchDays.add(day);
        }

        if (backfillDelayMs > 0) {
          await delay(backfillDelayMs);
        }
      }

      if (!costCenterSnapshot?.complete || !plan.ingestCostCenters) {
        continue;
      }

      const retryIncompleteDays = costCenterSnapshot.unassignedComplete
        ? plan.incompleteCostCenterDays
        : [];
      const daysToAggregate = uniqueDays(
        [...refreshedUserReportDays].filter(
          day => day >= reportWindowStartDate,
        ),
        plan.missingCostCenterDays.filter(day => day >= reportWindowStartDate),
        retryIncompleteDays.filter(day => day >= reportWindowStartDate),
      );
      for (const day of daysToAggregate) {
        if (failedRefetchDays.has(day)) {
          continue;
        }
        if (plan.missingDays.includes(day) && !loadedUserReportDays.has(day)) {
          continue;
        }
        try {
          await this.ingestCostCenterAggregates(
            day,
            plan.entity.metricsType,
            plan.entity.entityId,
            costCenterSnapshot,
            plan.missingCostCenterDays.includes(day) ||
              retryIncompleteDays.includes(day) ||
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

  runBackfill(
    fromDate: string,
    toDate: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    return TaskManagementV2.runSerialized(() =>
      this.runBackfillUnlocked(fromDate, toDate, metricsType, entityId),
    );
  }

  private async runBackfillUnlocked(
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
    const reportWindowStartDate = getReportWindowStartDate();
    const missingFromDate =
      ingestUsers && fromDate < reportWindowStartDate
        ? reportWindowStartDate
        : fromDate;
    if (missingFromDate !== fromDate) {
      logger.info(
        `[TaskManagementV2] Limiting user, team, and cost-center backfill to GitHub's report window starting ${missingFromDate}.`,
      );
    }

    const ingestionDays = getDaysInRange(fromDate, toDate);
    const missingCostCenterDays = costCenterIngestionAvailable
      ? await db.getDaysMissingCostCenterAggregation(
          metricsType,
          entityId,
          missingFromDate,
          toDate,
        )
      : [];
    const incompleteCostCenterDays = costCenterIngestionAvailable
      ? await db.getDaysWithIncompleteCostCenterAggregation(
          metricsType,
          entityId,
          missingFromDate,
          toDate,
        )
      : [];
    const costCenterDays = ingestionDays.filter(
      day => day >= reportWindowStartDate,
    );

    if (
      !ingestionDays.length &&
      !missingCostCenterDays.length &&
      !incompleteCostCenterDays.length
    ) {
      logger.info(
        `[TaskManagementV2] No days to process for backfill ${metricsType}:${entityId} in range ${fromDate}..${toDate}.`,
      );
      return;
    }

    if (ingestionDays.length) {
      logger.info(
        `[TaskManagementV2] Backfill re-ingesting ${ingestionDays.length} days for ${metricsType}:${entityId}.`,
      );
    }

    let costCenterSnapshot: CostCenterMembershipSnapshot | undefined;
    if (costCenterIngestionAvailable && costCenterDays.length) {
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

    const successfulUserRefreshDays = new Set<string>();
    for (const day of ingestionDays) {
      const userReport = await this.ingestDay(day, metricsType, entityId, {
        ingestTeams: ingestTeams && day >= reportWindowStartDate,
        ingestUsers: ingestUsers && day >= reportWindowStartDate,
        ingestCostCenters: costCenterIngestionAvailable,
        refetch: true,
        source: 'backfill',
      });
      if (
        day >= reportWindowStartDate &&
        userReport.refreshed &&
        userReport.succeeded
      ) {
        successfulUserRefreshDays.add(day);
      }

      if (backfillDelayMs > 0) {
        await delay(backfillDelayMs);
      }
    }

    if (!costCenterSnapshot?.complete) {
      return;
    }

    for (const day of successfulUserRefreshDays) {
      if (
        incompleteCostCenterDays.includes(day) &&
        !missingCostCenterDays.includes(day) &&
        !costCenterSnapshot.unassignedComplete
      ) {
        continue;
      }

      try {
        await this.ingestCostCenterAggregates(
          day,
          metricsType,
          entityId,
          costCenterSnapshot,
          true,
        );
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed cost-center aggregation for ${metricsType}:${entityId} on ${day}; the day will be retried.`,
          error,
        );
      }
    }
  }

  private async getMissingIngestionDays(
    metricsType: MetricsScope,
    entityId: string,
    configuredFromDate: string,
    reportFromDate: string,
    toDate: string,
    requiredComponents: string[],
    ingestUsers: boolean,
  ): Promise<{ missingDays: string[] }> {
    const missingDays = await this.options.db.getMissingDays(
      metricsType,
      entityId,
      reportFromDate,
      toDate,
      requiredComponents,
    );

    if (!ingestUsers || reportFromDate === configuredFromDate) {
      return { missingDays };
    }

    const missingTotalsDays = await this.options.db.getMissingDays(
      metricsType,
      entityId,
      configuredFromDate,
      toDate,
      ['totals'],
    );

    return {
      missingDays: uniqueDays(missingTotalsDays, missingDays),
    };
  }

  private async loadCostCenterSnapshot(
    entities: Array<{ metricsType: MetricsScope; entityId: string }>,
  ): Promise<CostCenterMembershipSnapshot> {
    const { api, db, logger } = this.options;
    // GithubClientV2 logs the warnings as it builds the snapshot.
    const snapshot = await api.fetchCostCenterMemberships();

    if (!snapshot.complete) {
      logger.warn(
        '[TaskManagementV2] Cost-center membership snapshot is incomplete; preserving existing membership and aggregate data and retrying on the next run.',
      );
      return snapshot;
    }

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

  private async fetchTotalsReport(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    strict: boolean = false,
  ): Promise<ParsedEnterpriseDocument | undefined> {
    const { api, logger } = this.options;
    const reportEnvelope =
      metricsType === 'enterprise'
        ? await api.fetchEnterpriseReportLinks(day)
        : await api.fetchOrganizationReportLinks(day);

    const totalLinks = reportEnvelope.download_links ?? [];
    if (totalLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} totals on ${day} — report data may not yet be available for this period.`,
      );
      return undefined;
    }

    const parsedReports: ParsedEnterpriseDocument[] = [];
    for (const url of totalLinks) {
      const document = await api.downloadDocument(url);
      parsedReports.push(
        metricsType === 'organization'
          ? parseOrganizationDocument(document, metricsType, entityId, {
              strict,
            })
          : parseEnterpriseDocument(document, metricsType, entityId, {
              strict,
            }),
      );
    }

    const parsed = mergeTotalsReports(parsedReports);
    if (parsed.dailyTotals.length === 0) {
      logger.warn(
        `[TaskManagementV2] Parsed 0 daily rows for ${metricsType} totals on ${day} — not marking as success.`,
      );
      return undefined;
    }

    validateReportScope(
      [
        ...parsed.dailyTotals,
        ...parsed.prMetrics,
        ...parsed.byFeature,
        ...parsed.byIde,
        ...parsed.byLanguageFeature,
        ...parsed.byModelFeature,
        ...parsed.byLanguageModel,
        ...parsed.byCli,
        ...parsed.byVscodeAgent,
        ...parsed.customizationMetrics,
        ...parsed.customizationSummaries,
      ],
      day,
      metricsType,
      entityId,
      'Totals',
    );

    return parsed;
  }

  private async persistTotalsReport(
    parsed: ParsedEnterpriseDocument,
  ): Promise<void> {
    const { db } = this.options;
    await db.insertDailyTotals(parsed.dailyTotals);
    await db.insertPrMetrics(parsed.prMetrics);
    await db.insertByFeature(parsed.byFeature);
    await db.insertByIde(parsed.byIde);
    await db.insertByLanguageFeature(parsed.byLanguageFeature);
    await db.insertByModelFeature(parsed.byModelFeature);
    await db.insertByLanguageModel(parsed.byLanguageModel);
    await db.insertByCli(parsed.byCli);
    await db.insertByVscodeAgent(parsed.byVscodeAgent);
    await db.insertCustomizationMetrics(parsed.customizationMetrics);
    await db.insertCustomizationSummaries(parsed.customizationSummaries);
  }

  private async ingestTotals(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<boolean> {
    const parsed = await this.fetchTotalsReport(day, metricsType, entityId);
    if (!parsed) {
      return false;
    }

    await this.persistTotalsReport(parsed);
    return true;
  }

  private async fetchUserReport(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    strict: boolean = false,
  ): Promise<ParsedUserDocument | undefined> {
    const { api, logger } = this.options;
    let userReportEnvelope: Awaited<
      ReturnType<GithubClientV2['fetchEnterpriseUserReportLinks']>
    >;
    try {
      userReportEnvelope =
        metricsType === 'enterprise'
          ? await api.fetchEnterpriseUserReportLinks(day)
          : await api.fetchOrganizationUserReportLinks(day);
    } catch (error) {
      if (
        metricsType === 'organization' &&
        logOrganizationMetricsPermissionError(logger, 'per-user', day, error)
      ) {
        return undefined;
      }
      throw error;
    }

    const userLinks = userReportEnvelope.download_links ?? [];
    if (userLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} users on ${day} — user metrics will be skipped.`,
      );
      return undefined;
    }

    const userMetrics: V2UserMetricRow[] = [];
    const userBreakdowns: UserBreakdownData[] = [];
    const customizationMetrics: ParsedUserDocument['customizationMetrics'] = [];
    const customizationSummaries: ParsedUserDocument['customizationSummaries'] =
      [];

    for (const url of userLinks) {
      const rows = await api.downloadNdjsonDocument(url);
      const parsed = parseUserDocument(rows, metricsType, entityId, {
        strict,
      });
      userMetrics.push(...parsed.userMetrics);
      userBreakdowns.push(...parsed.userBreakdowns);
      customizationMetrics.push(...parsed.customizationMetrics);
      customizationSummaries.push(...parsed.customizationSummaries);
    }

    if (strict && userMetrics.length === 0) {
      logger.warn(
        `[TaskManagementV2] Parsed 0 user rows for ${metricsType} users on ${day} — preserving the stored report.`,
      );
      return undefined;
    }

    validateReportScope(
      [...userMetrics, ...customizationMetrics, ...customizationSummaries],
      day,
      metricsType,
      entityId,
      'User',
    );

    return {
      userMetrics,
      userBreakdowns,
      customizationMetrics,
      customizationSummaries,
    };
  }

  private async persistUserReport(report: ParsedUserDocument): Promise<void> {
    const { db } = this.options;
    await db.insertCustomizationMetrics(report.customizationMetrics);
    await db.insertCustomizationSummaries(report.customizationSummaries);
    await db.insertUserMetrics(report.userMetrics);

    const firstMetric = report.userMetrics[0];
    if (!firstMetric) {
      return;
    }
    const breakdownRows = buildUserBreakdownRows(
      report.userBreakdowns,
      firstMetric.day,
      firstMetric.metrics_type,
      firstMetric.entity_id,
    );
    await db.insertUserMetricsByFeature(breakdownRows.byFeature);
    await db.insertUserMetricsByIde(breakdownRows.byIde);
    await db.insertUserMetricsByLanguageFeature(
      breakdownRows.byLanguageFeature,
    );
    await db.insertUserMetricsByModelFeature(breakdownRows.byModelFeature);
    await db.insertUserMetricsByLanguageModel(breakdownRows.byLanguageModel);
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
    const report = await this.fetchUserReport(day, metricsType, entityId);
    if (!report) {
      return { loaded: false, userMetrics: [], userBreakdowns: [] };
    }

    await this.persistUserReport(report);
    return {
      loaded: true,
      userMetrics: report.userMetrics,
      userBreakdowns: report.userBreakdowns,
    };
  }

  private async fetchUserTeamsReport(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    strict: boolean = false,
  ): Promise<V2UserTeamRow[] | undefined> {
    const { api, logger } = this.options;
    let userTeamsEnvelope: Awaited<
      ReturnType<GithubClientV2['fetchEnterpriseUserTeamsLinks']>
    >;
    try {
      userTeamsEnvelope =
        metricsType === 'enterprise'
          ? await api.fetchEnterpriseUserTeamsLinks(day)
          : await api.fetchOrganizationUserTeamsLinks(day);
    } catch (error) {
      if (
        metricsType === 'organization' &&
        logOrganizationMetricsPermissionError(logger, 'user-teams', day, error)
      ) {
        return undefined;
      }
      throw error;
    }

    const userTeamLinks = userTeamsEnvelope.download_links ?? [];
    if (userTeamLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} user-teams on ${day} — team aggregation will be skipped.`,
      );
      return undefined;
    }

    const userTeams: V2UserTeamRow[] = [];
    for (const url of userTeamLinks) {
      const rows = await api.downloadNdjsonDocument(url);
      userTeams.push(
        ...parseUserTeamsDocument(rows, metricsType, entityId, { strict }),
      );
    }

    if (strict && userTeams.length === 0) {
      logger.warn(
        `[TaskManagementV2] Parsed 0 user-team rows for ${metricsType} on ${day} — preserving the stored report.`,
      );
      return undefined;
    }

    validateReportScope(userTeams, day, metricsType, entityId, 'User-team');
    return userTeams;
  }

  private async ingestTeamAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userMetrics: V2UserMetricRow[],
    userBreakdowns: UserBreakdownData[],
    userReportLoaded: boolean,
  ): Promise<boolean> {
    const { db, logger } = this.options;
    const userTeams = await this.fetchUserTeamsReport(
      day,
      metricsType,
      entityId,
    );
    if (!userTeams) {
      return false;
    }

    if (!userReportLoaded) {
      logger.warn(
        `[TaskManagementV2] Skipping ${metricsType} team aggregation for ${day} because the per-user report was not loaded.`,
      );
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

    await db.replaceTeamAggregates(
      day,
      metricsType,
      entityId,
      userTeams,
      teamAggregates,
    );

    return userMetrics.length > 0;
  }

  private async ingestCostCenterAggregates(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    snapshot: CostCenterMembershipSnapshot,
    markEmptyDay: boolean,
  ): Promise<boolean> {
    const { db } = this.options;
    let userMetrics = await db.getUserMetricsForDay(metricsType, entityId, day);

    if (!userMetrics.length && !markEmptyDay) {
      return false;
    }

    let userBreakdowns = userMetrics.length
      ? await db.getUserBreakdownsForDay(metricsType, entityId, day)
      : [];
    // Legacy successful ingestion logs can predate persisted user breakdowns.
    if (userMetrics.length > 0 && userBreakdowns.length === 0) {
      const report = await this.ingestUserReports(day, metricsType, entityId);
      if (!report.loaded || report.userBreakdowns.length === 0) {
        return false;
      }
      userMetrics = await db.getUserMetricsForDay(metricsType, entityId, day);
      userBreakdowns = report.userBreakdowns;
    }

    const memberships = new Map<number, string>();
    const userCostCentersByUserId = new Map<number, V2UserCostCenterRow>();
    for (const metric of userMetrics) {
      const membership = snapshot.members.get(metric.user_id);
      if (!membership) {
        continue;
      }
      memberships.set(metric.user_id, membership.costCenterId);

      userCostCentersByUserId.set(metric.user_id, {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        user_id: metric.user_id,
        user_login: membership.login,
        cost_center_id: membership.costCenterId,
      });
    }
    const userCostCenters = [...userCostCentersByUserId.values()];

    const aggregates = aggregateCostCenterMetrics(
      userMetrics,
      userBreakdowns,
      memberships,
      day,
      metricsType,
      entityId,
      snapshot.unassignedComplete,
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
        byCli: aggregates.byCli,
        byVscodeAgent: aggregates.byVscodeAgent,
        customizationMetrics: aggregates.costCenterCustomizationMetrics,
      },
      snapshot.unassignedComplete,
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
    replaceComponents: boolean = false,
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

    const ingestionLog = {
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      status,
      components_loaded: JSON.stringify(componentsLoaded),
      error_message: errorMessage,
      source: context.source,
    } as const;
    if (replaceComponents) {
      await db.upsertIngestionLog(ingestionLog, { replaceComponents: true });
    } else {
      await db.upsertIngestionLog(ingestionLog);
    }

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

  private async failRefetch(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
    previousLog: V2IngestionLogRow | undefined,
    message: string,
  ): Promise<{ loaded: boolean; refreshed: boolean; succeeded: boolean }> {
    const { db, logger } = this.options;
    logger.warn(
      `[TaskManagementV2] Refetch for ${metricsType}:${entityId} on ${day} was not applied; preserving stored report data.`,
    );

    if (!previousLog) {
      await db.upsertIngestionLog({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        status: 'error',
        components_loaded: '[]',
        error_message: message,
        source: context.source,
      });
    }

    return { loaded: false, refreshed: false, succeeded: false };
  }

  private async refetchDay(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
    previousLog: V2IngestionLogRow | undefined,
    requiredComponents: string[],
  ): Promise<{ loaded: boolean; refreshed: boolean; succeeded: boolean }> {
    const { db } = this.options;
    const totals = await this.fetchTotalsReport(
      day,
      metricsType,
      entityId,
      true,
    );
    if (!totals) {
      return this.failRefetch(
        day,
        metricsType,
        entityId,
        context,
        previousLog,
        'The totals report was unavailable or empty.',
      );
    }

    const userReport = context.ingestUsers
      ? await this.fetchUserReport(day, metricsType, entityId, true)
      : undefined;
    if (context.ingestUsers && !userReport) {
      return this.failRefetch(
        day,
        metricsType,
        entityId,
        context,
        previousLog,
        'The user report was unavailable or empty.',
      );
    }

    const userTeams = context.ingestTeams
      ? await this.fetchUserTeamsReport(day, metricsType, entityId, true)
      : undefined;
    if (context.ingestTeams && (!userReport || !userTeams)) {
      return this.failRefetch(
        day,
        metricsType,
        entityId,
        context,
        previousLog,
        'The user-team report was unavailable or empty.',
      );
    }

    const reports: V2RawReportDayData = { totals };
    if (userReport) {
      reports.users = {
        userMetrics: userReport.userMetrics,
        breakdowns: buildUserBreakdownRows(
          userReport.userBreakdowns,
          day,
          metricsType,
          entityId,
        ),
        customizationMetrics: userReport.customizationMetrics,
        customizationSummaries: userReport.customizationSummaries,
      };
      // Rebuild any existing cost-center slices after their source users change.
      reports.markCostCenterPending = true;
    }

    await db.replaceRawReportDay(day, metricsType, entityId, reports);

    const componentsLoaded = ['totals'];
    if (userReport) {
      componentsLoaded.push('users');
    }
    if (context.ingestTeams && userReport && userTeams) {
      const teamAggregates = aggregateTeamMetrics(
        userReport.userMetrics,
        userTeams,
        userReport.userBreakdowns,
        day,
        metricsType,
        entityId,
      );
      await db.replaceTeamAggregates(
        day,
        metricsType,
        entityId,
        userTeams,
        teamAggregates,
      );
      componentsLoaded.push('teams');
    }

    await this.logIngestionOutcome(
      day,
      metricsType,
      entityId,
      context,
      componentsLoaded,
      requiredComponents,
      true,
    );

    return {
      loaded: Boolean(userReport),
      refreshed: Boolean(userReport),
      succeeded: true,
    };
  }

  private async ingestDay(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
  ): Promise<{ loaded: boolean; refreshed: boolean; succeeded: boolean }> {
    const { db, logger } = this.options;
    const requiredComponents = getRequiredComponents(
      context.ingestUsers,
      context.ingestTeams,
    );
    const componentsLoaded = new Set<string>();
    let userReportsLoaded = false;
    let userReportsRefreshed = false;
    let previousLog: V2IngestionLogRow | undefined;

    try {
      previousLog = (
        await db.getIngestionLog(metricsType, entityId, day, day)
      )[0];
      if (context.refetch) {
        return await this.refetchDay(
          day,
          metricsType,
          entityId,
          context,
          previousLog,
          requiredComponents,
        );
      }

      const previouslyLoaded = new Set(
        parseComponentsLoaded(previousLog?.components_loaded),
      );
      for (const component of previouslyLoaded) {
        componentsLoaded.add(component);
      }

      if (
        !previouslyLoaded.has('totals') &&
        (await this.ingestTotals(day, metricsType, entityId))
      ) {
        componentsLoaded.add('totals');
      }

      if (context.ingestUsers) {
        const storedUserMetrics = previouslyLoaded.has('users')
          ? await db.getUserMetricsForDay(metricsType, entityId, day)
          : [];
        const storedUserBreakdowns =
          storedUserMetrics.length > 0
            ? await db.getUserBreakdownsForDay(metricsType, entityId, day)
            : [];
        // Reuse only complete stored reports; users loaded before breakdowns were persisted lack them.
        const reuseStoredReport =
          storedUserMetrics.length > 0 && storedUserBreakdowns.length > 0;
        const userReport = reuseStoredReport
          ? {
              loaded: true,
              userMetrics: storedUserMetrics,
              userBreakdowns: storedUserBreakdowns,
            }
          : await this.ingestUserReports(day, metricsType, entityId);
        userReportsLoaded = userReport.loaded;
        userReportsRefreshed = userReport.loaded && !reuseStoredReport;
        if (userReportsRefreshed && context.ingestCostCenters) {
          await db.markCostCenterAggregationPending(day, metricsType, entityId);
        }
        if (userReport.loaded) {
          componentsLoaded.add('users');
        }

        if (
          context.ingestTeams &&
          (await this.ingestTeamAggregates(
            day,
            metricsType,
            entityId,
            userReport.userMetrics,
            userReport.userBreakdowns,
            userReport.loaded,
          ))
        ) {
          componentsLoaded.add('teams');
        }
      }

      await this.logIngestionOutcome(
        day,
        metricsType,
        entityId,
        context,
        [...componentsLoaded],
        requiredComponents,
      );
      return {
        loaded: userReportsLoaded,
        refreshed: userReportsRefreshed,
        succeeded: requiredComponents.every(component =>
          componentsLoaded.has(component),
        ),
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error(
        `[TaskManagementV2] Failed ingest for ${metricsType}:${entityId} on ${day}.`,
        error,
      );

      if (context.refetch) {
        if (!previousLog) {
          await db.upsertIngestionLog({
            day,
            metrics_type: metricsType,
            entity_id: entityId,
            status: 'error',
            components_loaded: '[]',
            error_message: message,
            source: context.source,
          });
        }
        return { loaded: false, refreshed: false, succeeded: false };
      }

      await db.upsertIngestionLog({
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        status: 'error',
        components_loaded: JSON.stringify([...componentsLoaded]),
        error_message: message,
        source: context.source,
      });
      return {
        loaded: userReportsLoaded,
        refreshed: userReportsRefreshed,
        succeeded: false,
      };
    }
  }
}
