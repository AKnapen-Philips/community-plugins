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
import type { Knex } from 'knex';
import {
  MetricsScope,
  V2UserTeamRow,
  V2UserCostCenterRow,
} from '@backstage-community/plugin-copilot-common';
import {
  DatabaseHandlerV2,
  parseComponentsLoaded,
} from '../db/DatabaseHandlerV2';
import { GithubClientV2 } from '../client/GithubClientV2';
import {
  InaccessibleEnterpriseOrganizationError,
  type CostCenterMembershipSnapshot,
} from '../client/GithubClientV2';
import {
  parseEnterpriseDocument,
  parseOrganizationDocument,
  parseUserDocument,
  parseUserTeamsDocument,
  ParsedEnterpriseDocument,
  ParsedUserDocument,
} from '../utils/reportParser';
import { buildUserBreakdownRows } from '../utils/userBreakdownRows';

type IngestSource = 'scheduled' | 'backfill';

type IngestContext = {
  ingestTeams: boolean;
  ingestUsers: boolean;
  ingestCostCenters: boolean;
  source: IngestSource;
  refreshTeams?: boolean;
  organizationReportTarget?: string;
};

type PreparedUserReport = ParsedUserDocument & { loaded: boolean };

type IngestPlan = {
  entity: { metricsType: MetricsScope; entityId: string };
  missingDays: string[];
  missingCostCenterDays: string[];
  incompleteCostCenterDays: string[];
  ingestUsers: boolean;
  ingestTeams: boolean;
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
const ORGANIZATION_TEAM_BOOTSTRAP_DAYS = 30;
const ORGANIZATION_TEAM_MAX_ORGANIZATIONS_PER_RUN = 6;
const ORGANIZATION_TEAM_MAX_BACKLOG_DAYS_PER_RUN = 3;
const ORGANIZATION_TEAM_REFRESH_DAYS = 3;
const ORGANIZATION_TEAM_RETRY_HOURS = 23;
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

function isSupportedCostCenterScope(
  metricsType: MetricsScope,
  entityId: string,
  enterprise?: string,
  organization?: string,
): boolean {
  if (metricsType === 'enterprise') {
    return Boolean(enterprise && entityId === enterprise);
  }
  return Boolean(
    enterprise &&
      organization &&
      entityId.trim().toLowerCase() === organization.trim().toLowerCase(),
  );
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
    const ingestOrganizationTeamReports =
      config.getOptionalBoolean('copilot.ingestOrganizationTeamReports') ??
      false;

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
    for (const entity of entities) {
      const costCenterEnabledForEntity =
        ingestCostCenters && Boolean(enterprise);
      const ingestTeamsForEntity =
        ingestTeams ||
        (ingestOrganizationTeamReports &&
          Boolean(enterprise) &&
          entity.metricsType === 'organization');
      const ingestUsers = ingestTeamsForEntity || costCenterEnabledForEntity;
      const requiredComponents = getRequiredComponents(
        ingestUsers,
        ingestTeamsForEntity,
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
      const missingCostCenterDays = costCenterEnabledForEntity
        ? await db.getDaysMissingCostCenterMemberships(
            entity.metricsType,
            entity.entityId,
            missingFromDate,
            yesterday,
          )
        : [];
      const incompleteCostCenterDays = costCenterEnabledForEntity
        ? await db.getDaysWithIncompleteCostCenterMemberships(
            entity.metricsType,
            entity.entityId,
            missingFromDate,
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
        incompleteCostCenterDays,
        ingestUsers,
        ingestTeams: ingestTeamsForEntity,
        ingestCostCenters: costCenterEnabledForEntity,
      });
    }

    const costCenterPlans = plans.filter(
      plan =>
        plan.ingestCostCenters &&
        (plan.missingDays.some(day => day >= reportWindowStartDate) ||
          plan.missingCostCenterDays.some(
            day => day >= reportWindowStartDate,
          ) ||
          plan.incompleteCostCenterDays.some(
            day => day >= reportWindowStartDate,
          )),
    );
    const costCenterDays = uniqueDays(
      ...costCenterPlans.map(plan => [
        ...plan.missingDays.filter(day => day >= reportWindowStartDate),
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

    let configuredOrganizationObservedAt: Date | undefined;
    if (ingestOrganizationTeamReports) {
      if (!enterprise) {
        logger.warn(
          '[TaskManagementV2] copilot.ingestOrganizationTeamReports is enabled, but copilot.enterprise is not configured; skipping multi-organization team ingestion.',
        );
      } else {
        configuredOrganizationObservedAt =
          await this.runOrganizationTeamIngestion(enterprise, organization);
      }
    }

    for (const plan of plans) {
      const isConfiguredOrganization =
        plan.entity.metricsType === 'organization';
      if (
        ingestOrganizationTeamReports &&
        enterprise &&
        isConfiguredOrganization &&
        !configuredOrganizationObservedAt
      ) {
        continue;
      }

      const loadedUserReportDays = new Set<string>();
      const refreshedUserReportDays = new Set<string>();
      for (const day of plan.missingDays) {
        const userReport = await this.ingestDay(
          day,
          plan.entity.metricsType,
          plan.entity.entityId,
          {
            ingestTeams: plan.ingestTeams && day >= reportWindowStartDate,
            ingestUsers: plan.ingestUsers && day >= reportWindowStartDate,
            ingestCostCenters: plan.ingestCostCenters,
            source: 'scheduled',
          },
        );
        if (userReport.loaded) {
          loadedUserReportDays.add(day);
        }
        if (userReport.refreshed) {
          refreshedUserReportDays.add(day);
        }

        if (backfillDelayMs > 0) {
          await delay(backfillDelayMs);
        }
      }

      if (costCenterSnapshot?.complete && plan.ingestCostCenters) {
        const retryIncompleteDays = costCenterSnapshot.unassignedComplete
          ? plan.incompleteCostCenterDays
          : [];
        const daysToAggregate = uniqueDays(
          [...refreshedUserReportDays].filter(
            day => day >= reportWindowStartDate,
          ),
          plan.missingCostCenterDays.filter(
            day => day >= reportWindowStartDate,
          ),
          retryIncompleteDays.filter(day => day >= reportWindowStartDate),
        );
        for (const day of daysToAggregate) {
          if (
            plan.missingDays.includes(day) &&
            !loadedUserReportDays.has(day)
          ) {
            continue;
          }
          try {
            await this.ingestCostCenterMemberships(
              day,
              plan.entity.metricsType,
              plan.entity.entityId,
              costCenterSnapshot,
              plan.missingCostCenterDays.includes(day) ||
                retryIncompleteDays.includes(day) ||
                loadedUserReportDays.has(day),
              refreshedUserReportDays.has(day),
            );
          } catch (error) {
            logger.error(
              `[TaskManagementV2] Failed cost-center membership ingestion for ${plan.entity.metricsType}:${plan.entity.entityId} on ${day}; the day will be retried.`,
              error,
            );
          }
        }
      }

      if (
        ingestOrganizationTeamReports &&
        configuredOrganizationObservedAt &&
        isConfiguredOrganization &&
        enterprise &&
        organization
      ) {
        const completedAt = DateTime.utc();
        await db.markLegacyConfiguredOrganization(
          enterprise,
          organization,
          completedAt.toJSDate(),
          completedAt.plus({ hours: ORGANIZATION_TEAM_RETRY_HOURS }).toJSDate(),
          configuredOrganizationObservedAt,
        );
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
    await this.runBackfillInternal(fromDate, toDate, metricsType, entityId);
  }

  private async runBackfillInternal(
    fromDate: string,
    toDate: string,
    metricsType: MetricsScope,
    entityId: string,
    discoveredOrganization?: string,
  ): Promise<void> {
    const { api, config, db, logger } = this.options;
    const ingestTeams =
      config.getOptionalBoolean('copilot.ingestTeams') ?? false;
    const ingestCostCenters =
      config.getOptionalBoolean('copilot.ingestCostCenters') ?? false;
    const ingestOrganizationTeamReports =
      config.getOptionalBoolean('copilot.ingestOrganizationTeamReports') ??
      false;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    const isConfiguredOrganizationBackfill = Boolean(
      !discoveredOrganization &&
        ingestOrganizationTeamReports &&
        enterprise &&
        organization &&
        metricsType === 'organization' &&
        entityId.toLowerCase() === organization.toLowerCase(),
    );
    let canonicalOrganization = discoveredOrganization;
    if (isConfiguredOrganizationBackfill && enterprise) {
      canonicalOrganization =
        await db.getCurrentAccessibleEnterpriseOrganizationLogin(
          enterprise,
          entityId,
        );
      if (!canonicalOrganization) {
        throw new Error(
          'Configured organization is not available for backfill.',
        );
      }
    }
    const backfillEntityId = canonicalOrganization ?? entityId;
    const ingestTeamsForScope =
      ingestTeams ||
      isConfiguredOrganizationBackfill ||
      Boolean(discoveredOrganization);
    const costCenterIngestionAvailable =
      !discoveredOrganization &&
      ingestCostCenters &&
      isSupportedCostCenterScope(
        metricsType,
        backfillEntityId,
        enterprise,
        organization,
      );

    if (!discoveredOrganization && ingestCostCenters && !enterprise) {
      logger.warn(
        '[TaskManagementV2] copilot.ingestCostCenters is enabled, but copilot.enterprise is not configured; skipping cost-center ingestion.',
      );
    } else if (
      !discoveredOrganization &&
      ingestCostCenters &&
      metricsType === 'organization' &&
      !costCenterIngestionAvailable
    ) {
      logger.warn(
        `[TaskManagementV2] Organization cost-center ingestion requires copilot.enterprise and the configured copilot.organization; skipping cost-center ingestion for organization:${backfillEntityId}.`,
      );
    }

    const ingestUsers = ingestTeamsForScope || costCenterIngestionAvailable;
    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    const requiredComponents = getRequiredComponents(
      ingestUsers,
      ingestTeamsForScope,
    );
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

    const { missingDays } = await this.getMissingIngestionDays(
      metricsType,
      backfillEntityId,
      fromDate,
      missingFromDate,
      toDate,
      requiredComponents,
      ingestUsers,
    );
    const missingCostCenterDays = costCenterIngestionAvailable
      ? await db.getDaysMissingCostCenterMemberships(
          metricsType,
          backfillEntityId,
          missingFromDate,
          toDate,
        )
      : [];
    const incompleteCostCenterDays = costCenterIngestionAvailable
      ? await db.getDaysWithIncompleteCostCenterMemberships(
          metricsType,
          backfillEntityId,
          missingFromDate,
          toDate,
        )
      : [];
    const costCenterDays = uniqueDays(
      missingDays.filter(day => day >= reportWindowStartDate),
      missingCostCenterDays.filter(day => day >= reportWindowStartDate),
      incompleteCostCenterDays.filter(day => day >= reportWindowStartDate),
    );

    if (
      !missingDays.length &&
      !missingCostCenterDays.length &&
      !incompleteCostCenterDays.length
    ) {
      logger.info(
        `[TaskManagementV2] No missing days for backfill ${metricsType}:${backfillEntityId} in range ${fromDate}..${toDate}.`,
      );
      return;
    }

    if (missingDays.length) {
      logger.info(
        `[TaskManagementV2] Backfill processing ${missingDays.length} days for ${metricsType}:${backfillEntityId}.`,
      );
    }

    let costCenterSnapshot: CostCenterMembershipSnapshot | undefined;
    if (costCenterIngestionAvailable && costCenterDays.length) {
      try {
        costCenterSnapshot = await this.loadCostCenterSnapshot([
          { metricsType, entityId: backfillEntityId },
        ]);
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed to load cost-center membership for enterprise:${enterprise}; cost-center days will be retried.`,
          error,
        );
      }
    }

    const observedAt = DateTime.utc().toJSDate();
    let organizationBackfillIncomplete = false;
    let organizationReportsReady = false;
    try {
      if (discoveredOrganization) {
        const yesterday = DateTime.utc().minus({ days: 1 }).toISODate();
        if (!yesterday) {
          throw new Error('Unable to compute yesterday date.');
        }
        await api.verifyEnterpriseOrganizationAccess(
          discoveredOrganization,
          yesterday,
        );
      }

      const loadedUserReportDays = new Set<string>();
      const refreshedUserReportDays = new Set<string>();
      for (const day of missingDays) {
        const userReport = await this.ingestDay(
          day,
          metricsType,
          backfillEntityId,
          {
            ingestTeams: ingestTeamsForScope && day >= reportWindowStartDate,
            ingestUsers: ingestUsers && day >= reportWindowStartDate,
            ingestCostCenters: costCenterIngestionAvailable,
            source: 'backfill',
            ...(discoveredOrganization
              ? { organizationReportTarget: discoveredOrganization }
              : {}),
          },
        );
        if (discoveredOrganization) {
          if (!userReport.complete) {
            organizationBackfillIncomplete = true;
          }
          if (userReport.totalsLoaded) {
            organizationReportsReady = true;
          }
        }
        if (userReport.loaded) {
          loadedUserReportDays.add(day);
        }
        if (userReport.refreshed) {
          refreshedUserReportDays.add(day);
        }

        if (backfillDelayMs > 0) {
          await delay(backfillDelayMs);
        }
      }

      if (costCenterSnapshot?.complete) {
        const retryIncompleteDays = costCenterSnapshot.unassignedComplete
          ? incompleteCostCenterDays
          : [];
        for (const day of costCenterDays) {
          if (
            incompleteCostCenterDays.includes(day) &&
            !missingCostCenterDays.includes(day) &&
            !retryIncompleteDays.includes(day)
          ) {
            continue;
          }
          if (missingDays.includes(day) && !loadedUserReportDays.has(day)) {
            continue;
          }
          if (
            !missingCostCenterDays.includes(day) &&
            !retryIncompleteDays.includes(day) &&
            !refreshedUserReportDays.has(day)
          ) {
            continue;
          }

          try {
            await this.ingestCostCenterMemberships(
              day,
              metricsType,
              backfillEntityId,
              costCenterSnapshot,
              missingCostCenterDays.includes(day) ||
                retryIncompleteDays.includes(day) ||
                loadedUserReportDays.has(day),
              refreshedUserReportDays.has(day),
            );
          } catch (error) {
            logger.error(
              `[TaskManagementV2] Failed cost-center membership ingestion for ${metricsType}:${backfillEntityId} on ${day}; the day will be retried.`,
              error,
            );
          }
        }
      }

      if (discoveredOrganization && enterprise) {
        const completedAt = DateTime.utc();
        await db.updateEnterpriseOrganizationState(
          enterprise,
          discoveredOrganization,
          {
            accessState: 'accessible',
            ...(organizationReportsReady ? { reportsReady: true } : {}),
            lastAttemptedAt: completedAt.toJSDate(),
            ...(organizationBackfillIncomplete
              ? { nextAttemptAt: completedAt.plus({ hours: 1 }).toJSDate() }
              : {}),
          },
          observedAt,
        );
      } else if (enterprise && canonicalOrganization) {
        const completedAt = DateTime.utc();
        await db.markLegacyConfiguredOrganization(
          enterprise,
          canonicalOrganization,
          completedAt.toJSDate(),
          undefined,
          observedAt,
        );
      }
    } catch (error) {
      if (
        discoveredOrganization &&
        enterprise &&
        error instanceof InaccessibleEnterpriseOrganizationError
      ) {
        const failedAt = DateTime.utc();
        await db.updateEnterpriseOrganizationState(
          enterprise,
          discoveredOrganization,
          {
            accessState: 'inaccessible',
            reportsReady: false,
            lastAttemptedAt: failedAt.toJSDate(),
            nextAttemptAt: failedAt.plus({ hours: 24 }).toJSDate(),
          },
          observedAt,
        );
        throw new Error('Organization is no longer available.');
      }
      throw error;
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

  private async prepareTotals(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    organizationReportTarget?: string,
  ): Promise<ParsedEnterpriseDocument[]> {
    const { api, logger } = this.options;
    let reportEnvelope: Awaited<
      ReturnType<GithubClientV2['fetchEnterpriseReportLinks']>
    >;
    if (metricsType === 'enterprise') {
      reportEnvelope = await api.fetchEnterpriseReportLinks(day);
    } else if (organizationReportTarget) {
      reportEnvelope = await api.fetchEnterpriseOrganizationReportLinks(
        organizationReportTarget,
        day,
      );
    } else {
      reportEnvelope = await api.fetchOrganizationReportLinks(day);
    }

    const totalLinks = reportEnvelope.download_links ?? [];
    if (totalLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} totals on ${day} — report data may not yet be available for this period.`,
      );
      return [];
    }

    let totalRowsParsed = 0;
    const documents: ParsedEnterpriseDocument[] = [];

    for (const url of totalLinks) {
      const document = await api.downloadDocument(url);
      const parsed =
        metricsType === 'organization'
          ? parseOrganizationDocument(document, metricsType, entityId)
          : parseEnterpriseDocument(document, metricsType, entityId);

      documents.push(parsed);
      totalRowsParsed += parsed.dailyTotals.length;
    }

    if (totalRowsParsed === 0) {
      logger.warn(
        `[TaskManagementV2] Parsed 0 daily rows for ${metricsType} totals on ${day} — not marking as success.`,
      );
      return [];
    }

    return documents;
  }

  private async prepareUserReports(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    organizationReportTarget?: string,
    strict = false,
  ): Promise<PreparedUserReport> {
    const { api, logger } = this.options;
    const report: PreparedUserReport = {
      loaded: false,
      userMetrics: [],
      userBreakdowns: [],
      customizationMetrics: [],
      customizationSummaries: [],
    };
    let userReportEnvelope: Awaited<
      ReturnType<GithubClientV2['fetchEnterpriseUserReportLinks']>
    >;
    try {
      if (metricsType === 'enterprise') {
        userReportEnvelope = await api.fetchEnterpriseUserReportLinks(day);
      } else if (organizationReportTarget) {
        userReportEnvelope =
          await api.fetchEnterpriseOrganizationUserReportLinks(
            organizationReportTarget,
            day,
          );
      } else {
        userReportEnvelope = await api.fetchOrganizationUserReportLinks(day);
      }
    } catch (error) {
      if (
        metricsType === 'organization' &&
        logOrganizationMetricsPermissionError(logger, 'per-user', day, error)
      ) {
        return report;
      }
      throw error;
    }

    const userLinks = userReportEnvelope.download_links ?? [];
    if (userLinks.length === 0) {
      logger.warn(
        `[TaskManagementV2] No download links for ${metricsType} users on ${day} — user metrics will be skipped.`,
      );
      return report;
    }

    for (const url of userLinks) {
      const rows = strict
        ? await api.downloadNdjsonDocument(url, { strict: true })
        : await api.downloadNdjsonDocument(url);
      const parsed = strict
        ? parseUserDocument(rows, metricsType, entityId, {
            strict: true,
            expectedDay: day,
          })
        : parseUserDocument(rows, metricsType, entityId);
      report.userMetrics.push(...parsed.userMetrics);
      report.userBreakdowns.push(...parsed.userBreakdowns);
      report.customizationMetrics.push(...parsed.customizationMetrics);
      report.customizationSummaries.push(...parsed.customizationSummaries);
    }

    report.loaded = true;
    return report;
  }

  private async persistTotals(
    documents: ParsedEnterpriseDocument[],
    transaction: Knex.Transaction,
  ): Promise<void> {
    const { db } = this.options;
    for (const parsed of documents) {
      await db.insertDailyTotals(parsed.dailyTotals, transaction);
      await db.insertPrMetrics(parsed.prMetrics, transaction);
      await db.insertByFeature(parsed.byFeature, transaction);
      await db.insertByIde(parsed.byIde, transaction);
      await db.insertByLanguageFeature(parsed.byLanguageFeature, transaction);
      await db.insertByModelFeature(parsed.byModelFeature, transaction);
      await db.insertByLanguageModel(parsed.byLanguageModel, transaction);
      await db.insertByCli(parsed.byCli, transaction);
      await db.insertByVscodeAgent(parsed.byVscodeAgent, transaction);
      await db.insertCustomizationMetrics(
        parsed.customizationMetrics,
        transaction,
      );
      await db.insertCustomizationSummaries(
        parsed.customizationSummaries,
        transaction,
      );
    }
  }

  private async persistUserReport(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    report: PreparedUserReport,
    transaction: Knex.Transaction,
    replaceExisting: boolean = false,
  ): Promise<void> {
    const { db } = this.options;
    if (replaceExisting) {
      await db.deleteUserReportForDay(day, metricsType, entityId, transaction);
    }
    const breakdownRows = buildUserBreakdownRows(
      report.userBreakdowns,
      day,
      metricsType,
      entityId,
    );
    await db.insertUserMetrics(report.userMetrics, transaction);
    await db.insertUserMetricsByFeature(breakdownRows.byFeature, transaction);
    await db.insertUserMetricsByIde(breakdownRows.byIde, transaction);
    await db.insertUserMetricsByLanguageFeature(
      breakdownRows.byLanguageFeature,
      transaction,
    );
    await db.insertUserMetricsByModelFeature(
      breakdownRows.byModelFeature,
      transaction,
    );
    await db.insertUserMetricsByLanguageModel(
      breakdownRows.byLanguageModel,
      transaction,
    );
    await db.insertCustomizationMetrics(
      report.customizationMetrics,
      transaction,
    );
    await db.insertCustomizationSummaries(
      report.customizationSummaries,
      transaction,
    );
  }

  private async prepareTeamMemberships(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    userReportLoaded: boolean,
    organizationReportTarget?: string,
    strict = false,
  ): Promise<V2UserTeamRow[] | undefined> {
    const { api, logger } = this.options;
    let userTeamsEnvelope: Awaited<
      ReturnType<GithubClientV2['fetchEnterpriseUserTeamsLinks']>
    >;
    try {
      if (metricsType === 'enterprise') {
        userTeamsEnvelope = await api.fetchEnterpriseUserTeamsLinks(day);
      } else if (organizationReportTarget) {
        userTeamsEnvelope = await api.fetchEnterpriseOrganizationUserTeamsLinks(
          organizationReportTarget,
          day,
        );
      } else {
        userTeamsEnvelope = await api.fetchOrganizationUserTeamsLinks(day);
      }
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
        `[TaskManagementV2] No download links for ${metricsType} user-teams on ${day} — team memberships will be skipped.`,
      );
      return undefined;
    }

    if (!userReportLoaded) {
      logger.warn(
        `[TaskManagementV2] Skipping ${metricsType} team memberships for ${day} because the per-user report was not loaded.`,
      );
      return undefined;
    }

    const userTeams: V2UserTeamRow[] = [];
    for (const url of userTeamLinks) {
      const rows = strict
        ? await api.downloadNdjsonDocument(url, { strict: true })
        : await api.downloadNdjsonDocument(url);
      userTeams.push(
        ...(strict
          ? parseUserTeamsDocument(rows, metricsType, entityId, {
              strict: true,
              expectedDay: day,
            })
          : parseUserTeamsDocument(rows, metricsType, entityId)),
      );
    }

    return userTeams;
  }

  private async ingestCostCenterMemberships(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    snapshot: CostCenterMembershipSnapshot,
    markEmptyDay: boolean,
    refreshForUserReport: boolean,
  ): Promise<boolean> {
    const { db } = this.options;
    const storedUserMetrics = await db.getUserMetricsForDay(
      metricsType,
      entityId,
      day,
    );

    if (!storedUserMetrics.length && !markEmptyDay) {
      return false;
    }

    const storedUserBreakdowns = storedUserMetrics.length
      ? await db.getUserBreakdownsForDay(metricsType, entityId, day)
      : [];
    let repair: PreparedUserReport | undefined;
    const previousLog = (
      await db.getIngestionLog(metricsType, entityId, day, day)
    )[0];
    // Legacy successful ingestion logs can predate persisted user breakdowns.
    if (
      storedUserMetrics.length > 0 &&
      storedUserBreakdowns.length === 0 &&
      !parseComponentsLoaded(previousLog?.components_loaded).includes(
        'userBreakdowns',
      )
    ) {
      // This repair marks raw breakdowns authoritative over legacy details.
      repair = await this.prepareUserReports(
        day,
        metricsType,
        entityId,
        undefined,
        true,
      );
      if (!repair.loaded || repair.userMetrics.length === 0) {
        return false;
      }
    }
    const userCostCenters: V2UserCostCenterRow[] = [
      ...snapshot.members.entries(),
    ].map(([userId, membership]) => ({
      day,
      metrics_type: metricsType,
      entity_id: entityId,
      user_id: userId,
      user_login: membership.login,
      cost_center_id: membership.costCenterId,
    }));

    return db.withIngestionDay(
      day,
      metricsType,
      entityId,
      async (transaction, existingLog) => {
        const missingDays = await db.getDaysMissingCostCenterMemberships(
          metricsType,
          entityId,
          day,
          day,
          transaction,
        );
        const incompleteDays =
          await db.getDaysWithIncompleteCostCenterMemberships(
            metricsType,
            entityId,
            day,
            day,
            transaction,
          );
        if (
          !missingDays.includes(day) &&
          !(snapshot.unassignedComplete && incompleteDays.includes(day)) &&
          !(refreshForUserReport && snapshot.complete)
        ) {
          return false;
        }
        const currentMetrics = await db.getUserMetricsForDay(
          metricsType,
          entityId,
          day,
          transaction,
        );
        const currentBreakdowns = currentMetrics.length
          ? await db.getUserBreakdownsForDay(
              metricsType,
              entityId,
              day,
              transaction,
            )
          : [];
        const componentsLoaded = new Set(
          parseComponentsLoaded(existingLog.components_loaded),
        );
        if (
          repair &&
          currentMetrics.length > 0 &&
          currentBreakdowns.length === 0 &&
          !componentsLoaded.has('userBreakdowns')
        ) {
          await this.persistUserReport(
            day,
            metricsType,
            entityId,
            repair,
            transaction,
          );
          componentsLoaded.add('userBreakdowns');
          await db.upsertIngestionLog(
            {
              ...existingLog,
              day,
              metrics_type: metricsType,
              entity_id: entityId,
              components_loaded: JSON.stringify([...componentsLoaded]),
            },
            transaction,
          );
        }
        await db.replaceCostCenterMemberships(
          day,
          metricsType,
          entityId,
          userCostCenters,
          snapshot.unassignedComplete,
          transaction,
        );
        return true;
      },
    );
  }

  private async logIngestionOutcome(
    day: string,
    metricsType: MetricsScope,
    entityId: string,
    context: IngestContext,
    componentsLoaded: string[],
    requiredComponents: string[],
    transaction: Knex.Transaction,
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

    await db.upsertIngestionLog(
      {
        day,
        metrics_type: metricsType,
        entity_id: entityId,
        status,
        components_loaded: JSON.stringify(componentsLoaded),
        error_message: errorMessage,
        source: context.source,
      },
      transaction,
    );

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
  ): Promise<{
    loaded: boolean;
    refreshed: boolean;
    complete: boolean;
    totalsLoaded: boolean;
  }> {
    const { db, logger } = this.options;
    let requiredComponents = getRequiredComponents(
      context.ingestUsers,
      context.ingestTeams,
    );
    let previouslyLoaded = new Set<string>();
    let requiresUserBreakdownRepair = false;
    let userBreakdownRepairReady = false;

    try {
      const previousLog = (
        await db.getIngestionLog(metricsType, entityId, day, day)
      )[0];
      previouslyLoaded = new Set(
        parseComponentsLoaded(previousLog?.components_loaded),
      );
      // Refresh missing user history before replacing a recent team snapshot.
      requiresUserBreakdownRepair = Boolean(
        context.refreshTeams &&
          context.ingestUsers &&
          context.ingestTeams &&
          !previouslyLoaded.has('userBreakdowns'),
      );
      if (requiresUserBreakdownRepair) {
        requiredComponents = [...requiredComponents, 'userBreakdowns'];
      }
      const totals = previouslyLoaded.has('totals')
        ? []
        : await this.prepareTotals(
            day,
            metricsType,
            entityId,
            context.organizationReportTarget,
          );
      let userReport: PreparedUserReport | undefined;
      let preparedTeams: Awaited<
        ReturnType<TaskManagementV2['prepareTeamMemberships']>
      >;
      if (context.ingestUsers) {
        const storedUserMetrics = await db.getUserMetricsForDay(
          metricsType,
          entityId,
          day,
        );
        const storedUserBreakdowns =
          storedUserMetrics.length > 0 && !requiresUserBreakdownRepair
            ? await db.getUserBreakdownsForDay(metricsType, entityId, day)
            : [];
        const reuseStoredReport =
          !requiresUserBreakdownRepair &&
          previouslyLoaded.has('users') &&
          ((storedUserMetrics.length > 0 &&
            previouslyLoaded.has('userBreakdowns')) ||
            storedUserBreakdowns.length > 0 ||
            (storedUserMetrics.length === 0 &&
              requiredComponents.every(component =>
                previouslyLoaded.has(component),
              )));
        if (!reuseStoredReport) {
          if (requiresUserBreakdownRepair) {
            userReport = await this.prepareUserReports(
              day,
              metricsType,
              entityId,
              context.organizationReportTarget,
              true,
            );
            userBreakdownRepairReady = userReport.loaded;
          } else {
            userReport = await this.prepareUserReports(
              day,
              metricsType,
              entityId,
              context.organizationReportTarget,
            );
          }
        }
        if (
          context.ingestTeams &&
          (!previouslyLoaded.has('teams') || context.refreshTeams)
        ) {
          if (requiresUserBreakdownRepair) {
            if (userBreakdownRepairReady && userReport?.loaded) {
              preparedTeams = await this.prepareTeamMemberships(
                day,
                metricsType,
                entityId,
                true,
                context.organizationReportTarget,
                true,
              );
              userBreakdownRepairReady = preparedTeams !== undefined;
            }
          } else {
            preparedTeams = await this.prepareTeamMemberships(
              day,
              metricsType,
              entityId,
              userReport?.loaded ?? reuseStoredReport,
              context.organizationReportTarget,
            );
          }
        }
        if (requiresUserBreakdownRepair && !userBreakdownRepairReady) {
          logger.warn(
            `[TaskManagementV2] Skipping team refresh for ${metricsType}:${entityId} on ${day} because complete user report history or team memberships were unavailable; preserving existing breakdowns, legacy aggregates, and memberships.`,
          );
        }
      }

      return await db.withIngestionDay(
        day,
        metricsType,
        entityId,
        async (transaction, existingLog) => {
          const componentsLoaded = new Set(
            parseComponentsLoaded(existingLog.components_loaded),
          );
          const userBreakdownRepairRequired = Boolean(
            context.refreshTeams &&
              context.ingestUsers &&
              context.ingestTeams &&
              !componentsLoaded.has('userBreakdowns'),
          );
          if (
            userBreakdownRepairRequired &&
            !requiredComponents.includes('userBreakdowns')
          ) {
            requiredComponents = [...requiredComponents, 'userBreakdowns'];
          }
          const repairSuperseded =
            requiresUserBreakdownRepair &&
            componentsLoaded.has('userBreakdowns');
          let currentUserCount = 0;
          let hasCurrentBreakdowns = false;
          if (context.ingestUsers) {
            const currentMetrics = await db.getUserMetricsForDay(
              metricsType,
              entityId,
              day,
              transaction,
            );
            currentUserCount = currentMetrics.length;
            hasCurrentBreakdowns =
              currentUserCount > 0 &&
              (
                await db.getUserBreakdownsForDay(
                  metricsType,
                  entityId,
                  day,
                  transaction,
                )
              ).length > 0;
          }
          const needsUserRepair =
            userBreakdownRepairRequired ||
            (componentsLoaded.has('users') &&
              ((currentUserCount > 0 &&
                !hasCurrentBreakdowns &&
                !componentsLoaded.has('userBreakdowns')) ||
                (currentUserCount === 0 &&
                  !requiredComponents.every(component =>
                    componentsLoaded.has(component),
                  ))));
          const repairReadyForTransaction =
            !userBreakdownRepairRequired ||
            (requiresUserBreakdownRepair &&
              userBreakdownRepairReady &&
              userReport?.loaded === true &&
              preparedTeams !== undefined);
          const refreshTeams = context.refreshTeams && Boolean(preparedTeams);
          if (
            requiredComponents.every(component =>
              componentsLoaded.has(component),
            ) &&
            !needsUserRepair &&
            (!refreshTeams || repairSuperseded)
          ) {
            return {
              loaded: context.ingestUsers && componentsLoaded.has('users'),
              refreshed: false,
              complete: true,
              totalsLoaded: true,
            };
          }

          if (!componentsLoaded.has('totals') && totals.length > 0) {
            await this.persistTotals(totals, transaction);
            componentsLoaded.add('totals');
          }
          let refreshed = false;
          if (
            userReport?.loaded &&
            (!componentsLoaded.has('users') || needsUserRepair) &&
            repairReadyForTransaction &&
            !repairSuperseded
          ) {
            await this.persistUserReport(
              day,
              metricsType,
              entityId,
              userReport,
              transaction,
              userBreakdownRepairRequired,
            );
            componentsLoaded.add('users');
            componentsLoaded.add('userBreakdowns');
            refreshed = true;
          }
          if (
            preparedTeams &&
            repairReadyForTransaction &&
            !repairSuperseded &&
            (!componentsLoaded.has('teams') || context.refreshTeams) &&
            componentsLoaded.has('users')
          ) {
            await db.replaceTeamMemberships(
              day,
              metricsType,
              entityId,
              preparedTeams,
              transaction,
            );
            if (
              currentUserCount > 0 ||
              (refreshed && (userReport?.userMetrics.length ?? 0) > 0) ||
              (userBreakdownRepairRequired && userBreakdownRepairReady)
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
            transaction,
          );
          return {
            loaded:
              context.ingestUsers &&
              componentsLoaded.has('users') &&
              (refreshed ||
                !userReport ||
                (currentUserCount > 0 &&
                  (hasCurrentBreakdowns ||
                    componentsLoaded.has('userBreakdowns')))),
            refreshed,
            complete: requiredComponents.every(component =>
              componentsLoaded.has(component),
            ),
            totalsLoaded: componentsLoaded.has('totals'),
          };
        },
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error(
        `[TaskManagementV2] Failed ingest for ${metricsType}:${entityId} on ${day}.`,
        error,
      );

      const failureState = await db.withIngestionDay(
        day,
        metricsType,
        entityId,
        async (transaction, existingLog) => {
          const componentsLoaded = parseComponentsLoaded(
            existingLog.components_loaded,
          );
          const alreadyComplete =
            existingLog.status === 'success' &&
            requiredComponents.every(component =>
              componentsLoaded.includes(component),
            );
          if (!alreadyComplete) {
            await db.upsertIngestionLog(
              {
                day,
                metrics_type: metricsType,
                entity_id: entityId,
                status: 'error',
                components_loaded: JSON.stringify(componentsLoaded),
                error_message: message,
                source: context.source,
              },
              transaction,
            );
          }
          return { alreadyComplete, componentsLoaded };
        },
      );
      if (
        context.organizationReportTarget &&
        error instanceof InaccessibleEnterpriseOrganizationError
      ) {
        throw error;
      }
      return {
        loaded:
          failureState.alreadyComplete &&
          context.ingestUsers &&
          failureState.componentsLoaded.includes('users'),
        refreshed: false,
        complete: failureState.alreadyComplete,
        totalsLoaded: failureState.componentsLoaded.includes('totals'),
      };
    }
  }

  async runOrganizationBackfill(
    organization: string,
    fromDate: string,
    toDate: string,
  ): Promise<void> {
    const { config, db } = this.options;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const enabled =
      config.getOptionalBoolean('copilot.ingestOrganizationTeamReports') ??
      false;
    if (!enterprise || !enabled) {
      throw new Error(
        'Organization-team backfill requires copilot.enterprise and copilot.ingestOrganizationTeamReports.',
      );
    }
    const canonicalOrganization =
      await db.getCurrentAccessibleEnterpriseOrganizationLogin(
        enterprise,
        organization,
      );
    if (!canonicalOrganization) {
      throw new Error('Organization is not available for team backfill.');
    }

    const from = DateTime.fromISO(fromDate, { zone: 'utc' }).startOf('day');
    const to = DateTime.fromISO(toDate, { zone: 'utc' }).startOf('day');
    const normalizedFrom = from.toISODate();
    const normalizedTo = to.toISODate();
    if (!normalizedFrom || !normalizedTo || from > to) {
      return;
    }
    await this.runBackfillInternal(
      normalizedFrom,
      normalizedTo,
      'organization',
      canonicalOrganization,
      canonicalOrganization,
    );
  }

  private getDayRange(fromDate: string, toDate: string): string[] {
    const from = DateTime.fromISO(fromDate, { zone: 'utc' }).startOf('day');
    const to = DateTime.fromISO(toDate, { zone: 'utc' }).startOf('day');
    if (!from.isValid || !to.isValid || from > to) {
      return [];
    }

    const days: string[] = [];
    for (let day = from; day <= to; day = day.plus({ days: 1 })) {
      const isoDate = day.toISODate();
      if (isoDate) {
        days.push(isoDate);
      }
    }
    return days;
  }

  private async runOrganizationTeamIngestion(
    enterprise: string,
    configuredOrganization?: string,
  ): Promise<Date | undefined> {
    const { api, db, logger } = this.options;
    const now = DateTime.utc();
    let organizations: string[];
    try {
      organizations = await api.fetchEnterpriseOrganizations();
      await db.syncEnterpriseOrganizations(
        enterprise,
        organizations,
        now.toJSDate(),
      );
    } catch (error) {
      await db.markEnterpriseOrganizationDirectoryFailed(
        enterprise,
        now.toJSDate(),
      );
      logger.error(
        '[TaskManagementV2] Failed to refresh the enterprise organization directory; organization-team views are unavailable until discovery succeeds.',
        error,
      );
      return undefined;
    }

    let configuredOrganizationObservedAt: Date | undefined;
    if (
      configuredOrganization &&
      organizations.some(
        item => item.toLowerCase() === configuredOrganization.toLowerCase(),
      )
    ) {
      const observedAt = DateTime.utc().toJSDate();
      try {
        const yesterday = DateTime.utc().minus({ days: 1 }).toISODate();
        if (!yesterday) {
          throw new Error('Unable to compute yesterday date.');
        }
        await api.verifyEnterpriseOrganizationAccess(
          configuredOrganization,
          yesterday,
        );
        const checkedAt = DateTime.utc();
        await db.markLegacyConfiguredOrganization(
          enterprise,
          configuredOrganization,
          checkedAt.toJSDate(),
          checkedAt.plus({ hours: ORGANIZATION_TEAM_RETRY_HOURS }).toJSDate(),
          observedAt,
        );
        configuredOrganizationObservedAt = observedAt;
      } catch (error) {
        if (error instanceof InaccessibleEnterpriseOrganizationError) {
          const failedAt = DateTime.utc();
          await db.markEnterpriseOrganizationInaccessible(
            enterprise,
            configuredOrganization,
            failedAt.toJSDate(),
            failedAt.plus({ hours: 24 }).toJSDate(),
            observedAt,
          );
          logger.warn(
            '[TaskManagementV2] The configured organization is no longer accessible; hiding its organization-team filters while retaining imported history.',
          );
        } else {
          logger.error(
            '[TaskManagementV2] Failed to verify access to the configured organization; retaining its previous directory state.',
            error,
          );
        }
      }
    }

    const organizationsToIngest = await db.getEnterpriseOrganizationsToIngest(
      enterprise,
      DateTime.utc().toJSDate(),
      ORGANIZATION_TEAM_MAX_ORGANIZATIONS_PER_RUN,
      1,
      configuredOrganization,
    );
    for (const organization of organizationsToIngest) {
      const observedAt = DateTime.utc().toJSDate();
      try {
        await this.ingestEnterpriseOrganizationWorkItem(
          enterprise,
          organization,
          observedAt,
        );
      } catch (error) {
        const failedAt = DateTime.utc();
        if (error instanceof InaccessibleEnterpriseOrganizationError) {
          try {
            await db.updateEnterpriseOrganizationState(
              enterprise,
              organization,
              {
                accessState: 'inaccessible',
                reportsReady: false,
                lastAttemptedAt: failedAt.toJSDate(),
                nextAttemptAt: failedAt.plus({ hours: 24 }).toJSDate(),
              },
              observedAt,
            );
          } catch (stateError) {
            logger.warn(
              `[TaskManagementV2] Could not update access state for organization:${organization}.`,
              stateError,
            );
          }
          logger.warn(
            `[TaskManagementV2] Organization-team ingestion is inaccessible for organization:${organization}; imported history will be retained.`,
          );
        } else {
          try {
            await db.updateEnterpriseOrganizationState(
              enterprise,
              organization,
              {
                lastAttemptedAt: failedAt.toJSDate(),
                nextAttemptAt: failedAt.plus({ hours: 1 }).toJSDate(),
              },
              observedAt,
            );
          } catch (stateError) {
            logger.warn(
              `[TaskManagementV2] Could not schedule a retry for organization:${organization}.`,
              stateError,
            );
          }
          logger.error(
            `[TaskManagementV2] Organization-team ingestion failed for organization:${organization}; it will be retried.`,
            error,
          );
        }
      }
    }
    return configuredOrganizationObservedAt;
  }

  private async ingestEnterpriseOrganizationWorkItem(
    enterprise: string,
    organization: string,
    observedAt: Date,
  ): Promise<void> {
    const { api, config, db } = this.options;
    const yesterday = DateTime.utc().minus({ days: 1 }).toISODate();
    const bootstrapStart = DateTime.utc()
      .startOf('day')
      .minus({ days: ORGANIZATION_TEAM_BOOTSTRAP_DAYS })
      .toISODate();
    if (!yesterday || !bootstrapStart) {
      throw new Error('Unable to compute the organization-team report window.');
    }
    const start = DateTime.fromISO(bootstrapStart);
    const effectiveStart =
      bootstrapStart < GITHUB_REPORTS_EARLIEST_DATE
        ? GITHUB_REPORTS_EARLIEST_DATE
        : start.toISODate();
    if (!effectiveStart) {
      throw new Error('Unable to compute the organization-team start date.');
    }

    const reportWindowStartDate = getReportWindowStartDate();
    const missingFromDate =
      effectiveStart < reportWindowStartDate
        ? reportWindowStartDate
        : effectiveStart;
    const { missingDays } = await this.getMissingIngestionDays(
      'organization',
      organization,
      effectiveStart,
      missingFromDate,
      yesterday,
      getRequiredComponents(true, true),
      true,
    );
    const trailingStart = DateTime.fromISO(yesterday)
      .minus({ days: ORGANIZATION_TEAM_REFRESH_DAYS - 1 })
      .toISODate();
    if (!trailingStart) {
      throw new Error('Unable to compute the organization-team retry window.');
    }
    const trailingDays = this.getDayRange(
      trailingStart < GITHUB_REPORTS_EARLIEST_DATE
        ? GITHUB_REPORTS_EARLIEST_DATE
        : trailingStart,
      yesterday,
    );
    const backlogDays = missingDays
      .filter(day => !trailingDays.includes(day))
      .slice(0, ORGANIZATION_TEAM_MAX_BACKLOG_DAYS_PER_RUN);
    const days = uniqueDays(backlogDays, trailingDays);
    if (days.length === 0) {
      return;
    }

    await api.verifyEnterpriseOrganizationAccess(organization, yesterday);

    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    let incomplete = false;
    let reportsReady = false;
    for (const day of days) {
      const includeUserAndTeamReports = day >= reportWindowStartDate;
      const result = await this.ingestDay(day, 'organization', organization, {
        ingestTeams: includeUserAndTeamReports,
        ingestUsers: includeUserAndTeamReports,
        ingestCostCenters: false,
        source: 'scheduled',
        refreshTeams: trailingDays.includes(day),
        organizationReportTarget: organization,
      });
      if (!result.complete) {
        incomplete = true;
      }
      if (result.totalsLoaded) {
        reportsReady = true;
      }

      if (backfillDelayMs > 0) {
        await delay(backfillDelayMs);
      }
    }

    const completedAt = DateTime.utc();
    await db.updateEnterpriseOrganizationState(
      enterprise,
      organization,
      {
        accessState: 'accessible',
        ...(reportsReady ? { reportsReady: true } : {}),
        lastAttemptedAt: completedAt.toJSDate(),
        nextAttemptAt: completedAt
          .plus({
            hours: incomplete ? 1 : ORGANIZATION_TEAM_RETRY_HOURS,
          })
          .toJSDate(),
      },
      observedAt,
    );
  }
}
