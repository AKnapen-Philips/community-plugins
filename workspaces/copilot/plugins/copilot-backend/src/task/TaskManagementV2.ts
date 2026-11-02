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
  V2UserMetricRow,
  V2UserTeamRow,
} from '@backstage-community/plugin-copilot-common';
import { DatabaseHandlerV2 } from '../db/DatabaseHandlerV2';
import type {
  TeamAggregationSource,
  TeamAggregationTransition,
  TeamMembershipSnapshotAttempt,
} from '../db/DatabaseHandlerV2';
import { GithubClientV2 } from '../client/GithubClientV2';
import {
  parseEnterpriseDocument,
  parseOrganizationDocument,
  parseUserDocument,
  parseUserTeamsDocument,
  UserBreakdownData,
} from '../utils/reportParser';
import { aggregateTeamMetrics } from '../utils/teamAggregator';
import { buildUserBreakdownRows } from '../utils/userBreakdownRows';

type IngestSource = 'scheduled' | 'backfill';

type IngestContext = {
  ingestTeams: boolean;
  source: IngestSource;
  teamAggregationSource?: TeamAggregationSource;
  teamAggregationGeneration?: string;
  organizationTeamMembershipSnapshot?: TeamMembershipSnapshotAttempt;
};

type EnterpriseTeamAggregationContext = {
  source: TeamAggregationSource;
  activeGeneration: string;
  organizationMembershipSnapshot?: TeamMembershipSnapshotAttempt;
};

export type TaskManagementV2Options = {
  db: DatabaseHandlerV2;
  api: GithubClientV2;
  config: Config;
  logger: LoggerService;
};

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const GITHUB_REPORTS_EARLIEST_DATE = '2025-10-10';
const scopeRunQueues = new Map<string, Promise<void>>();

async function withScopeRunLocks(
  scopes: Array<{ metricsType: MetricsScope; entityId: string }>,
  run: () => Promise<void>,
): Promise<void> {
  const keys = [
    ...new Set(
      scopes.map(({ metricsType, entityId }) =>
        JSON.stringify([metricsType, entityId]),
      ),
    ),
  ].sort();

  const acquire = async (index: number): Promise<void> => {
    if (index === keys.length) {
      await run();
      return;
    }

    const key = keys[index];
    const previous = scopeRunQueues.get(key) ?? Promise.resolve();
    let release!: () => void;
    const lock = new Promise<void>(resolve => {
      release = resolve;
    });
    scopeRunQueues.set(key, lock);

    try {
      await previous;
      await acquire(index + 1);
    } finally {
      release();
      if (scopeRunQueues.get(key) === lock) {
        scopeRunQueues.delete(key);
      }
    }
  };

  await acquire(0);
}

function getRequiredComponents(ingestTeams: boolean): string[] {
  return ingestTeams ? ['totals', 'users', 'teams'] : ['totals'];
}

function getReportWindowStartDate(now = DateTime.utc()): string {
  const rollingStart = now
    .toUTC()
    .startOf('day')
    .minus({ years: 1 })
    .toISODate();
  return rollingStart && rollingStart > GITHUB_REPORTS_EARLIEST_DATE
    ? rollingStart
    : GITHUB_REPORTS_EARLIEST_DATE;
}

function parseComponentsLoaded(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value.filter(
      (component): component is string => typeof component === 'string',
    );
  }
  if (typeof value !== 'string') {
    return [];
  }
  try {
    const parsed: unknown = JSON.parse(value);
    return Array.isArray(parsed)
      ? parsed.filter(
          (component): component is string => typeof component === 'string',
        )
      : [];
  } catch {
    return [];
  }
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
    const { config } = this.options;
    const scopes: Array<{ metricsType: MetricsScope; entityId: string }> = [];
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    if (enterprise) {
      scopes.push({ metricsType: 'enterprise', entityId: enterprise });
    }
    if (organization) {
      scopes.push({ metricsType: 'organization', entityId: organization });
    }

    await withScopeRunLocks(scopes, () => this.runAsyncUnlocked());
  }

  private async runAsyncUnlocked(): Promise<void> {
    const { config, logger } = this.options;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    const backfillFromDate =
      config.getOptionalString('copilot.backfillFromDate') ?? '2025-10-10';
    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    const ingestTeams =
      config.getOptionalBoolean('copilot.ingestTeams') ?? false;

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

    const reportWindowStartDate = getReportWindowStartDate();
    const requiredComponents = getRequiredComponents(ingestTeams);

    for (const entity of entities) {
      let missingDays: string[];
      if (ingestTeams && backfillFromDate < reportWindowStartDate) {
        const [missingTotalsDays, missingReportDays] = await Promise.all([
          this.options.db.getMissingDays(
            entity.metricsType,
            entity.entityId,
            backfillFromDate,
            yesterday,
            ['totals'],
          ),
          this.options.db.getMissingDays(
            entity.metricsType,
            entity.entityId,
            reportWindowStartDate,
            yesterday,
            requiredComponents,
          ),
        ]);
        missingDays = [
          ...new Set([...missingTotalsDays, ...missingReportDays]),
        ].sort();
      } else {
        missingDays = await this.options.db.getMissingDays(
          entity.metricsType,
          entity.entityId,
          backfillFromDate,
          yesterday,
          requiredComponents,
        );
      }

      const teamAggregationContext =
        entity.metricsType === 'enterprise' && ingestTeams && enterprise
          ? await this.prepareEnterpriseTeamAggregation(
              enterprise,
              organization,
              yesterday,
              missingDays.some(day => day >= reportWindowStartDate),
            )
          : undefined;

      if (!missingDays.length) {
        logger.info(
          `[TaskManagementV2] No missing days for ${entity.metricsType}:${entity.entityId}.`,
        );
        continue;
      }

      logger.info(
        `[TaskManagementV2] Found ${missingDays.length} missing days for ${entity.metricsType}:${entity.entityId}.`,
      );

      for (const day of missingDays) {
        await this.ingestDay(day, entity.metricsType, entity.entityId, {
          ingestTeams: ingestTeams && day >= reportWindowStartDate,
          source: 'scheduled',
          teamAggregationSource:
            entity.metricsType === 'enterprise'
              ? teamAggregationContext?.source
              : undefined,
          teamAggregationGeneration:
            entity.metricsType === 'enterprise'
              ? teamAggregationContext?.activeGeneration
              : undefined,
          organizationTeamMembershipSnapshot:
            entity.metricsType === 'enterprise'
              ? teamAggregationContext?.organizationMembershipSnapshot
              : undefined,
        });

        if (backfillDelayMs > 0) {
          await delay(backfillDelayMs);
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
    await withScopeRunLocks([{ metricsType, entityId }], () =>
      this.runBackfillUnlocked(fromDate, toDate, metricsType, entityId),
    );
  }

  private async runBackfillUnlocked(
    fromDate: string,
    toDate: string,
    metricsType: MetricsScope,
    entityId: string,
  ): Promise<void> {
    const { config } = this.options;
    const ingestTeams =
      config.getOptionalBoolean('copilot.ingestTeams') ?? false;
    const enterprise = config.getOptionalString('copilot.enterprise');
    const organization = config.getOptionalString('copilot.organization');
    const backfillDelayMs =
      config.getOptionalNumber('copilot.backfillDelayMs') ?? 200;
    const requiredComponents = getRequiredComponents(ingestTeams);
    const reportWindowStartDate = getReportWindowStartDate();

    let missingDays: string[];
    if (ingestTeams && fromDate < reportWindowStartDate) {
      const [missingTotalsDays, missingReportDays] = await Promise.all([
        this.options.db.getMissingDays(
          metricsType,
          entityId,
          fromDate,
          toDate,
          ['totals'],
        ),
        this.options.db.getMissingDays(
          metricsType,
          entityId,
          reportWindowStartDate,
          toDate,
          requiredComponents,
        ),
      ]);
      missingDays = [
        ...new Set([...missingTotalsDays, ...missingReportDays]),
      ].sort();
    } else {
      missingDays = await this.options.db.getMissingDays(
        metricsType,
        entityId,
        fromDate,
        toDate,
        requiredComponents,
      );
    }

    const teamAggregationContext =
      metricsType === 'enterprise' &&
      entityId === enterprise &&
      ingestTeams &&
      enterprise
        ? await this.prepareEnterpriseTeamAggregation(
            enterprise,
            organization,
            toDate,
            missingDays.some(day => day >= reportWindowStartDate),
          )
        : undefined;

    if (!missingDays.length) {
      this.options.logger.info(
        `[TaskManagementV2] No missing days for backfill ${metricsType}:${entityId} in range ${fromDate}..${toDate}.`,
      );
      return;
    }

    this.options.logger.info(
      `[TaskManagementV2] Backfill processing ${missingDays.length} days for ${metricsType}:${entityId}.`,
    );

    for (const day of missingDays) {
      await this.ingestDay(day, metricsType, entityId, {
        ingestTeams: ingestTeams && day >= reportWindowStartDate,
        source: 'backfill',
        teamAggregationSource:
          metricsType === 'enterprise'
            ? teamAggregationContext?.source
            : undefined,
        teamAggregationGeneration:
          metricsType === 'enterprise'
            ? teamAggregationContext?.activeGeneration
            : undefined,
        organizationTeamMembershipSnapshot:
          metricsType === 'enterprise'
            ? teamAggregationContext?.organizationMembershipSnapshot
            : undefined,
      });

      if (backfillDelayMs > 0) {
        await delay(backfillDelayMs);
      }
    }
  }

  private async prepareEnterpriseTeamAggregation(
    enterprise: string,
    organization: string | undefined,
    probeDay: string,
    loadActiveFallbackSnapshot: boolean,
  ): Promise<EnterpriseTeamAggregationContext> {
    const { api, db, logger } = this.options;
    const initialState = await db.getTeamAggregationState(
      'enterprise',
      enterprise,
    );
    let desiredSource = initialState.pendingSource ?? initialState.activeSource;
    let organizationMembershipSnapshot:
      | TeamMembershipSnapshotAttempt
      | undefined;

    if (!organization) {
      desiredSource = 'native';
    } else {
      try {
        const access = await api.probeOrganizationUserTeamsReport(probeDay);
        if (access === 'available' || access === 'installation-unavailable') {
          desiredSource = 'native';
        } else {
          desiredSource = 'organization-fallback';
        }
      } catch (error) {
        logger.warn(
          `[TaskManagementV2] Unable to verify organization Copilot team-report access; keeping the active enterprise team source (${desiredSource}).`,
          error,
        );
        desiredSource = initialState.pendingSource ?? initialState.activeSource;
      }
    }

    let transition: TeamAggregationTransition | null = null;
    if (
      desiredSource !== initialState.activeSource ||
      initialState.pendingSource
    ) {
      try {
        transition = await db.beginTeamAggregationTransition(
          'enterprise',
          enterprise,
          desiredSource,
        );
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Unable to start the ${desiredSource} enterprise team aggregation transition.`,
          error,
        );
      }
    }

    if (transition?.pendingSource === 'organization-fallback') {
      try {
        organizationMembershipSnapshot =
          await this.loadOrganizationTeamMembershipSnapshot(
            enterprise,
            transition.pendingGeneration,
          );
        await this.rebuildEnterpriseTeamAggregates(
          enterprise,
          transition,
          organizationMembershipSnapshot,
        );
      } catch (error) {
        organizationMembershipSnapshot = undefined;
        logger.warn(
          '[TaskManagementV2] Unable to complete the organization team membership snapshot; the active enterprise team source will remain unchanged.',
          error,
        );
      }
    } else if (transition) {
      try {
        await this.rebuildEnterpriseTeamAggregates(
          enterprise,
          transition,
          undefined,
        );
      } catch (error) {
        logger.error(
          `[TaskManagementV2] Failed to transition enterprise team aggregation to ${desiredSource}; the previously active source will remain in use.`,
          error,
        );
      }
    } else if (
      desiredSource === 'organization-fallback' &&
      initialState.activeSource === 'organization-fallback' &&
      loadActiveFallbackSnapshot
    ) {
      try {
        organizationMembershipSnapshot =
          await this.loadOrganizationTeamMembershipSnapshot(enterprise);
      } catch (error) {
        organizationMembershipSnapshot = undefined;
        logger.warn(
          '[TaskManagementV2] Unable to refresh organization team membership; fallback team aggregation will be retried.',
          error,
        );
      }
    }

    const state = await db.getTeamAggregationState('enterprise', enterprise);
    if (
      state.activeSource === 'organization-fallback' &&
      loadActiveFallbackSnapshot &&
      !organizationMembershipSnapshot
    ) {
      try {
        organizationMembershipSnapshot =
          await this.loadOrganizationTeamMembershipSnapshot(enterprise);
      } catch (error) {
        organizationMembershipSnapshot = undefined;
        logger.warn(
          '[TaskManagementV2] Unable to load organization team membership; fallback team aggregation will be retried.',
          error,
        );
      }
    }

    return {
      source: state.activeSource,
      activeGeneration: state.activeGeneration,
      organizationMembershipSnapshot,
    };
  }

  private async loadOrganizationTeamMembershipSnapshot(
    enterprise: string,
    transitionGeneration?: string,
  ): Promise<TeamMembershipSnapshotAttempt> {
    const { api, db } = this.options;
    const snapshotState = await db.beginTeamMembershipSnapshot(
      'enterprise',
      enterprise,
      transitionGeneration,
    );

    if (!snapshotState.complete) {
      await api.fetchOrganizationTeamMemberships({
        afterCursor: snapshotState.cursor,
        onPage: async batch => {
          await db.saveTeamMembershipSnapshotBatch(
            'enterprise',
            enterprise,
            snapshotState,
            batch.memberships,
            batch.cursor,
            batch.complete,
          );
        },
      });
    }

    const completedState = await db.getTeamMembershipSnapshotState(
      'enterprise',
      enterprise,
      snapshotState,
    );
    if (!completedState.complete) {
      throw new Error('Organization team membership snapshot is incomplete.');
    }
    return {
      generation: completedState.generation,
      attemptToken: completedState.attemptToken,
    };
  }

  private async rebuildEnterpriseTeamAggregates(
    enterprise: string,
    transition: TeamAggregationTransition,
    organizationMembershipSnapshot?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    const { api, db, logger } = this.options;
    const source = transition.pendingSource;
    const generation = transition.pendingGeneration;
    const reportWindowStartDate = getReportWindowStartDate();

    for (;;) {
      const requiredDays = await db.getUserMetricDays('enterprise', enterprise);
      const missingDays = await db.getMissingTeamAggregationDays(
        'enterprise',
        enterprise,
        source,
        generation,
      );

      for (const day of missingDays) {
        try {
          if (source === 'native' && day < reportWindowStartDate) {
            const activatedDuringSkip = await db.markTeamAggregationUnavailable(
              day,
              'enterprise',
              enterprise,
              source,
              generation,
              requiredDays,
              `GitHub user-team reports are only available from ${reportWindowStartDate}.`,
            );
            if (activatedDuringSkip) {
              return true;
            }
            continue;
          }

          const userMetrics = await db.getUserMetricsForDay(
            'enterprise',
            enterprise,
            day,
          );
          const userBreakdowns = await db.getUserBreakdownsForDay(
            'enterprise',
            enterprise,
            day,
          );

          if (source === 'organization-fallback') {
            if (!organizationMembershipSnapshot) {
              throw new Error(
                'Organization team membership is unavailable for fallback aggregation.',
              );
            }
            await db.beginTeamAggregationDayStaging(
              day,
              'enterprise',
              enterprise,
              source,
              generation,
              organizationMembershipSnapshot,
            );
            let cursor = 0;
            for (;;) {
              const batch = await db.getTeamMembershipSnapshotBatchForDay(
                'enterprise',
                enterprise,
                organizationMembershipSnapshot,
                day,
                cursor,
              );
              if (batch.memberships.length > 0) {
                const aggregates = aggregateTeamMetrics(
                  userMetrics,
                  batch.memberships,
                  userBreakdowns,
                  day,
                  'enterprise',
                  enterprise,
                );
                await db.stageTeamAggregationBatch(
                  day,
                  'enterprise',
                  enterprise,
                  source,
                  generation,
                  batch.memberships,
                  aggregates,
                  organizationMembershipSnapshot,
                );
              }
              if (batch.nextCursor === null) {
                break;
              }
              cursor = batch.nextCursor;
            }

            const activatedDuringStage =
              await db.completeStagedTeamAggregationDay(
                day,
                'enterprise',
                enterprise,
                source,
                generation,
                organizationMembershipSnapshot,
                requiredDays,
              );
            if (activatedDuringStage) {
              return true;
            }
          } else {
            const envelope = await api.fetchEnterpriseUserTeamsLinks(day);
            const userTeams: V2UserTeamRow[] = [];
            for (const url of envelope.download_links ?? []) {
              const rows = await api.downloadNdjsonDocument(url);
              userTeams.push(
                ...parseUserTeamsDocument(rows, 'enterprise', enterprise),
              );
            }

            const aggregates = aggregateTeamMetrics(
              userMetrics,
              userTeams,
              userBreakdowns,
              day,
              'enterprise',
              enterprise,
            );
            const activatedDuringStage = await db.stageTeamAggregates(
              day,
              'enterprise',
              enterprise,
              source,
              generation,
              requiredDays,
              userTeams,
              aggregates,
            );
            if (activatedDuringStage) {
              return true;
            }
          }
        } catch (error) {
          const message =
            error instanceof Error ? error.message : String(error);
          await db
            .recordTeamAggregationFailure(
              day,
              'enterprise',
              enterprise,
              source,
              generation,
              message,
              organizationMembershipSnapshot,
            )
            .catch(() => undefined);
          logger.error(
            `[TaskManagementV2] Failed to rebuild ${source} team aggregation for enterprise:${enterprise} on ${day}; the active source remains unchanged.`,
            error,
          );
          return false;
        }
      }

      const activated = await db.activateTeamAggregationSource(
        'enterprise',
        enterprise,
        source,
        generation,
        requiredDays,
        organizationMembershipSnapshot,
      );
      if (activated) {
        return true;
      }

      const state = await db.getTeamAggregationState('enterprise', enterprise);
      if (
        state.activeSource === source &&
        state.activeGeneration === generation &&
        !state.pendingGeneration
      ) {
        return true;
      }

      if (
        state.pendingSource !== source ||
        state.pendingGeneration !== generation
      ) {
        return false;
      }
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
      await db.insertCustomizationMetrics(parsed.customizationMetrics);
      await db.insertCustomizationSummaries(parsed.customizationSummaries);

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
      await db.insertCustomizationMetrics(parsed.customizationMetrics);
      await db.insertCustomizationSummaries(parsed.customizationSummaries);
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
    teamAggregationSource: TeamAggregationSource,
    teamAggregationGeneration?: string,
    organizationTeamMembershipSnapshot?: TeamMembershipSnapshotAttempt,
  ): Promise<boolean> {
    const { api, db, logger } = this.options;
    const source =
      metricsType === 'enterprise' ? teamAggregationSource : 'native';

    if (source === 'organization-fallback') {
      if (!teamAggregationGeneration || !organizationTeamMembershipSnapshot) {
        logger.warn(
          `[TaskManagementV2] Organization team membership is unavailable for enterprise:${entityId} on ${day}; fallback aggregation will be retried.`,
        );
        return false;
      }

      let aggregateUserMetrics = userMetrics;
      let aggregateUserBreakdowns = userBreakdowns;
      if (aggregateUserMetrics.length === 0) {
        aggregateUserMetrics = await db.getUserMetricsForDay(
          metricsType,
          entityId,
          day,
        );
        if (aggregateUserMetrics.length === 0) {
          logger.warn(
            `[TaskManagementV2] No stored per-user metrics for enterprise:${entityId} on ${day}; fallback team aggregation will be retried without replacing the day.`,
          );
          return false;
        }
        aggregateUserBreakdowns = await db.getUserBreakdownsForDay(
          metricsType,
          entityId,
          day,
        );
      }

      await db.beginTeamAggregationDayStaging(
        day,
        metricsType,
        entityId,
        source,
        teamAggregationGeneration,
        organizationTeamMembershipSnapshot,
      );
      let cursor = 0;
      for (;;) {
        const batch = await db.getTeamMembershipSnapshotBatchForDay(
          metricsType,
          entityId,
          organizationTeamMembershipSnapshot,
          day,
          cursor,
        );
        if (batch.memberships.length > 0) {
          const aggregates = aggregateTeamMetrics(
            aggregateUserMetrics,
            batch.memberships,
            aggregateUserBreakdowns,
            day,
            metricsType,
            entityId,
          );
          await db.stageTeamAggregationBatch(
            day,
            metricsType,
            entityId,
            source,
            teamAggregationGeneration,
            batch.memberships,
            aggregates,
            organizationTeamMembershipSnapshot,
          );
        }
        if (batch.nextCursor === null) {
          break;
        }
        cursor = batch.nextCursor;
      }

      await db.replaceStagedTeamAggregates(
        day,
        metricsType,
        entityId,
        source,
        teamAggregationGeneration,
        organizationTeamMembershipSnapshot,
      );
      return true;
    }

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

    const userTeams: V2UserTeamRow[] = [];
    for (const url of userTeamLinks) {
      const rows = await api.downloadNdjsonDocument(url);
      userTeams.push(...parseUserTeamsDocument(rows, metricsType, entityId));
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
      source,
      userTeams,
      teamAggregates,
    );

    return userMetrics.length > 0;
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
    const requiredComponents = getRequiredComponents(context.ingestTeams);
    const componentsLoaded = new Set<string>();
    let userReportsLoaded = false;

    try {
      const previousLog = (
        await db.getIngestionLog(metricsType, entityId, day, day)
      )[0];
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

      if (context.ingestTeams) {
        const userReport = previouslyLoaded.has('users')
          ? {
              loaded: true,
              userMetrics: await db.getUserMetricsForDay(
                metricsType,
                entityId,
                day,
              ),
              userBreakdowns: await db.getUserBreakdownsForDay(
                metricsType,
                entityId,
                day,
              ),
            }
          : await this.ingestUserReports(day, metricsType, entityId);
        userReportsLoaded = userReport.loaded;
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
            context.teamAggregationSource ?? 'native',
            context.teamAggregationGeneration,
            context.organizationTeamMembershipSnapshot,
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
        components_loaded: JSON.stringify([...componentsLoaded]),
        error_message: message,
        source: context.source,
      });
      return userReportsLoaded;
    }
  }
}
