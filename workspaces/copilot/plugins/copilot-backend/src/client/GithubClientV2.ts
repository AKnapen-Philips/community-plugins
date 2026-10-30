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
import { Config } from '@backstage/config';
import { LoggerService } from '@backstage/backend-plugin-api';
import { Octokit } from '@octokit/rest';
import { createAppAuth, type StrategyOptions } from '@octokit/auth-app';
import {
  MetricsScope,
  ReportEnvelope,
} from '@backstage-community/plugin-copilot-common';
import { resolveCostCenterMemberships } from '../utils/costCenterMembership';
import type {
  CostCenterMembership,
  CostCenterResource,
  CostCenterWithResources,
  GithubUser,
} from '../utils/costCenterMembership';
import {
  CopilotConfig,
  getCopilotConfig,
  getGithubCredentials,
} from '../utils/GithubUtils';

// GitHub serves user content from this domain on both github.com and GHE.com.
const GITHUB_USER_CONTENT_DOMAIN = 'githubusercontent.com';

// Object storage the signed report links can fall back to. GitHub documents the
// Azure Blob Storage fallback for github.com and GHE.com alike, so this list is
// not scoped to the configured host.
const STORAGE_DOWNLOAD_HOSTS = [
  /^[a-zA-Z0-9.-]+\.s3\.amazonaws\.com$/,
  /^[a-zA-Z0-9.-]+\.s3\.[a-z0-9-]+\.amazonaws\.com$/,
  /^[a-zA-Z0-9-]+\.blob\.core\.windows\.net$/,
];

// Follow at most this many redirects, re-checking the allowlist on every hop.
const MAX_DOWNLOAD_REDIRECTS = 5;

// GHE.com serves the API from api.SUBDOMAIN.ghe.com and the reports from
// copilot-reports.SUBDOMAIN.ghe.com.
const GHE_COM_API_HOST = /^api\.([a-z0-9-]+\.ghe\.com)$/;

interface CostCenterApiResource {
  type: string;
  name: string;
}

interface CostCenterApiItem {
  id: string;
  name: string;
  resources?: CostCenterApiResource[];
}

interface EnterpriseTeamApiItem {
  id: number | string;
  slug: string;
  name?: string;
  created_at?: string;
}

interface CostCenterOrganizationClient {
  octokit: Octokit;
  membersPermission?: string;
}

interface CostCenterOrganizationRoster {
  members: GithubUser[];
  skipReason?: string;
  oauthScopesHeaderMissing?: boolean;
}

class MissingCostCenterOrganizationInstallationError extends Error {
  constructor() {
    super('The GitHub App is not installed for this organization.');
    this.name = 'MissingCostCenterOrganizationInstallationError';
  }
}

export interface OrganizationTeamMembership {
  teamId: number;
  teamSlug: string;
  userId: number;
}

export interface OrganizationTeamMembershipBatch {
  memberships: OrganizationTeamMembership[];
  cursor: string | null;
  complete: boolean;
}

export interface OrganizationTeamMembershipFetchOptions {
  afterCursor?: string | null;
  onPage?: (batch: OrganizationTeamMembershipBatch) => Promise<void>;
}

export type OrganizationUserTeamsReportAccess =
  | 'available'
  | 'unavailable'
  | 'installation-unavailable';

type GraphQLRateLimit = {
  cost: number;
  remaining: number;
  resetAt: string;
};

type GraphQLTeamMembersPage = {
  nodes: Array<{ databaseId: number | null }>;
  pageInfo: { hasNextPage: boolean; endCursor: string | null };
};

type GraphQLTeamNode = {
  id: string;
  databaseId: number | null;
  slug: string;
  members: GraphQLTeamMembersPage;
};

type GraphQLOrganizationTeams = {
  nodes: GraphQLTeamNode[];
  pageInfo: { hasNextPage: boolean; endCursor: string | null };
};

type GraphQLOrganizationTeamsPage = {
  organization: { teams: GraphQLOrganizationTeams } | null;
  rateLimit: GraphQLRateLimit;
};

type GraphQLTeamMembersResponse = {
  node: {
    databaseId: number | null;
    slug: string;
    members: GraphQLTeamMembersPage;
  } | null;
  rateLimit: GraphQLRateLimit;
};

const ORGANIZATION_TEAMS_PAGE_SIZE = 25;
const ORGANIZATION_TEAMS_PAGE_SIZE_FLOOR = 5;
const ORGANIZATION_TEAMS_INITIAL_MEMBER_PAGE_SIZE = 50;
const ORGANIZATION_TEAM_MEMBERS_PAGE_SIZE = 100;
const GRAPHQL_LOW_REMAINING_THRESHOLD = 100;
const GRAPHQL_RATE_LIMIT_MAX_ATTEMPTS = 5;
const GRAPHQL_RATE_LIMIT_RETRY_BASE_MS = 1_000;
const GRAPHQL_RATE_LIMIT_RETRY_MAX_MS = 30_000;
const GRAPHQL_SERVER_ERROR_MAX_ATTEMPTS = 3;
const GRAPHQL_SERVER_ERROR_RETRY_BASE_MS = 1_000;
const GRAPHQL_SERVER_ERROR_RETRY_MAX_MS = 4_000;

const ORGANIZATION_TEAMS_QUERY = `
  query OrganizationTeams(
    $organization: String!
    $cursor: String
    $teamPageSize: Int!
  ) {
    organization(login: $organization) {
      teams(first: $teamPageSize, after: $cursor) {
        nodes {
          id
          databaseId
          slug
          members(first: ${ORGANIZATION_TEAMS_INITIAL_MEMBER_PAGE_SIZE}, membership: ALL) {
            nodes {
              databaseId
            }
            pageInfo {
              hasNextPage
              endCursor
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    rateLimit {
      cost
      remaining
      resetAt
    }
  }
`;

const ORGANIZATION_TEAM_MEMBERS_QUERY = `
  query OrganizationTeamMembers($teamId: ID!, $cursor: String) {
    node(id: $teamId) {
      ... on Team {
        databaseId
        slug
        members(first: ${ORGANIZATION_TEAM_MEMBERS_PAGE_SIZE}, after: $cursor, membership: ALL) {
          nodes {
            databaseId
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    rateLimit {
      cost
      remaining
      resetAt
    }
  }
`;

export interface CostCenterMembershipSnapshot {
  costCenters: Array<{ id: string; name: string }>;
  members: Map<number, CostCenterMembership>;
  warnings: string[];
  complete: boolean;
  unassignedComplete: boolean;
}

function isWithinDomain(hostname: string, domain: string): boolean {
  return hostname === domain || hostname.endsWith(`.${domain}`);
}

function buildApiUrl(
  apiBaseUrl: string,
  path: string,
  query?: Record<string, string | number>,
): string {
  const url = new URL(apiBaseUrl);
  url.pathname = `${url.pathname.replace(/\/+$/, '')}${path}`;
  url.search = '';
  url.hash = '';
  for (const [key, value] of Object.entries(query ?? {})) {
    url.searchParams.set(key, String(value));
  }
  return url.toString();
}

function encodeTeamSlug(teamSlug: string): string {
  return encodeURIComponent(teamSlug).replace(/%3A/gi, ':');
}

function hasNextLink(headers?: { link?: string; Link?: string }): boolean {
  return /rel="?next"?/i.test(headers?.link ?? headers?.Link ?? '');
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function getHttpStatus(error: unknown): number | undefined {
  if (!isRecord(error)) {
    return undefined;
  }
  if (typeof error.status === 'number') {
    return error.status;
  }
  const response = isRecord(error.response) ? error.response : undefined;
  return typeof response?.status === 'number' ? response.status : undefined;
}

function getResponseHeader(headers: unknown, name: string): string | undefined {
  if (!isRecord(headers)) {
    return undefined;
  }
  const getHeader = headers.get;
  if (typeof getHeader === 'function') {
    const header = getHeader.call(headers, name);
    return typeof header === 'string' ? header : undefined;
  }
  const header = Object.entries(headers).find(
    ([key]) => key.toLowerCase() === name.toLowerCase(),
  )?.[1];
  return typeof header === 'string' ? header : undefined;
}

function getGraphQLErrorEntries(error: unknown): unknown[] {
  if (!isRecord(error)) {
    return [];
  }

  const response = isRecord(error.response) ? error.response : undefined;
  const data = isRecord(response?.data) ? response.data : undefined;
  return [
    ...(Array.isArray(error.errors) ? error.errors : []),
    ...(Array.isArray(data?.errors) ? data.errors : []),
  ];
}

function isGraphQLRateLimitError(error: unknown): boolean {
  const status = getHttpStatus(error);
  const response =
    isRecord(error) && isRecord(error.response) ? error.response : undefined;
  const headers =
    isRecord(error) && error.headers !== undefined
      ? error.headers
      : response?.headers;
  const message =
    isRecord(error) && typeof error.message === 'string' ? error.message : '';
  const rateLimitEntry = getGraphQLErrorEntries(error).some(entry => {
    if (!isRecord(entry)) {
      return false;
    }

    return (
      entry.type === 'RATE_LIMITED' ||
      (typeof entry.message === 'string' &&
        /rate limit|secondary rate/i.test(entry.message))
    );
  });

  return (
    rateLimitEntry ||
    status === 429 ||
    ((status === 403 || status === undefined) &&
      (getResponseHeader(headers, 'retry-after') !== undefined ||
        getResponseHeader(headers, 'x-ratelimit-remaining') === '0' ||
        /rate limit|secondary rate/i.test(message)))
  );
}

function isGraphQLServerError(error: unknown): boolean {
  const status = getHttpStatus(error);
  return status === 502 || status === 503 || status === 504;
}

function getRetryDelayFromHeader(
  value: string | undefined,
): number | undefined {
  if (!value) {
    return undefined;
  }

  const seconds = Number(value);
  if (Number.isFinite(seconds)) {
    return Math.max(0, seconds * 1_000);
  }

  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, date - Date.now()) : undefined;
}

function getGraphQLRateLimitRetryDelay(
  error: unknown,
  retryNumber: number,
): number {
  const response =
    isRecord(error) && isRecord(error.response) ? error.response : undefined;
  const headers =
    isRecord(error) && error.headers !== undefined
      ? error.headers
      : response?.headers;
  const retryAfterDelay = getRetryDelayFromHeader(
    getResponseHeader(headers, 'retry-after'),
  );
  if (retryAfterDelay !== undefined) {
    return retryAfterDelay;
  }

  const resetHeader = getResponseHeader(headers, 'x-ratelimit-reset');
  if (resetHeader) {
    const resetAt = Number(resetHeader) * 1_000;
    if (Number.isFinite(resetAt)) {
      return Math.max(0, resetAt - Date.now() + 1_000);
    }
  }

  const responseData = isRecord(response?.data) ? response.data : undefined;
  const errorData =
    isRecord(error) && isRecord(error.data) ? error.data : undefined;
  const responseBody = responseData ?? errorData;
  const data = isRecord(responseBody?.data) ? responseBody.data : undefined;
  const rateLimit = data?.rateLimit ?? responseBody?.rateLimit;
  if (isRecord(rateLimit) && typeof rateLimit.resetAt === 'string') {
    const resetAt = Date.parse(rateLimit.resetAt);
    if (Number.isFinite(resetAt)) {
      return Math.max(0, resetAt - Date.now() + 1_000);
    }
  }

  return Math.min(
    GRAPHQL_RATE_LIMIT_RETRY_BASE_MS * 2 ** (retryNumber - 1),
    GRAPHQL_RATE_LIMIT_RETRY_MAX_MS,
  );
}

function getGraphQLServerErrorRetryDelay(retryNumber: number): number {
  return Math.min(
    GRAPHQL_SERVER_ERROR_RETRY_BASE_MS * 2 ** (retryNumber - 1),
    GRAPHQL_SERVER_ERROR_RETRY_MAX_MS,
  );
}

function isRateLimitError(error: unknown): boolean {
  if (!isRecord(error)) {
    return false;
  }

  const response = isRecord(error.response) ? error.response : undefined;
  const headers = error.headers ?? response?.headers;
  const remaining = getResponseHeader(headers, 'x-ratelimit-remaining');
  const retryAfter = getResponseHeader(headers, 'retry-after');
  const message = typeof error.message === 'string' ? error.message : '';

  return (
    remaining === '0' ||
    retryAfter !== undefined ||
    /rate limit|secondary rate/i.test(message)
  );
}

function isSafeGitHubId(id: unknown): id is number {
  return typeof id === 'number' && Number.isSafeInteger(id) && id > 0;
}

function sanitizeGraphQLError(error: unknown): Error {
  const status = getHttpStatus(error);
  return Object.assign(
    new Error(
      status
        ? `GitHub GraphQL team membership request failed with HTTP ${status}.`
        : 'GitHub GraphQL team membership request failed.',
    ),
    status ? { status } : {},
  );
}

function hasOrganizationMemberScope(scopes: string): boolean {
  const grantedScopes = new Set(
    scopes.split(',').map(scope => scope.trim().toLowerCase()),
  );
  return ['read:org', 'write:org', 'admin:org'].some(scope =>
    grantedScopes.has(scope),
  );
}

function hasMembersReadPermission(permission?: string): boolean {
  return (
    permission !== undefined &&
    ['read', 'write', 'admin'].includes(permission.toLowerCase())
  );
}

function getInstallationId(data: unknown): number {
  const id = isRecord(data) ? data.id : undefined;
  if (typeof id !== 'number' || !Number.isSafeInteger(id) || id <= 0) {
    throw new Error('Unexpected GitHub App installation response.');
  }
  return id;
}

function toGithubUser(value: unknown): GithubUser | undefined {
  if (!isRecord(value) || typeof value.login !== 'string') {
    return undefined;
  }

  let id: number;
  if (typeof value.id === 'number') {
    id = value.id;
  } else if (typeof value.id === 'string') {
    id = Number(value.id);
  } else {
    return undefined;
  }
  if (!Number.isSafeInteger(id) || id <= 0) {
    return undefined;
  }

  return { id, login: value.login };
}

function toGithubUsers(value: unknown): GithubUser[] {
  if (!Array.isArray(value)) {
    throw new Error('Unexpected GitHub user list response.');
  }
  return value.map(user => {
    const parsed = toGithubUser(user);
    if (!parsed) {
      throw new Error('Unexpected GitHub user entry in list response.');
    }
    return parsed;
  });
}

function getCostCenterDetailPage(data: unknown): {
  resources: CostCenterApiResource[];
  hasNextPage: boolean;
} {
  if (!isRecord(data)) {
    throw new Error('Unexpected GitHub cost center response.');
  }

  let nestedCenter = data;
  if (isRecord(data.costCenter)) {
    nestedCenter = data.costCenter;
  } else if (isRecord(data.cost_center)) {
    nestedCenter = data.cost_center;
  }
  const resources = Array.isArray(data.resources)
    ? data.resources
    : nestedCenter.resources;

  if (
    !Array.isArray(resources) ||
    !resources.every(
      resource =>
        isRecord(resource) &&
        typeof resource.type === 'string' &&
        typeof resource.name === 'string',
    )
  ) {
    throw new Error('Unexpected GitHub cost center resources response.');
  }

  const hasNextPage =
    data.has_next_page ??
    data.hasNextPage ??
    nestedCenter.has_next_page ??
    nestedCenter.hasNextPage;

  return {
    resources: resources as CostCenterApiResource[],
    hasNextPage: hasNextPage === true,
  };
}

// `copilot.host` may carry a port, which URL.hostname never does.
function normalizeConfiguredHost(githubHost: string): string {
  try {
    return new URL(`https://${githubHost}`).hostname;
  } catch {
    return githubHost.toLowerCase();
  }
}

/**
 * SSRF guard for signed report download links. The allowed hosts are derived
 * from the configured GitHub host and API base URL so that a github.com
 * deployment never trusts a GHE.com tenant, and vice versa.
 */
function isAllowedDownloadHost(
  url: URL,
  githubHost: string,
  apiBaseUrl: string,
): boolean {
  const hostname = url.hostname.toLowerCase();
  const host = normalizeConfiguredHost(githubHost);

  // An empty host would make every domain check below match.
  if (!host) {
    return false;
  }

  // Lets `copilot.host` name an `api.SUBDOMAIN.ghe.com` integration entry, which
  // on its own would not cover the reports on the sibling host.
  const tenantDomain = GHE_COM_API_HOST.exec(new URL(apiBaseUrl).hostname)?.[1];

  if (
    isWithinDomain(hostname, host) ||
    (tenantDomain !== undefined && isWithinDomain(hostname, tenantDomain)) ||
    isWithinDomain(hostname, GITHUB_USER_CONTENT_DOMAIN)
  ) {
    return true;
  }

  return STORAGE_DOWNLOAD_HOSTS.some(pattern => pattern.test(hostname));
}

export class GithubClientV2 {
  private enterpriseOctokit?: Octokit;
  private organizationOctokit?: Octokit;
  private enterpriseAuthStrategy?: string | StrategyOptions;
  private organizationMembersPermission?: string;
  private readonly costCenterOrganizationOctokits = new Map<
    string,
    Promise<CostCenterOrganizationClient>
  >();

  constructor(
    private readonly copilotConfig: CopilotConfig,
    private readonly config: Config,
    private readonly logger: LoggerService,
  ) {}

  static async fromConfig(
    config: Config,
    logger: LoggerService,
  ): Promise<GithubClientV2> {
    const info = getCopilotConfig(config);
    return new GithubClientV2(info, config, logger);
  }

  private getBaseOctokitConfig(): NonNullable<
    ConstructorParameters<typeof Octokit>[0]
  > {
    return {
      baseUrl: this.copilotConfig.apiBaseUrl,
    };
  }

  private createOctokit(
    options: NonNullable<ConstructorParameters<typeof Octokit>[0]> = {},
  ): Octokit {
    const octokit = new Octokit({
      ...this.getBaseOctokitConfig(),
      ...options,
    });
    octokit.hook.before('request', requestOptions => {
      requestOptions.headers.accept = 'application/vnd.github+json';
      requestOptions.headers['x-github-api-version'] ??= '2026-03-10';
    });
    return octokit;
  }

  private async getAppJwtOctokit(
    authStrategy: StrategyOptions,
  ): Promise<Octokit> {
    const { token } = await createAppAuth(authStrategy)({ type: 'app' });
    const octokit = this.createOctokit();
    octokit.hook.before('request', requestOptions => {
      requestOptions.headers.authorization = `Bearer ${token}`;
    });
    return octokit;
  }

  private createInstallationOctokit(
    authStrategy: StrategyOptions,
    installationId: number,
  ): Octokit {
    return this.createOctokit({
      authStrategy: createAppAuth,
      auth: {
        appId: authStrategy.appId,
        privateKey: authStrategy.privateKey,
        installationId,
      },
    });
  }

  private async getOctokit(type: MetricsScope): Promise<Octokit> {
    const credentials = await getGithubCredentials(
      this.config,
      this.copilotConfig,
    );
    const authStrategy = credentials[type];

    if (!authStrategy) {
      throw new Error(
        `No credentials configured for ${type}. Please configure GitHub integration.`,
      );
    }

    if (typeof authStrategy === 'string') {
      if (type === 'enterprise') {
        this.enterpriseAuthStrategy = authStrategy;
      }
      return this.createOctokit({
        auth: authStrategy,
      });
    }

    if (type === 'enterprise') {
      this.enterpriseAuthStrategy = authStrategy;
    }

    const orgName =
      type === 'organization'
        ? this.copilotConfig.organization
        : this.copilotConfig.enterprise;
    if (!orgName) {
      throw new Error(`No ${type} name configured.`);
    }

    let installationId: number;
    if (type === 'enterprise') {
      const appJwtOctokit = await this.getAppJwtOctokit(authStrategy);
      try {
        const { data } = await appJwtOctokit.request(
          `GET /enterprises/${encodeURIComponent(orgName)}/installation`,
        );
        installationId = getInstallationId(data);
      } catch (error) {
        if (getHttpStatus(error) !== 404) {
          throw error;
        }
        const { data } = await appJwtOctokit.rest.apps.getOrgInstallation({
          org: orgName,
        });
        installationId = getInstallationId(data);
      }
    } else {
      const appOctokit = this.createOctokit({
        authStrategy: createAppAuth,
        auth: {
          appId: authStrategy.appId,
          privateKey: authStrategy.privateKey,
        },
      });
      const { data } = await appOctokit.rest.apps.getOrgInstallation({
        org: orgName,
      });
      if (type === 'organization') {
        const permissions =
          isRecord(data) && isRecord(data.permissions)
            ? data.permissions
            : undefined;
        this.organizationMembersPermission =
          typeof permissions?.members === 'string'
            ? permissions.members.toLowerCase()
            : undefined;
      }
      installationId = getInstallationId(data);
    }

    return this.createInstallationOctokit(authStrategy, installationId);
  }

  private async getEnterpriseOctokit(): Promise<Octokit> {
    if (!this.enterpriseOctokit) {
      this.enterpriseOctokit = await this.getOctokit('enterprise');
    }
    return this.enterpriseOctokit;
  }

  private async getOrganizationOctokit(): Promise<Octokit> {
    if (!this.organizationOctokit) {
      this.organizationOctokit = await this.getOctokit('organization');
    }
    return this.organizationOctokit;
  }

  private async getCostCenterOrganizationOctokit(
    organization: string,
    enterpriseOctokit: Octokit,
  ): Promise<CostCenterOrganizationClient> {
    const authStrategy = this.enterpriseAuthStrategy;
    if (!authStrategy) {
      throw new Error('No enterprise credentials configured.');
    }
    if (typeof authStrategy === 'string') {
      return { octokit: enterpriseOctokit };
    }

    const cacheKey = organization.toLowerCase();
    let octokitPromise = this.costCenterOrganizationOctokits.get(cacheKey);
    if (!octokitPromise) {
      octokitPromise = (async () => {
        const appJwtOctokit = await this.getAppJwtOctokit(authStrategy);
        const installationUrl = buildApiUrl(
          this.copilotConfig.apiBaseUrl,
          `/orgs/${encodeURIComponent(organization)}/installation`,
        );
        let data: unknown;
        try {
          ({ data } = await appJwtOctokit.request(`GET ${installationUrl}`));
        } catch (error) {
          if (getHttpStatus(error) === 404) {
            throw new MissingCostCenterOrganizationInstallationError();
          }
          throw error;
        }
        const installationId = getInstallationId(data);
        const permissions =
          isRecord(data) && isRecord(data.permissions)
            ? data.permissions
            : undefined;
        return {
          octokit: this.createInstallationOctokit(authStrategy, installationId),
          membersPermission:
            typeof permissions?.members === 'string'
              ? permissions.members.toLowerCase()
              : undefined,
        };
      })();
      this.costCenterOrganizationOctokits.set(cacheKey, octokitPromise);
    }

    try {
      return await octokitPromise;
    } catch (error) {
      if (
        this.costCenterOrganizationOctokits.get(cacheKey) === octokitPromise
      ) {
        this.costCenterOrganizationOctokits.delete(cacheKey);
      }
      throw error;
    }
  }

  private async fetchPaginatedList<T>(
    octokit: Octokit,
    path: string,
    description: string,
    onFirstPage?: (headers: unknown) => boolean | void,
  ): Promise<T[]> {
    const results: T[] = [];
    let page = 1;
    let hasNextPage = true;

    while (hasNextPage) {
      const url = buildApiUrl(this.copilotConfig.apiBaseUrl, path, {
        per_page: 100,
        page,
      });
      const response = await octokit.request(`GET ${url}`);
      if (!Array.isArray(response.data)) {
        throw new Error(
          `Unexpected GitHub response while listing ${description}.`,
        );
      }
      const stopAfterFirstPage =
        page === 1 && onFirstPage?.(response.headers) === false;
      results.push(...(response.data as T[]));
      hasNextPage = !stopAfterFirstPage && hasNextLink(response.headers);
      page += 1;
    }

    return results;
  }

  private async fetchOrganizationMembers(
    octokit: Octokit,
    organization: string,
  ): Promise<{
    members: GithubUser[];
    oauthScopes?: string;
    hasOAuthScopesHeader: boolean;
  }> {
    let oauthScopes: string | undefined;
    let hasOAuthScopesHeader = false;
    const members = await this.fetchPaginatedList<unknown>(
      octokit,
      `/orgs/${encodeURIComponent(organization)}/members`,
      `organization "${organization}" members`,
      headers => {
        const value = getResponseHeader(headers, 'x-oauth-scopes');
        if (value !== undefined) {
          hasOAuthScopesHeader = true;
          oauthScopes = value;
        }
        return !(
          typeof this.enterpriseAuthStrategy === 'string' &&
          hasOAuthScopesHeader &&
          !hasOrganizationMemberScope(oauthScopes ?? '')
        );
      },
    );

    return {
      members: toGithubUsers(members),
      oauthScopes,
      hasOAuthScopesHeader,
    };
  }

  private async fetchCostCenterResources(
    octokit: Octokit,
    enterprise: string,
    costCenterId: string,
  ): Promise<CostCenterResource[]> {
    const resources: CostCenterResource[] = [];
    let page = 1;
    let hasNextPage: boolean;

    do {
      const url = buildApiUrl(
        this.copilotConfig.apiBaseUrl,
        `/enterprises/${encodeURIComponent(
          enterprise,
        )}/settings/billing/cost-centers/${encodeURIComponent(costCenterId)}`,
        { per_page: 100, page },
      );
      const response = await octokit.request(`GET ${url}`);
      const resultPage = getCostCenterDetailPage(response.data);
      resources.push(
        ...resultPage.resources.map(resource => ({
          type: resource.type,
          name: resource.name,
        })),
      );
      hasNextPage = resultPage.hasNextPage;
      page += 1;
    } while (hasNextPage);

    return resources;
  }

  private async fetchEnterpriseTeamMemberships(
    octokit: Octokit,
    enterprise: string,
    teamSlug: string,
  ): Promise<GithubUser[]> {
    const authentication =
      typeof this.enterpriseAuthStrategy === 'string'
        ? await octokit.auth()
        : await octokit.auth({ type: 'installation' });
    if (!isRecord(authentication) || typeof authentication.token !== 'string') {
      throw new Error('Could not resolve GitHub credentials for team members.');
    }

    const members: GithubUser[] = [];
    let page = 1;
    let hasNextPage = true;

    while (hasNextPage) {
      // Octokit's route parser treats `:slug` in a URL as a template variable;
      // use fetch so the enterprise team's raw `ent:` slug reaches GitHub.
      const path =
        `/enterprises/${encodeURIComponent(enterprise)}/teams/` +
        `${encodeTeamSlug(teamSlug)}/memberships`;
      const url = buildApiUrl(this.copilotConfig.apiBaseUrl, path, {
        per_page: 100,
        page,
      });
      const response = await fetch(url, {
        method: 'GET',
        redirect: 'manual',
        headers: {
          Accept: 'application/vnd.github+json',
          'X-GitHub-Api-Version': '2026-03-10',
          'User-Agent': 'backstage-community-copilot',
          authorization: `Bearer ${authentication.token}`,
        },
      });
      if (!response.ok) {
        throw Object.assign(
          new Error(
            `GitHub team membership request failed with HTTP ${response.status}.`,
          ),
          { status: response.status },
        );
      }

      members.push(...toGithubUsers(await response.json()));
      hasNextPage = hasNextLink({
        link: response.headers.get('link') ?? undefined,
      });
      page += 1;
    }

    return members;
  }

  private addCostCenterWarning(warnings: string[], warning: string): void {
    warnings.push(warning);
    this.logger.warn(`[GithubClientV2] ${warning}`);
  }

  async fetchCostCenterMemberships(): Promise<CostCenterMembershipSnapshot> {
    const enterprise = this.copilotConfig.enterprise;
    if (!enterprise) {
      throw new Error('No enterprise name configured.');
    }

    const enterpriseOctokit = await this.getEnterpriseOctokit();
    const listUrl = buildApiUrl(
      this.copilotConfig.apiBaseUrl,
      `/enterprises/${encodeURIComponent(
        enterprise,
      )}/settings/billing/cost-centers`,
      { state: 'active' },
    );
    const listResponse = await enterpriseOctokit.request(`GET ${listUrl}`);
    const listData = listResponse.data as {
      costCenters?: CostCenterApiItem[];
    };
    if (!Array.isArray(listData?.costCenters)) {
      throw new Error('Unexpected GitHub cost center list response.');
    }

    const listedCostCenters = listData.costCenters.map(costCenter => {
      if (
        typeof costCenter.id !== 'string' ||
        typeof costCenter.name !== 'string'
      ) {
        throw new Error('Unexpected GitHub cost center entry.');
      }
      return { id: costCenter.id, name: costCenter.name };
    });

    const warnings: string[] = [];
    let complete = true;
    let unassignedComplete = true;

    // The list response has no resource pagination; load each center by ID so
    // its has_next_page flag can provide a complete resource snapshot.
    const costCenters: CostCenterWithResources[] = [];
    for (const costCenter of listedCostCenters) {
      try {
        costCenters.push({
          ...costCenter,
          resources: await this.fetchCostCenterResources(
            enterpriseOctokit,
            enterprise,
            costCenter.id,
          ),
        });
      } catch (error) {
        complete = false;
        const status = getHttpStatus(error);
        const reason = status
          ? `GitHub returned HTTP ${status}`
          : 'the GitHub request failed';
        this.addCostCenterWarning(
          warnings,
          `Could not load resources for cost center "${costCenter.name}" (${reason}); the membership snapshot is incomplete.`,
        );
        costCenters.push({ ...costCenter, resources: [] });
      }
    }

    const resources = costCenters.flatMap(costCenter =>
      costCenter.resources.map(resource => ({ costCenter, resource })),
    );

    const teamResources = resources.filter(
      ({ resource }) => resource.type === 'Team',
    );
    let enterpriseTeams: EnterpriseTeamApiItem[] = [];
    let enterpriseTeamsLoaded = teamResources.length === 0;
    if (teamResources.length > 0) {
      try {
        enterpriseTeams = await this.fetchPaginatedList<EnterpriseTeamApiItem>(
          enterpriseOctokit,
          `/enterprises/${encodeURIComponent(enterprise)}/teams`,
          'enterprise teams',
        );
        enterpriseTeamsLoaded = true;
      } catch (error) {
        complete = false;
        const status = getHttpStatus(error);
        const reason = status
          ? `GitHub returned HTTP ${status}`
          : 'the GitHub request failed';
        this.addCostCenterWarning(
          warnings,
          `Could not list enterprise teams for cost-center membership (${reason}); the membership snapshot is incomplete.`,
        );
      }
    }

    for (const { resource } of resources) {
      if (
        resource.type !== 'Team' &&
        resource.type !== 'Org' &&
        resource.type !== 'User'
      ) {
        this.logger.debug(
          `[GithubClientV2] Ignoring cost center resource type "${resource.type}" (${resource.name}).`,
        );
      }
    }

    for (const { costCenter, resource } of teamResources) {
      if (!enterpriseTeamsLoaded) {
        continue;
      }

      const team = enterpriseTeams.find(
        candidate =>
          candidate.slug.toLowerCase() === resource.name.toLowerCase() ||
          candidate.name?.toLowerCase() === resource.name.toLowerCase() ||
          String(candidate.id) === resource.name,
      );
      if (!team) {
        complete = false;
        this.addCostCenterWarning(
          warnings,
          `Could not resolve enterprise team "${resource.name}" for cost center "${costCenter.name}"; the membership snapshot is incomplete.`,
        );
        continue;
      }

      try {
        resource.members = await this.fetchEnterpriseTeamMemberships(
          enterpriseOctokit,
          enterprise,
          team.slug,
        );
      } catch (error) {
        complete = false;
        const status = getHttpStatus(error);
        const reason = status
          ? `GitHub returned HTTP ${status}`
          : 'the GitHub request failed';
        this.addCostCenterWarning(
          warnings,
          `Could not load members of enterprise team "${resource.name}" for cost center "${costCenter.name}" (${reason}); the membership snapshot is incomplete.`,
        );
        continue;
      }

      if (typeof team?.id === 'number') {
        resource.teamId = team.id;
      } else if (typeof team?.id === 'string') {
        resource.teamId = Number(team.id);
      }
      if (
        resource.teamId !== undefined &&
        !Number.isSafeInteger(resource.teamId)
      ) {
        resource.teamId = undefined;
      }
      resource.teamCreatedAt = team?.created_at;
    }

    const orgRosterPromises = new Map<
      string,
      Promise<CostCenterOrganizationRoster>
    >();
    let warnedAboutFineGrainedPatScopes = false;
    for (const { costCenter, resource } of resources) {
      if (resource.type !== 'Org') {
        continue;
      }

      const cacheKey = resource.name.toLowerCase();
      let rosterPromise = orgRosterPromises.get(cacheKey);
      if (!rosterPromise) {
        rosterPromise = (async () => {
          const orgClient = await this.getCostCenterOrganizationOctokit(
            resource.name,
            enterpriseOctokit,
          );
          if (
            typeof this.enterpriseAuthStrategy !== 'string' &&
            !hasMembersReadPermission(orgClient.membersPermission)
          ) {
            return {
              members: [],
              skipReason:
                'the GitHub App installation is missing the "members: read" permission required to read a complete roster',
            };
          }

          const roster = await this.fetchOrganizationMembers(
            orgClient.octokit,
            resource.name,
          );
          if (typeof this.enterpriseAuthStrategy === 'string') {
            if (!roster.hasOAuthScopesHeader) {
              return {
                members: roster.members,
                oauthScopesHeaderMissing: true,
              };
            } else if (!hasOrganizationMemberScope(roster.oauthScopes ?? '')) {
              return {
                members: [],
                skipReason:
                  'the classic GitHub PAT is missing one of the "read:org", "write:org", or "admin:org" scopes required to read a complete roster',
              };
            }
          }

          return { members: roster.members };
        })();
        orgRosterPromises.set(cacheKey, rosterPromise);
      }

      try {
        const roster = await rosterPromise;
        if (
          roster.oauthScopesHeaderMissing &&
          !warnedAboutFineGrainedPatScopes
        ) {
          complete = false;
          warnedAboutFineGrainedPatScopes = true;
          this.addCostCenterWarning(
            warnings,
            'The GitHub PAT response did not include the "x-oauth-scopes" header; organization roster completeness cannot be verified for fine-grained PATs, so this membership snapshot is incomplete.',
          );
        }
        if (roster.skipReason) {
          complete = false;
          this.addCostCenterWarning(
            warnings,
            `Skipping cost center organization "${resource.name}": ${roster.skipReason}.`,
          );
          continue;
        }
        const { members } = roster;
        if (members.length === 0) {
          complete = false;
          this.addCostCenterWarning(
            warnings,
            `Skipping cost center organization "${resource.name}": its member roster is empty, so this membership snapshot is incomplete.`,
          );
          continue;
        }
        resource.members = members;
      } catch (error) {
        if (error instanceof MissingCostCenterOrganizationInstallationError) {
          unassignedComplete = false;
          this.addCostCenterWarning(
            warnings,
            `Skipping organization resource "${resource.name}" in cost center "${costCenter.name}": the GitHub App is not installed for this organization; its users will not be counted in the no-cost-center bucket.`,
          );
          continue;
        }

        complete = false;
        const status = getHttpStatus(error);
        let reason = 'the GitHub request failed';
        if (status === 404) {
          reason =
            'the App installation was not found or could not be accessed';
        } else if (status) {
          reason = `GitHub returned HTTP ${status}`;
        }
        this.addCostCenterWarning(
          warnings,
          `Skipping cost center organization "${resource.name}": ${reason}.`,
        );
      }
    }

    const membersByLogin = new Map<string, GithubUser>();
    for (const { resource } of resources) {
      if (resource.type !== 'Team' && resource.type !== 'Org') {
        continue;
      }
      for (const member of resource.members ?? []) {
        membersByLogin.set(member.login.toLowerCase(), member);
      }
    }

    for (const { costCenter, resource } of resources) {
      if (resource.type !== 'User') {
        continue;
      }

      const expandedMember = membersByLogin.get(resource.name.toLowerCase());
      if (expandedMember) {
        resource.members = [expandedMember];
        continue;
      }

      try {
        const userUrl = buildApiUrl(
          this.copilotConfig.apiBaseUrl,
          `/users/${encodeURIComponent(resource.name)}`,
        );
        const response = await enterpriseOctokit.request(`GET ${userUrl}`);
        const user = toGithubUser(response.data);
        if (user && user.login.toLowerCase() === resource.name.toLowerCase()) {
          resource.members = [user];
        } else {
          complete = false;
          this.addCostCenterWarning(
            warnings,
            `Could not resolve cost center user "${resource.name}" in "${costCenter.name}"; the membership snapshot is incomplete.`,
          );
        }
      } catch (error) {
        const status = getHttpStatus(error);
        if (status === 404) {
          // A direct-user 404 is terminal: that user cannot contribute metrics.
          this.addCostCenterWarning(
            warnings,
            `Could not resolve direct cost center user "${resource.name}" in "${costCenter.name}" (GitHub returned HTTP 404); skipping the unavailable user.`,
          );
          continue;
        }

        complete = false;
        const reason = status
          ? `GitHub returned HTTP ${status}`
          : 'the GitHub request failed';
        this.addCostCenterWarning(
          warnings,
          `Could not resolve cost center user "${resource.name}" in "${costCenter.name}" (${reason}); the membership snapshot is incomplete.`,
        );
      }
    }

    return {
      costCenters: listedCostCenters,
      members: resolveCostCenterMemberships(costCenters),
      warnings,
      complete,
      unassignedComplete,
    };
  }

  private async fetchReportLinks(
    octokit: Octokit,
    path: string,
  ): Promise<ReportEnvelope> {
    const response = await octokit.request(`GET ${path}`);

    if (response.status === 204 || !response.data) {
      this.logger.warn(
        `[GithubClientV2] Received status ${response.status} for ${path} — no report data available for this period.`,
      );
      return { download_links: [] };
    }

    const envelope = response.data as ReportEnvelope;
    if (!envelope.download_links?.length) {
      this.logger.warn(
        `[GithubClientV2] Empty download_links in response for ${path} — report may not yet be available.`,
      );
    }

    return envelope;
  }

  async fetchEnterpriseReportLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getEnterpriseOctokit();
    const path = `/enterprises/${this.copilotConfig.enterprise}/copilot/metrics/reports/enterprise-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async fetchOrganizationReportLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getOrganizationOctokit();
    const path = `/orgs/${this.copilotConfig.organization}/copilot/metrics/reports/organization-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async fetchEnterpriseUserReportLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getEnterpriseOctokit();
    const path = `/enterprises/${this.copilotConfig.enterprise}/copilot/metrics/reports/users-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async fetchOrganizationUserReportLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getOrganizationOctokit();
    const path = `/orgs/${this.copilotConfig.organization}/copilot/metrics/reports/users-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async fetchEnterpriseUserTeamsLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getEnterpriseOctokit();
    const path = `/enterprises/${this.copilotConfig.enterprise}/copilot/metrics/reports/user-teams-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async fetchOrganizationUserTeamsLinks(day: string): Promise<ReportEnvelope> {
    const octokit = await this.getOrganizationOctokit();
    const path = `/orgs/${this.copilotConfig.organization}/copilot/metrics/reports/user-teams-1-day?day=${day}`;
    return this.fetchReportLinks(octokit, path);
  }

  async probeOrganizationUserTeamsReport(
    day: string,
  ): Promise<OrganizationUserTeamsReportAccess> {
    let octokit: Octokit;
    try {
      octokit = await this.getOrganizationOctokit();
    } catch (error) {
      if (getHttpStatus(error) === 404) {
        this.logger.warn(
          '[GithubClientV2] The organization GitHub App installation could not be resolved; keeping native enterprise team reports.',
        );
        return 'installation-unavailable';
      }
      throw error;
    }
    const organization = this.copilotConfig.organization;
    if (!organization) {
      throw new Error('No organization name configured.');
    }

    const path = `/orgs/${encodeURIComponent(
      organization,
    )}/copilot/metrics/reports/user-teams-1-day?day=${encodeURIComponent(day)}`;

    try {
      await octokit.request(`GET ${path}`);
      return 'available';
    } catch (error) {
      const status = getHttpStatus(error);
      if (
        (status === 403 || status === 404) &&
        !(status === 403 && isRateLimitError(error))
      ) {
        this.logger.warn(
          `[GithubClientV2] Organization Copilot user-team reports are unavailable (HTTP ${status}).`,
        );
        return 'unavailable';
      }
      throw error;
    }
  }

  private async waitForGraphQLRateLimit(
    rateLimit: GraphQLRateLimit,
  ): Promise<void> {
    if (rateLimit.remaining >= GRAPHQL_LOW_REMAINING_THRESHOLD) {
      return;
    }

    const resetAt = Date.parse(rateLimit.resetAt);
    if (!Number.isFinite(resetAt)) {
      throw new Error('GitHub returned an invalid GraphQL rate-limit reset.');
    }

    const delayMs = resetAt - Date.now() + 1_000;
    if (delayMs <= 0) {
      return;
    }

    this.logger.warn(
      '[GithubClientV2] GitHub GraphQL rate limit is low; pausing team membership pagination until reset.',
    );
    await new Promise(resolve => setTimeout(resolve, delayMs));
  }

  private async graphqlWithRetry<T>(
    octokit: Octokit,
    query: string,
    variables: Record<string, unknown>,
  ): Promise<T> {
    let rateLimitRetries = 0;
    let serverErrorRetries = 0;

    for (;;) {
      try {
        return await octokit.graphql<T>(query, variables);
      } catch (error) {
        if (
          isGraphQLRateLimitError(error) &&
          rateLimitRetries < GRAPHQL_RATE_LIMIT_MAX_ATTEMPTS - 1
        ) {
          rateLimitRetries += 1;
          const delayMs = getGraphQLRateLimitRetryDelay(
            error,
            rateLimitRetries,
          );
          this.logger.warn(
            `[GithubClientV2] GitHub GraphQL rate limit reached; retrying team membership request (${rateLimitRetries}/${
              GRAPHQL_RATE_LIMIT_MAX_ATTEMPTS - 1
            }).`,
          );
          await new Promise(resolve => setTimeout(resolve, delayMs));
          continue;
        }

        if (
          isGraphQLServerError(error) &&
          serverErrorRetries < GRAPHQL_SERVER_ERROR_MAX_ATTEMPTS - 1
        ) {
          serverErrorRetries += 1;
          const delayMs = getGraphQLServerErrorRetryDelay(serverErrorRetries);
          this.logger.warn(
            `[GithubClientV2] GitHub GraphQL returned HTTP ${getHttpStatus(
              error,
            )}; retrying team membership request (${serverErrorRetries}/${
              GRAPHQL_SERVER_ERROR_MAX_ATTEMPTS - 1
            }).`,
          );
          await new Promise(resolve => setTimeout(resolve, delayMs));
          continue;
        }

        throw sanitizeGraphQLError(error);
      }
    }
  }

  private async appendGraphQLTeamMembers(
    team: GraphQLTeamNode,
    initialRateLimit: GraphQLRateLimit,
    memberships: OrganizationTeamMembership[],
    octokit: Octokit,
  ): Promise<GraphQLRateLimit> {
    const teamId = team.databaseId;
    if (!isSafeGitHubId(teamId) || !team.slug) {
      throw new Error('GitHub returned an invalid organization team.');
    }

    const appendMembers = (members: GraphQLTeamMembersPage) => {
      for (const member of members.nodes) {
        if (!isSafeGitHubId(member.databaseId)) {
          throw new Error(
            'GitHub returned an invalid organization team member.',
          );
        }
        memberships.push({
          teamId,
          teamSlug: team.slug,
          userId: member.databaseId,
        });
      }
    };

    appendMembers(team.members);
    let hasNextPage = team.members.pageInfo.hasNextPage;
    let cursor = team.members.pageInfo.endCursor;
    let rateLimit = initialRateLimit;
    const seenCursors = new Set<string>();

    while (hasNextPage) {
      if (!cursor) {
        throw new Error(
          'GitHub omitted the cursor for an organization team member page.',
        );
      }
      if (seenCursors.has(cursor)) {
        throw new Error(
          'GitHub repeated an organization team member cursor; the membership snapshot is incomplete.',
        );
      }
      seenCursors.add(cursor);

      await this.waitForGraphQLRateLimit(rateLimit);

      const response = await this.graphqlWithRetry<GraphQLTeamMembersResponse>(
        octokit,
        ORGANIZATION_TEAM_MEMBERS_QUERY,
        { teamId: team.id, cursor },
      );

      if (
        !response.node ||
        response.node.databaseId !== teamId ||
        response.node.slug !== team.slug
      ) {
        throw new Error(
          'GitHub returned an incomplete organization team member page.',
        );
      }

      const nextMembers = response.node.members;
      const nextHasNextPage = nextMembers.pageInfo.hasNextPage;
      const nextCursor = nextMembers.pageInfo.endCursor;
      if (nextHasNextPage && (!nextCursor || seenCursors.has(nextCursor))) {
        throw new Error(
          'GitHub repeated or omitted an organization team member cursor; the membership snapshot is incomplete.',
        );
      }

      appendMembers(nextMembers);
      hasNextPage = nextHasNextPage;
      cursor = nextCursor;
      rateLimit = response.rateLimit;
    }

    return rateLimit;
  }

  async fetchOrganizationTeamMemberships(
    options: OrganizationTeamMembershipFetchOptions = {},
  ): Promise<OrganizationTeamMembership[]> {
    const organization = this.copilotConfig.organization;
    if (!organization) {
      throw new Error('No organization name configured.');
    }

    const octokit = await this.getOrganizationOctokit();
    if (
      this.organizationMembersPermission !== undefined &&
      !hasMembersReadPermission(this.organizationMembersPermission)
    ) {
      throw new Error(
        'The GitHub App needs Members: read or higher permission to list organization team members.',
      );
    }

    const memberships: OrganizationTeamMembership[] = [];
    let cursor: string | null = options.afterCursor ?? null;
    let hasNextPage = true;
    let rateLimit: GraphQLRateLimit | undefined;
    let teamPageSize = ORGANIZATION_TEAMS_PAGE_SIZE;
    const seenTeamCursors = new Set<string>();

    while (hasNextPage) {
      if (cursor) {
        if (seenTeamCursors.has(cursor)) {
          throw new Error(
            'GitHub repeated an organization team cursor; the membership snapshot is incomplete.',
          );
        }
        seenTeamCursors.add(cursor);
      }

      if (rateLimit) {
        await this.waitForGraphQLRateLimit(rateLimit);
      }

      let teamPage: GraphQLOrganizationTeams | undefined;
      let pageMemberships: OrganizationTeamMembership[] = [];
      let pageRateLimit: GraphQLRateLimit | undefined;
      while (!teamPage) {
        try {
          const response =
            await this.graphqlWithRetry<GraphQLOrganizationTeamsPage>(
              octokit,
              ORGANIZATION_TEAMS_QUERY,
              { organization, cursor, teamPageSize },
            );

          const responseTeamPage = response.organization?.teams;
          if (!responseTeamPage) {
            throw new Error(
              'GitHub returned an incomplete organization team list.',
            );
          }

          const membershipsForAttempt: OrganizationTeamMembership[] = [];
          let attemptRateLimit = response.rateLimit;
          for (const team of responseTeamPage.nodes) {
            attemptRateLimit = await this.appendGraphQLTeamMembers(
              team,
              attemptRateLimit,
              membershipsForAttempt,
              octokit,
            );
          }

          teamPage = responseTeamPage;
          pageMemberships = membershipsForAttempt;
          pageRateLimit = attemptRateLimit;
        } catch (error) {
          if (
            !isGraphQLServerError(error) ||
            teamPageSize <= ORGANIZATION_TEAMS_PAGE_SIZE_FLOOR
          ) {
            throw error;
          }

          const smallerPageSize = Math.max(
            ORGANIZATION_TEAMS_PAGE_SIZE_FLOOR,
            Math.floor(teamPageSize / 2),
          );
          this.logger.warn(
            `[GithubClientV2] GitHub GraphQL returned HTTP ${getHttpStatus(
              error,
            )} for an organization team page; retrying the same cursor with a smaller page size (${teamPageSize} to ${smallerPageSize}).`,
          );
          teamPageSize = smallerPageSize;
        }
      }

      if (!pageRateLimit) {
        throw new Error('GitHub returned an invalid organization team page.');
      }

      const nextHasNextPage = teamPage.pageInfo.hasNextPage;
      const nextCursor = teamPage.pageInfo.endCursor;
      if (nextHasNextPage && (!nextCursor || seenTeamCursors.has(nextCursor))) {
        throw new Error(
          'GitHub repeated or omitted an organization team cursor; the membership snapshot is incomplete.',
        );
      }

      rateLimit = pageRateLimit;
      hasNextPage = nextHasNextPage;
      cursor = nextCursor;
      if (options.onPage) {
        await options.onPage({
          memberships: pageMemberships,
          cursor: hasNextPage ? cursor : null,
          complete: !hasNextPage,
        });
      } else {
        memberships.push(...pageMemberships);
      }
    }

    return memberships;
  }

  /**
   * Download a signed document URL containing a JSON payload (array or object).
   * SSRF guard: only allows downloads from hosts derived from the configured
   * GitHub host, plus the documented object storage fallbacks.
   * Logs only origin+pathname (never query string which contains credentials).
   * Use for entity-level reports (enterprise-1-day, organization-1-day).
   */
  async downloadDocument(url: string): Promise<unknown> {
    const body = await this.downloadText(url);
    return JSON.parse(body);
  }

  /**
   * Download a signed document URL containing newline-delimited JSON (NDJSON).
   * Each non-empty line is a separate JSON object. Returns all parsed rows.
   * Use for per-user and user-teams reports which are NDJSON format.
   */
  async downloadNdjsonDocument(url: string): Promise<unknown[]> {
    const body = await this.downloadText(url);
    const rows: unknown[] = [];
    for (const line of body.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        rows.push(JSON.parse(trimmed));
      } catch (err) {
        this.logger.warn(
          `[GithubClientV2] Skipping unparseable NDJSON line: ${err}`,
        );
      }
    }
    return rows;
  }

  /**
   * Parses a download URL and applies the SSRF guard. Called for the initial
   * URL and again for every redirect target, so a signed link on an allowed
   * host cannot bounce the download to an internal address.
   */
  private assertAllowedDownloadUrl(url: string): URL {
    let parsedUrl: URL;
    try {
      parsedUrl = new URL(url);
    } catch {
      // The URL carries a signature, so it must never reach the logs.
      throw new Error('Malformed download URL');
    }

    if (parsedUrl.protocol !== 'https:') {
      throw new Error(
        `Refused to download over insecure protocol: ${parsedUrl.protocol}`,
      );
    }

    if (
      !isAllowedDownloadHost(
        parsedUrl,
        this.copilotConfig.host,
        this.copilotConfig.apiBaseUrl,
      )
    ) {
      throw new Error(
        `Refused to download from disallowed host: ${parsedUrl.hostname}`,
      );
    }

    return parsedUrl;
  }

  private async downloadText(url: string): Promise<string> {
    let currentUrl = url;

    for (let hop = 0; hop <= MAX_DOWNLOAD_REDIRECTS; hop++) {
      const { origin, pathname } = this.assertAllowedDownloadUrl(currentUrl);
      this.logger.debug(
        `[GithubClientV2] Downloading document: ${origin}${pathname}`,
      );

      const response = await fetch(currentUrl, { redirect: 'manual' });

      if (response.status >= 300 && response.status < 400) {
        const location = response.headers.get('location');
        if (!location) {
          throw new Error(
            `Failed to download document: HTTP ${response.status} ${response.statusText} without a location header`,
          );
        }
        // Drain the redirect body so undici can reuse the connection. A
        // truncated body must not fail a download whose target we already have.
        await response.text().catch(() => undefined);

        let nextUrl: URL;
        try {
          nextUrl = new URL(location, currentUrl);
        } catch {
          // Node attaches the base URL, which carries a signature, to the
          // thrown error, so none of it may reach the caller.
          throw new Error('Malformed redirect location');
        }

        currentUrl = nextUrl.toString();
        continue;
      }

      if (!response.ok) {
        throw new Error(
          `Failed to download document: HTTP ${response.status} ${response.statusText}`,
        );
      }

      return response.text();
    }

    throw new Error(
      `Failed to download document: exceeded ${MAX_DOWNLOAD_REDIRECTS} redirects`,
    );
  }
}
