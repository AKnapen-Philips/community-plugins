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

import { GithubClientV2 } from './GithubClientV2';
import { Octokit } from '@octokit/rest';

const mockRequest = jest.fn();
const mockGraphql = jest.fn();
const mockGetOrgInstallation = jest.fn();
const mockHookBefore = jest.fn();
const mockOutgoingHeaders: Array<Record<string, string>> = [];
const mockAppAuthentication = jest
  .fn()
  .mockResolvedValue({ token: 'fake-app-jwt' });
const mockCreateAppAuth = jest.fn((_options: unknown) => mockAppAuthentication);

// Mock Octokit
jest.mock('@octokit/rest', () => ({
  Octokit: jest.fn().mockImplementation(() => {
    const requestHooks: Array<
      (requestOptions: { headers: Record<string, string> }) => void
    > = [];
    return {
      request: (...args: any[]) => {
        const requestOptions = {
          headers: {
            accept: 'application/vnd.github.v3+json',
          } as Record<string, string>,
        };
        requestHooks.forEach(requestHook => requestHook(requestOptions));
        mockOutgoingHeaders.push({ ...requestOptions.headers });
        return mockRequest(...args);
      },
      rest: {
        apps: {
          getOrgInstallation: mockGetOrgInstallation,
        },
      },
      graphql: (...args: any[]) => mockGraphql(...args),
      hook: {
        before: (
          name: string,
          requestHook: (requestOptions: {
            headers: Record<string, string>;
          }) => void,
        ) => {
          mockHookBefore(name, requestHook);
          if (name === 'request') {
            requestHooks.push(requestHook);
          }
        },
      },
    };
  }),
}));

jest.mock('@octokit/auth-app', () => ({
  createAppAuth: (options: unknown) => mockCreateAppAuth(options),
}));

// Mock GithubUtils
jest.mock('../utils/GithubUtils', () => ({
  getCopilotConfig: jest.fn().mockReturnValue({
    host: 'github.com',
    enterprise: 'my-enterprise',
    organization: 'my-org',
    apiBaseUrl: 'https://api.github.com',
  }),
  getGithubCredentials: jest.fn().mockResolvedValue({
    enterprise: 'fake-token',
    organization: 'fake-token',
  }),
}));

function httpError(status: number): Error & { status: number } {
  return Object.assign(new Error(`HTTP ${status}`), { status });
}

describe('GithubClientV2', () => {
  let mockConfig: any;
  let mockLogger: any;
  let originalFetch: any;

  beforeEach(() => {
    jest.clearAllMocks();
    mockRequest.mockReset();
    mockGraphql.mockReset();
    mockGetOrgInstallation.mockReset().mockResolvedValue({ data: { id: 999 } });
    mockHookBefore.mockReset();
    mockOutgoingHeaders.length = 0;
    mockAppAuthentication
      .mockReset()
      .mockResolvedValue({ token: 'fake-app-jwt' });
    mockCreateAppAuth.mockReset().mockReturnValue(mockAppAuthentication);
    mockConfig = {} as any;
    mockLogger = {
      debug: jest.fn(),
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };

    // Mock global fetch
    originalFetch = (global as any).fetch;
    const mockFetch = jest.fn();
    (global as any).fetch = mockFetch;
  });

  afterEach(() => {
    (global as any).fetch = originalFetch;
  });

  it('downloadDocument rejects disallowed hosts (SSRF guard)', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    await expect(
      client.downloadDocument('https://internal.example.com/evil'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);
  });

  it('downloadDocument accepts allowed github/githubusercontent/s3 hosts and logs origin+pathname only', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { hello: 'world' };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
      status: 200,
      statusText: 'OK',
    });

    const url = 'https://downloads.github.com/reports/report.json?sig=secret';
    const result = await client.downloadDocument(url);
    expect(result).toEqual(fakeJson);
    expect(mockLogger.debug).toHaveBeenCalled();
    const logged = mockLogger.debug.mock.calls[0][0] as string;
    expect(logged).toContain(
      'https://downloads.github.com/reports/report.json',
    );
    expect(logged).not.toContain('sig=');
  });

  it('downloadDocument accepts raw.githubusercontent.com host', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { ok: true };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
      status: 200,
      statusText: 'OK',
    });

    const url =
      'https://raw.githubusercontent.com/my/repo/main/report.json?sig=xyz';
    const result = await client.downloadDocument(url);
    expect(result).toEqual(fakeJson);
  });

  it('downloadDocument accepts s3.amazonaws.com style hosts', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { ok: true };
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
      status: 200,
      statusText: 'OK',
    });

    const url = 'https://my-bucket.s3.amazonaws.com/report.json?sig=abc';
    const result = await client.downloadDocument(url);
    expect(result).toEqual(fakeJson);
  });

  it('scopes the download host allowlist to the configured GitHub host', async () => {
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { ok: true };
    const download = (client: GithubClientV2, url: string) => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
        status: 200,
        statusText: 'OK',
      });
      return client.downloadDocument(url);
    };

    const gheClient = new GithubClientV2(
      {
        host: 'octocorp.ghe.com',
        enterprise: 'octocorp',
        apiBaseUrl: 'https://api.octocorp.ghe.com',
      },
      mockConfig,
      mockLogger,
    );

    await expect(
      download(gheClient, 'https://objects.octocorp.ghe.com/report.json?sig=a'),
    ).resolves.toEqual(fakeJson);
    await expect(
      download(
        gheClient,
        'https://mystorage.blob.core.windows.net/report.json?sig=b',
      ),
    ).resolves.toEqual(fakeJson);
    await expect(
      download(
        gheClient,
        'https://raw.githubusercontent.com/report.json?sig=c',
      ),
    ).resolves.toEqual(fakeJson);

    // A GHE.com tenant must not trust github.com or another tenant.
    await expect(
      download(gheClient, 'https://downloads.github.com/report.json?sig=d'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);
    await expect(
      download(gheClient, 'https://objects.evilcorp.ghe.com/report.json'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);

    // And a github.com deployment must not trust a GHE.com tenant.
    const dotComClient = await GithubClientV2.fromConfig(
      mockConfig,
      mockLogger,
    );
    await expect(
      download(dotComClient, 'https://objects.octocorp.ghe.com/report.json'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);

    // Object storage is a documented fallback for both, so it stays allowed
    // regardless of which host is configured.
    await expect(
      download(dotComClient, 'https://mystorage.blob.core.windows.net/r.json'),
    ).resolves.toEqual(fakeJson);

    // The guard must reject before issuing the request, so only the four
    // allowed downloads ever reached fetch.
    expect(mockFetch).toHaveBeenCalledTimes(4);
  });

  it('normalises the configured host and rejects non-https download links', async () => {
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { ok: true };
    const download = (client: GithubClientV2, url: string) => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
        status: 200,
        statusText: 'OK',
      });
      return client.downloadDocument(url);
    };

    // Casing and a port on copilot.host must not break host matching, and the
    // host itself is allowed, not just its subdomains.
    const gheClient = new GithubClientV2(
      {
        host: 'Octocorp.GHE.com:8443',
        enterprise: 'octocorp',
        apiBaseUrl: 'https://api.octocorp.ghe.com',
      },
      mockConfig,
      mockLogger,
    );

    await expect(
      download(gheClient, 'https://octocorp.ghe.com/report.json?sig=a'),
    ).resolves.toEqual(fakeJson);

    // Plain HTTP is never followed, even on an otherwise allowed host.
    await expect(
      download(gheClient, 'http://objects.octocorp.ghe.com/report.json'),
    ).rejects.toThrow(/Refused to download over insecure protocol: http:/i);

    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('allows the GHE.com tenant of the API base url when copilot.host is the API host', async () => {
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const fakeJson = { ok: true };
    const download = (client: GithubClientV2, url: string) => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        text: jest.fn().mockResolvedValue(JSON.stringify(fakeJson)),
        status: 200,
        statusText: 'OK',
      });
      return client.downloadDocument(url);
    };

    const apiHostClient = new GithubClientV2(
      {
        host: 'api.octocorp.ghe.com',
        enterprise: 'octocorp',
        apiBaseUrl: 'https://api.octocorp.ghe.com',
      },
      mockConfig,
      mockLogger,
    );

    await expect(
      download(
        apiHostClient,
        'https://copilot-reports.octocorp.ghe.com/report.json?sig=a',
      ),
    ).resolves.toEqual(fakeJson);
    await expect(
      download(apiHostClient, 'https://octocorp.ghe.com/report.json?sig=b'),
    ).resolves.toEqual(fakeJson);

    await expect(
      download(
        apiHostClient,
        'https://copilot-reports.evilcorp.ghe.com/report.json',
      ),
    ).rejects.toThrow(/Refused to download from disallowed host/i);
    await expect(
      download(
        apiHostClient,
        'https://copilot-reports.octocorp.ghe.com.evil.example/report.json',
      ),
    ).rejects.toThrow(/Refused to download from disallowed host/i);

    // Only a GHE.com API host extends the allowlist to its parent domain.
    const otherApiHostClient = new GithubClientV2(
      {
        host: 'api.example.com',
        enterprise: 'example',
        apiBaseUrl: 'https://api.example.com',
      },
      mockConfig,
      mockLogger,
    );
    await expect(
      download(otherApiHostClient, 'https://reports.example.com/report.json'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);

    expect(mockFetch).toHaveBeenCalledTimes(2);
  });

  it('re-applies the host allowlist to redirect targets', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;

    // A signed link on an allowed host must not be able to bounce the download
    // to an internal address.
    mockFetch.mockResolvedValueOnce({
      status: 302,
      statusText: 'Found',
      ok: false,
      headers: { get: () => 'https://169.254.169.254/latest/meta-data/' },
      text: jest.fn().mockResolvedValue(''),
    });

    await expect(
      client.downloadDocument('https://downloads.github.com/report.json?sig=a'),
    ).rejects.toThrow(/Refused to download from disallowed host/i);

    // A redirect that stays within the allowlist is followed.
    mockFetch.mockResolvedValueOnce({
      status: 302,
      statusText: 'Found',
      ok: false,
      headers: { get: () => 'https://my-bucket.s3.amazonaws.com/report.json' },
      text: jest.fn().mockResolvedValue(''),
    });
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: jest.fn().mockResolvedValue(JSON.stringify({ ok: true })),
    });

    await expect(
      client.downloadDocument('https://downloads.github.com/report.json?sig=b'),
    ).resolves.toEqual({ ok: true });

    // `redirect: 'manual'` is what makes the per-hop re-check reachable at all;
    // without it undici would follow redirects internally and bypass the guard.
    expect(mockFetch).toHaveBeenCalledTimes(3);
    expect(mockFetch).toHaveBeenNthCalledWith(
      3,
      'https://my-bucket.s3.amazonaws.com/report.json',
      { redirect: 'manual' },
    );
  });

  it('points the GitHub App installation lookup at the configured API base url', async () => {
    const { getCopilotConfig, getGithubCredentials } = jest.requireMock(
      '../utils/GithubUtils',
    );
    getCopilotConfig.mockReturnValueOnce({
      host: 'octocorp.ghe.com',
      enterprise: 'octocorp',
      apiBaseUrl: 'https://api.octocorp.ghe.com',
    });
    getGithubCredentials.mockResolvedValueOnce({
      enterprise: { appId: 123, privateKey: 'private-key' },
    });
    mockRequest
      .mockResolvedValueOnce({ data: { id: 999 } })
      .mockResolvedValueOnce({ data: { download_links: [] } });

    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    await client.fetchEnterpriseReportLinks('2026-01-01');

    // Both the installation-lookup client and the report client must target the
    // tenant's API. The lookup one used to always hit api.github.com.
    const MockedOctokit = Octokit as unknown as jest.Mock;
    expect(MockedOctokit.mock.calls[0][0].baseUrl).toBe(
      'https://api.octocorp.ghe.com',
    );
    expect(MockedOctokit.mock.calls[1][0].baseUrl).toBe(
      'https://api.octocorp.ghe.com',
    );
  });

  it('resolves relative redirects and rejects malformed redirect chains', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    const redirectTo = (location: string | null) => ({
      status: 302,
      statusText: 'Found',
      ok: false,
      headers: { get: () => location },
      text: jest.fn().mockResolvedValue(''),
    });

    // A relative Location resolves against the URL it came from.
    mockFetch.mockResolvedValueOnce(redirectTo('/reports/final.json'));
    mockFetch.mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: jest.fn().mockResolvedValue(JSON.stringify({ ok: true })),
    });

    await expect(
      client.downloadDocument(
        'https://my-bucket.s3.eu-north-1.amazonaws.com/a/b.json?sig=1',
      ),
    ).resolves.toEqual({ ok: true });
    expect(mockFetch).toHaveBeenNthCalledWith(
      2,
      'https://my-bucket.s3.eu-north-1.amazonaws.com/reports/final.json',
      { redirect: 'manual' },
    );

    // A redirect without a Location header is an error, not a silent retry.
    mockFetch.mockResolvedValueOnce(redirectTo(null));
    await expect(
      client.downloadDocument('https://downloads.github.com/r.json?sig=2'),
    ).rejects.toThrow(/without a location header/i);

    // A Location that will not parse must not surface the signed URL, which
    // Node attaches to the error it throws.
    mockFetch.mockResolvedValueOnce(redirectTo('http://%'));
    const error: any = await client
      .downloadDocument('https://downloads.github.com/r.json?sig=secret')
      .catch(e => e);
    expect(error.message).toBe('Malformed redirect location');
    // Node attaches the base URL to the error it throws; ours must not.
    expect(Object.keys(error)).not.toContain('base');
    expect(JSON.stringify(error)).not.toContain('sig=secret');

    // And an endless redirect chain is capped rather than spinning forever.
    mockFetch.mockResolvedValue(
      redirectTo('https://downloads.github.com/loop.json'),
    );
    await expect(
      client.downloadDocument('https://downloads.github.com/loop.json?sig=3'),
    ).rejects.toThrow(/exceeded 5 redirects/i);
  });

  it('downloadDocument throws on non-OK HTTP status', async () => {
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    const mockFetch = (global as any).fetch as jest.MockedFunction<any>;
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 404,
      statusText: 'Not Found',
    });

    await expect(
      client.downloadDocument(
        'https://downloads.github.com/reports/missing.json?sig=x',
      ),
    ).rejects.toThrow(/Failed to download document: HTTP 404 Not Found/);
  });

  it('fetchEnterpriseReportLinks calls correct enterprise path and sets API version header', async () => {
    // Arrange
    mockRequest.mockResolvedValueOnce({
      data: {
        download_links: [
          'https://my-bucket.s3.amazonaws.com/report.json?sig=1',
        ],
      },
    });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    // Act
    const res = await client.fetchEnterpriseReportLinks('2026-01-01');

    // Assert
    expect(mockRequest).toHaveBeenCalledWith(
      `GET /enterprises/my-enterprise/copilot/metrics/reports/enterprise-1-day?day=2026-01-01`,
    );

    expect(mockOutgoingHeaders[0]).toEqual({
      accept: 'application/vnd.github+json',
      'x-github-api-version': '2026-03-10',
    });

    expect(res.download_links[0]).toContain('s3.amazonaws.com');
  });

  it.each([
    ['200', { data: { download_links: [] }, status: 200 }],
    ['204', { data: undefined, status: 204 }],
  ])(
    'treats an organization user-teams report response of %s as authorized',
    async (_status, response) => {
      mockRequest.mockResolvedValueOnce(response);
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

      await expect(
        client.probeOrganizationUserTeamsReport('2026-01-04'),
      ).resolves.toBe('available');
    },
  );

  it.each([403, 404])(
    'detects an unavailable organization user-teams report on HTTP %s',
    async status => {
      mockRequest.mockRejectedValueOnce(httpError(status));
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

      await expect(
        client.probeOrganizationUserTeamsReport('2026-01-04'),
      ).resolves.toBe('unavailable');
    },
  );

  it('does not classify a rate-limited 403 as an unavailable report', async () => {
    mockRequest.mockRejectedValueOnce(
      Object.assign(httpError(403), {
        response: { headers: { 'x-ratelimit-remaining': '0' } },
      }),
    );
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.probeOrganizationUserTeamsReport('2026-01-04'),
    ).rejects.toMatchObject({ status: 403 });
  });

  it.each([401, 429, 500])(
    'does not classify HTTP %s as an unavailable organization report',
    async status => {
      mockRequest.mockRejectedValueOnce(httpError(status));
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

      await expect(
        client.probeOrganizationUserTeamsReport('2026-01-04'),
      ).rejects.toMatchObject({ status });
    },
  );

  it('does not classify a network failure as an unavailable organization report', async () => {
    mockRequest.mockRejectedValueOnce(new Error('network failure'));
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.probeOrganizationUserTeamsReport('2026-01-04'),
    ).rejects.toThrow('network failure');
  });

  it('classifies a missing organization installation separately from report access', async () => {
    const { getGithubCredentials } = jest.requireMock('../utils/GithubUtils');
    getGithubCredentials.mockResolvedValueOnce({
      enterprise: 'enterprise-pat',
      organization: { appId: 123, privateKey: 'private-key' },
    });
    mockGetOrgInstallation.mockRejectedValueOnce(httpError(404));
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.probeOrganizationUserTeamsReport('2026-01-04'),
    ).resolves.toBe('installation-unavailable');
  });

  it('fetches all organization teams and ALL child-team members in paginated GraphQL batches', async () => {
    const rateLimit = {
      cost: 1,
      remaining: 12_000,
      resetAt: '2026-10-22T00:00:00Z',
    };
    mockGraphql
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [
              {
                id: 'team-node-1',
                databaseId: 101,
                slug: 'parent-team',
                members: {
                  nodes: [{ databaseId: 501 }],
                  pageInfo: { hasNextPage: true, endCursor: 'member-cursor' },
                },
              },
            ],
            pageInfo: { hasNextPage: true, endCursor: 'team-cursor' },
          },
        },
        rateLimit,
      })
      .mockResolvedValueOnce({
        node: {
          databaseId: 101,
          slug: 'parent-team',
          members: {
            nodes: [{ databaseId: 502 }],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit,
      })
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [
              {
                id: 'team-node-2',
                databaseId: 102,
                slug: 'child-team',
                members: {
                  nodes: [{ databaseId: 503 }],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
            ],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit,
      });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(client.fetchOrganizationTeamMemberships()).resolves.toEqual([
      { teamId: 101, teamSlug: 'parent-team', userId: 501 },
      { teamId: 101, teamSlug: 'parent-team', userId: 502 },
      { teamId: 102, teamSlug: 'child-team', userId: 503 },
    ]);

    expect(mockGraphql).toHaveBeenCalledTimes(3);
    expect(mockGraphql.mock.calls[0][0]).toContain(
      'members(first: 50, membership: ALL)',
    );
    expect(mockGraphql.mock.calls[0][1]).toEqual({
      organization: 'my-org',
      cursor: null,
      teamPageSize: 25,
    });
    expect(mockGraphql.mock.calls[1][0]).toContain('membership: ALL');
    expect(mockGraphql.mock.calls[1][1]).toEqual({
      teamId: 'team-node-1',
      cursor: 'member-cursor',
    });
    expect(mockGraphql.mock.calls[2][1]).toEqual({
      organization: 'my-org',
      cursor: 'team-cursor',
      teamPageSize: 25,
    });
  });

  it('resumes from a saved team cursor and checkpoints each GraphQL batch', async () => {
    mockGraphql.mockResolvedValueOnce({
      organization: {
        teams: {
          nodes: [
            {
              id: 'team-node-3',
              databaseId: 103,
              slug: 'resumed-team',
              members: {
                nodes: [{ databaseId: 601 }],
                pageInfo: { hasNextPage: false, endCursor: null },
              },
            },
          ],
          pageInfo: { hasNextPage: false, endCursor: null },
        },
      },
      rateLimit: {
        cost: 1,
        remaining: 12_000,
        resetAt: '2026-10-22T00:00:00Z',
      },
    });
    const onPage = jest.fn().mockResolvedValue(undefined);
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.fetchOrganizationTeamMemberships({
        afterCursor: 'saved-team-cursor',
        onPage,
      }),
    ).resolves.toEqual([]);

    expect(mockGraphql).toHaveBeenCalledWith(expect.any(String), {
      organization: 'my-org',
      cursor: 'saved-team-cursor',
      teamPageSize: 25,
    });
    expect(onPage).toHaveBeenCalledWith({
      memberships: [{ teamId: 103, teamSlug: 'resumed-team', userId: 601 }],
      cursor: null,
      complete: true,
    });
  });

  it('fails an incomplete member snapshot when GraphQL repeats a member cursor', async () => {
    const rateLimit = {
      cost: 1,
      remaining: 12_000,
      resetAt: '2026-10-22T00:00:00Z',
    };
    mockGraphql
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [
              {
                id: 'team-node-1',
                databaseId: 101,
                slug: 'demo-team',
                members: {
                  nodes: [{ databaseId: 501 }],
                  pageInfo: {
                    hasNextPage: true,
                    endCursor: 'member-cursor',
                  },
                },
              },
            ],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit,
      })
      .mockResolvedValueOnce({
        node: {
          databaseId: 101,
          slug: 'demo-team',
          members: {
            nodes: [{ databaseId: 502 }],
            pageInfo: {
              hasNextPage: true,
              endCursor: 'member-cursor',
            },
          },
        },
        rateLimit,
      });
    const onPage = jest.fn().mockResolvedValue(undefined);
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.fetchOrganizationTeamMemberships({ onPage }),
    ).rejects.toThrow(
      'GitHub repeated or omitted an organization team member cursor',
    );
    expect(onPage).not.toHaveBeenCalled();
  });

  it('fails an incomplete team snapshot when GraphQL repeats a team cursor', async () => {
    const rateLimit = {
      cost: 1,
      remaining: 12_000,
      resetAt: '2026-10-22T00:00:00Z',
    };
    mockGraphql
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: true, endCursor: 'team-cursor' },
          },
        },
        rateLimit,
      })
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: true, endCursor: 'team-cursor' },
          },
        },
        rateLimit,
      });
    const onPage = jest.fn().mockResolvedValue(undefined);
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(
      client.fetchOrganizationTeamMemberships({ onPage }),
    ).rejects.toThrow('GitHub repeated or omitted an organization team cursor');
    expect(onPage).toHaveBeenCalledTimes(1);
    expect(onPage).toHaveBeenCalledWith({
      memberships: [],
      cursor: 'team-cursor',
      complete: false,
    });
  });

  it('retries transient GraphQL 502 responses at the same team page size', async () => {
    jest.useFakeTimers();
    mockGraphql.mockRejectedValueOnce(httpError(502)).mockResolvedValueOnce({
      organization: {
        teams: {
          nodes: [],
          pageInfo: { hasNextPage: false, endCursor: null },
        },
      },
      rateLimit: {
        cost: 1,
        remaining: 12_000,
        resetAt: '2026-10-22T00:00:00Z',
      },
    });

    try {
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
      const membershipsPromise = client.fetchOrganizationTeamMemberships();
      await jest.runAllTimersAsync();
      await expect(membershipsPromise).resolves.toEqual([]);

      expect(mockGraphql).toHaveBeenCalledTimes(2);
      expect(mockGraphql.mock.calls.map(([, variables]) => variables)).toEqual([
        { organization: 'my-org', cursor: null, teamPageSize: 25 },
        { organization: 'my-org', cursor: null, teamPageSize: 25 },
      ]);
    } finally {
      jest.useRealTimers();
    }
  });

  it('halves the organization team page after repeated GraphQL 502 responses', async () => {
    jest.useFakeTimers();
    mockGraphql
      .mockRejectedValueOnce(httpError(502))
      .mockRejectedValueOnce(httpError(502))
      .mockRejectedValueOnce(httpError(502))
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit: {
          cost: 1,
          remaining: 12_000,
          resetAt: '2026-10-22T00:00:00Z',
        },
      });

    try {
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
      const membershipsPromise = client.fetchOrganizationTeamMemberships();
      await jest.runAllTimersAsync();
      await expect(membershipsPromise).resolves.toEqual([]);

      expect(mockGraphql.mock.calls.map(([, variables]) => variables)).toEqual([
        { organization: 'my-org', cursor: null, teamPageSize: 25 },
        { organization: 'my-org', cursor: null, teamPageSize: 25 },
        { organization: 'my-org', cursor: null, teamPageSize: 25 },
        { organization: 'my-org', cursor: null, teamPageSize: 12 },
      ]);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining(
          'retrying the same cursor with a smaller page size',
        ),
      );
    } finally {
      jest.useRealTimers();
    }
  });

  it('rejects organization App installations without members read permission', async () => {
    const { getGithubCredentials } = jest.requireMock('../utils/GithubUtils');
    getGithubCredentials.mockResolvedValueOnce({
      enterprise: 'enterprise-pat',
      organization: { appId: 123, privateKey: 'private-key' },
    });
    mockGetOrgInstallation.mockResolvedValueOnce({
      data: { id: 999, permissions: { members: 'none' } },
    });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(client.fetchOrganizationTeamMemberships()).rejects.toThrow(
      'The GitHub App needs Members: read or higher permission',
    );
    expect(mockGraphql).not.toHaveBeenCalled();
  });

  it.each(['read', 'write', 'admin'])(
    'accepts organization App Members: %s permission',
    async permission => {
      const { getGithubCredentials } = jest.requireMock('../utils/GithubUtils');
      getGithubCredentials.mockResolvedValueOnce({
        enterprise: 'enterprise-pat',
        organization: { appId: 123, privateKey: 'private-key' },
      });
      mockGetOrgInstallation.mockResolvedValueOnce({
        data: { id: 999, permissions: { members: permission } },
      });
      mockGraphql.mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit: {
          cost: 1,
          remaining: 12_000,
          resetAt: '2026-10-22T00:00:00Z',
        },
      });
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

      await expect(client.fetchOrganizationTeamMemberships()).resolves.toEqual(
        [],
      );
      expect(mockGraphql).toHaveBeenCalledTimes(1);
    },
  );

  it('rejects an incomplete GraphQL membership snapshot', async () => {
    mockGraphql
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [
              {
                id: 'team-node-1',
                databaseId: 101,
                slug: 'parent-team',
                members: {
                  nodes: [],
                  pageInfo: { hasNextPage: false, endCursor: null },
                },
              },
            ],
            pageInfo: { hasNextPage: true, endCursor: 'team-cursor' },
          },
        },
        rateLimit: {
          cost: 1,
          remaining: 12_000,
          resetAt: '2026-10-22T00:00:00Z',
        },
      })
      .mockRejectedValueOnce(
        Object.assign(new Error('GraphQL failed'), {
          status: 403,
        }),
      );
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    await expect(client.fetchOrganizationTeamMemberships()).rejects.toThrow(
      'GitHub GraphQL team membership request failed with HTTP 403.',
    );
  });

  it('backs off before continuing GraphQL pagination when rate limit remaining is low', async () => {
    jest.useFakeTimers().setSystemTime(new Date('2026-10-22T00:00:00Z'));
    const rateLimit = {
      cost: 1,
      remaining: 99,
      resetAt: '2026-10-22T00:00:01Z',
    };
    mockGraphql
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: true, endCursor: 'teams-next' },
          },
        },
        rateLimit,
      })
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit: { ...rateLimit, remaining: 12_000 },
      });

    try {
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
      const membershipsPromise = client.fetchOrganizationTeamMemberships();
      await jest.runOnlyPendingTimersAsync();
      await expect(membershipsPromise).resolves.toEqual([]);

      expect(mockGraphql).toHaveBeenCalledTimes(2);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('GraphQL rate limit is low'),
      );
    } finally {
      jest.useRealTimers();
    }
  });

  it('retries GraphQL RATE_LIMITED errors with bounded exponential backoff', async () => {
    jest.useFakeTimers();
    mockGraphql
      .mockRejectedValueOnce(
        Object.assign(new Error('GraphQL request failed'), {
          errors: [{ type: 'RATE_LIMITED', message: 'Rate limited' }],
        }),
      )
      .mockResolvedValueOnce({
        organization: {
          teams: {
            nodes: [],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        },
        rateLimit: {
          cost: 1,
          remaining: 12_000,
          resetAt: '2026-10-22T00:00:00Z',
        },
      });

    try {
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
      const membershipsPromise = client.fetchOrganizationTeamMemberships();
      const membershipsResult = membershipsPromise.then(result => {
        expect(result).toEqual([]);
      });
      await jest.runAllTimersAsync();
      await membershipsResult;
      expect(mockGraphql).toHaveBeenCalledTimes(2);
      expect(mockLogger.warn).toHaveBeenCalledWith(
        expect.stringContaining('rate limit reached; retrying'),
      );
    } finally {
      jest.useRealTimers();
    }
  });

  it.each([403, 429])(
    'retries HTTP %s when GitHub provides Retry-After',
    async status => {
      jest.useFakeTimers();
      mockGraphql
        .mockRejectedValueOnce(
          Object.assign(new Error('GitHub rate limit response'), {
            status,
            headers: { 'retry-after': '1' },
          }),
        )
        .mockResolvedValueOnce({
          organization: {
            teams: {
              nodes: [],
              pageInfo: { hasNextPage: false, endCursor: null },
            },
          },
          rateLimit: {
            cost: 1,
            remaining: 12_000,
            resetAt: '2026-10-22T00:00:00Z',
          },
        });

      try {
        const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
        const membershipsPromise = client.fetchOrganizationTeamMemberships();
        const membershipsResult = membershipsPromise.then(result => {
          expect(result).toEqual([]);
        });
        await jest.advanceTimersByTimeAsync(1_000);
        await membershipsResult;
        expect(mockGraphql).toHaveBeenCalledTimes(2);
      } finally {
        jest.useRealTimers();
      }
    },
  );

  it('stops retrying a persistent GraphQL rate-limit error after five attempts', async () => {
    jest.useFakeTimers();
    mockGraphql.mockRejectedValue(
      Object.assign(new Error('GraphQL request failed'), {
        errors: [{ type: 'RATE_LIMITED' }],
      }),
    );

    try {
      const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
      const membershipsPromise = client.fetchOrganizationTeamMemberships();
      const membershipsResult = membershipsPromise.then(
        () => {
          throw new Error('Expected the team membership request to reject.');
        },
        error => {
          expect(error.message).toContain(
            'GitHub GraphQL team membership request failed.',
          );
        },
      );
      await jest.runAllTimersAsync();
      await membershipsResult;
      expect(mockGraphql).toHaveBeenCalledTimes(5);
    } finally {
      jest.useRealTimers();
    }
  });

  it('discovers enterprise GitHub App installations with the App-JWT endpoint', async () => {
    const { getGithubCredentials } = jest.requireMock('../utils/GithubUtils');
    getGithubCredentials.mockResolvedValueOnce({
      enterprise: { appId: 123, privateKey: 'private-key' },
    });
    mockRequest
      .mockResolvedValueOnce({ data: { id: 174085427 } })
      .mockResolvedValueOnce({ data: { download_links: [] } });

    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    await client.fetchEnterpriseReportLinks('2026-01-01');

    expect(mockRequest).toHaveBeenNthCalledWith(
      1,
      'GET /enterprises/my-enterprise/installation',
    );
    expect(mockGetOrgInstallation).not.toHaveBeenCalled();
    expect(mockOutgoingHeaders[0]).toEqual({
      accept: 'application/vnd.github+json',
      'x-github-api-version': '2026-03-10',
      authorization: 'Bearer fake-app-jwt',
    });

    const MockedOctokit = Octokit as unknown as jest.Mock;
    expect(MockedOctokit.mock.calls[1][0].auth).toEqual({
      appId: 123,
      privateKey: 'private-key',
      installationId: 174085427,
    });
  });

  it('falls back to the org-installation endpoint when enterprise lookup returns 404', async () => {
    const { getGithubCredentials } = jest.requireMock('../utils/GithubUtils');
    getGithubCredentials.mockResolvedValueOnce({
      enterprise: { appId: 123, privateKey: 'private-key' },
    });
    mockRequest
      .mockRejectedValueOnce(
        Object.assign(new Error('Not found'), { status: 404 }),
      )
      .mockResolvedValueOnce({ data: { download_links: [] } });
    mockGetOrgInstallation.mockResolvedValueOnce({ data: { id: 321 } });

    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);
    await client.fetchEnterpriseReportLinks('2026-01-01');

    expect(mockGetOrgInstallation).toHaveBeenCalledWith({
      org: 'my-enterprise',
    });
    const MockedOctokit = Octokit as unknown as jest.Mock;
    expect(MockedOctokit.mock.calls[1][0].auth.installationId).toBe(321);
  });

  it('fetchOrganizationReportLinks calls correct org path', async () => {
    mockRequest.mockResolvedValueOnce({
      data: {
        download_links: ['https://raw.githubusercontent.com/report.json?sig=1'],
      },
    });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    const res = await client.fetchOrganizationReportLinks('2026-01-02');

    expect(mockRequest).toHaveBeenCalledWith(
      `GET /orgs/my-org/copilot/metrics/reports/organization-1-day?day=2026-01-02`,
    );
    expect(res.download_links[0]).toContain('raw.githubusercontent.com');
  });

  it('fetchEnterpriseUserReportLinks calls correct users path', async () => {
    mockRequest.mockResolvedValueOnce({
      data: {
        download_links: ['https://raw.githubusercontent.com/users.json?sig=1'],
      },
    });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    const res = await client.fetchEnterpriseUserReportLinks('2026-01-03');

    expect(mockRequest).toHaveBeenCalledWith(
      `GET /enterprises/my-enterprise/copilot/metrics/reports/users-1-day?day=2026-01-03`,
    );
    expect(res.download_links[0]).toContain('raw.githubusercontent.com');
  });

  it('fetchEnterpriseUserTeamsLinks calls correct user-teams path', async () => {
    mockRequest.mockResolvedValueOnce({
      data: {
        download_links: [
          'https://raw.githubusercontent.com/user-teams.json?sig=1',
        ],
      },
    });
    const client = await GithubClientV2.fromConfig(mockConfig, mockLogger);

    const res = await client.fetchEnterpriseUserTeamsLinks('2026-01-04');

    expect(mockRequest).toHaveBeenCalledWith(
      `GET /enterprises/my-enterprise/copilot/metrics/reports/user-teams-1-day?day=2026-01-04`,
    );
    expect(res.download_links[0]).toContain('raw.githubusercontent.com');
  });
});
