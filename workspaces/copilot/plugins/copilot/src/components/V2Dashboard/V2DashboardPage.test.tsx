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

import { ConfigReader } from '@backstage/core-app-api';
import { configApiRef } from '@backstage/core-plugin-api';
import { renderInTestApp, TestApiProvider } from '@backstage/test-utils';
import { act, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import {
  NO_COST_CENTER_ID,
  V2DashboardData,
  V2CostCenterOption,
} from '@backstage-community/plugin-copilot-common';
import { CopilotApiV2, copilotApiV2Ref } from '../../api';
import { V2DashboardPage } from './V2DashboardPage';

jest.mock('./charts', () => {
  const Noop = () => null;
  return {
    IDEActiveUsersChart: Noop,
    AvgChatRequestsChart: Noop,
    RequestsByChatModeChart: Noop,
    CodeCompletionsChart: Noop,
    CodeCompletionsAcceptanceChart: Noop,
    ModelUsagePerDayChart: Noop,
    ChatModelUsageDonut: Noop,
    ModelUsagePerChatModeChart: Noop,
    LanguageUsagePerDayChart: Noop,
    LanguageUsageDonut: Noop,
    ModelUsagePerLanguageChart: Noop,
    DailyLOCChart: Noop,
    UserLOCByFeatureChart: Noop,
    AgentLOCByFeatureChart: Noop,
    UserLOCByModelChart: Noop,
    AgentLOCByModelChart: Noop,
    UserLOCByLanguageChart: Noop,
    AgentLOCByLanguageChart: Noop,
    AiCreditsConsumptionChart: Noop,
  };
});

jest.mock('./CopilotUsageSummary', () => ({
  CopilotUsageSummary: () => null,
}));

jest.mock('./CodeGenerationSummary', () => ({
  CodeGenerationSummary: () => null,
}));

jest.mock('./ConsumptionSummary', () => ({
  ConsumptionSummary: () => null,
}));

const EMPTY_DASHBOARD: V2DashboardData = {
  daily: [],
  byFeature: [],
  byLanguage: [],
  byModelFeature: [],
  byLanguageModel: [],
  prMetrics: [],
};

const createDeferred = <T,>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
};

const createApi = ({
  costCenters = [],
  teams = ['platform', 'product'],
}: {
  costCenters?: V2CostCenterOption[];
  teams?: string[];
} = {}): jest.Mocked<CopilotApiV2> => ({
  getDailyMetrics: jest.fn().mockResolvedValue([]),
  getPrMetrics: jest.fn().mockResolvedValue([]),
  getByFeature: jest.fn().mockResolvedValue([]),
  getByIde: jest.fn().mockResolvedValue([]),
  getLanguageByFeature: jest.fn().mockResolvedValue([]),
  fetchCostCenters: jest.fn().mockResolvedValue(costCenters),
  getTeams: jest.fn().mockResolvedValue(teams),
  getPeriodRange: jest.fn().mockResolvedValue(null),
  getBackfillStatus: jest.fn().mockResolvedValue([]),
  getByModelFeature: jest.fn().mockResolvedValue([]),
  getByLanguageModel: jest.fn().mockResolvedValue([]),
  getDashboardData: jest.fn().mockResolvedValue(EMPTY_DASHBOARD),
});

const createDashboard = (
  api: jest.Mocked<CopilotApiV2>,
  scope: {
    enterprise?: string;
    organization?: string;
    defaultView?: 'enterprise' | 'organization';
  } = { enterprise: 'enterprise-1' },
) => {
  const copilotConfig: Record<string, string> = {};
  if (scope.enterprise) {
    copilotConfig.enterprise = scope.enterprise;
  }
  if (scope.organization) {
    copilotConfig.organization = scope.organization;
  }
  if (scope.defaultView) {
    copilotConfig.defaultView = scope.defaultView;
  }
  return (
    <TestApiProvider
      apis={[
        [configApiRef, new ConfigReader({ copilot: copilotConfig })],
        [copilotApiV2Ref, api],
      ]}
    >
      <V2DashboardPage />
    </TestApiProvider>
  );
};

const renderDashboard = async (
  api: jest.Mocked<CopilotApiV2>,
  scope: {
    enterprise?: string;
    organization?: string;
    defaultView?: 'enterprise' | 'organization';
  } = { enterprise: 'enterprise-1' },
) => renderInTestApp(createDashboard(api, scope));

describe('V2DashboardPage cost center filter', () => {
  it('hides the selector when no cost centers are available', async () => {
    const api = createApi();
    await renderDashboard(api);

    await waitFor(() => expect(api.fetchCostCenters).toHaveBeenCalled());
    expect(
      screen.queryByRole('button', { name: /Cost center/ }),
    ).not.toBeInTheDocument();
  });

  it('hides the selector for organization-only configuration', async () => {
    const api = createApi({
      costCenters: [{ id: 'cc-platform', name: 'Platform' }],
    });
    await renderDashboard(api, { organization: 'org-1' });

    await waitFor(() => expect(api.fetchCostCenters).toHaveBeenCalled());
    expect(
      screen.queryByRole('button', { name: /Cost center/ }),
    ).not.toBeInTheDocument();
    expect(api.fetchCostCenters).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'organization', entityId: 'org-1' }),
    );
  });

  it('filters organization teams and metrics by cost center while All stays unfiltered', async () => {
    const api = createApi({
      costCenters: [{ id: 'cc-platform', name: 'Platform' }],
    });
    api.getTeams.mockImplementation(({ costCenter }) =>
      Promise.resolve(costCenter ? ['org-team'] : ['org-team', 'other-team']),
    );
    const user = userEvent.setup();
    await renderDashboard(api, {
      enterprise: 'enterprise-1',
      organization: 'org-1',
      defaultView: 'organization',
    });

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Platform' }));

    await waitFor(() =>
      expect(api.fetchCostCenters).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
        }),
      ),
    );
    await waitFor(() =>
      expect(api.getTeams).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          costCenter: 'cc-platform',
        }),
      ),
    );
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenLastCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          team: undefined,
          costCenter: 'cc-platform',
        }),
      ),
    );

    await user.click(await screen.findByRole('button', { name: /Team/ }));
    await user.click(await screen.findByRole('option', { name: 'org-team' }));
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenLastCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          team: 'org-team',
          costCenter: 'cc-platform',
        }),
      ),
    );

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(
      await screen.findByRole('option', { name: 'All cost centers' }),
    );
    await waitFor(() =>
      expect(api.getTeams).toHaveBeenLastCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          costCenter: undefined,
        }),
      ),
    );
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenLastCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          team: undefined,
          costCenter: undefined,
        }),
      ),
    );
  });

  it('orders cost center options by name and places no cost center last', async () => {
    const api = createApi({
      costCenters: [
        { id: 'cc-product', name: 'Product' },
        { id: NO_COST_CENTER_ID, name: null },
        { id: 'cc-platform', name: 'Platform' },
      ],
    });
    const user = userEvent.setup();
    await renderDashboard(api);

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    const options = await screen.findAllByRole('option');

    expect(options.map(option => option.textContent)).toEqual([
      'All cost centers',
      'Platform',
      'Product',
      'No cost center',
    ]);
  });

  it('filters teams and metrics by the selected cost center and shows Consumption', async () => {
    const api = createApi({
      costCenters: [{ id: 'cc-platform', name: 'Platform' }],
    });
    const user = userEvent.setup();
    await renderDashboard(api);

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Platform' }));

    await waitFor(() =>
      expect(api.getTeams).toHaveBeenCalledWith(
        expect.objectContaining({ costCenter: 'cc-platform' }),
      ),
    );
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({ costCenter: 'cc-platform' }),
      ),
    );
    expect(
      await screen.findByRole('tab', { name: 'Consumption' }),
    ).toBeVisible();
  });

  it('does not offer old cost center or team options while a new entity loads', async () => {
    const oldCostCenters = [{ id: 'cc-old', name: 'Old center' }];
    const api = createApi({
      costCenters: oldCostCenters,
      teams: ['old-team'],
    });
    const user = userEvent.setup();
    const rendered = await renderDashboard(api, {
      enterprise: 'enterprise-1',
    });

    await user.click(await screen.findByRole('button', { name: /Team/ }));
    await user.click(await screen.findByRole('option', { name: 'old-team' }));
    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Old center' }));
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({
          entityId: 'enterprise-1',
          team: undefined,
          costCenter: 'cc-old',
        }),
      ),
    );

    const pendingCostCenters = createDeferred<V2CostCenterOption[]>();
    const pendingTeams = createDeferred<string[]>();
    api.fetchCostCenters.mockImplementation(({ entityId }) =>
      entityId === 'enterprise-2'
        ? pendingCostCenters.promise
        : Promise.resolve(oldCostCenters),
    );
    api.getTeams.mockImplementation(({ entityId }) =>
      entityId === 'enterprise-2'
        ? pendingTeams.promise
        : Promise.resolve(['old-team']),
    );
    rendered.rerender(createDashboard(api, { enterprise: 'enterprise-2' }));

    await waitFor(() =>
      expect(api.fetchCostCenters).toHaveBeenCalledWith(
        expect.objectContaining({ entityId: 'enterprise-2' }),
      ),
    );
    await waitFor(() =>
      expect(api.getTeams).toHaveBeenCalledWith(
        expect.objectContaining({
          entityId: 'enterprise-2',
          costCenter: undefined,
        }),
      ),
    );
    expect(
      screen.queryByRole('button', { name: /Cost center/ }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: /Team/ }),
    ).not.toBeInTheDocument();
    expect(
      api.getDashboardData.mock.calls.every(
        ([params]) =>
          params.entityId !== 'enterprise-2' ||
          (params.team === undefined && params.costCenter === undefined),
      ),
    ).toBe(true);

    await act(async () => {
      pendingCostCenters.resolve([{ id: 'cc-new', name: 'New center' }]);
      pendingTeams.resolve(['new-team']);
      await Promise.all([pendingCostCenters.promise, pendingTeams.promise]);
    });

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    const newCenterOption = await screen.findByRole('option', {
      name: 'New center',
    });
    expect(
      screen.queryByRole('option', { name: 'Old center' }),
    ).not.toBeInTheDocument();
    await user.click(newCenterOption);
    await user.click(await screen.findByRole('button', { name: /Team/ }));
    expect(
      await screen.findByRole('option', { name: 'new-team' }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('option', { name: 'old-team' }),
    ).not.toBeInTheDocument();
    await user.click(await screen.findByRole('option', { name: 'new-team' }));

    const refreshCostCenters = createDeferred<V2CostCenterOption[]>();
    const refreshTeams = createDeferred<string[]>();
    const refreshApi = createApi();
    refreshApi.fetchCostCenters.mockReturnValue(refreshCostCenters.promise);
    refreshApi.getTeams.mockReturnValue(refreshTeams.promise);
    rendered.rerender(
      createDashboard(refreshApi, { enterprise: 'enterprise-2' }),
    );

    await waitFor(() => {
      expect(
        screen.getByRole('button', { name: /Cost center/ }),
      ).toBeDisabled();
      expect(screen.getByRole('button', { name: /Team/ })).toBeDisabled();
    });

    await act(async () => {
      refreshCostCenters.resolve([{ id: 'cc-new', name: 'New center' }]);
      refreshTeams.resolve(['new-team']);
      await Promise.all([refreshCostCenters.promise, refreshTeams.promise]);
    });
  });

  it('does not carry an enterprise cost center into organization scope', async () => {
    const enterpriseApi = createApi({
      costCenters: [{ id: 'cc-platform', name: 'Platform' }],
    });
    const user = userEvent.setup();
    const rendered = await renderDashboard(enterpriseApi);

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Platform' }));
    await waitFor(() =>
      expect(enterpriseApi.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({ costCenter: 'cc-platform' }),
      ),
    );

    const organizationApi = createApi();
    rendered.rerender(
      createDashboard(organizationApi, {
        enterprise: 'enterprise-1',
        organization: 'org-1',
        defaultView: 'organization',
      }),
    );

    await waitFor(() =>
      expect(organizationApi.getTeams).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          costCenter: undefined,
        }),
      ),
    );
    await waitFor(() =>
      expect(organizationApi.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'organization',
          entityId: 'org-1',
          team: undefined,
          costCenter: undefined,
        }),
      ),
    );
    expect(
      organizationApi.getTeams.mock.calls.every(
        ([params]) =>
          params.type !== 'organization' || params.costCenter === undefined,
      ),
    ).toBe(true);
    expect(
      organizationApi.getDashboardData.mock.calls.every(
        ([params]) =>
          params.type !== 'organization' || params.costCenter === undefined,
      ),
    ).toBe(true);
  });

  it('clears the selected cost center when refreshing the list fails', async () => {
    const initialApi = createApi({
      costCenters: [{ id: 'cc-platform', name: 'Platform' }],
    });
    const user = userEvent.setup();
    const rendered = await renderDashboard(initialApi);

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Platform' }));
    await waitFor(() =>
      expect(initialApi.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({ costCenter: 'cc-platform' }),
      ),
    );

    const refreshApi = createApi();
    refreshApi.fetchCostCenters.mockRejectedValue(new Error('Request failed'));
    rendered.rerender(createDashboard(refreshApi));

    await waitFor(() =>
      expect(refreshApi.getDashboardData).toHaveBeenLastCalledWith(
        expect.objectContaining({ costCenter: undefined }),
      ),
    );
  });

  it('resets a selected team when it is absent from the cost center team list', async () => {
    const api = createApi({
      costCenters: [{ id: 'cc-product', name: 'Product' }],
    });
    api.getTeams.mockImplementation(({ costCenter }) =>
      Promise.resolve(costCenter ? ['product'] : ['platform', 'product']),
    );
    const user = userEvent.setup();
    await renderDashboard(api);

    await user.click(await screen.findByRole('button', { name: /Team/ }));
    await user.click(await screen.findByRole('option', { name: 'platform' }));
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenCalledWith(
        expect.objectContaining({ team: 'platform' }),
      ),
    );

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(await screen.findByRole('option', { name: 'Product' }));

    await waitFor(() =>
      expect(api.getTeams).toHaveBeenCalledWith(
        expect.objectContaining({ costCenter: 'cc-product' }),
      ),
    );
    await waitFor(() =>
      expect(api.getDashboardData).toHaveBeenLastCalledWith(
        expect.objectContaining({
          team: undefined,
          costCenter: 'cc-product',
        }),
      ),
    );
  });

  it('shows Consumption for the no-cost-center option', async () => {
    const api = createApi({
      costCenters: [{ id: NO_COST_CENTER_ID, name: null }],
    });
    const user = userEvent.setup();
    await renderDashboard(api);

    await user.click(
      await screen.findByRole('button', { name: /Cost center/ }),
    );
    await user.click(
      await screen.findByRole('option', { name: 'No cost center' }),
    );

    expect(
      await screen.findByRole('tab', { name: 'Consumption' }),
    ).toBeVisible();
  });
});
