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

import { resolveCostCenterMemberships } from './costCenterMembership';

describe('resolveCostCenterMemberships', () => {
  it('applies User > oldest Team > Org precedence and ignores other resources', () => {
    const memberships = resolveCostCenterMemberships([
      {
        id: 'direct',
        name: 'Direct',
        resources: [
          {
            type: 'User',
            name: 'ALICE',
            members: [{ id: 1, login: 'Alice' }],
          },
          {
            type: 'Repo',
            name: 'ignored-repo',
            members: [{ id: 4, login: 'dave' }],
          },
          {
            type: 'FutureResource',
            name: 'ignored-unknown',
            members: [{ id: 5, login: 'erin' }],
          },
        ],
      },
      {
        id: 'newer-team',
        name: 'Newer team',
        resources: [
          {
            type: 'Team',
            name: 'ent:newer',
            teamId: 1,
            teamCreatedAt: '2024-01-01T00:00:00Z',
            members: [
              { id: 1, login: 'Alice' },
              { id: 2, login: 'Bob' },
            ],
          },
        ],
      },
      {
        id: 'oldest-team-high-id',
        name: 'Oldest team with higher ID',
        resources: [
          {
            type: 'Team',
            name: 'ent:older-high-id',
            teamId: 50,
            teamCreatedAt: '2022-01-01T00:00:00Z',
            members: [{ id: 2, login: 'Bob' }],
          },
        ],
      },
      {
        id: 'oldest-team-low-id',
        name: 'Oldest team with lower ID',
        resources: [
          {
            type: 'Team',
            name: 'ent:older-low-id',
            teamId: 20,
            teamCreatedAt: '2022-01-01T00:00:00Z',
            members: [{ id: 2, login: 'Bob' }],
          },
        ],
      },
      {
        id: 'zeta-org',
        name: 'Zeta organization',
        resources: [
          {
            type: 'Org',
            name: 'Zeta-Org',
            members: [
              { id: 1, login: 'Alice' },
              { id: 3, login: 'Carol' },
            ],
          },
        ],
      },
      {
        id: 'alpha-org',
        name: 'Alpha organization',
        resources: [
          {
            type: 'Org',
            name: 'alpha-org',
            members: [{ id: 3, login: 'Carol' }],
          },
        ],
      },
    ]);

    expect(memberships).toEqual(
      new Map([
        [1, { costCenterId: 'direct', login: 'Alice' }],
        [2, { costCenterId: 'oldest-team-low-id', login: 'Bob' }],
        [3, { costCenterId: 'alpha-org', login: 'Carol' }],
      ]),
    );
  });

  it('uses the lowest numeric team ID when creation timestamps are unavailable', () => {
    const memberships = resolveCostCenterMemberships([
      {
        id: 'higher-id',
        name: 'Higher ID',
        resources: [
          {
            type: 'Team',
            name: 'ent:higher',
            teamId: 12,
            members: [{ id: 10, login: 'frank' }],
          },
        ],
      },
      {
        id: 'lower-id',
        name: 'Lower ID',
        resources: [
          {
            type: 'Team',
            name: 'ent:lower',
            teamId: 4,
            members: [{ id: 10, login: 'frank' }],
          },
        ],
      },
    ]);

    expect(memberships.get(10)).toEqual({
      costCenterId: 'lower-id',
      login: 'frank',
    });
  });
});
