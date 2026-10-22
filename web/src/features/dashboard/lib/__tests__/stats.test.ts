/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { describe, expect, it } from 'vitest'

import { calculateDashboardStats } from '../stats'

describe('dashboard cache usage', () => {
  it('sums cache reads and writes separately from total tokens', () => {
    expect(
      calculateDashboardStats([
        {
          created_at: 1,
          quota: 10,
          count: 1,
          token_used: 100,
          cache_read_tokens: 30,
          cache_creation_tokens: 20,
        },
        {
          created_at: 2,
          quota: 20,
          count: 2,
          token_used: 200,
          cache_read_tokens: 60,
          cache_creation_tokens: 40,
        },
      ])
    ).toEqual({
      totalQuota: 30,
      totalCount: 3,
      totalTokens: 300,
      totalCacheReadTokens: 90,
      totalCacheCreationTokens: 60,
    })
  })

  it('keeps older rows without cache fields valid', () => {
    expect(
      calculateDashboardStats([{ created_at: 1, token_used: 100 }])
    ).toEqual({
      totalQuota: 0,
      totalCount: 0,
      totalTokens: 100,
      totalCacheReadTokens: 0,
      totalCacheCreationTokens: 0,
    })
    expect(calculateDashboardStats([]).totalCacheReadTokens).toBe(0)
  })

  it('ignores negative and non-finite upstream cache totals', () => {
    const result = calculateDashboardStats([
      { created_at: 1, cache_read_tokens: -5, cache_creation_tokens: Infinity },
      {
        created_at: 2,
        cache_read_tokens: Number.NaN,
        cache_creation_tokens: -2,
      },
    ])
    expect(result.totalCacheReadTokens).toBe(0)
    expect(result.totalCacheCreationTokens).toBe(0)
  })
})
