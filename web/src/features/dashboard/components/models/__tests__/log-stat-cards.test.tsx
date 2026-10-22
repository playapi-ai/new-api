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
import { render, screen, waitFor, within } from '@testing-library/react'
import { expect, it, vi } from 'vitest'

import { getUserQuotaDates } from '@/features/dashboard/api'

import { LogStatCards } from '../log-stat-cards'

vi.mock('@/features/dashboard/api', () => ({ getUserQuotaDates: vi.fn() }))

it('shows cache totals in accessible cards and retains full large values', async () => {
  vi.mocked(getUserQuotaDates).mockResolvedValue({
    success: true,
    data: [
      {
        created_at: 3600,
        token_used: 100,
        cache_read_tokens: 123456789,
        cache_creation_tokens: 45,
      },
    ],
  })
  render(<LogStatCards />)
  const cacheRead = screen.getByRole('group', { name: 'Cache Read' })
  const cacheWrite = screen.getByRole('group', { name: 'Cache Write' })
  await waitFor(() => expect(cacheRead).toHaveAttribute('aria-busy', 'false'))
  expect(within(cacheRead).getByTitle('123,456,789')).toBeVisible()
  expect(within(cacheWrite).getByText('45')).toBeVisible()
  expect(screen.getAllByRole('group')).toHaveLength(7)
  expect(cacheRead.parentElement).toHaveClass(
    'grid-cols-2',
    'sm:grid-cols-3',
    'lg:grid-cols-7'
  )
})

it('announces loading for both cache cards while the request is pending', () => {
  vi.mocked(getUserQuotaDates).mockReturnValue(new Promise(() => {}))
  render(<LogStatCards />)
  for (const name of ['Cache Read', 'Cache Write']) {
    expect(screen.getByRole('group', { name })).toHaveAttribute(
      'aria-busy',
      'true'
    )
  }
})

it('shows unavailable cache values when the dashboard request fails', async () => {
  vi.mocked(getUserQuotaDates).mockRejectedValue(new Error('request failed'))
  render(<LogStatCards />)
  for (const name of ['Cache Read', 'Cache Write']) {
    const card = screen.getByRole('group', { name })
    expect(await within(card).findByText('--')).toBeVisible()
    expect(card).toHaveAttribute('aria-busy', 'false')
  }
})
