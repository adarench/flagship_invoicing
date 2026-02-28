'use client'
import { Search } from 'lucide-react'
import type { MatchType, ReviewStatus } from '@/lib/types'

interface FilterBarProps {
  search: string
  onSearch: (v: string) => void
  matchType: string
  onMatchType: (v: string) => void
  status: string
  onStatus: (v: string) => void
}

const MATCH_TYPES = ['all', 'secondary', 'retention', 'fuzzy']
const STATUSES: ReviewStatus[] = ['needs_review', 'approved', 'rejected']

export function FilterBar({ search, onSearch, matchType, onMatchType, status, onStatus }: FilterBarProps) {
  return (
    <div className="flex flex-wrap items-center gap-3">
      {/* Search */}
      <div className="relative min-w-[200px]">
        <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
        <input
          type="text"
          value={search}
          onChange={e => onSearch(e.target.value)}
          placeholder="Search vendor, invoice…"
          className="w-full rounded-md border border-gray-300 py-2 pl-9 pr-3 text-sm focus:border-primary focus:outline-none"
        />
      </div>

      {/* Match type filter */}
      <select
        value={matchType}
        onChange={e => onMatchType(e.target.value)}
        className="rounded-md border border-gray-300 py-2 pl-3 pr-7 text-sm focus:border-primary focus:outline-none"
      >
        {MATCH_TYPES.map(t => (
          <option key={t} value={t}>
            {t === 'all' ? 'All Types' : t.charAt(0).toUpperCase() + t.slice(1)}
          </option>
        ))}
      </select>

      {/* Status filter */}
      <select
        value={status}
        onChange={e => onStatus(e.target.value)}
        className="rounded-md border border-gray-300 py-2 pl-3 pr-7 text-sm focus:border-primary focus:outline-none"
      >
        <option value="all">All Statuses</option>
        {STATUSES.map(s => (
          <option key={s} value={s}>
            {s.replace('_', ' ').replace(/\b\w/g, c => c.toUpperCase())}
          </option>
        ))}
      </select>
    </div>
  )
}
