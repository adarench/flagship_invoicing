'use client'
import { useState, useMemo } from 'react'
import { useParams } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { FilterBar } from '@/components/review/FilterBar'
import { ReviewTable } from '@/components/review/ReviewTable'
import { SkeletonTable } from '@/components/ui/Skeleton'
import { useReviewQueue } from '@/hooks/useReviewQueue'

export default function ReviewPage() {
  const params = useParams<{ id: string }>()
  const jobId = params.id

  const { data: queue, isLoading, error } = useReviewQueue(jobId)

  const [search, setSearch] = useState('')
  const [matchType, setMatchType] = useState('all')
  const [status, setStatus] = useState('all')

  const filtered = useMemo(() => {
    if (!queue) return []
    return queue.items.filter(item => {
      const q = search.toLowerCase()
      if (q && !item.vendor.toLowerCase().includes(q) && !item.invoice_no.toLowerCase().includes(q)) return false
      if (matchType !== 'all' && item.match_type !== matchType) return false
      if (status !== 'all' && item.status !== status) return false
      return true
    })
  }, [queue, search, matchType, status])

  return (
    <AppShell jobId={jobId}>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Review Queue</h1>
          {queue && (
            <p className="mt-0.5 text-sm text-gray-500">
              {queue.pending} pending · {queue.approved} approved · {queue.rejected} rejected
            </p>
          )}
        </div>
      </div>

      <div className="mb-4">
        <FilterBar
          search={search}
          onSearch={setSearch}
          matchType={matchType}
          onMatchType={setMatchType}
          status={status}
          onStatus={setStatus}
        />
      </div>

      {error ? (
        <div className="rounded-xl bg-red-50 p-4 text-sm text-red-700">{String(error)}</div>
      ) : isLoading ? (
        <SkeletonTable rows={6} />
      ) : (
        <ReviewTable items={filtered} jobId={jobId} />
      )}
    </AppShell>
  )
}
