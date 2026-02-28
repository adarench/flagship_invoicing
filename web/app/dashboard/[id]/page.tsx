'use client'
import { useParams } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { KPICards } from '@/components/dashboard/KPICards'
import { MatchBreakdownChart } from '@/components/dashboard/MatchBreakdownChart'
import { CoverageMatrix } from '@/components/dashboard/CoverageMatrix'
import { MissingDocumentsPanel } from '@/components/dashboard/MissingDocumentsPanel'
import { TopUnmatchedVendors } from '@/components/dashboard/TopUnmatchedVendors'
import { SkeletonCard, SkeletonTable } from '@/components/ui/Skeleton'
import { useJobSummary, useJobCoverage } from '@/hooks/useJob'
import { useReviewQueue } from '@/hooks/useReviewQueue'
import Link from 'next/link'
import Button from '@/components/ui/Button'

export default function DashboardPage() {
  const params = useParams<{ id: string }>()
  const jobId = params.id

  const { data: summary, isLoading: sLoading, error: sError } = useJobSummary(jobId)
  const { data: coverage, isLoading: cLoading } = useJobCoverage(jobId)
  const { data: queue } = useReviewQueue(jobId)

  if (sError) {
    return (
      <AppShell jobId={jobId}>
        <div className="rounded-xl bg-red-50 p-6 text-red-700">
          <p className="font-medium">Failed to load summary</p>
          <p className="mt-1 text-sm">{String(sError)}</p>
        </div>
      </AppShell>
    )
  }

  return (
    <AppShell jobId={jobId}>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Reconciliation Dashboard</h1>
          <p className="mt-0.5 font-mono text-xs text-gray-400">{jobId}</p>
        </div>
        <div className="flex gap-2">
          <Link href={`/review/${jobId}`}>
            <Button variant="secondary" size="sm">Review Queue</Button>
          </Link>
          <Link href={`/exports/${jobId}`}>
            <Button variant="secondary" size="sm">Exports</Button>
          </Link>
        </div>
      </div>

      <div className="space-y-6">
        {sLoading ? (
          <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
            {[1,2,3,4].map(i => <SkeletonCard key={i} />)}
          </div>
        ) : summary ? (
          <>
            <KPICards summary={summary} />
            {summary.banks_loaded.length < 4 && (
              <MissingDocumentsPanel summary={summary} />
            )}
          </>
        ) : null}

        <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
          {sLoading ? <SkeletonCard /> : summary ? (
            <MatchBreakdownChart breakdown={summary.breakdown} />
          ) : null}

          {queue && (
            <div className="lg:col-span-2">
              <TopUnmatchedVendors reviewQueue={queue} />
            </div>
          )}
        </div>

        {cLoading ? (
          <SkeletonTable rows={3} />
        ) : coverage ? (
          <CoverageMatrix coverage={coverage} />
        ) : null}
      </div>
    </AppShell>
  )
}
