'use client'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { Download } from 'lucide-react'
import { AppShell } from '@/components/layout/AppShell'
import { PacketList } from '@/components/packets/PacketList'
import { SkeletonTable } from '@/components/ui/Skeleton'
import Button from '@/components/ui/Button'
import { packetsZipUrl } from '@/lib/api'
import { useReviewQueue } from '@/hooks/useReviewQueue'

export default function PacketsPage() {
  const params = useParams<{ id: string }>()
  const jobId = params.id

  const { data: queue, isLoading, error } = useReviewQueue(jobId)

  // Show all non-unmatched items (approved + needs_review)
  const items = queue?.items.filter(i => i.match_type !== 'unmatched') ?? []

  return (
    <AppShell jobId={jobId}>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Audit Packets</h1>
          <p className="mt-0.5 text-sm text-gray-500">
            Generate per-match PDF audit packets for your records.
          </p>
        </div>
        {items.length > 0 && (
          <a href={packetsZipUrl(jobId)} download>
            <Button variant="secondary" size="sm">
              <Download className="h-4 w-4" /> Download All (.zip)
            </Button>
          </a>
        )}
      </div>

      {error ? (
        <div className="rounded-xl bg-red-50 p-4 text-sm text-red-700">{String(error)}</div>
      ) : isLoading ? (
        <SkeletonTable rows={4} />
      ) : (
        <PacketList items={items} jobId={jobId} />
      )}
    </AppShell>
  )
}
