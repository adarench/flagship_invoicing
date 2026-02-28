'use client'
import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { DataTable, type Column } from '@/components/ui/DataTable'
import { Badge } from '@/components/ui/Badge'
import { SkeletonTable } from '@/components/ui/Skeleton'
import Button from '@/components/ui/Button'
import { useJobHistory } from '@/hooks/useJob'
import { relativeTime } from '@/lib/utils'
import type { JobHistory } from '@/lib/types'
import { Plus } from 'lucide-react'

const stateVariant = (s: string) =>
  s === 'completed' ? 'success' :
  s === 'error'     ? 'danger' :
  s === 'running'   ? 'info' : 'gray'

export default function HistoryPage() {
  const router = useRouter()
  const { data: jobs, isLoading, error } = useJobHistory()

  const columns: Column<JobHistory>[] = [
    {
      key: 'job_id',
      header: 'Job ID',
      render: row => (
        <span className="font-mono text-xs text-gray-600">{row.job_id.slice(0, 8)}…</span>
      ),
    },
    {
      key: 'state',
      header: 'Status',
      sortable: true,
      render: row => (
        <Badge variant={stateVariant(row.state)}>
          {row.state}
        </Badge>
      ),
    },
    {
      key: 'created_at',
      header: 'Started',
      sortable: true,
      render: row => relativeTime(row.created_at),
    },
    {
      key: 'updated_at',
      header: 'Last Updated',
      render: row => relativeTime(row.updated_at),
    },
    {
      key: 'error_message',
      header: 'Error',
      render: row => row.error_message
        ? <span className="text-xs text-red-600">{String(row.error_message).slice(0, 60)}</span>
        : null,
    },
  ]

  return (
    <AppShell>
      <div className="mb-6 flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Job History</h1>
        <Link href="/upload">
          <Button size="sm">
            <Plus className="h-4 w-4" /> New Job
          </Button>
        </Link>
      </div>

      {error ? (
        <div className="rounded-xl bg-red-50 p-4 text-sm text-red-700">{String(error)}</div>
      ) : isLoading ? (
        <SkeletonTable rows={5} />
      ) : (
        <DataTable
          columns={columns}
          data={jobs ?? []}
          onRowClick={row => {
            if (row.state === 'completed') router.push(`/dashboard/${row.job_id}`)
            else if (row.state === 'running' || row.state === 'pending') router.push(`/job/${row.job_id}/progress`)
          }}
          emptyMessage="No reconciliation jobs yet. Start one from the Upload page."
        />
      )}
    </AppShell>
  )
}
