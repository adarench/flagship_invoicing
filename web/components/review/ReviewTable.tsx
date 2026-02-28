'use client'
import { useRouter } from 'next/navigation'
import { DataTable, type Column } from '@/components/ui/DataTable'
import { MatchTypeBadge } from '@/components/ui/MatchTypeBadge'
import { ConfidenceBadge } from '@/components/ui/ConfidenceBadge'
import { Badge } from '@/components/ui/Badge'
import { formatCurrency, formatDate, truncate } from '@/lib/utils'
import type { ReviewItem } from '@/lib/types'

interface ReviewTableProps {
  items: ReviewItem[]
  jobId: string
}

const statusVariant = (s: string) =>
  s === 'approved' ? 'success' : s === 'rejected' ? 'danger' : 'warning'

export function ReviewTable({ items, jobId }: ReviewTableProps) {
  const router = useRouter()

  const columns: Column<ReviewItem>[] = [
    {
      key: 'vendor',
      header: 'Vendor',
      sortable: true,
      render: row => <span className="font-medium text-gray-900">{truncate(row.vendor, 30)}</span>,
    },
    { key: 'invoice_no', header: 'Invoice #', sortable: true },
    {
      key: 'pid_amount',
      header: 'PID Amount',
      sortable: true,
      render: row => formatCurrency(row.pid_amount),
    },
    {
      key: 'bank_amount',
      header: 'Bank Amount',
      render: row => row.bank_amount != null ? formatCurrency(row.bank_amount) : '—',
    },
    {
      key: 'match_type',
      header: 'Type',
      sortable: true,
      render: row => <MatchTypeBadge type={row.match_type} />,
    },
    {
      key: 'match_confidence',
      header: 'Confidence',
      sortable: true,
      render: row => <ConfidenceBadge confidence={row.match_confidence} />,
    },
    {
      key: 'check_date',
      header: 'Check Date',
      render: row => formatDate(row.check_date),
    },
    {
      key: 'status',
      header: 'Status',
      sortable: true,
      render: row => (
        <Badge variant={statusVariant(row.status)}>
          {row.status.replace('_', ' ')}
        </Badge>
      ),
    },
  ]

  return (
    <DataTable
      columns={columns}
      data={items}
      onRowClick={row => router.push(`/review/${jobId}/detail/${encodeURIComponent(row.match_id)}`)}
      emptyMessage="No items match the current filters"
    />
  )
}
