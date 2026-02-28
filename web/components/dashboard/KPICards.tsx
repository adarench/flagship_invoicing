import { Card } from '@/components/ui/Card'
import { formatCurrency, formatPercent } from '@/lib/utils'
import type { Summary } from '@/lib/types'
import { CheckCircle2, XCircle, DollarSign, TrendingUp } from 'lucide-react'

interface KPICardsProps {
  summary: Summary
}

export function KPICards({ summary }: KPICardsProps) {
  const kpis = [
    {
      label: 'Match Rate',
      value: formatPercent(summary.match_rate),
      icon: TrendingUp,
      color: 'text-primary',
      bg: 'bg-primary/10',
    },
    {
      label: 'Matched Records',
      value: `${summary.matched} / ${summary.total_pid_records}`,
      icon: CheckCircle2,
      color: 'text-success',
      bg: 'bg-green-50',
    },
    {
      label: 'Unmatched Records',
      value: String(summary.unmatched),
      icon: XCircle,
      color: 'text-danger',
      bg: 'bg-red-50',
    },
    {
      label: 'Total PID Amount',
      value: formatCurrency(summary.total_pid_amount),
      icon: DollarSign,
      color: 'text-gray-700',
      bg: 'bg-gray-100',
    },
  ]

  return (
    <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
      {kpis.map(({ label, value, icon: Icon, color, bg }) => (
        <Card key={label} padding="md">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium uppercase tracking-wide text-gray-400">{label}</p>
              <p className={`mt-1.5 text-2xl font-bold ${color}`}>{value}</p>
            </div>
            <span className={`rounded-lg p-2 ${bg}`}>
              <Icon className={`h-5 w-5 ${color}`} />
            </span>
          </div>
        </Card>
      ))}
    </div>
  )
}
