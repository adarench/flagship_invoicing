import { Card, CardHeader, CardTitle } from '@/components/ui/Card'
import type { MatchBreakdown } from '@/lib/types'

interface MatchBreakdownChartProps {
  breakdown: MatchBreakdown
}

const COLORS: Record<string, string> = {
  primary:   'bg-primary',
  secondary: 'bg-blue-400',
  retention: 'bg-yellow-400',
  fuzzy:     'bg-orange-400',
  unmatched: 'bg-red-400',
}

const LABELS: Record<string, string> = {
  primary:   'Primary',
  secondary: 'Secondary',
  retention: 'Retention',
  fuzzy:     'Fuzzy',
  unmatched: 'Unmatched',
}

export function MatchBreakdownChart({ breakdown }: MatchBreakdownChartProps) {
  const entries = Object.entries(breakdown) as [keyof MatchBreakdown, number][]
  const total = entries.reduce((s, [, v]) => s + v, 0)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Match Breakdown</CardTitle>
      </CardHeader>

      {/* Stacked bar */}
      <div className="flex h-5 w-full overflow-hidden rounded-full">
        {entries.map(([key, value]) => {
          const pct = total > 0 ? (value / total) * 100 : 0
          if (pct === 0) return null
          return (
            <div
              key={key}
              style={{ width: `${pct}%` }}
              className={`${COLORS[key]} transition-all`}
              title={`${LABELS[key]}: ${value}`}
            />
          )
        })}
      </div>

      {/* Legend */}
      <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-3">
        {entries.map(([key, value]) => (
          <div key={key} className="flex items-center gap-2 text-sm">
            <span className={`h-3 w-3 rounded-sm ${COLORS[key]}`} />
            <span className="text-gray-600">{LABELS[key]}</span>
            <span className="ml-auto font-semibold text-gray-900">{value}</span>
          </div>
        ))}
        <div className="flex items-center gap-2 text-sm font-semibold col-span-full border-t border-gray-100 pt-2">
          <span className="text-gray-500">Total</span>
          <span className="ml-auto text-gray-900">{total}</span>
        </div>
      </div>
    </Card>
  )
}
