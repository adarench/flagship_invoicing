import { cn } from '@/lib/utils'

interface ConfidenceBadgeProps {
  confidence: number
  showPercent?: boolean
}

export function ConfidenceBadge({ confidence, showPercent = true }: ConfidenceBadgeProps) {
  const pct = Math.round(confidence * 100)

  const colorClass =
    pct >= 90 ? 'bg-green-100 text-green-700' :
    pct >= 70 ? 'bg-yellow-100 text-yellow-700' :
                'bg-red-100 text-red-700'

  return (
    <span className={cn('inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium', colorClass)}>
      {showPercent ? `${pct}%` : confidence.toFixed(2)}
    </span>
  )
}
