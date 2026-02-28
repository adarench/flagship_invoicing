import { Badge } from './Badge'
import type { MatchType } from '@/lib/types'

interface MatchTypeBadgeProps {
  type: MatchType | string
}

const CONFIG: Record<string, { label: string; variant: 'success' | 'info' | 'warning' | 'danger' | 'gray' }> = {
  primary:   { label: 'Primary',   variant: 'success' },
  secondary: { label: 'Secondary', variant: 'info' },
  retention: { label: 'Retention', variant: 'warning' },
  fuzzy:     { label: 'Fuzzy',     variant: 'warning' },
  unmatched: { label: 'Unmatched', variant: 'danger' },
}

export function MatchTypeBadge({ type }: MatchTypeBadgeProps) {
  const cfg = CONFIG[type] ?? { label: type, variant: 'gray' as const }
  return <Badge variant={cfg.variant}>{cfg.label}</Badge>
}
