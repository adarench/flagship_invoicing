import { Card, CardHeader, CardTitle } from '@/components/ui/Card'
import type { ReviewQueue } from '@/lib/types'

interface TopUnmatchedVendorsProps {
  reviewQueue: ReviewQueue
}

export function TopUnmatchedVendors({ reviewQueue }: TopUnmatchedVendorsProps) {
  // Count unmatched / needs-review by vendor
  const counts: Record<string, number> = {}
  reviewQueue.items.forEach(item => {
    if (!item.vendor) return
    counts[item.vendor] = (counts[item.vendor] || 0) + 1
  })

  const sorted = Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)

  return (
    <Card>
      <CardHeader>
        <CardTitle>Top Vendors Needing Review</CardTitle>
      </CardHeader>
      {sorted.length === 0 ? (
        <p className="text-sm text-gray-400">All matches auto-resolved.</p>
      ) : (
        <ul className="space-y-2">
          {sorted.map(([vendor, count]) => (
            <li key={vendor} className="flex items-center gap-2">
              <span className="flex-1 truncate text-sm text-gray-700">{vendor || '(unknown)'}</span>
              <span className="rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-medium text-red-700">
                {count}
              </span>
            </li>
          ))}
        </ul>
      )}
    </Card>
  )
}
