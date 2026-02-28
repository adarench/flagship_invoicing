import { PacketCard } from './PacketCard'
import type { ReviewItem } from '@/lib/types'

interface PacketListProps {
  items: ReviewItem[]
  jobId: string
}

export function PacketList({ items, jobId }: PacketListProps) {
  if (items.length === 0) {
    return <p className="text-sm text-gray-400 py-8 text-center">No reviewed matches available for packet generation.</p>
  }

  return (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {items.map(item => (
        <PacketCard key={item.match_id} item={item} jobId={jobId} />
      ))}
    </div>
  )
}
