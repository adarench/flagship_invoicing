'use client'
import { useState } from 'react'
import { FileText, Download } from 'lucide-react'
import { Card } from '@/components/ui/Card'
import Button from '@/components/ui/Button'
import { MatchTypeBadge } from '@/components/ui/MatchTypeBadge'
import { ConfidenceBadge } from '@/components/ui/ConfidenceBadge'
import { generatePacket } from '@/lib/api'
import { formatCurrency } from '@/lib/utils'
import type { ReviewItem } from '@/lib/types'

interface PacketCardProps {
  item: ReviewItem
  jobId: string
}

export function PacketCard({ item, jobId }: PacketCardProps) {
  const [packetUrl, setPacketUrl] = useState<string | null>(null)
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleGenerate() {
    setGenerating(true)
    setError(null)
    try {
      const res = await generatePacket(jobId, item.match_id)
      setPacketUrl(res.packet_url)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to generate packet')
    } finally {
      setGenerating(false)
    }
  }

  return (
    <Card padding="md">
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <FileText className="h-4 w-4 text-gray-400 shrink-0" />
            <span className="font-medium text-gray-900 truncate">{item.vendor || '(no vendor)'}</span>
          </div>
          <p className="text-xs text-gray-400">Invoice #{item.invoice_no} · {formatCurrency(item.pid_amount)}</p>
          <div className="mt-2 flex items-center gap-2">
            <MatchTypeBadge type={item.match_type} />
            <ConfidenceBadge confidence={item.match_confidence} />
          </div>
        </div>

        <div className="flex flex-col items-end gap-2">
          {packetUrl ? (
            <a href={packetUrl} download className="inline-flex items-center gap-1.5 rounded-md bg-green-50 px-3 py-1.5 text-xs font-medium text-green-700 hover:bg-green-100">
              <Download className="h-3.5 w-3.5" /> Download PDF
            </a>
          ) : (
            <Button variant="secondary" size="sm" loading={generating} onClick={handleGenerate}>
              Generate Packet
            </Button>
          )}
          {error && <p className="text-right text-xs text-red-600">{error}</p>}
        </div>
      </div>
    </Card>
  )
}
