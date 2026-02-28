'use client'
import { useState } from 'react'
import { Check, X } from 'lucide-react'
import Button from '@/components/ui/Button'
import { approveMatch, rejectMatch } from '@/lib/api'
import type { ReviewStatus } from '@/lib/types'

interface ApprovalButtonsProps {
  jobId: string
  matchId: string
  currentStatus: ReviewStatus
  onStatusChange?: (status: ReviewStatus) => void
}

export function ApprovalButtons({ jobId, matchId, currentStatus, onStatusChange }: ApprovalButtonsProps) {
  const [status, setStatus] = useState<ReviewStatus>(currentStatus)
  const [loading, setLoading] = useState<'approve' | 'reject' | null>(null)

  async function handleApprove() {
    setLoading('approve')
    try {
      await approveMatch(jobId, matchId)
      setStatus('approved')
      onStatusChange?.('approved')
    } finally {
      setLoading(null)
    }
  }

  async function handleReject() {
    setLoading('reject')
    try {
      await rejectMatch(jobId, matchId)
      setStatus('rejected')
      onStatusChange?.('rejected')
    } finally {
      setLoading(null)
    }
  }

  if (status === 'approved') {
    return (
      <div className="flex items-center gap-2 rounded-lg bg-green-50 px-4 py-2 text-sm font-medium text-green-700">
        <Check className="h-4 w-4" /> Approved
      </div>
    )
  }

  if (status === 'rejected') {
    return (
      <div className="flex items-center gap-2 rounded-lg bg-red-50 px-4 py-2 text-sm font-medium text-red-700">
        <X className="h-4 w-4" /> Rejected
      </div>
    )
  }

  return (
    <div className="flex gap-2">
      <Button
        variant="success"
        size="md"
        loading={loading === 'approve'}
        onClick={handleApprove}
      >
        <Check className="h-4 w-4" />
        Approve
      </Button>
      <Button
        variant="danger"
        size="md"
        loading={loading === 'reject'}
        onClick={handleReject}
      >
        <X className="h-4 w-4" />
        Reject
      </Button>
    </div>
  )
}
