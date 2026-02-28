'use client'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getReviewQueue, approveMatch, rejectMatch } from '@/lib/api'

export function useReviewQueue(jobId: string | null) {
  return useQuery({
    queryKey: ['job', jobId, 'review_queue'],
    queryFn: () => getReviewQueue(jobId!),
    enabled: !!jobId,
    staleTime: 5_000,
  })
}

export function useApproveMatch(jobId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (matchId: string) => approveMatch(jobId, matchId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['job', jobId, 'review_queue'] }),
  })
}

export function useRejectMatch(jobId: string) {
  const qc = useQueryClient()
  return useMutation({
    mutationFn: (matchId: string) => rejectMatch(jobId, matchId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['job', jobId, 'review_queue'] }),
  })
}
