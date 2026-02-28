'use client'
import { useQuery } from '@tanstack/react-query'
import { getMatchDetail } from '@/lib/api'

export function useMatchDetail(jobId: string | null, matchId: string | null) {
  return useQuery({
    queryKey: ['job', jobId, 'match', matchId],
    queryFn: () => getMatchDetail(jobId!, matchId!),
    enabled: !!jobId && !!matchId,
    staleTime: 10_000,
  })
}
