'use client'
import { useQuery } from '@tanstack/react-query'
import { getJobStatus, getJobHistory, getJobSummary, getCoverage, getPdfSources } from '@/lib/api'

export function useJobStatus(jobId: string | null, enabled = true) {
  return useQuery({
    queryKey: ['job', jobId, 'status'],
    queryFn: () => getJobStatus(jobId!),
    enabled: !!jobId && enabled,
    refetchInterval: (query) => {
      const state = query.state.data?.state
      if (state === 'completed' || state === 'error') return false
      return 2000
    },
    staleTime: 0,
  })
}

export function useJobSummary(jobId: string | null) {
  return useQuery({
    queryKey: ['job', jobId, 'summary'],
    queryFn: () => getJobSummary(jobId!),
    enabled: !!jobId,
    staleTime: 30_000,
  })
}

export function useJobCoverage(jobId: string | null) {
  return useQuery({
    queryKey: ['job', jobId, 'coverage'],
    queryFn: () => getCoverage(jobId!),
    enabled: !!jobId,
    staleTime: 30_000,
  })
}

export function useJobHistory() {
  return useQuery({
    queryKey: ['jobs', 'history'],
    queryFn: getJobHistory,
    staleTime: 10_000,
  })
}

export function useJobPdfSources(jobId: string | null) {
  return useQuery({
    queryKey: ['job', jobId, 'pdf_sources'],
    queryFn: () => getPdfSources(jobId!),
    enabled: !!jobId,
    staleTime: 30_000,
  })
}
