// lib/api.ts — All API calls, typed against lib/types.ts

import type {
  CreateJobResponse,
  JobStatus,
  JobHistory,
  Summary,
  Coverage,
  ReviewQueue,
  MatchDetail,
  ApproveRejectResponse,
  PacketGenerateResponse,
} from './types'

const BASE = process.env.NEXT_PUBLIC_API_URL || ''

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...init?.headers },
    ...init,
  })
  if (!res.ok) {
    let message = `API error ${res.status}`
    try {
      const body = await res.json()
      message = body.detail || message
    } catch {}
    throw new Error(message)
  }
  return res.json()
}

// ── Jobs ──────────────────────────────────────────────────────────────────────

export async function createJob(formData: FormData): Promise<CreateJobResponse> {
  const res = await fetch(`${BASE}/api/jobs/create`, {
    method: 'POST',
    body: formData,
  })
  if (!res.ok) {
    let message = `Upload failed (${res.status})`
    try {
      const body = await res.json()
      message = body.detail || message
    } catch {}
    throw new Error(message)
  }
  return res.json()
}

export async function getJobStatus(id: string): Promise<JobStatus> {
  return request<JobStatus>(`/api/jobs/${id}/status`)
}

export async function getJobHistory(): Promise<JobHistory[]> {
  return request<JobHistory[]>('/api/jobs/history')
}

// ── Summary / Coverage ────────────────────────────────────────────────────────

export async function getJobSummary(id: string): Promise<Summary> {
  return request<Summary>(`/api/jobs/${id}/summary`)
}

export async function getCoverage(id: string): Promise<Coverage> {
  return request<Coverage>(`/api/jobs/${id}/coverage`)
}

// ── Review ────────────────────────────────────────────────────────────────────

export async function getReviewQueue(id: string): Promise<ReviewQueue> {
  return request<ReviewQueue>(`/api/jobs/${id}/review_queue`)
}

export async function getMatchDetail(id: string, matchId: string): Promise<MatchDetail> {
  const encoded = encodeURIComponent(matchId)
  return request<MatchDetail>(`/api/jobs/${id}/match/${encoded}`)
}

export async function approveMatch(id: string, matchId: string): Promise<ApproveRejectResponse> {
  const encoded = encodeURIComponent(matchId)
  return request<ApproveRejectResponse>(`/api/jobs/${id}/match/${encoded}/approve`, {
    method: 'POST',
  })
}

export async function rejectMatch(id: string, matchId: string): Promise<ApproveRejectResponse> {
  const encoded = encodeURIComponent(matchId)
  return request<ApproveRejectResponse>(`/api/jobs/${id}/match/${encoded}/reject`, {
    method: 'POST',
  })
}

// ── Packets ───────────────────────────────────────────────────────────────────

export async function generatePacket(
  id: string,
  matchId: string,
): Promise<PacketGenerateResponse> {
  const encoded = encodeURIComponent(matchId)
  return request<PacketGenerateResponse>(`/api/jobs/${id}/match/${encoded}/packet`, {
    method: 'POST',
    headers: {},
  })
}

// ── Export URLs (direct download links) ──────────────────────────────────────

export function exportUrl(id: string, file: string): string {
  return `${BASE}/api/jobs/${id}/exports/${file}`
}

export function packetsZipUrl(id: string): string {
  return `${BASE}/api/jobs/${id}/packets.zip`
}

export function pdfPageUrl(id: string, filename: string, page: number): string {
  return `${BASE}/api/jobs/${id}/pdf/${encodeURIComponent(filename)}?page=${page}`
}

export async function getJobLog(id: string): Promise<string> {
  const res = await fetch(`${BASE}/api/jobs/${id}/log`)
  if (!res.ok) throw new Error('Log not available')
  return res.text()
}
