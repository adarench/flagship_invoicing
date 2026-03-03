// lib/types.ts — TypeScript interfaces matching Pydantic models

export interface StepStatus {
  step_name:
    | 'parse_pid'
    | 'parse_banks'
    | 'canonicalize_vendors'
    | 'match'
    | 'report'
    | 'build_artifacts'
  status: 'pending' | 'running' | 'done' | 'error'
}

export interface JobStatus {
  job_id: string
  state: 'pending' | 'running' | 'completed' | 'error'
  progress: number
  error_message?: string | null
  steps: StepStatus[]
  created_at: string
  updated_at: string
}

export interface JobHistory {
  job_id: string
  state: 'pending' | 'running' | 'completed' | 'error'
  created_at: string
  updated_at: string
  error_message?: string | null
}

export interface CreateJobResponse {
  job_id: string
}

export interface MatchBreakdown {
  primary: number
  secondary: number
  retention: number
  fuzzy: number
  unmatched: number
}

export interface Summary {
  job_id: string
  total_pid_records: number
  total_bank_transactions: number
  matched: number
  unmatched: number
  match_rate: number
  total_pid_amount: number
  total_matched_amount: number
  breakdown: MatchBreakdown
  banks_loaded: string[]
}

export interface CoveragePeriod {
  year: number
  month: number
  month_label: string
  transaction_count: number
}

export interface CoverageBank {
  bank: string
  periods: CoveragePeriod[]
  total_transactions: number
}

export interface Coverage {
  job_id: string
  banks: CoverageBank[]
}

export type ReviewStatus = 'needs_review' | 'approved' | 'rejected'
export type MatchType = 'primary' | 'secondary' | 'retention' | 'fuzzy' | 'unmatched'

export interface ReviewItem {
  match_id: string
  pid_id: string
  bank_id?: string | null
  vendor: string
  invoice_no: string
  pid_amount: number
  bank_amount?: number | null
  match_type: MatchType
  match_confidence: number
  notes: string
  status: ReviewStatus
  check_no: string
  check_date?: string | null
  bank_posted_date?: string | null
  bank_description?: string | null
  bank: string
}

export interface ReviewQueue {
  job_id: string
  items: ReviewItem[]
  total: number
  pending: number
  approved: number
  rejected: number
}

export interface MatchDetail extends ReviewItem {
  invoice_date?: string | null
  amount_diff?: number | null
  phase?: string | null
  reference?: string | null
}

export interface ApproveRejectResponse {
  match_id: string
  status: ReviewStatus
  message: string
}

export interface PacketGenerateResponse {
  match_id: string
  packet_url: string
}

export interface PdfSource {
  filename: string
  page_count: number
}

export interface PdfSourcesResponse {
  job_id: string
  sources: PdfSource[]
}
