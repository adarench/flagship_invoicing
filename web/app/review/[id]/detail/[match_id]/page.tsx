'use client'
import { useState } from 'react'
import { useParams } from 'next/navigation'
import Link from 'next/link'
import { ChevronLeft, FileText } from 'lucide-react'
import { AppShell } from '@/components/layout/AppShell'
import { MatchDetailView } from '@/components/review/MatchDetailView'
import { ApprovalButtons } from '@/components/review/ApprovalButtons'
import { Modal } from '@/components/ui/Modal'
import { PDFViewer } from '@/components/ui/PDFViewer'
import Button from '@/components/ui/Button'
import { Skeleton } from '@/components/ui/Skeleton'
import { useMatchDetail } from '@/hooks/useMatchDetail'
import { useJobPdfSources } from '@/hooks/useJob'

export default function MatchDetailPage() {
  const params = useParams<{ id: string; match_id: string }>()
  const jobId = params.id
  const matchId = decodeURIComponent(params.match_id)

  const { data: match, isLoading, error } = useMatchDetail(jobId, matchId)
  const { data: pdfSources } = useJobPdfSources(jobId)
  const [pdfOpen, setPdfOpen] = useState(false)
  const [pdfIndex, setPdfIndex] = useState(0)
  const selectedPdf = pdfSources?.sources?.[pdfIndex]

  return (
    <AppShell jobId={jobId}>
      <div className="mb-6">
        <Link
          href={`/review/${jobId}`}
          className="inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-800 mb-3"
        >
          <ChevronLeft className="h-4 w-4" /> Back to Review Queue
        </Link>
        <div className="flex items-start justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Match Detail</h1>
            <p className="mt-0.5 font-mono text-xs text-gray-400">{matchId}</p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setPdfOpen(true)}
              disabled={!selectedPdf}
            >
              <FileText className="h-4 w-4" /> View PDF
            </Button>
            {match && (
              <ApprovalButtons
                jobId={jobId}
                matchId={match.match_id}
                currentStatus={match.status}
              />
            )}
          </div>
        </div>
      </div>

      {error ? (
        <div className="rounded-xl bg-red-50 p-4 text-sm text-red-700">{String(error)}</div>
      ) : isLoading ? (
        <div className="space-y-4">
          <Skeleton className="h-48 w-full" />
          <Skeleton className="h-48 w-full" />
        </div>
      ) : match ? (
        <MatchDetailView match={match} />
      ) : null}

      {/* PDF Viewer Modal */}
      <Modal open={pdfOpen} onClose={() => setPdfOpen(false)} title="Bank Statement PDF" size="xl">
        {pdfSources?.sources && pdfSources.sources.length > 1 && (
          <div className="mb-3">
            <label className="text-xs font-medium text-gray-500">Source PDF</label>
            <select
              className="mt-1 w-full rounded-md border border-gray-300 bg-white px-2 py-1.5 text-sm"
              value={pdfIndex}
              onChange={(e) => setPdfIndex(Number(e.target.value))}
            >
              {pdfSources.sources.map((src, idx) => (
                <option key={src.filename} value={idx}>
                  {src.filename} ({src.page_count} pages)
                </option>
              ))}
            </select>
          </div>
        )}
        {selectedPdf ? (
          <PDFViewer
            jobId={jobId}
            filename={selectedPdf.filename}
            totalPages={selectedPdf.page_count}
          />
        ) : (
          <p className="text-sm text-gray-500">No bank PDF source found for this job.</p>
        )}
      </Modal>
    </AppShell>
  )
}
