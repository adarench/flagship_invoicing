'use client'
import { useState } from 'react'
import { ChevronLeft, ChevronRight, ZoomIn, ZoomOut } from 'lucide-react'
import { pdfPageUrl } from '@/lib/api'
import Button from './Button'

interface PDFViewerProps {
  jobId: string
  filename: string
  totalPages?: number
}

export function PDFViewer({ jobId, filename, totalPages = 999 }: PDFViewerProps) {
  const [page, setPage] = useState(0)
  const [scale, setScale] = useState(1)
  const [error, setError] = useState<string | null>(null)

  const url = pdfPageUrl(jobId, filename, page)
  const hasPages = totalPages > 0

  if (!filename) {
    return <p className="text-sm text-gray-500">No PDF file selected.</p>
  }

  return (
    <div className="flex flex-col gap-3">
      {/* Controls */}
      <div className="flex items-center gap-2">
        <Button
          variant="secondary"
          size="sm"
          onClick={() => setPage(p => Math.max(0, p - 1))}
          disabled={!hasPages || page === 0}
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <span className="text-sm text-gray-600">
          Page {hasPages ? page + 1 : 0}{hasPages ? ` / ${totalPages}` : ''}
        </span>
        <Button
          variant="secondary"
          size="sm"
          onClick={() => setPage(p => p + 1)}
          disabled={!hasPages || page >= totalPages - 1}
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
        <span className="ml-auto flex items-center gap-1">
          <Button variant="ghost" size="sm" onClick={() => setScale(s => Math.max(0.5, s - 0.25))}>
            <ZoomOut className="h-4 w-4" />
          </Button>
          <span className="text-xs text-gray-500">{Math.round(scale * 100)}%</span>
          <Button variant="ghost" size="sm" onClick={() => setScale(s => Math.min(3, s + 0.25))}>
            <ZoomIn className="h-4 w-4" />
          </Button>
        </span>
      </div>

      {/* Page image */}
      <div className="overflow-auto rounded-lg border border-gray-200 bg-gray-100 p-2">
        {error ? (
          <p className="p-4 text-sm text-red-600">{error}</p>
        ) : (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={url}
            alt={`Page ${page + 1}`}
            style={{ transform: `scale(${scale})`, transformOrigin: 'top left' }}
            className="max-w-none"
            onError={() => setError('Unable to load this PDF page.')}
            onLoad={() => setError(null)}
          />
        )}
      </div>
    </div>
  )
}
