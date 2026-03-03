'use client'
import { useParams } from 'next/navigation'
import { FileSpreadsheet, FileJson, Download } from 'lucide-react'
import { AppShell } from '@/components/layout/AppShell'
import { Card } from '@/components/ui/Card'
import { exportUrl } from '@/lib/api'

interface ExportItem {
  label: string
  description: string
  filename: string
  icon: React.ElementType
  color: string
}

export default function ExportsPage() {
  const params = useParams<{ id: string }>()
  const jobId = params.id

  const exports: ExportItem[] = [
    {
      label: 'Reconciled Records',
      description: 'All matched PID rows with corresponding bank transactions.',
      filename: 'reconciled.xlsx',
      icon: FileSpreadsheet,
      color: 'text-green-600',
    },
    {
      label: 'Unmatched Records',
      description: 'PID rows that could not be matched to any bank transaction.',
      filename: 'unmatched.xlsx',
      icon: FileSpreadsheet,
      color: 'text-red-500',
    },
    {
      label: 'Summary JSON',
      description: 'Machine-readable summary statistics for this reconciliation run.',
      filename: 'summary.json',
      icon: FileJson,
      color: 'text-blue-500',
    },
    {
      label: 'Raw OCR Data',
      description: 'Bank PDF extraction data (text and transaction rows).',
      filename: 'raw_ocr.json',
      icon: FileJson,
      color: 'text-purple-500',
    },
    {
      label: 'Packet Manifest',
      description: 'Generated packet index and metadata for this job.',
      filename: 'packet_manifest.json',
      icon: FileJson,
      color: 'text-indigo-500',
    },
  ]

  return (
    <AppShell jobId={jobId}>
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900">Export Files</h1>
        <p className="mt-0.5 text-sm text-gray-500">
          Download the reconciliation outputs for this job.
        </p>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        {exports.map(({ label, description, filename, icon: Icon, color }) => (
          <Card key={filename} padding="md" className="flex items-start gap-4">
            <span className={`mt-0.5 rounded-lg bg-gray-100 p-2 ${color}`}>
              <Icon className="h-5 w-5" />
            </span>
            <div className="flex-1 min-w-0">
              <p className="font-medium text-gray-900">{label}</p>
              <p className="mt-0.5 text-xs text-gray-400">{description}</p>
              <a
                href={exportUrl(jobId, filename)}
                download
                className="mt-3 inline-flex items-center gap-1.5 rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200"
              >
                <Download className="h-3.5 w-3.5" />
                {filename}
              </a>
            </div>
          </Card>
        ))}
      </div>
    </AppShell>
  )
}
