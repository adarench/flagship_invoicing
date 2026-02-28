'use client'
import { useState, useCallback } from 'react'
import { useRouter } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { Card } from '@/components/ui/Card'
import { Dropzone } from '@/components/ui/Dropzone'
import Button from '@/components/ui/Button'
import { createJob } from '@/lib/api'

export default function UploadPage() {
  const router = useRouter()
  const [pidFiles, setPidFiles] = useState<File[]>([])
  const [bankFiles, setBankFiles] = useState<File[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const canSubmit = pidFiles.length > 0 && bankFiles.length > 0

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!canSubmit) return
    setLoading(true)
    setError(null)

    try {
      const fd = new FormData()
      fd.append('pid_file', pidFiles[0])
      bankFiles.forEach(f => fd.append('bank_files', f))

      const { job_id } = await createJob(fd)
      router.push(`/job/${job_id}/progress`)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Upload failed')
      setLoading(false)
    }
  }

  return (
    <AppShell>
      <div className="mx-auto max-w-2xl">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900">Start New Reconciliation</h1>
          <p className="mt-1 text-sm text-gray-500">
            Upload your PID spreadsheet and bank statement PDFs to begin processing.
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-6">
          <Card>
            <h2 className="mb-4 text-sm font-semibold text-gray-700">1. PID Spreadsheet</h2>
            <Dropzone
              accept=".xlsx,.xls,.csv"
              label="Drop PID file here or click to browse"
              hint="Accepts .xlsx, .xls, .csv"
              files={pidFiles}
              onFiles={files => setPidFiles([files[0]])}
              onRemove={() => setPidFiles([])}
            />
          </Card>

          <Card>
            <h2 className="mb-4 text-sm font-semibold text-gray-700">2. Bank Statement PDFs</h2>
            <Dropzone
              accept=".pdf"
              multiple
              label="Drop bank PDFs here or click to browse"
              hint="Accepts multiple PDF files (one per bank)"
              files={bankFiles}
              onFiles={files => setBankFiles(prev => [...prev, ...files])}
              onRemove={i => setBankFiles(prev => prev.filter((_, idx) => idx !== i))}
            />
          </Card>

          {error && (
            <div className="rounded-lg bg-red-50 px-4 py-3 text-sm text-red-700">
              {error}
            </div>
          )}

          <div className="flex justify-end">
            <Button type="submit" size="lg" loading={loading} disabled={!canSubmit}>
              Start Reconciliation
            </Button>
          </div>
        </form>
      </div>
    </AppShell>
  )
}
