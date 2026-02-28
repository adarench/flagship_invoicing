'use client'
import { useEffect } from 'react'
import { useRouter, useParams } from 'next/navigation'
import { AppShell } from '@/components/layout/AppShell'
import { Card } from '@/components/ui/Card'
import { ProgressStepper } from '@/components/ui/ProgressStepper'
import { JobLogConsole } from '@/components/ui/JobLogConsole'
import { useJobStatus } from '@/hooks/useJob'
import { AlertCircle, CheckCircle2 } from 'lucide-react'
import Button from '@/components/ui/Button'

export default function ProgressPage() {
  const params = useParams<{ id: string }>()
  const jobId = params.id
  const router = useRouter()
  const { data: job, error } = useJobStatus(jobId)

  // Auto-redirect when completed
  useEffect(() => {
    if (job?.state === 'completed') {
      const t = setTimeout(() => router.push(`/dashboard/${jobId}`), 1500)
      return () => clearTimeout(t)
    }
  }, [job?.state, jobId, router])

  const isRunning = job?.state === 'running' || job?.state === 'pending'

  return (
    <AppShell>
      <div className="mx-auto max-w-2xl">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-gray-900">Processing Job</h1>
          <p className="mt-1 font-mono text-xs text-gray-400">{jobId}</p>
        </div>

        {/* Progress bar */}
        <Card className="mb-4">
          <div className="mb-4 flex items-center justify-between">
            <span className="text-sm font-medium text-gray-700">
              {job?.state === 'completed' ? 'Complete!' :
               job?.state === 'error'     ? 'Failed' :
               'Processing…'}
            </span>
            <span className="text-sm font-semibold text-primary">{job?.progress ?? 0}%</span>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-gray-200">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${job?.progress ?? 0}%` }}
            />
          </div>
        </Card>

        {/* Steps */}
        {job?.steps && job.steps.length > 0 && (
          <Card className="mb-4">
            <ProgressStepper steps={job.steps} />
          </Card>
        )}

        {/* Error */}
        {job?.state === 'error' && (
          <div className="mb-4 flex items-start gap-3 rounded-xl bg-red-50 p-4 text-red-700">
            <AlertCircle className="h-5 w-5 shrink-0 mt-0.5" />
            <div>
              <p className="font-medium">Pipeline failed</p>
              <p className="mt-1 text-sm">{job.error_message}</p>
            </div>
          </div>
        )}

        {/* Success */}
        {job?.state === 'completed' && (
          <div className="mb-4 flex items-center gap-3 rounded-xl bg-green-50 p-4 text-green-700">
            <CheckCircle2 className="h-5 w-5" />
            <span className="font-medium">Reconciliation complete — redirecting to dashboard…</span>
          </div>
        )}

        {/* Log console */}
        <Card padding="none">
          <div className="border-b border-gray-200 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">Run Log</p>
          </div>
          <JobLogConsole jobId={jobId} running={isRunning} className="h-64 rounded-b-xl rounded-t-none" />
        </Card>

        <div className="mt-4 flex justify-end gap-3">
          {job?.state === 'completed' && (
            <Button onClick={() => router.push(`/dashboard/${jobId}`)}>
              View Dashboard
            </Button>
          )}
          {job?.state === 'error' && (
            <Button variant="secondary" onClick={() => router.push('/upload')}>
              Start New Job
            </Button>
          )}
        </div>
      </div>
    </AppShell>
  )
}
