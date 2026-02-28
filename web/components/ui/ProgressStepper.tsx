import { Check, Loader2, AlertCircle } from 'lucide-react'
import { cn } from '@/lib/utils'
import type { StepStatus } from '@/lib/types'

const STEP_LABELS: Record<string, string> = {
  parse_pid:            'Parse PID',
  parse_banks:          'Parse Bank PDFs',
  canonicalize_vendors: 'Canonicalize Vendors',
  match:                'Match Records',
  report:               'Generate Reports',
  build_artifacts:      'Build Artifacts',
}

interface ProgressStepperProps {
  steps: StepStatus[]
}

export function ProgressStepper({ steps }: ProgressStepperProps) {
  return (
    <ol className="space-y-3">
      {steps.map((step, i) => {
        const isDone    = step.status === 'done'
        const isRunning = step.status === 'running'
        const isError   = step.status === 'error'
        const isPending = step.status === 'pending'

        return (
          <li key={step.step_name} className="flex items-center gap-4">
            {/* Icon */}
            <div
              className={cn(
                'flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-sm font-medium',
                isDone    && 'bg-success text-white',
                isRunning && 'bg-primary text-white',
                isError   && 'bg-danger text-white',
                isPending && 'bg-gray-200 text-gray-400',
              )}
            >
              {isDone    ? <Check className="h-4 w-4" /> :
               isRunning ? <Loader2 className="h-4 w-4 animate-spin" /> :
               isError   ? <AlertCircle className="h-4 w-4" /> :
               <span>{i + 1}</span>}
            </div>

            {/* Label */}
            <span
              className={cn(
                'text-sm font-medium',
                isDone    && 'text-gray-700',
                isRunning && 'text-primary',
                isError   && 'text-danger',
                isPending && 'text-gray-400',
              )}
            >
              {STEP_LABELS[step.step_name] ?? step.step_name}
            </span>

            {/* Status badge */}
            <span
              className={cn(
                'ml-auto text-xs',
                isDone    && 'text-success',
                isRunning && 'text-primary',
                isError   && 'text-danger',
                isPending && 'text-gray-400',
              )}
            >
              {step.status}
            </span>
          </li>
        )
      })}
    </ol>
  )
}
