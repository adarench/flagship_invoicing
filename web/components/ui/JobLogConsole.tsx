'use client'
import { useEffect, useRef, useState } from 'react'
import { getJobLog } from '@/lib/api'
import { cn } from '@/lib/utils'

interface JobLogConsoleProps {
  jobId: string
  running: boolean
  className?: string
}

export function JobLogConsole({ jobId, running, className }: JobLogConsoleProps) {
  const [log, setLog] = useState('')
  const ref = useRef<HTMLPreElement>(null)

  useEffect(() => {
    let timer: ReturnType<typeof setInterval>

    async function fetch() {
      try {
        const text = await getJobLog(jobId)
        setLog(text)
      } catch {}
    }

    fetch()
    if (running) {
      timer = setInterval(fetch, 2000)
    }
    return () => clearInterval(timer)
  }, [jobId, running])

  // Auto-scroll to bottom
  useEffect(() => {
    if (ref.current) {
      ref.current.scrollTop = ref.current.scrollHeight
    }
  }, [log])

  return (
    <pre
      ref={ref}
      className={cn(
        'overflow-auto rounded-lg bg-gray-900 p-4 text-xs font-mono text-green-400 leading-relaxed',
        className,
      )}
    >
      {log || 'Waiting for log output…'}
    </pre>
  )
}
