'use client'
import { useCallback, useState } from 'react'
import { UploadCloud, X, FileText } from 'lucide-react'
import { cn } from '@/lib/utils'

interface DropzoneProps {
  accept?: string
  multiple?: boolean
  onFiles: (files: File[]) => void
  label: string
  hint?: string
  files?: File[]
  onRemove?: (index: number) => void
}

export function Dropzone({ accept, multiple, onFiles, label, hint, files = [], onRemove }: DropzoneProps) {
  const [dragging, setDragging] = useState(false)

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      setDragging(false)
      const dropped = Array.from(e.dataTransfer.files)
      if (dropped.length) onFiles(dropped)
    },
    [onFiles],
  )

  return (
    <div>
      <label
        onDragOver={e => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={handleDrop}
        className={cn(
          'flex cursor-pointer flex-col items-center justify-center rounded-xl border-2 border-dashed p-8 transition-colors',
          dragging ? 'border-primary bg-primary/5' : 'border-gray-300 hover:border-primary/60 hover:bg-gray-50',
        )}
      >
        <input
          type="file"
          accept={accept}
          multiple={multiple}
          className="sr-only"
          onChange={e => {
            const picked = Array.from(e.target.files || [])
            if (picked.length) onFiles(picked)
            e.target.value = ''
          }}
        />
        <UploadCloud className={cn('mb-3 h-10 w-10', dragging ? 'text-primary' : 'text-gray-400')} />
        <p className="text-sm font-medium text-gray-700">{label}</p>
        {hint && <p className="mt-1 text-xs text-gray-400">{hint}</p>}
      </label>

      {files.length > 0 && (
        <ul className="mt-3 space-y-1.5">
          {files.map((f, i) => (
            <li key={i} className="flex items-center justify-between rounded-lg bg-gray-50 px-3 py-2 text-sm">
              <span className="flex items-center gap-2 text-gray-700">
                <FileText className="h-4 w-4 text-gray-400" />
                {f.name}
                <span className="text-xs text-gray-400">({(f.size / 1024).toFixed(0)} KB)</span>
              </span>
              {onRemove && (
                <button
                  type="button"
                  onClick={() => onRemove(i)}
                  className="ml-2 text-gray-400 hover:text-red-500"
                >
                  <X className="h-4 w-4" />
                </button>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
