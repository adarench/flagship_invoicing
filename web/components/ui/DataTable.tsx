'use client'
import { useState } from 'react'
import { ChevronUp, ChevronDown } from 'lucide-react'
import { cn } from '@/lib/utils'

export interface Column<T> {
  key: keyof T | string
  header: string
  render?: (row: T) => React.ReactNode
  sortable?: boolean
  className?: string
}

interface DataTableProps<T extends object> {
  columns: Column<T>[]
  data: T[]
  onRowClick?: (row: T) => void
  emptyMessage?: string
  className?: string
}

export function DataTable<T extends object>({
  columns,
  data,
  onRowClick,
  emptyMessage = 'No data',
  className,
}: DataTableProps<T>) {
  const [sortKey, setSortKey] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')

  function handleSort(key: string) {
    if (sortKey === key) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir('asc')
    }
  }

  const sorted = [...data].sort((a, b) => {
    if (!sortKey) return 0
    const av = (a as Record<string, unknown>)[sortKey]
    const bv = (b as Record<string, unknown>)[sortKey]
    if (av === bv) return 0
    const asNum = Number(av)
    const bsNum = Number(bv)
    if (!Number.isNaN(asNum) && !Number.isNaN(bsNum)) {
      const cmpNum = asNum < bsNum ? -1 : 1
      return sortDir === 'asc' ? cmpNum : -cmpNum
    }

    const asDate = new Date(String(av ?? '')).getTime()
    const bsDate = new Date(String(bv ?? '')).getTime()
    if (!Number.isNaN(asDate) && !Number.isNaN(bsDate)) {
      const cmpDate = asDate < bsDate ? -1 : 1
      return sortDir === 'asc' ? cmpDate : -cmpDate
    }

    const cmp = String(av ?? '').localeCompare(String(bv ?? ''), undefined, { sensitivity: 'base' })
    return sortDir === 'asc' ? cmp : -cmp
  })

  return (
    <div className={cn('overflow-x-auto rounded-lg border border-gray-200', className)}>
      <table className="w-full text-sm">
        <thead className="bg-gray-50 text-xs font-medium text-gray-500 uppercase tracking-wide">
          <tr>
            {columns.map(col => (
              <th
                key={String(col.key)}
                className={cn(
                  'px-4 py-3 text-left',
                  col.sortable && 'cursor-pointer select-none hover:text-gray-700',
                  col.className,
                )}
                onClick={col.sortable ? () => handleSort(String(col.key)) : undefined}
              >
                <span className="inline-flex items-center gap-1">
                  {col.header}
                  {col.sortable && sortKey === String(col.key) ? (
                    sortDir === 'asc' ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />
                  ) : null}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 bg-white">
          {sorted.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="px-4 py-8 text-center text-gray-400">
                {emptyMessage}
              </td>
            </tr>
          ) : (
            sorted.map((row, i) => (
              <tr
                key={i}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={cn(
                  'transition-colors',
                  onRowClick && 'cursor-pointer hover:bg-blue-50',
                )}
              >
                {columns.map(col => (
                  <td key={String(col.key)} className={cn('px-4 py-3 text-gray-700', col.className)}>
                    {col.render ? col.render(row) : String((row as Record<string, unknown>)[String(col.key)] ?? '')}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}
