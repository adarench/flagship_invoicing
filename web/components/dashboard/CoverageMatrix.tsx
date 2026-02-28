import { Card, CardHeader, CardTitle } from '@/components/ui/Card'
import type { Coverage } from '@/lib/types'
import { cn } from '@/lib/utils'

interface CoverageMatrixProps {
  coverage: Coverage
}

export function CoverageMatrix({ coverage }: CoverageMatrixProps) {
  // Gather all unique month_labels across all banks
  const allLabels = Array.from(
    new Set(
      coverage.banks.flatMap(b => b.periods.map(p => p.month_label)),
    ),
  ).sort((a, b) => {
    const da = new Date(a)
    const db = new Date(b)
    return da.getTime() - db.getTime()
  })

  return (
    <Card>
      <CardHeader>
        <CardTitle>Bank Statement Coverage</CardTitle>
      </CardHeader>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr>
              <th className="pr-4 text-left text-gray-500 font-medium">Bank</th>
              {allLabels.map(label => (
                <th key={label} className="px-1.5 text-center text-gray-500 font-medium whitespace-nowrap">
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {coverage.banks.map(bank => {
              const periodMap = Object.fromEntries(bank.periods.map(p => [p.month_label, p.transaction_count]))
              return (
                <tr key={bank.bank}>
                  <td className="py-2 pr-4 font-medium text-gray-700">{bank.bank}</td>
                  {allLabels.map(label => {
                    const count = periodMap[label]
                    return (
                      <td key={label} className="px-1.5 py-2 text-center">
                        {count != null ? (
                          <span
                            className={cn(
                              'inline-flex h-7 w-10 items-center justify-center rounded text-xs font-medium',
                              count > 0
                                ? 'bg-primary/10 text-primary'
                                : 'bg-gray-100 text-gray-400',
                            )}
                          >
                            {count}
                          </span>
                        ) : (
                          <span className="inline-flex h-7 w-10 items-center justify-center rounded bg-gray-50 text-gray-300">
                            —
                          </span>
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </Card>
  )
}
