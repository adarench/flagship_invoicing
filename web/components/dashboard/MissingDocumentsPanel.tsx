import { Card, CardHeader, CardTitle } from '@/components/ui/Card'
import type { Summary } from '@/lib/types'
import { AlertTriangle } from 'lucide-react'

const ALL_BANKS = ['FLAG', 'AG', 'WDCPL', 'GPM']

interface MissingDocumentsPanelProps {
  summary: Summary
}

export function MissingDocumentsPanel({ summary }: MissingDocumentsPanelProps) {
  const missing = ALL_BANKS.filter(b => !summary.banks_loaded.includes(b))

  if (missing.length === 0) {
    return null
  }

  return (
    <Card className="border-yellow-200 bg-yellow-50">
      <CardHeader>
        <CardTitle className="text-yellow-700">Missing Bank Statements</CardTitle>
        <AlertTriangle className="h-4 w-4 text-yellow-500" />
      </CardHeader>
      <p className="mb-3 text-sm text-yellow-700">
        The following bank PDFs were not provided in this job. Match rate will be lower until all banks are included.
      </p>
      <div className="flex flex-wrap gap-2">
        {missing.map(bank => (
          <span key={bank} className="rounded-md bg-yellow-100 px-3 py-1 text-sm font-medium text-yellow-800">
            {bank}
          </span>
        ))}
      </div>
    </Card>
  )
}
