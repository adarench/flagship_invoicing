import { formatCurrency, formatDate } from '@/lib/utils'
import { MatchTypeBadge } from '@/components/ui/MatchTypeBadge'
import { ConfidenceBadge } from '@/components/ui/ConfidenceBadge'
import type { MatchDetail } from '@/lib/types'

interface MatchDetailViewProps {
  match: MatchDetail
}

function Row({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="flex justify-between py-2 border-b border-gray-100 last:border-0 text-sm">
      <span className="text-gray-500 font-medium">{label}</span>
      <span className="text-gray-900 max-w-xs text-right">{value ?? '—'}</span>
    </div>
  )
}

export function MatchDetailView({ match }: MatchDetailViewProps) {
  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
      {/* PID Details */}
      <div>
        <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-400">PID Record</h3>
        <div className="rounded-xl border border-gray-200 bg-white p-4">
          <Row label="PID ID"       value={match.pid_id} />
          <Row label="Vendor"       value={match.vendor} />
          <Row label="Invoice #"    value={match.invoice_no} />
          <Row label="Invoice Date" value={formatDate(match.invoice_date)} />
          <Row label="PID Amount"   value={<span className="font-semibold">{formatCurrency(match.pid_amount)}</span>} />
          <Row label="Check #"      value={match.check_no} />
          <Row label="Check Date"   value={formatDate(match.check_date)} />
          <Row label="Bank"         value={match.bank} />
          <Row label="Phase"        value={match.phase} />
          <Row label="Reference"    value={match.reference} />
        </div>
      </div>

      {/* Bank Details */}
      <div>
        <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-400">Bank Transaction</h3>
        <div className="rounded-xl border border-gray-200 bg-white p-4">
          <Row label="Bank ID"      value={match.bank_id} />
          <Row label="Posted Date"  value={formatDate(match.bank_posted_date)} />
          <Row label="Bank Amount"  value={match.bank_amount != null ? <span className="font-semibold">{formatCurrency(match.bank_amount)}</span> : null} />
          <Row label="Amount Diff"  value={match.amount_diff != null ? formatCurrency(match.amount_diff) : null} />
          <Row label="Description"  value={match.bank_description} />
        </div>
      </div>

      {/* Match metadata */}
      <div className="lg:col-span-2">
        <h3 className="mb-3 text-xs font-semibold uppercase tracking-wide text-gray-400">Match Metadata</h3>
        <div className="rounded-xl border border-gray-200 bg-white p-4">
          <div className="flex flex-wrap gap-6">
            <div>
              <p className="text-xs text-gray-400">Type</p>
              <div className="mt-1"><MatchTypeBadge type={match.match_type} /></div>
            </div>
            <div>
              <p className="text-xs text-gray-400">Confidence</p>
              <div className="mt-1"><ConfidenceBadge confidence={match.match_confidence} /></div>
            </div>
            <div>
              <p className="text-xs text-gray-400">Notes</p>
              <p className="mt-1 text-sm text-gray-700">{match.notes || '—'}</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
