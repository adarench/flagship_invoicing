import Link from 'next/link'
import { LayoutDashboard } from 'lucide-react'

export function Navbar() {
  return (
    <header className="fixed top-0 left-0 right-0 z-40 h-14 border-b border-gray-200 bg-white">
      <div className="flex h-full items-center gap-4 px-6">
        <Link href="/" className="flex items-center gap-2 font-semibold text-gray-900">
          <LayoutDashboard className="h-5 w-5 text-primary" />
          <span>Flagborough Finance</span>
        </Link>
        <span className="text-xs text-gray-400 font-normal">Reconciliation Portal</span>

        <nav className="ml-auto flex items-center gap-1">
          <Link
            href="/history"
            className="rounded-md px-3 py-1.5 text-sm text-gray-600 hover:bg-gray-100 hover:text-gray-900"
          >
            History
          </Link>
        </nav>
      </div>
    </header>
  )
}
