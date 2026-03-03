'use client'
import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { Upload, BarChart2, CheckSquare, FileText, Download, Clock } from 'lucide-react'
import { cn } from '@/lib/utils'

interface SidebarProps {
  jobId?: string
}

export function Sidebar({ jobId }: SidebarProps) {
  const pathname = usePathname()

  const jobLinks = jobId
    ? [
        { href: `/dashboard/${jobId}`,  label: 'Dashboard',  icon: BarChart2 },
        { href: `/review/${jobId}`,      label: 'Review',     icon: CheckSquare },
        { href: `/packets/${jobId}`,     label: 'Packets',    icon: FileText },
        { href: `/exports/${jobId}`,     label: 'Exports',    icon: Download },
      ]
    : []

  const globalLinks = [
    { href: '/upload',  label: 'New Job', icon: Upload },
    { href: '/history', label: 'History', icon: Clock },
  ]

  return (
    <aside className="fixed left-0 top-14 bottom-0 hidden w-56 border-r border-gray-200 bg-white px-3 py-4 md:block">
      <nav className="space-y-0.5">
        {globalLinks.map(({ href, label, icon: Icon }) => (
          <Link
            key={href}
            href={href}
            className={cn(
              'flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors',
              pathname === href
                ? 'bg-primary/10 text-primary'
                : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900',
            )}
          >
            <Icon className="h-4 w-4 shrink-0" />
            {label}
          </Link>
        ))}

        {jobLinks.length > 0 && (
          <>
            <div className="my-3 border-t border-gray-200" />
            <p className="px-3 pb-1 text-xs font-semibold uppercase tracking-wide text-gray-400">
              Current Job
            </p>
            {jobLinks.map(({ href, label, icon: Icon }) => (
              <Link
                key={href}
                href={href}
                className={cn(
                  'flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors',
                  pathname.startsWith(href)
                    ? 'bg-primary/10 text-primary'
                    : 'text-gray-600 hover:bg-gray-100 hover:text-gray-900',
                )}
              >
                <Icon className="h-4 w-4 shrink-0" />
                {label}
              </Link>
            ))}
          </>
        )}
      </nav>
    </aside>
  )
}
