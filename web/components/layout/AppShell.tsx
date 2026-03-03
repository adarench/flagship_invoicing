import { Navbar } from './Navbar'
import { Sidebar } from './Sidebar'

interface AppShellProps {
  children: React.ReactNode
  jobId?: string
}

export function AppShell({ children, jobId }: AppShellProps) {
  return (
    <>
      <Navbar />
      <Sidebar jobId={jobId} />
      <main className="mt-14 min-h-[calc(100vh-3.5rem)] bg-gray-50 p-4 md:ml-56 md:p-6">
        {children}
      </main>
    </>
  )
}
