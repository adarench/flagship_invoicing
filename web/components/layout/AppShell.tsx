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
      <main className="ml-56 mt-14 min-h-[calc(100vh-3.5rem)] bg-gray-50 p-6">
        {children}
      </main>
    </>
  )
}
