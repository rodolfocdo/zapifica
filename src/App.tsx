import { useEffect, useState } from 'react'
import type { Session } from '@supabase/supabase-js'
import { DashboardLayout } from './layouts/DashboardLayout'
import { supabase } from './lib/supabase'
import { AgendaPage } from './pages/AgendaPage'
import { CrmPage } from './pages/CrmPage'
import { HomePage } from './pages/HomePage'
import { LoginPage } from './pages/LoginPage'
import { SettingsPage } from './pages/SettingsPage'
import { ZapVoicePage } from './pages/ZapVoicePage'
import type { DashboardNavId } from './components/Sidebar'

function navTitle(id: DashboardNavId): string {
  switch (id) {
    case 'home':
      return 'Início'
    case 'crm':
      return 'CRM / Funil'
    case 'agenda':
      return 'Agenda Suprema'
    case 'zap-voice':
      return 'Zap Voice'
    case 'settings':
      return 'Configurações'
    default:
      return 'Zapifica'
  }
}

function DashboardContent({ activeNav }: { activeNav: DashboardNavId }) {
  switch (activeNav) {
    case 'home':
      return <HomePage />
    case 'crm':
      return <CrmPage />
    case 'agenda':
      return <AgendaPage />
    case 'zap-voice':
      return <ZapVoicePage />
    case 'settings':
      return <SettingsPage />
    default:
      return <HomePage />
  }
}

function AuthLoadingScreen() {
  return (
    <div className="flex min-h-dvh flex-col items-center justify-center gap-4 bg-zinc-950">
      <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-600 to-brand-700 shadow-lg shadow-[0_12px_40px_rgba(106,0,184,0.35)]">
        <div className="h-6 w-6 animate-pulse rounded-md bg-white/30" />
      </div>
      <p className="text-sm font-medium text-zinc-500">Carregando sessão…</p>
    </div>
  )
}

export default function App() {
  const [session, setSession] = useState<Session | null>(null)
  const [authReady, setAuthReady] = useState(false)
  const [activeNav, setActiveNav] = useState<DashboardNavId>('home')

  useEffect(() => {
    let cancelled = false

    void supabase.auth.getSession().then(({ data: { session: s } }) => {
      if (!cancelled) {
        setSession(s)
        setAuthReady(true)
      }
    })

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((_event, s) => {
      if (!cancelled) {
        setSession(s)
        if (!s) {
          setActiveNav('home')
        }
      }
    })

    return () => {
      cancelled = true
      subscription.unsubscribe()
    }
  }, [])

  if (!authReady) {
    return <AuthLoadingScreen />
  }

  if (!session) {
    return <LoginPage />
  }

  return (
    <DashboardLayout
      activeNav={activeNav}
      onNavigate={setActiveNav}
      title={navTitle(activeNav)}
    >
      <DashboardContent activeNav={activeNav} />
    </DashboardLayout>
  )
}
