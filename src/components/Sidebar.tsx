import {
  Home,
  LogOut,
  Mic2,
  Settings2,
  SquareKanban,
} from 'lucide-react'
import logoZapifica from '../assets/logo-zapifica.png'
import { supabase } from '../lib/supabase'

export type DashboardNavId = 'home' | 'crm' | 'zap-voice' | 'settings'

type NavItem = {
  id: DashboardNavId
  label: string
  icon: typeof Home
}

const items: NavItem[] = [
  { id: 'home', label: 'Início', icon: Home },
  { id: 'crm', label: 'CRM / Funil', icon: SquareKanban },
  { id: 'zap-voice', label: 'Zap Voice', icon: Mic2 },
  { id: 'settings', label: 'Configurações', icon: Settings2 },
]

type SidebarProps = {
  active: DashboardNavId
  onNavigate: (id: DashboardNavId) => void
}

export function Sidebar({ active, onNavigate }: SidebarProps) {
  return (
    <aside className="flex h-full w-64 shrink-0 flex-col border-r border-zinc-800/80 bg-zinc-950 text-zinc-100">
      <div className="border-b border-zinc-800/80 px-5 py-5">
        <div className="flex w-fit items-center justify-center rounded-xl bg-white px-4 py-2 shadow-sm">
          <img
            src={logoZapifica}
            alt="Zapifica"
            className="h-8 w-auto max-w-[148px] object-contain"
            width={148}
            height={32}
          />
        </div>
        <p className="mt-2 truncate text-xs font-medium text-zinc-500">
          CRM conversacional
        </p>
      </div>

      <nav className="flex flex-1 flex-col gap-1 p-3" aria-label="Principal">
        {items.map(({ id, label, icon: Icon }) => {
          const isActive = active === id
          return (
            <button
              key={id}
              type="button"
              onClick={() => onNavigate(id)}
              className={`flex w-full items-center gap-3 rounded-xl px-3 py-2.5 text-left text-sm font-medium transition ${
                isActive
                  ? 'bg-white/[0.08] text-white ring-1 ring-inset ring-white/10'
                  : 'text-zinc-400 hover:bg-white/[0.04] hover:text-zinc-100'
              }`}
            >
              <span
                className={`flex h-9 w-9 items-center justify-center rounded-lg ${
                  isActive
                    ? 'bg-brand-600/25 text-brand-200'
                    : 'bg-zinc-900 text-zinc-500'
                }`}
              >
                <Icon className="h-4 w-4" aria-hidden />
              </span>
              {label}
              {isActive ? (
                <span className="ml-auto h-1.5 w-1.5 rounded-full bg-brand-600 shadow-[0_0_14px_rgba(106,0,184,0.9)]" />
              ) : null}
            </button>
          )
        })}
      </nav>

      <div className="space-y-3 border-t border-zinc-800/80 p-4">
        <div className="rounded-xl bg-zinc-900/80 p-3 ring-1 ring-zinc-800">
          <p className="text-xs font-medium text-zinc-400">Plano</p>
          <p className="mt-0.5 text-sm font-semibold text-white">Premium</p>
          <p className="mt-1 text-xs leading-relaxed text-zinc-500">
            Automação e voz ilimitadas no seu espaço de trabalho.
          </p>
        </div>
        <button
          type="button"
          onClick={() => void supabase.auth.signOut()}
          className="flex w-full items-center justify-center gap-2 rounded-xl border border-zinc-700/80 bg-zinc-900/50 py-2.5 text-sm font-medium text-zinc-300 transition hover:border-zinc-600 hover:bg-zinc-800/80 hover:text-white"
        >
          <LogOut className="h-4 w-4" aria-hidden />
          Sair
        </button>
      </div>
    </aside>
  )
}
