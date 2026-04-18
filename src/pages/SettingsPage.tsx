import { Settings2 } from 'lucide-react'

export function SettingsPage() {
  return (
    <div className="rounded-2xl border border-dashed border-zinc-300 bg-white/60 p-10 text-center shadow-sm">
      <Settings2 className="mx-auto h-10 w-10 text-zinc-700" aria-hidden />
      <h2 className="mt-4 text-lg font-semibold text-zinc-900">Configurações</h2>
      <p className="mx-auto mt-2 max-w-md text-sm text-zinc-600">
        Conta, equipe, permissões e integrações aparecerão neste módulo.
      </p>
    </div>
  )
}
