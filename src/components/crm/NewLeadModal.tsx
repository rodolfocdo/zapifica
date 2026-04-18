import { useState, type FormEvent } from 'react'
import { X } from 'lucide-react'

type NewLeadModalProps = {
  open: boolean
  onClose: () => void
  onSave: (nome: string, telefone: string) => Promise<{ error: string | null }>
}

export function NewLeadModal({ open, onClose, onSave }: NewLeadModalProps) {
  const [nome, setNome] = useState('')
  const [telefone, setTelefone] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  if (!open) return null

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const n = nome.trim()
    const t = telefone.trim()
    if (!n || !t) {
      setError('Preencha nome e WhatsApp.')
      return
    }
    setSubmitting(true)
    const { error: saveError } = await onSave(n, t)
    setSubmitting(false)
    if (saveError) {
      setError(saveError)
      return
    }
    setNome('')
    setTelefone('')
    onClose()
  }

  function handleClose() {
    if (submitting) return
    setError(null)
    setNome('')
    setTelefone('')
    onClose()
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-zinc-950/60 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby="new-lead-title"
    >
      <button
        type="button"
        className="absolute inset-0 cursor-default"
        aria-label="Fechar modal"
        onClick={handleClose}
      />

      <div className="relative w-full max-w-md rounded-2xl border border-zinc-200/90 bg-white p-6 shadow-2xl shadow-zinc-900/20 ring-1 ring-zinc-100">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2
              id="new-lead-title"
              className="text-lg font-semibold tracking-tight text-zinc-900"
            >
              Novo lead
            </h2>
            <p className="mt-1 text-sm text-zinc-500">
              O lead entra na coluna <span className="font-medium">Novo Lead</span>{' '}
              com temperatura <span className="font-medium">Frio</span>.
            </p>
          </div>
          <button
            type="button"
            onClick={handleClose}
            disabled={submitting}
            className="rounded-lg p-1.5 text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-700 disabled:opacity-50"
            aria-label="Fechar"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {error ? (
          <div
            role="alert"
            className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-800"
          >
            {error}
          </div>
        ) : null}

        <form className="mt-6 space-y-4" onSubmit={handleSubmit}>
          <div>
            <label
              htmlFor="new-lead-nome"
              className="mb-1.5 block text-sm font-medium text-zinc-700"
            >
              Nome
            </label>
            <input
              id="new-lead-nome"
              type="text"
              value={nome}
              onChange={(e) => setNome(e.target.value)}
              disabled={submitting}
              placeholder="Nome do contato"
              className="h-11 w-full rounded-xl border border-zinc-200 bg-zinc-50 px-3 text-sm text-zinc-900 outline-none transition focus:border-brand-400 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
            />
          </div>
          <div>
            <label
              htmlFor="new-lead-telefone"
              className="mb-1.5 block text-sm font-medium text-zinc-700"
            >
              WhatsApp
            </label>
            <input
              id="new-lead-telefone"
              type="text"
              value={telefone}
              onChange={(e) => setTelefone(e.target.value)}
              disabled={submitting}
              placeholder="(11) 98765-4321"
              className="h-11 w-full rounded-xl border border-zinc-200 bg-zinc-50 px-3 text-sm text-zinc-900 outline-none transition focus:border-brand-400 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
            />
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <button
              type="button"
              onClick={handleClose}
              disabled={submitting}
              className="rounded-xl border border-zinc-200 bg-white px-4 py-2.5 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50 disabled:opacity-50"
            >
              Cancelar
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="rounded-xl bg-gradient-to-r from-brand-600 to-brand-700 px-4 py-2.5 text-sm font-semibold text-white shadow-md shadow-[0_8px_24px_rgba(106,0,184,0.35)] transition hover:from-brand-500 hover:to-brand-600 disabled:opacity-60"
            >
              {submitting ? 'Salvando…' : 'Salvar'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
