import { useCallback, useEffect, useState } from 'react'
import { Loader2, Megaphone, X } from 'lucide-react'

export type NewCampaignModalProps = {
  open: boolean
  onClose: () => void
  /** Retorno alinhado a `sendTextMessage` da Evolution. */
  onSend: (
    number: string,
    text: string,
  ) => Promise<{ ok: boolean; error: string | null }>
}

export function NewCampaignModal({
  open,
  onClose,
  onSend,
}: NewCampaignModalProps) {
  const [destination, setDestination] = useState('')
  const [message, setMessage] = useState('')
  const [sending, setSending] = useState(false)

  useEffect(() => {
    if (open) {
      setDestination('')
      setMessage('')
      setSending(false)
    }
  }, [open])

  const handleClose = useCallback(() => {
    if (sending) return
    onClose()
  }, [onClose, sending])

  const handleSubmit = useCallback(async () => {
    if (sending) return
    setSending(true)
    try {
      const result = await onSend(destination, message)
      if (result.ok) {
        setDestination('')
        setMessage('')
        onClose()
      }
    } finally {
      setSending(false)
    }
  }, [destination, message, onClose, onSend, sending])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-zinc-950/70 p-4 backdrop-blur-md"
      role="dialog"
      aria-modal="true"
      aria-labelledby="campaign-modal-title"
    >
      <button
        type="button"
        className="absolute inset-0 cursor-default"
        aria-label="Fechar"
        onClick={handleClose}
        disabled={sending}
      />
      <div className="relative w-full max-w-lg overflow-hidden rounded-2xl border border-zinc-200/90 bg-white shadow-2xl ring-1 ring-zinc-100">
        <div className="pointer-events-none absolute -right-20 -top-20 h-56 w-56 rounded-full bg-brand-500/10 blur-3xl" />
        <div className="pointer-events-none absolute -bottom-24 -left-16 h-48 w-48 rounded-full bg-brand-green/10 blur-3xl" />

        <div className="relative border-b border-zinc-100 bg-gradient-to-r from-white via-brand-50/30 to-emerald-50/20 px-6 pb-5 pt-6">
          <button
            type="button"
            onClick={handleClose}
            disabled={sending}
            className="absolute right-3 top-3 rounded-xl p-2 text-zinc-400 transition hover:bg-white/80 hover:text-zinc-700 disabled:opacity-40"
            aria-label="Fechar"
          >
            <X className="h-5 w-5" />
          </button>

          <div className="mb-4 inline-flex w-fit items-center gap-2 rounded-full bg-white/90 px-3 py-1 text-xs font-semibold text-brand-700 shadow-sm ring-1 ring-brand-100">
            <Megaphone className="h-3.5 w-3.5" aria-hidden />
            Disparo direto
          </div>
          <h2
            id="campaign-modal-title"
            className="pr-10 text-xl font-semibold tracking-tight text-zinc-900"
          >
            Nova Campanha
          </h2>
          <p className="mt-1.5 max-w-md text-sm leading-relaxed text-zinc-600">
            Envie sua primeira mensagem pela instância conectada. Use o número
            completo com código do país.
          </p>
        </div>

        <div className="relative space-y-5 px-6 py-6">
          <div>
            <label
              htmlFor="campaign-destination"
              className="mb-2 block text-sm font-medium text-zinc-800"
            >
              Número de Destino (com DDD)
            </label>
            <input
              id="campaign-destination"
              type="text"
              inputMode="tel"
              autoComplete="tel"
              placeholder="5548999999999"
              value={destination}
              onChange={(e) => setDestination(e.target.value)}
              disabled={sending}
              className="w-full rounded-xl border border-zinc-200 bg-zinc-50/50 px-4 py-3 text-sm text-zinc-900 shadow-inner ring-zinc-100 transition placeholder:text-zinc-400 focus:border-brand-400 focus:bg-white focus:outline-none focus:ring-2 focus:ring-brand-500/20 disabled:opacity-60"
            />
          </div>

          <div>
            <label
              htmlFor="campaign-message"
              className="mb-2 block text-sm font-medium text-zinc-800"
            >
              Sua Mensagem
            </label>
            <textarea
              id="campaign-message"
              rows={5}
              placeholder="Escreva o texto que será enviado ao contato…"
              value={message}
              onChange={(e) => setMessage(e.target.value)}
              disabled={sending}
              className="w-full resize-none rounded-xl border border-zinc-200 bg-zinc-50/50 px-4 py-3 text-sm leading-relaxed text-zinc-900 shadow-inner transition placeholder:text-zinc-400 focus:border-brand-400 focus:bg-white focus:outline-none focus:ring-2 focus:ring-brand-500/20 disabled:opacity-60"
            />
          </div>
        </div>

        <div className="relative flex flex-col-reverse gap-3 border-t border-zinc-100 bg-zinc-50/50 px-6 py-4 sm:flex-row sm:justify-end">
          <button
            type="button"
            onClick={handleClose}
            disabled={sending}
            className="inline-flex w-full items-center justify-center rounded-xl border border-zinc-200 bg-white px-5 py-3 text-sm font-semibold text-zinc-700 shadow-sm transition hover:border-zinc-300 hover:bg-zinc-50 disabled:opacity-50 sm:w-auto"
          >
            Cancelar
          </button>
          <button
            type="button"
            onClick={() => void handleSubmit()}
            disabled={sending}
            className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-brand-green px-5 py-3 text-sm font-bold text-white shadow-[0_0_0_1px_rgba(255,255,255,0.15)_inset,0_10px_32px_rgba(37,211,102,0.45)] transition hover:bg-[var(--color-brand-green-dark)] hover:shadow-[0_0_0_1px_rgba(255,255,255,0.2)_inset,0_14px_40px_rgba(37,211,102,0.55)] focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-green disabled:opacity-60 sm:w-auto"
          >
            {sending ? (
              <>
                <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
                Enviando...
              </>
            ) : (
              'Disparar Mensagem'
            )}
          </button>
        </div>
      </div>
    </div>
  )
}
