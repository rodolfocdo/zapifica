import { useEffect, useState, type FormEvent } from 'react'
import {
  Calendar,
  Clock,
  Image as ImageIcon,
  Megaphone,
  MessageSquare,
  Mic,
  User,
  Users,
  X,
} from 'lucide-react'
import { supabase } from '../../lib/supabase'

export type AgendaCalendarEvent = {
  id: string
  title: string
  category: string
  client_id: string | null
  start_at: string
  end_at: string
  sync_kanban: boolean
}

type LeadOption = { id: string; nome: string; telefone: string }

type NewEventModalProps = {
  open: boolean
  onClose: () => void
  /** Data base (meia-noite local) sugerida ao abrir a partir da grade */
  initialDate: Date
  /** Hora inicial sugerida (0–23) ao clicar na grade */
  initialHour: number
  maskedPersonalPhone: string
  onSaved: () => void
}

const CATEGORIAS: { value: string; label: string }[] = [
  { value: 'reuniao', label: 'Reunião' },
  { value: 'ligacao', label: 'Ligação' },
  { value: 'visita', label: 'Visita' },
  { value: 'suporte', label: 'Suporte' },
  { value: 'outro', label: 'Outro' },
]

function pad2(n: number) {
  return String(n).padStart(2, '0')
}

function toDateInputValue(d: Date) {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
}

function toTimeInputValue(d: Date) {
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}`
}

function parseLocalDateTime(dateStr: string, timeStr: string): Date | null {
  const [y, m, day] = dateStr.split('-').map(Number)
  const [hh, mm] = timeStr.split(':').map(Number)
  if (!y || !m || !day || Number.isNaN(hh) || Number.isNaN(mm)) return null
  return new Date(y, m - 1, day, hh, mm, 0, 0)
}

function formatDispatchDisplay(d: Date) {
  return d.toLocaleString('pt-BR', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function parseDispatchDisplay(s: string): Date | null {
  const t = s.trim()
  const m = t.match(
    /^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})$/,
  )
  if (!m) return null
  const [, dd, mo, yy, hh, mm] = m
  return new Date(
    Number(yy),
    Number(mo) - 1,
    Number(dd),
    Number(hh),
    Number(mm),
    0,
    0,
  )
}

export function NewEventModal({
  open,
  onClose,
  initialDate,
  initialHour,
  maskedPersonalPhone,
  onSaved,
}: NewEventModalProps) {
  const [title, setTitle] = useState('')
  const [category, setCategory] = useState('reuniao')
  const [clientId, setClientId] = useState<string>('')
  const [startDate, setStartDate] = useState('')
  const [startTime, setStartTime] = useState('09:00')
  const [endDate, setEndDate] = useState('')
  const [endTime, setEndTime] = useState('10:00')
  const [syncKanban, setSyncKanban] = useState(false)

  const [dispatchActive, setDispatchActive] = useState(false)
  const [recipientType, setRecipientType] = useState<'personal' | 'segment'>(
    'personal',
  )
  const [contentType, setContentType] = useState<'text' | 'audio' | 'image'>(
    'text',
  )
  const [messageBody, setMessageBody] = useState(
    'Olá! Lembrete da Zapifica…',
  )
  const [dispatchAtDisplay, setDispatchAtDisplay] = useState('')

  const [leads, setLeads] = useState<LeadOption[]>([])
  const [segmentIds, setSegmentIds] = useState<Set<string>>(new Set())

  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open) return
    const base = new Date(initialDate)
    const start = new Date(base)
    start.setHours(initialHour, 0, 0, 0)
    const end = new Date(start)
    end.setHours(start.getHours() + 1)

    setStartDate(toDateInputValue(start))
    setStartTime(toTimeInputValue(start))
    setEndDate(toDateInputValue(end))
    setEndTime(toTimeInputValue(end))
    setDispatchAtDisplay(formatDispatchDisplay(start))
    setTitle('')
    setCategory('reuniao')
    setClientId('')
    setSyncKanban(false)
    setDispatchActive(false)
    setRecipientType('personal')
    setContentType('text')
    setMessageBody('Olá! Lembrete da Zapifica…')
    setSegmentIds(new Set())
    setError(null)

    void (async () => {
      const { data } = await supabase
        .from('leads')
        .select('id, nome, telefone')
        .order('nome', { ascending: true })
      setLeads((data ?? []) as LeadOption[])
    })()
  }, [open, initialDate, initialHour])

  if (!open) return null

  function toggleSegment(id: string) {
    setSegmentIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function selectAllSegment() {
    const withPhone = leads.filter((l) => l.telefone?.trim())
    setSegmentIds(new Set(withPhone.map((l) => l.id)))
  }

  async function handleSubmit(e: FormEvent) {
    e.preventDefault()
    setError(null)
    const t = title.trim()
    if (!t) {
      setError('Informe o título do evento.')
      return
    }
    const start = parseLocalDateTime(startDate, startTime)
    const end = parseLocalDateTime(endDate, endTime)
    if (!start || !end) {
      setError('Datas e horários inválidos.')
      return
    }
    if (end <= start) {
      setError('O término deve ser depois do início.')
      return
    }

    let dispatchAt: Date | null = null
    if (dispatchActive) {
      dispatchAt = parseDispatchDisplay(dispatchAtDisplay)
      if (!dispatchAt) {
        setError(
          'Use o horário exato do disparo no formato DD/MM/AAAA HH:mm.',
        )
        return
      }
      if (!messageBody.trim() && contentType === 'text') {
        setError('Escreva a mensagem do lembrete ou desative o disparo.')
        return
      }
      if (
        recipientType === 'segment' &&
        segmentIds.size === 0
      ) {
        setError('Selecione ao menos um cliente com telefone no segmento.')
        return
      }
    }

    const {
      data: { user },
      error: userError,
    } = await supabase.auth.getUser()
    if (userError || !user) {
      setError('Sessão inválida. Entre novamente.')
      return
    }

    setSubmitting(true)

    const row = {
      user_id: user.id,
      title: t,
      category,
      client_id: clientId || null,
      start_at: start.toISOString(),
      end_at: end.toISOString(),
      sync_kanban: syncKanban,
    }

    const { data: ev, error: evErr } = await supabase
      .from('events')
      .insert(row)
      .select('id')
      .single()

    if (evErr || !ev) {
      setSubmitting(false)
      setError(
        evErr?.message?.includes('relation')
          ? 'Tabelas da agenda ainda não criadas. Execute a migração no Supabase.'
          : (evErr?.message ?? 'Não foi possível salvar o evento.'),
      )
      return
    }

    if (dispatchActive && dispatchAt) {
      const { error: smErr } = await supabase.from('scheduled_messages').insert({
        event_id: ev.id,
        user_id: user.id,
        is_active: true,
        recipient_type: recipientType,
        content_type: contentType,
        message_body: messageBody.trim() || null,
        scheduled_at: dispatchAt.toISOString(),
        status: 'pending',
        segment_lead_ids:
          recipientType === 'segment' ? [...segmentIds] : [],
      })
      if (smErr) {
        await supabase.from('events').delete().eq('id', ev.id)
        setSubmitting(false)
        setError(smErr.message ?? 'Erro ao salvar o disparo agendado.')
        return
      }
    }

    setSubmitting(false)
    onSaved()
    onClose()
  }

  function handleClose() {
    if (submitting) return
    onClose()
  }

  const leadsWithPhone = leads.filter((l) => l.telefone?.trim())

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-zinc-950/55 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-labelledby="new-event-title"
    >
      <button
        type="button"
        className="absolute inset-0 cursor-default"
        aria-label="Fechar modal"
        onClick={handleClose}
      />

      <div className="relative max-h-[min(92dvh,920px)] w-full max-w-2xl overflow-y-auto rounded-[32px] border border-zinc-200/90 bg-white p-8 shadow-2xl shadow-zinc-900/15 ring-1 ring-zinc-100">
        <div className="flex items-start justify-between gap-4">
          <h2
            id="new-event-title"
            className="text-xl font-semibold tracking-tight text-zinc-900"
          >
            Novo Evento na Agenda
          </h2>
          <button
            type="button"
            onClick={handleClose}
            disabled={submitting}
            className="rounded-xl p-2 text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-700 disabled:opacity-50"
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

        <form className="mt-6 space-y-6" onSubmit={handleSubmit}>
          <div>
            <label
              htmlFor="evt-title"
              className="mb-1.5 block text-sm font-semibold text-zinc-700"
            >
              Título do Evento
            </label>
            <input
              id="evt-title"
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              disabled={submitting}
              placeholder="Ex: Reunião de Alinhamento"
              className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 px-4 text-sm text-zinc-900 outline-none transition placeholder:text-zinc-400 focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
            />
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label
                htmlFor="evt-cat"
                className="mb-1.5 block text-sm font-semibold text-zinc-700"
              >
                Categoria
              </label>
              <div className="relative">
                <select
                  id="evt-cat"
                  value={category}
                  onChange={(e) => setCategory(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full appearance-none rounded-xl border border-zinc-200 bg-zinc-50 px-4 pr-10 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                >
                  {CATEGORIAS.map((c) => (
                    <option key={c.value} value={c.value}>
                      {c.label}
                    </option>
                  ))}
                </select>
                <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400">
                  ▾
                </span>
              </div>
            </div>
            <div>
              <label
                htmlFor="evt-client"
                className="mb-1.5 block text-sm font-semibold text-zinc-700"
              >
                Cliente vinculado (calendário)
              </label>
              <div className="relative">
                <select
                  id="evt-client"
                  value={clientId}
                  onChange={(e) => setClientId(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full appearance-none rounded-xl border border-zinc-200 bg-zinc-50 px-4 pr-10 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                >
                  <option value="">Nenhum</option>
                  {leads.map((l) => (
                    <option key={l.id} value={l.id}>
                      {l.nome}
                    </option>
                  ))}
                </select>
                <span className="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-zinc-400">
                  ▾
                </span>
              </div>
            </div>
          </div>

          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <p className="mb-1.5 text-sm font-semibold text-zinc-700">
                Data de Início
              </p>
              <div className="relative">
                <Calendar className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-400" />
                <input
                  type="date"
                  value={startDate}
                  onChange={(e) => setStartDate(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 pl-10 pr-3 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                />
              </div>
            </div>
            <div>
              <p className="mb-1.5 text-sm font-semibold text-zinc-700">
                Horário
              </p>
              <div className="relative">
                <Clock className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-400" />
                <input
                  type="time"
                  value={startTime}
                  onChange={(e) => setStartTime(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 pl-10 pr-3 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                />
              </div>
            </div>
            <div>
              <p className="mb-1.5 text-sm font-semibold text-zinc-700">
                Data de Término
              </p>
              <div className="relative">
                <Calendar className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-400" />
                <input
                  type="date"
                  value={endDate}
                  onChange={(e) => setEndDate(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 pl-10 pr-3 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                />
              </div>
            </div>
            <div>
              <p className="mb-1.5 text-sm font-semibold text-zinc-700">
                Horário Fim
              </p>
              <div className="relative">
                <Clock className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-400" />
                <input
                  type="time"
                  value={endTime}
                  onChange={(e) => setEndTime(e.target.value)}
                  disabled={submitting}
                  className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 pl-10 pr-3 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                />
              </div>
            </div>
          </div>

          <div className="overflow-hidden rounded-2xl border border-emerald-200/80 bg-emerald-50/60 ring-1 ring-emerald-100/80">
            <div className="flex items-center gap-2 border-b border-emerald-200/60 px-4 py-3">
              <Megaphone className="h-5 w-5 text-[var(--color-brand-green)]" />
              <span className="text-xs font-bold uppercase tracking-wide text-emerald-700">
                Lembrete via WhatsApp
              </span>
            </div>
            <div className="space-y-4 p-4">
              <label className="flex cursor-pointer items-start gap-3">
                <input
                  type="checkbox"
                  checked={dispatchActive}
                  onChange={(e) => setDispatchActive(e.target.checked)}
                  disabled={submitting}
                  className="mt-1 h-5 w-5 rounded border-emerald-300 text-[var(--color-brand-green)] focus:ring-[var(--color-brand-green)]"
                />
                <span className="text-sm font-semibold leading-snug text-emerald-900">
                  Ativar disparo agendado (Evolution API)
                </span>
              </label>

              {dispatchActive ? (
                <div className="space-y-5 border-t border-emerald-200/50 pt-4">
                  <div>
                    <p className="mb-2 text-xs font-bold uppercase tracking-wide text-emerald-800">
                      Destinatários
                    </p>
                    <div className="grid gap-3 sm:grid-cols-1">
                      <label
                        className={`flex cursor-pointer flex-col rounded-2xl border p-4 transition ${
                          recipientType === 'personal'
                            ? 'border-emerald-300 bg-emerald-50/90 ring-2 ring-[var(--color-brand-green)]/25'
                            : 'border-zinc-200 bg-white hover:border-zinc-300'
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <input
                            type="radio"
                            name="recip"
                            checked={recipientType === 'personal'}
                            onChange={() => setRecipientType('personal')}
                            className="mt-1 text-[var(--color-brand-green)] focus:ring-[var(--color-brand-green)]"
                          />
                          <User className="mt-0.5 h-5 w-5 text-zinc-500" />
                          <div className="min-w-0 flex-1">
                            <p className="text-sm font-semibold text-zinc-900">
                              Meu número (lembrete pessoal)
                            </p>
                            <p className="mt-1 text-xs leading-relaxed text-zinc-500">
                              Usa o telefone da agência (tenant) ou o número no
                              seu perfil — sem digitar manualmente.
                            </p>
                            <p className="mt-3 rounded-xl border border-zinc-200 bg-zinc-50 px-3 py-2 text-xs font-medium tabular-nums text-zinc-700">
                              Enviar para {maskedPersonalPhone}
                            </p>
                          </div>
                        </div>
                      </label>

                      <label
                        className={`flex cursor-pointer flex-col rounded-2xl border p-4 transition ${
                          recipientType === 'segment'
                            ? 'border-emerald-300 bg-emerald-50/90 ring-2 ring-[var(--color-brand-green)]/25'
                            : 'border-zinc-200 bg-white hover:border-zinc-300'
                        }`}
                      >
                        <div className="flex items-start gap-3">
                          <input
                            type="radio"
                            name="recip"
                            checked={recipientType === 'segment'}
                            onChange={() => setRecipientType('segment')}
                            className="mt-1 text-[var(--color-brand-green)] focus:ring-[var(--color-brand-green)]"
                          />
                          <Users className="mt-0.5 h-5 w-5 text-zinc-500" />
                          <div className="min-w-0 flex-1">
                            <p className="text-sm font-semibold text-zinc-900">
                              Segmento de clientes
                            </p>
                            <p className="mt-1 text-xs leading-relaxed text-zinc-500">
                              Marque um ou vários clientes, ou use &quot;Selecionar
                              todos&quot; (apenas quem tem telefone válido).
                            </p>
                          </div>
                        </div>
                      </label>
                    </div>

                    {recipientType === 'segment' ? (
                      <div className="mt-3 rounded-xl border border-zinc-200 bg-zinc-50/80 p-3">
                        <div className="mb-2 flex flex-wrap items-center justify-between gap-2">
                          <span className="text-xs font-medium text-zinc-600">
                            {segmentIds.size} selecionado(s)
                          </span>
                          <button
                            type="button"
                            onClick={selectAllSegment}
                            disabled={submitting || leadsWithPhone.length === 0}
                            className="text-xs font-semibold text-brand-600 hover:text-brand-700 disabled:opacity-40"
                          >
                            Selecionar todos
                          </button>
                        </div>
                        <ul className="max-h-40 space-y-1 overflow-y-auto pr-1">
                          {leadsWithPhone.map((l) => (
                            <li key={l.id}>
                              <label className="flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1.5 text-sm hover:bg-white">
                                <input
                                  type="checkbox"
                                  checked={segmentIds.has(l.id)}
                                  onChange={() => toggleSegment(l.id)}
                                  className="rounded border-zinc-300 text-brand-600 focus:ring-brand-600/30"
                                />
                                <span className="truncate text-zinc-800">
                                  {l.nome}
                                </span>
                              </label>
                            </li>
                          ))}
                        </ul>
                        {leadsWithPhone.length === 0 ? (
                          <p className="text-xs text-zinc-500">
                            Nenhum lead com telefone no CRM.
                          </p>
                        ) : null}
                      </div>
                    ) : null}
                  </div>

                  <div>
                    <p className="mb-2 text-xs font-bold uppercase tracking-wide text-emerald-800">
                      Tipo de conteúdo
                    </p>
                    <div className="grid grid-cols-3 gap-2">
                      {(
                        [
                          {
                            id: 'text' as const,
                            label: 'Texto',
                            icon: MessageSquare,
                          },
                          { id: 'audio' as const, label: 'Áudio', icon: Mic },
                          {
                            id: 'image' as const,
                            label: 'Imagem',
                            icon: ImageIcon,
                          },
                        ] as const
                      ).map(({ id, label, icon: Icon }) => {
                        const on = contentType === id
                        return (
                          <button
                            key={id}
                            type="button"
                            disabled={submitting}
                            onClick={() => setContentType(id)}
                            className={`flex flex-col items-center gap-1.5 rounded-xl border px-2 py-3 text-xs font-semibold transition ${
                              on
                                ? 'border-[var(--color-brand-green)] bg-emerald-50 text-emerald-900'
                                : 'border-zinc-200 bg-white text-zinc-600 hover:border-zinc-300'
                            }`}
                          >
                            <Icon className="h-5 w-5" />
                            {label}
                          </button>
                        )
                      })}
                    </div>
                  </div>

                  <div>
                    <label
                      htmlFor="evt-msg"
                      className="mb-1.5 block text-sm font-semibold text-zinc-700"
                    >
                      Mensagem
                    </label>
                    <textarea
                      id="evt-msg"
                      rows={4}
                      value={messageBody}
                      onChange={(e) => setMessageBody(e.target.value)}
                      disabled={submitting}
                      placeholder="Olá! Lembrete da Zapifica…"
                      className="w-full resize-none rounded-xl border border-zinc-200 bg-zinc-50 px-4 py-3 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                    />
                  </div>

                  <div>
                    <label
                      htmlFor="evt-dispatch"
                      className="mb-1.5 block text-xs font-bold uppercase tracking-wide text-emerald-800"
                    >
                      Horário exato do disparo
                    </label>
                    <div className="relative">
                      <input
                        id="evt-dispatch"
                        type="text"
                        value={dispatchAtDisplay}
                        onChange={(e) => setDispatchAtDisplay(e.target.value)}
                        disabled={submitting}
                        placeholder="18/04/2026 07:00"
                        className="h-12 w-full rounded-xl border border-zinc-200 bg-zinc-50 px-4 pr-10 text-sm text-zinc-900 outline-none transition focus:border-brand-500 focus:bg-white focus:ring-4 focus:ring-brand-600/15 disabled:opacity-60"
                      />
                      <Calendar className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-400" />
                    </div>
                    <p className="mt-1 text-xs text-zinc-500">
                      Formato: DD/MM/AAAA HH:mm (horário do robô Evolution).
                    </p>
                  </div>
                </div>
              ) : null}
            </div>
          </div>

          <div>
            <p className="mb-2 text-sm font-semibold text-zinc-900">
              Integrações automáticas
            </p>
            <label className="flex cursor-pointer items-start gap-3 rounded-xl border border-zinc-200 bg-zinc-50/50 p-4">
              <input
                type="checkbox"
                checked={syncKanban}
                onChange={(e) => setSyncKanban(e.target.checked)}
                disabled={submitting}
                className="mt-0.5 h-4 w-4 rounded border-zinc-300 text-brand-600 focus:ring-brand-600/30"
              />
              <div>
                <span className="text-sm font-semibold text-zinc-800">
                  Sincronizar com Kanban
                </span>
                <p className="mt-1 text-xs leading-relaxed text-zinc-500">
                  Cria automaticamente um card de produção para este evento.
                </p>
              </div>
            </label>
          </div>

          <div className="flex justify-end gap-2 border-t border-zinc-100 pt-4">
            <button
              type="button"
              onClick={handleClose}
              disabled={submitting}
              className="rounded-xl border border-zinc-200 bg-white px-5 py-2.5 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50 disabled:opacity-50"
            >
              Cancelar
            </button>
            <button
              type="submit"
              disabled={submitting}
              className="rounded-xl bg-gradient-to-r from-brand-600 to-brand-700 px-6 py-2.5 text-sm font-semibold text-white shadow-md shadow-[0_8px_24px_rgba(106,0,184,0.35)] transition hover:from-brand-500 hover:to-brand-600 disabled:opacity-60"
            >
              {submitting ? 'Salvando…' : 'Salvar evento'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
