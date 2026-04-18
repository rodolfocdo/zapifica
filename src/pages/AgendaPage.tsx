import { useCallback, useEffect, useMemo, useState } from 'react'
import type { User } from '@supabase/supabase-js'
import {
  CalendarDays,
  ChevronLeft,
  ChevronRight,
  Clock,
  Plus,
} from 'lucide-react'
import { supabase } from '../lib/supabase'
import { NewEventModal } from '../components/agenda/NewEventModal'

type CalendarView = 'day' | 'week' | 'month'

type ScheduledMessageRow = {
  status: string
  is_active?: boolean | null
  scheduled_at: string | null
  last_error: string | null
  evolution_message_id: string | null
}

type AgendaEvent = {
  id: string
  title: string
  category: string
  client_id: string | null
  start_at: string
  end_at: string
  sync_kanban: boolean
  scheduled_messages?: ScheduledMessageRow | ScheduledMessageRow[] | null
}

function pickScheduledMessage(
  ev: AgendaEvent,
): ScheduledMessageRow | null {
  const sm = ev.scheduled_messages
  if (!sm) return null
  if (Array.isArray(sm)) return sm[0] ?? null
  return sm
}

/** Só exibe UI de WhatsApp quando há disparo ativo (não é o “stub” sem lembrete). */
function pickDisparoWhatsappAtivo(
  ev: AgendaEvent,
): ScheduledMessageRow | null {
  const sm = pickScheduledMessage(ev)
  if (!sm) return null
  if (sm.is_active === false) return null
  if (sm.status === 'cancelled') return null
  return sm
}

function agendamentoStatusLabel(status: string): string {
  switch (status) {
    case 'pending':
      return 'Pendente'
    case 'processing':
      return 'Processando'
    case 'sent':
      return 'Enviado'
    case 'failed':
    case 'error':
      return 'Erro'
    case 'cancelled':
      return 'Cancelado'
    default:
      return status
  }
}

function agendamentoStatusClass(status: string): string {
  switch (status) {
    case 'pending':
      return 'bg-amber-100 text-amber-900 ring-amber-200/80'
    case 'processing':
      return 'bg-sky-100 text-sky-900 ring-sky-200/80'
    case 'sent':
      return 'bg-emerald-100 text-emerald-900 ring-emerald-200/80'
    case 'failed':
    case 'error':
      return 'bg-rose-100 text-rose-900 ring-rose-200/80'
    case 'cancelled':
      return 'bg-zinc-100 text-zinc-600 ring-zinc-200/80'
    default:
      return 'bg-zinc-100 text-zinc-700 ring-zinc-200/80'
  }
}

const START_HOUR = 6
const END_HOUR = 22
const SLOT_PX = 56
const HOURS = Array.from(
  { length: END_HOUR - START_HOUR + 1 },
  (_, i) => START_HOUR + i,
)

function pad2(n: number) {
  return String(n).padStart(2, '0')
}

function startOfDay(d: Date) {
  const x = new Date(d)
  x.setHours(0, 0, 0, 0)
  return x
}

function endOfDay(d: Date) {
  const x = new Date(d)
  x.setHours(23, 59, 59, 999)
  return x
}

function addDays(d: Date, n: number) {
  const x = new Date(d)
  x.setDate(x.getDate() + n)
  return x
}

/** Semana começando no domingo (padrão visual próximo ao Google Calendar BR) */
function startOfWeekSunday(d: Date) {
  const x = startOfDay(d)
  const day = x.getDay()
  return addDays(x, -day)
}

function addMonths(d: Date, n: number) {
  const x = new Date(d)
  x.setMonth(x.getMonth() + n)
  return x
}

function monthMatrix(anchor: Date): Date[][] {
  const first = new Date(anchor.getFullYear(), anchor.getMonth(), 1)
  const start = startOfWeekSunday(first)
  const weeks: Date[][] = []
  let cur = new Date(start)
  for (let w = 0; w < 6; w++) {
    const row: Date[] = []
    for (let c = 0; c < 7; c++) {
      row.push(new Date(cur))
      cur = addDays(cur, 1)
    }
    weeks.push(row)
  }
  return weeks
}

function eventBlocksForDay(
  events: AgendaEvent[],
  day: Date,
): { ev: AgendaEvent; top: number; height: number }[] {
  const dayStart = startOfDay(day).getTime()
  const dayEnd = endOfDay(day).getTime()
  const out: { ev: AgendaEvent; top: number; height: number }[] = []
  for (const ev of events) {
    const s = new Date(ev.start_at).getTime()
    const e = new Date(ev.end_at).getTime()
    if (e <= dayStart || s >= dayEnd) continue
    const clipStart = Math.max(s, dayStart)
    const clipEnd = Math.min(e, dayEnd)
    const startMin =
      (clipStart - dayStart) / 60000 - START_HOUR * 60
    const endMin = (clipEnd - dayStart) / 60000 - START_HOUR * 60
    const top = (startMin / 60) * SLOT_PX
    const height = Math.max(((endMin - startMin) / 60) * SLOT_PX, 20)
    out.push({ ev, top, height })
  }
  return out
}

function categoryLabel(c: string) {
  const m: Record<string, string> = {
    reuniao: 'Reunião',
    ligacao: 'Ligação',
    visita: 'Visita',
    suporte: 'Suporte',
    outro: 'Outro',
  }
  return m[c] ?? c
}

function maskPersonalPhone(user: User | null): string {
  if (!user) return '+55 ***** ****'
  const raw =
    (user.phone as string | undefined) ||
    (user.user_metadata?.phone as string | undefined) ||
    (user.user_metadata?.whatsapp as string | undefined) ||
    ''
  const digits = raw.replace(/\D/g, '')
  if (digits.length < 4) return '+55 ***** ****'
  const last = digits.slice(-4)
  return `+55 ***** ${last}`
}

export function AgendaPage() {
  const [tab, setTab] = useState<'calendario' | 'historico'>('calendario')
  const [view, setView] = useState<CalendarView>('day')
  const [cursor, setCursor] = useState(() => new Date())
  const [events, setEvents] = useState<AgendaEvent[]>([])
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [modalSeed, setModalSeed] = useState<{ date: Date; hour: number }>({
    date: new Date(),
    hour: 9,
  })
  const [user, setUser] = useState<User | null>(null)

  /** Janela ampla para histórico e navegação sem refetch a cada mudança de data */
  const syncWindow = useMemo(() => {
    const now = new Date()
    return {
      start: startOfDay(addMonths(now, -6)),
      end: endOfDay(addMonths(now, 6)),
    }
  }, [])

  const fetchEvents = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    const rs = syncWindow.start.toISOString()
    const re = syncWindow.end.toISOString()
    const { data, error } = await supabase
      .from('events')
      .select(
        `id, title, category, client_id, start_at, end_at, sync_kanban,
         scheduled_messages ( status, is_active, scheduled_at, last_error, evolution_message_id )`,
      )
      .lte('start_at', re)
      .gte('end_at', rs)
      .order('start_at', { ascending: true })

    if (error) {
      setLoadError(
        error.message.includes('relation')
          ? 'Execute a migração SQL no Supabase para criar as tabelas da agenda.'
          : 'Não foi possível carregar os eventos.',
      )
      setEvents([])
      setLoading(false)
      return
    }
    setEvents((data ?? []) as AgendaEvent[])
    setLoading(false)
  }, [syncWindow.start, syncWindow.end])

  useEffect(() => {
    void fetchEvents()
  }, [fetchEvents])

  useEffect(() => {
    void supabase.auth.getUser().then(({ data: { user: u } }) => {
      setUser(u)
    })
  }, [])

  const headerDateLabel = useMemo(() => {
    if (view === 'month') {
      return cursor.toLocaleDateString('pt-BR', {
        month: 'long',
        year: 'numeric',
      })
    }
    if (view === 'week') {
      const ws = startOfWeekSunday(cursor)
      const we = addDays(ws, 6)
      const sameMonth = ws.getMonth() === we.getMonth()
      const a = ws.toLocaleDateString('pt-BR', {
        day: 'numeric',
        month: 'short',
      })
      const b = we.toLocaleDateString(
        'pt-BR',
        sameMonth
          ? { day: 'numeric', month: 'long', year: 'numeric' }
          : { day: 'numeric', month: 'short', year: 'numeric' },
      )
      return `${a} – ${b}`
    }
    return cursor.toLocaleDateString('pt-BR', {
      weekday: 'long',
      day: 'numeric',
      month: 'long',
    })
  }, [view, cursor])

  function goToday() {
    setCursor(new Date())
  }

  function goPrev() {
    if (view === 'day') setCursor((d) => addDays(d, -1))
    else if (view === 'week') setCursor((d) => addDays(d, -7))
    else setCursor((d) => addMonths(d, -1))
  }

  function goNext() {
    if (view === 'day') setCursor((d) => addDays(d, 1))
    else if (view === 'week') setCursor((d) => addDays(d, 7))
    else setCursor((d) => addMonths(d, 1))
  }

  const weekDays = useMemo(() => {
    const ws = startOfWeekSunday(cursor)
    return Array.from({ length: 7 }, (_, i) => addDays(ws, i))
  }, [cursor])

  const monthWeeks = useMemo(() => monthMatrix(cursor), [cursor])

  const historico = useMemo(() => {
    const now = Date.now()
    return [...events]
      .filter((e) => new Date(e.end_at).getTime() < now)
      .sort(
        (a, b) =>
          new Date(b.start_at).getTime() - new Date(a.start_at).getTime(),
      )
  }, [events])

  const gridHeight = (END_HOUR - START_HOUR + 1) * SLOT_PX

  function openNewAt(day: Date, hour: number) {
    setModalSeed({ date: day, hour })
    setModalOpen(true)
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <span className="inline-flex h-2 w-2 rounded-full bg-brand-600 shadow-[0_0_10px_rgba(106,0,184,0.7)]" />
            <h2 className="text-xl font-semibold tracking-tight text-zinc-900">
              Agenda Suprema
            </h2>
          </div>
          <p className="mt-1 max-w-2xl text-sm leading-relaxed text-zinc-600">
            Organize compromissos no estilo Google Calendar e programe lembretes
            automáticos via WhatsApp (Evolution API).
          </p>
        </div>
        <button
          type="button"
          onClick={() => openNewAt(startOfDay(cursor), 9)}
          className="inline-flex items-center justify-center gap-2 self-start rounded-xl bg-gradient-to-r from-brand-600 to-brand-700 px-5 py-2.5 text-sm font-semibold text-white shadow-lg shadow-[0_10px_28px_rgba(106,0,184,0.35)] transition hover:from-brand-500 hover:to-brand-600"
        >
          <Plus className="h-4 w-4" />
          + Evento
        </button>
      </div>

      <div className="inline-flex rounded-xl border border-zinc-200 bg-white p-1 shadow-sm">
        <button
          type="button"
          onClick={() => setTab('calendario')}
          className={`inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-semibold transition ${
            tab === 'calendario'
              ? 'bg-brand-600 text-white shadow-md shadow-brand-600/25'
              : 'text-zinc-600 hover:bg-zinc-50'
          }`}
        >
          <CalendarDays className="h-4 w-4" />
          Calendário
        </button>
        <button
          type="button"
          onClick={() => setTab('historico')}
          className={`inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-semibold transition ${
            tab === 'historico'
              ? 'bg-brand-600 text-white shadow-md shadow-brand-600/25'
              : 'text-zinc-600 hover:bg-zinc-50'
          }`}
        >
          <Clock className="h-4 w-4" />
          Histórico
        </button>
      </div>

      {tab === 'historico' ? (
        <div className="rounded-2xl border border-zinc-200 bg-white p-6 shadow-sm">
          <h3 className="text-sm font-semibold text-zinc-900">
            Eventos concluídos
          </h3>
          {loading ? (
            <p className="mt-4 text-sm text-zinc-500">Carregando…</p>
          ) : historico.length === 0 ? (
            <p className="mt-4 text-sm text-zinc-500">
              Nenhum evento passado neste intervalo sincronizado.
            </p>
          ) : (
            <ul className="mt-4 divide-y divide-zinc-100">
              {historico.map((e) => {
                const sm = pickDisparoWhatsappAtivo(e)
                return (
                  <li
                    key={e.id}
                    className="flex flex-col gap-1 py-3 sm:flex-row sm:items-center sm:justify-between"
                  >
                    <div className="min-w-0">
                      <p className="font-medium text-zinc-900">{e.title}</p>
                      <p className="text-xs text-zinc-500">
                        {categoryLabel(e.category)}
                        {e.sync_kanban ? ' · Kanban' : ''}
                      </p>
                      {sm ? (
                        <p className="mt-1">
                          <span
                            className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ring-1 ${agendamentoStatusClass(sm.status)}`}
                            title={sm.last_error ?? undefined}
                          >
                            WhatsApp: {agendamentoStatusLabel(sm.status)}
                          </span>
                          {sm.last_error ? (
                            <span className="mt-1 block truncate text-[11px] text-rose-700">
                              {sm.last_error}
                            </span>
                          ) : null}
                        </p>
                      ) : null}
                    </div>
                    <p className="shrink-0 text-sm tabular-nums text-zinc-600">
                      {new Date(e.start_at).toLocaleString('pt-BR', {
                        day: '2-digit',
                        month: '2-digit',
                        hour: '2-digit',
                        minute: '2-digit',
                      })}{' '}
                      —{' '}
                      {new Date(e.end_at).toLocaleTimeString('pt-BR', {
                        hour: '2-digit',
                        minute: '2-digit',
                      })}
                    </p>
                  </li>
                )
              })}
            </ul>
          )}
        </div>
      ) : (
        <div className="overflow-hidden rounded-2xl border border-zinc-200 bg-white shadow-sm ring-1 ring-zinc-100/80">
          <div className="flex flex-col gap-4 border-b border-zinc-100 px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-6">
            <div className="flex flex-wrap items-center gap-2">
              <span className="mr-1 text-xs font-bold uppercase tracking-wide text-zinc-400">
                Agenda
              </span>
              {(['day', 'week', 'month'] as const).map((v) => (
                <button
                  key={v}
                  type="button"
                  onClick={() => setView(v)}
                  className={`rounded-full px-3 py-1.5 text-xs font-semibold capitalize transition ${
                    view === v
                      ? 'bg-amber-400 text-amber-950 shadow-sm'
                      : 'bg-zinc-100 text-zinc-600 hover:bg-zinc-200'
                  }`}
                >
                  {v === 'day' ? 'Dia' : v === 'week' ? 'Semana' : 'Mês'}
                </button>
              ))}
            </div>

            <div className="flex flex-wrap items-center justify-center gap-2 sm:flex-1">
              <button
                type="button"
                onClick={goPrev}
                className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-zinc-200 bg-white text-zinc-600 transition hover:bg-zinc-50"
                aria-label="Anterior"
              >
                <ChevronLeft className="h-5 w-5" />
              </button>
              <p className="min-w-[10rem] text-center text-sm font-semibold capitalize text-zinc-900 sm:min-w-[14rem]">
                {headerDateLabel}
              </p>
              <button
                type="button"
                onClick={goNext}
                className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-zinc-200 bg-white text-zinc-600 transition hover:bg-zinc-50"
                aria-label="Próximo"
              >
                <ChevronRight className="h-5 w-5" />
              </button>
            </div>

            <button
              type="button"
              onClick={goToday}
              className="rounded-full border border-amber-300/80 bg-amber-50 px-4 py-1.5 text-xs font-semibold text-amber-900 transition hover:bg-amber-100"
            >
              Hoje
            </button>
          </div>

          {loadError ? (
            <div className="px-6 py-4 text-sm text-rose-700">{loadError}</div>
          ) : null}

          {view === 'month' ? (
            <div className="p-4 sm:p-6">
              <div className="grid grid-cols-7 gap-1 text-center text-[11px] font-semibold uppercase tracking-wide text-zinc-500">
                {['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'].map((d) => (
                  <div key={d} className="py-2">
                    {d}
                  </div>
                ))}
              </div>
              <div className="mt-1 grid grid-cols-7 gap-1">
                {monthWeeks.flat().map((cell) => {
                  const inMonth = cell.getMonth() === cursor.getMonth()
                  const isToday =
                    cell.toDateString() === new Date().toDateString()
                  const dayEvents = events.filter((ev) => {
                    const s = startOfDay(cell).getTime()
                    const e = endOfDay(cell).getTime()
                    const es = new Date(ev.start_at).getTime()
                    const ee = new Date(ev.end_at).getTime()
                    return es < e && ee > s
                  })
                  return (
                    <button
                      key={cell.toISOString()}
                      type="button"
                      onClick={() => {
                        setCursor(new Date(cell))
                        setView('day')
                      }}
                      className={`flex min-h-[88px] flex-col rounded-xl border p-1.5 text-left transition hover:border-brand-300 ${
                        inMonth
                          ? 'border-zinc-100 bg-zinc-50/50'
                          : 'border-transparent bg-zinc-50/20 text-zinc-400'
                      } ${isToday ? 'ring-2 ring-brand-500/40' : ''}`}
                    >
                      <span
                        className={`text-xs font-semibold ${isToday ? 'text-brand-700' : ''}`}
                      >
                        {cell.getDate()}
                      </span>
                      <div className="mt-1 flex flex-1 flex-col gap-0.5 overflow-hidden">
                        {dayEvents.slice(0, 3).map((ev) => {
                          const sm = pickDisparoWhatsappAtivo(ev)
                          return (
                            <span
                              key={ev.id}
                              className="truncate rounded bg-brand-100/90 px-1 py-0.5 text-[10px] font-medium text-brand-900"
                              title={
                                sm?.last_error
                                  ? `WhatsApp: ${agendamentoStatusLabel(sm.status)} — ${sm.last_error}`
                                  : undefined
                              }
                            >
                              {ev.title}
                              {sm ? (
                                <span className="ml-0.5 text-[9px] opacity-80">
                                  ·{agendamentoStatusLabel(sm.status)}
                                </span>
                              ) : null}
                            </span>
                          )
                        })}
                        {dayEvents.length > 3 ? (
                          <span className="text-[10px] text-zinc-500">
                            +{dayEvents.length - 3}
                          </span>
                        ) : null}
                      </div>
                    </button>
                  )
                })}
              </div>
            </div>
          ) : view === 'week' ? (
            <div className="overflow-x-auto">
              <div
                className="inline-flex min-w-[720px] flex-col"
                style={{ minHeight: gridHeight + 48 }}
              >
                <div className="flex border-b border-zinc-100">
                  <div className="w-14 shrink-0" />
                  {weekDays.map((d) => {
                    const isToday =
                      d.toDateString() === new Date().toDateString()
                    return (
                      <button
                        key={d.toISOString()}
                        type="button"
                        onClick={() => {
                          setCursor(new Date(d))
                          setView('day')
                        }}
                        className={`min-w-0 flex-1 border-l border-zinc-100 px-1 py-2 text-center transition hover:bg-zinc-50 ${
                          isToday ? 'bg-brand-50/50' : ''
                        }`}
                      >
                        <p className="text-[10px] font-semibold uppercase text-zinc-500">
                          {d.toLocaleDateString('pt-BR', { weekday: 'short' })}
                        </p>
                        <p
                          className={`text-sm font-bold ${isToday ? 'text-brand-700' : 'text-zinc-900'}`}
                        >
                          {d.getDate()}
                        </p>
                      </button>
                    )
                  })}
                </div>
                <div className="flex">
                  <div className="w-14 shrink-0 border-r border-zinc-100">
                    {HOURS.map((h) => (
                      <div
                        key={h}
                        style={{ height: SLOT_PX }}
                        className="pr-2 pt-0.5 text-right text-[11px] font-medium tabular-nums text-zinc-400"
                      >
                        {pad2(h)}:00
                      </div>
                    ))}
                  </div>
                  {weekDays.map((day) => (
                    <div
                      key={day.toISOString()}
                      className="relative min-w-0 flex-1 border-l border-zinc-100"
                      style={{ height: gridHeight }}
                    >
                      {HOURS.map((h) => (
                        <button
                          key={h}
                          type="button"
                          onClick={() => openNewAt(day, h)}
                          className="group absolute left-0 right-0 border-b border-zinc-100/90 transition hover:bg-amber-50/40"
                          style={{ top: (h - START_HOUR) * SLOT_PX, height: SLOT_PX }}
                          aria-label={`Novo evento às ${h}:00`}
                        >
                          <span className="absolute left-1/2 top-1/2 flex h-6 w-6 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full bg-amber-400/0 text-amber-500 opacity-0 transition group-hover:bg-amber-400/20 group-hover:opacity-100">
                            <Plus className="h-4 w-4" />
                          </span>
                        </button>
                      ))}
                      {eventBlocksForDay(events, day).map(({ ev, top, height }) => {
                        const sm = pickDisparoWhatsappAtivo(ev)
                        return (
                          <div
                            key={ev.id}
                            className="pointer-events-none absolute left-1 right-1 overflow-hidden rounded-lg border border-brand-200/80 bg-gradient-to-br from-brand-50 to-white px-2 py-1 text-left shadow-sm"
                            style={{ top, height }}
                          >
                            <p className="truncate text-[11px] font-semibold text-brand-950">
                              {ev.title}
                            </p>
                            <p className="truncate text-[10px] text-brand-800/80">
                              {categoryLabel(ev.category)}
                            </p>
                            {sm ? (
                              <p className="mt-0.5 truncate">
                                <span
                                  className={`inline-flex max-w-full truncate rounded-full px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide ring-1 ${agendamentoStatusClass(sm.status)}`}
                                  title={
                                    sm.last_error
                                      ? sm.last_error
                                      : sm.evolution_message_id
                                        ? `ID: ${sm.evolution_message_id}`
                                        : undefined
                                  }
                                >
                                  WhatsApp: {agendamentoStatusLabel(sm.status)}
                                </span>
                              </p>
                            ) : null}
                          </div>
                        )
                      })}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <div
                className="relative flex"
                style={{ minHeight: gridHeight }}
              >
                <div className="w-14 shrink-0 border-r border-zinc-100">
                  {HOURS.map((h) => (
                    <div
                      key={h}
                      style={{ height: SLOT_PX }}
                      className="pr-2 pt-0.5 text-right text-[11px] font-medium tabular-nums text-zinc-400"
                    >
                      {pad2(h)}:00
                    </div>
                  ))}
                </div>
                <div
                  className="relative min-w-0 flex-1"
                  style={{ height: gridHeight }}
                >
                  {HOURS.map((h) => (
                    <button
                      key={h}
                      type="button"
                      onClick={() => openNewAt(startOfDay(cursor), h)}
                      className="group absolute left-0 right-0 border-b border-zinc-100/90 transition hover:bg-amber-50/50"
                      style={{ top: (h - START_HOUR) * SLOT_PX, height: SLOT_PX }}
                      aria-label={`Novo evento às ${h}:00`}
                    >
                      <span className="absolute left-1/2 top-1/2 flex h-7 w-7 -translate-x-1/2 -translate-y-1/2 items-center justify-center rounded-full bg-amber-400/15 text-amber-600 opacity-0 shadow-sm transition group-hover:opacity-100">
                        <Plus className="h-4 w-4" />
                      </span>
                    </button>
                  ))}
                  {eventBlocksForDay(events, cursor).map(
                    ({ ev, top, height }) => {
                      const sm = pickDisparoWhatsappAtivo(ev)
                      return (
                        <div
                          key={ev.id}
                          className="pointer-events-none absolute left-2 right-2 overflow-hidden rounded-xl border border-brand-200/90 bg-gradient-to-br from-brand-50 via-white to-brand-50/30 px-3 py-2 text-left shadow-md shadow-brand-900/5"
                          style={{ top, height }}
                        >
                          <p className="truncate text-sm font-semibold text-brand-950">
                            {ev.title}
                          </p>
                          <p className="truncate text-xs text-zinc-600">
                            {categoryLabel(ev.category)} ·{' '}
                            {new Date(ev.start_at).toLocaleTimeString('pt-BR', {
                              hour: '2-digit',
                              minute: '2-digit',
                            })}{' '}
                            —{' '}
                            {new Date(ev.end_at).toLocaleTimeString('pt-BR', {
                              hour: '2-digit',
                              minute: '2-digit',
                            })}
                          </p>
                          {sm ? (
                            <p className="mt-1 truncate">
                              <span
                                className={`inline-flex max-w-full truncate rounded-full px-2 py-0.5 text-[10px] font-bold uppercase tracking-wide ring-1 ${agendamentoStatusClass(sm.status)}`}
                                title={
                                  sm.last_error
                                    ? sm.last_error
                                    : sm.evolution_message_id
                                      ? `ID Evolution: ${sm.evolution_message_id}`
                                      : undefined
                                }
                              >
                                Disparo: {agendamentoStatusLabel(sm.status)}
                              </span>
                            </p>
                          ) : null}
                        </div>
                      )
                    },
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      <NewEventModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        initialDate={modalSeed.date}
        initialHour={modalSeed.hour}
        maskedPersonalPhone={maskPersonalPhone(user)}
        onSaved={async () => {
          await fetchEvents()
        }}
      />
    </div>
  )
}
