import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from 'react'
import {
  DndContext,
  DragOverlay,
  KeyboardSensor,
  PointerSensor,
  closestCorners,
  defaultDropAnimationSideEffects,
  useDroppable,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragOverEvent,
  type DragStartEvent,
  type DropAnimation,
  type UniqueIdentifier,
} from '@dnd-kit/core'
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'
import { Phone } from 'lucide-react'
import { supabase } from '../../lib/supabase'
import { NewLeadModal } from './NewLeadModal'

export type ColumnId = 'novo' | 'atendimento' | 'negociacao' | 'fechado'

export type LeadTemperature = 'frio' | 'morno' | 'quente'

export type Lead = {
  id: string
  name: string
  phone: string
  temperature: LeadTemperature
}

type LeadRow = {
  id: string
  nome: string
  telefone: string
  temperatura: LeadTemperature
  status_coluna: string
}

const COLUMN_ORDER: ColumnId[] = [
  'novo',
  'atendimento',
  'negociacao',
  'fechado',
]

const COLUMN_TITLES: Record<ColumnId, string> = {
  novo: 'Novo Lead',
  atendimento: 'Em Atendimento',
  negociacao: 'Negociação',
  fechado: 'Fechado',
}

function emptyColumns(): Record<ColumnId, string[]> {
  return {
    novo: [],
    atendimento: [],
    negociacao: [],
    fechado: [],
  }
}

function isColumnId(v: string): v is ColumnId {
  return COLUMN_ORDER.includes(v as ColumnId)
}

function isTemperature(v: string): v is LeadTemperature {
  return v === 'frio' || v === 'morno' || v === 'quente'
}

function rowToLead(row: LeadRow): Lead | null {
  if (!isTemperature(row.temperatura)) return null
  return {
    id: row.id,
    name: row.nome,
    phone: row.telefone,
    temperature: row.temperatura,
  }
}

function groupRowsIntoBoard(rows: LeadRow[]): {
  columns: Record<ColumnId, string[]>
  leadsMap: Record<string, Lead>
} {
  const columns = emptyColumns()
  const leadsMap: Record<string, Lead> = {}

  for (const row of rows) {
    const lead = rowToLead(row)
    if (!lead) continue
    const col = isColumnId(row.status_coluna) ? row.status_coluna : 'novo'
    leadsMap[lead.id] = lead
    columns[col].push(lead.id)
  }

  return { columns, leadsMap }
}

function findContainer(
  items: Record<ColumnId, string[]>,
  id: UniqueIdentifier,
): ColumnId | undefined {
  const s = String(id)
  if (COLUMN_ORDER.includes(s as ColumnId)) {
    return s as ColumnId
  }
  for (const col of COLUMN_ORDER) {
    if (items[col].includes(s)) return col
  }
  return undefined
}

function revertLeadToColumn(
  prev: Record<ColumnId, string[]>,
  leadId: string,
  fromCol: ColumnId,
  toCol: ColumnId,
): Record<ColumnId, string[]> {
  const from = [...prev[fromCol]]
  const to = [...prev[toCol]]
  const idx = from.indexOf(leadId)
  if (idx === -1) return prev
  from.splice(idx, 1)
  to.push(leadId)
  return { ...prev, [fromCol]: from, [toCol]: to }
}

const tempStyles: Record<
  LeadTemperature,
  { label: string; className: string }
> = {
  frio: {
    label: 'Frio',
    className:
      'bg-sky-100 text-sky-800 ring-1 ring-sky-200/80 dark:bg-sky-950/40 dark:text-sky-200 dark:ring-sky-800',
  },
  morno: {
    label: 'Morno',
    className:
      'bg-amber-100 text-amber-900 ring-1 ring-amber-200/80 dark:bg-amber-950/40 dark:text-amber-200 dark:ring-amber-800',
  },
  quente: {
    label: 'Quente',
    className:
      'bg-rose-100 text-rose-800 ring-1 ring-rose-200/80 dark:bg-rose-950/40 dark:text-rose-200 dark:ring-rose-900',
  },
}

function LeadCardFace({ lead }: { lead: Lead }) {
  const temp = tempStyles[lead.temperature]
  return (
    <article className="rounded-xl border border-zinc-200/90 bg-white p-4 shadow-sm ring-1 ring-zinc-100/80 transition hover:border-zinc-300 hover:shadow-md">
      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <h3 className="text-sm font-semibold tracking-tight text-zinc-900">
            {lead.name}
          </h3>
          <span
            className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${temp.className}`}
          >
            {temp.label}
          </span>
        </div>
        <p className="mt-2 flex items-center gap-1.5 text-xs text-zinc-500">
          <Phone className="h-3.5 w-3.5 shrink-0 text-emerald-500" aria-hidden />
          <span className="truncate tabular-nums text-zinc-600">{lead.phone}</span>
          <span className="text-[10px] font-medium uppercase tracking-wide text-zinc-400">
            WhatsApp
          </span>
        </p>
      </div>
    </article>
  )
}

function SortableLeadCard({ lead }: { lead: Lead }) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: lead.id })

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  }

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={`touch-none ${isDragging ? 'z-10 opacity-50' : 'cursor-grab active:cursor-grabbing'}`}
      {...attributes}
      {...listeners}
    >
      <LeadCardFace lead={lead} />
    </div>
  )
}

const dropAnimation: DropAnimation = {
  sideEffects: defaultDropAnimationSideEffects({
    styles: { active: { opacity: '0.5' } },
  }),
}

type DragStartInfo = { leadId: string; column: ColumnId }

export function CrmKanbanBoard() {
  const [columns, setColumns] = useState<Record<ColumnId, string[]>>(emptyColumns)
  const [leadsMap, setLeadsMap] = useState<Record<string, Lead>>({})
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [activeId, setActiveId] = useState<UniqueIdentifier | null>(null)
  const [modalOpen, setModalOpen] = useState(false)
  const [persistError, setPersistError] = useState<string | null>(null)

  const columnsRef = useRef(columns)
  columnsRef.current = columns

  const dragStartRef = useRef<DragStartInfo | null>(null)
  const columnsSnapshotRef = useRef<Record<ColumnId, string[]> | null>(null)

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  )

  const activeLead = activeId ? leadsMap[String(activeId)] : null

  const fetchLeads = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    const { data, error } = await supabase
      .from('leads')
      .select('id, nome, telefone, temperatura, status_coluna')
      .order('created_at', { ascending: true })

    if (error) {
      setLoadError('Não foi possível carregar os leads. Tente de novo.')
      setLoading(false)
      return
    }

    const rows = (data ?? []) as LeadRow[]
    const { columns: nextCols, leadsMap: nextLeads } = groupRowsIntoBoard(rows)
    columnsRef.current = nextCols
    setColumns(nextCols)
    setLeadsMap(nextLeads)
    setLoading(false)
  }, [])

  useEffect(() => {
    void fetchLeads()
  }, [fetchLeads])

  const handleDragOver = useCallback((event: DragOverEvent) => {
    const { active, over } = event
    if (!over || active.id === over.id) return

    setColumns((prev) => {
      const activeContainer = findContainer(prev, active.id)
      const overContainer = findContainer(prev, over.id)

      if (!activeContainer || !overContainer || activeContainer === overContainer) {
        return prev
      }

      const activeItems = [...prev[activeContainer]]
      const overItems = [...prev[overContainer]]
      const activeIndex = activeItems.indexOf(String(active.id))
      if (activeIndex === -1) return prev

      let newIndex: number
      if (COLUMN_ORDER.includes(String(over.id) as ColumnId)) {
        newIndex = overItems.length
      } else {
        const overIndex = overItems.indexOf(String(over.id))
        newIndex = overIndex >= 0 ? overIndex : overItems.length
      }

      const [moved] = activeItems.splice(activeIndex, 1)
      overItems.splice(newIndex, 0, moved)

      const next = {
        ...prev,
        [activeContainer]: activeItems,
        [overContainer]: overItems,
      }
      columnsRef.current = next
      return next
    })
  }, [])

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event
    setActiveId(null)

    const dragStart = dragStartRef.current
    dragStartRef.current = null

    if (!over) {
      if (columnsSnapshotRef.current) {
        const snap = columnsSnapshotRef.current
        columnsRef.current = snap
        setColumns(snap)
      }
      columnsSnapshotRef.current = null
      return
    }

    setColumns((prev) => {
      const activeContainer = findContainer(prev, active.id)
      const overContainer = findContainer(prev, over.id)

      if (!activeContainer || !overContainer) {
        columnsRef.current = prev
        return prev
      }

      if (activeContainer === overContainer) {
        const ordered = prev[activeContainer]
        const oldIndex = ordered.indexOf(String(active.id))
        if (oldIndex === -1) {
          columnsRef.current = prev
          return prev
        }

        let newIndex: number
        if (COLUMN_ORDER.includes(String(over.id) as ColumnId)) {
          newIndex = ordered.length - 1
        } else {
          newIndex = ordered.indexOf(String(over.id))
        }

        if (newIndex === -1 || oldIndex === newIndex) {
          columnsRef.current = prev
          return prev
        }

        const next = {
          ...prev,
          [activeContainer]: arrayMove(prev[activeContainer], oldIndex, newIndex),
        }
        columnsRef.current = next
        return next
      }

      columnsRef.current = prev
      return prev
    })

    columnsSnapshotRef.current = null

    if (!dragStart) return

    const endCol = findContainer(columnsRef.current, dragStart.leadId)
    if (!endCol || dragStart.column === endCol) return

    void (async () => {
      const { error } = await supabase
        .from('leads')
        .update({
          status_coluna: endCol,
          updated_at: new Date().toISOString(),
        })
        .eq('id', dragStart.leadId)

      if (error) {
        setPersistError('Não foi possível salvar a coluna. Desfazendo movimento.')
        setColumns((prev) => {
          const next = revertLeadToColumn(
            prev,
            dragStart.leadId,
            endCol,
            dragStart.column,
          )
          columnsRef.current = next
          return next
        })
        setTimeout(() => setPersistError(null), 5000)
      }
    })()
  }, [])

  const handleDragStart = useCallback((event: DragStartEvent) => {
    setActiveId(event.active.id)
    columnsSnapshotRef.current = structuredClone(columnsRef.current)
    const col = findContainer(columnsRef.current, event.active.id)
    if (col) {
      dragStartRef.current = { leadId: String(event.active.id), column: col }
    }
  }, [])

  const handleDragCancel = useCallback(() => {
    setActiveId(null)
    dragStartRef.current = null
    if (columnsSnapshotRef.current) {
      const snap = columnsSnapshotRef.current
      columnsRef.current = snap
      setColumns(snap)
    }
    columnsSnapshotRef.current = null
  }, [])

  const handleSaveNewLead = useCallback(async (nome: string, telefone: string) => {
    const {
      data: { user },
      error: userError,
    } = await supabase.auth.getUser()

    if (userError || !user) {
      return { error: 'Sessão inválida. Entre novamente.' }
    }

    const { data, error } = await supabase
      .from('leads')
      .insert({
        nome,
        telefone,
        temperatura: 'frio',
        status_coluna: 'novo',
        user_id: user.id,
      })
      .select('id, nome, telefone, temperatura, status_coluna')
      .single()

    if (error || !data) {
      return { error: error?.message ?? 'Erro ao criar lead.' }
    }

    const row = data as LeadRow
    const lead = rowToLead(row)
    if (!lead) {
      return { error: 'Resposta inválida do servidor.' }
    }

    setLeadsMap((prev) => ({ ...prev, [lead.id]: lead }))
    setColumns((prev) => {
      const next = {
        ...prev,
        novo: [...prev.novo, lead.id],
      }
      columnsRef.current = next
      return next
    })

    return { error: null }
  }, [])

  if (loading) {
    return (
      <div className="flex min-h-[min(50vh,400px)] flex-col items-center justify-center gap-3 rounded-2xl border border-zinc-200/80 bg-zinc-50/80">
        <div className="h-9 w-9 animate-spin rounded-full border-2 border-brand-600 border-t-transparent" />
        <p className="text-sm font-medium text-zinc-500">Carregando leads…</p>
      </div>
    )
  }

  if (loadError) {
    return (
      <div className="flex min-h-[min(50vh,400px)] flex-col items-center justify-center gap-4 rounded-2xl border border-zinc-200 bg-white p-8 text-center shadow-sm">
        <p className="text-sm text-zinc-600">{loadError}</p>
        <button
          type="button"
          onClick={() => void fetchLeads()}
          className="rounded-xl bg-zinc-900 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-zinc-800"
        >
          Tentar novamente
        </button>
      </div>
    )
  }

  return (
    <>
      {persistError ? (
        <div
          role="alert"
          className="mb-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-900"
        >
          {persistError}
        </div>
      ) : null}

      <DndContext
        sensors={sensors}
        collisionDetection={closestCorners}
        onDragStart={handleDragStart}
        onDragOver={handleDragOver}
        onDragEnd={handleDragEnd}
        onDragCancel={handleDragCancel}
      >
        <div className="-mx-1 flex min-h-[min(70vh,640px)] gap-4 overflow-x-auto overflow-y-hidden pb-2 pt-1">
          {COLUMN_ORDER.map((colId) => {
            const ids = columns[colId]
            return (
              <KanbanColumn
                key={colId}
                id={colId}
                title={COLUMN_TITLES[colId]}
                headerAction={
                  colId === 'novo' ? (
                    <button
                      type="button"
                      onClick={() => setModalOpen(true)}
                      className="inline-flex shrink-0 items-center rounded-lg border border-zinc-200/90 bg-white px-2.5 py-1 text-xs font-semibold tabular-nums text-zinc-600 shadow-sm transition hover:border-brand-200 hover:text-brand-700"
                    >
                      + Novo Lead
                    </button>
                  ) : null
                }
              >
                <SortableContext items={ids} strategy={verticalListSortingStrategy}>
                  <div className="flex flex-col gap-3">
                    {ids.length === 0 ? (
                      <p className="rounded-lg border border-dashed border-zinc-300/80 py-6 text-center text-xs text-zinc-400">
                        Nenhum lead aqui
                      </p>
                    ) : null}
                    {ids.map((id) => {
                      const lead = leadsMap[id]
                      if (!lead) return null
                      return <SortableLeadCard key={id} lead={lead} />
                    })}
                  </div>
                </SortableContext>
              </KanbanColumn>
            )
          })}
        </div>

        <DragOverlay dropAnimation={dropAnimation}>
          {activeLead ? (
            <div className="w-[min(100vw-2rem,280px)] cursor-grabbing shadow-2xl">
              <LeadCardFace lead={activeLead} />
            </div>
          ) : null}
        </DragOverlay>
      </DndContext>

      <NewLeadModal
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        onSave={handleSaveNewLead}
      />
    </>
  )
}

function KanbanColumn({
  id,
  title,
  headerAction,
  children,
}: {
  id: ColumnId
  title: string
  headerAction?: ReactNode
  children: ReactNode
}) {
  const { setNodeRef } = useDroppable({ id })

  return (
    <section
      className="flex w-[min(100vw-2rem,280px)] shrink-0 flex-col rounded-2xl border border-zinc-200/80 bg-zinc-100/90 shadow-inner ring-1 ring-zinc-200/40"
      aria-labelledby={`col-title-${id}`}
    >
      <header className="flex items-center justify-between gap-2 border-b border-zinc-200/80 px-4 py-3">
        <h2
          id={`col-title-${id}`}
          className="min-w-0 text-xs font-semibold uppercase tracking-wider text-zinc-500"
        >
          {title}
        </h2>
        {headerAction}
      </header>
      <div
        ref={setNodeRef}
        className="flex flex-1 flex-col gap-3 overflow-y-auto p-3"
        style={{ minHeight: '120px' }}
      >
        {children}
      </div>
    </section>
  )
}
