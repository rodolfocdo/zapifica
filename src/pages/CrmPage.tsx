import { CrmKanbanBoard } from '../components/crm/CrmKanbanBoard'

export function CrmPage() {
  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-xl font-semibold tracking-tight text-zinc-900">
          CRM / Funil
        </h2>
        <p className="mt-1 max-w-2xl text-sm leading-relaxed text-zinc-600">
          Organize leads em colunas, acompanhe temperatura e arraste cartões
          entre etapas. Em telas menores, use o scroll horizontal para ver todo
          o quadro.
        </p>
      </div>
      <CrmKanbanBoard />
    </div>
  )
}
