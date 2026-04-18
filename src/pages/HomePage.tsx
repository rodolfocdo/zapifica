import { MessageSquare, TrendingUp, Users } from 'lucide-react'
import { StatCard } from '../components/StatCard'

export function HomePage() {
  return (
    <div className="space-y-8">
      <section className="rounded-2xl border border-zinc-200/80 bg-gradient-to-br from-white via-white to-brand-50/80 p-6 shadow-sm lg:p-8">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <p className="text-sm font-medium text-brand-600">Painel de controle</p>
            <h2 className="mt-1 text-2xl font-semibold tracking-tight text-zinc-900">
              Olá, equipe Zapifica
            </h2>
            <p className="mt-2 max-w-xl text-sm leading-relaxed text-zinc-600">
              Acompanhe leads, mensagens e engajamento em um só lugar. Os números
              abaixo são exemplos para demonstrar o layout.
            </p>
          </div>
          <div className="flex gap-2">
            <span className="inline-flex items-center rounded-full bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 ring-1 ring-emerald-600/15">
              Tudo operacional
            </span>
            <span className="inline-flex items-center rounded-full bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-600">
              Última sincronização: agora
            </span>
          </div>
        </div>
      </section>

      <section>
        <h3 className="mb-4 text-sm font-semibold uppercase tracking-wide text-zinc-500">
          Indicadores principais
        </h3>
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          <StatCard
            title="Leads capturados"
            value="1.284"
            subtitle="Últimos 30 dias"
            icon={Users}
            trend="+12,4% vs. período anterior"
          />
          <StatCard
            title="Mensagens enviadas"
            value="18.432"
            subtitle="Campanhas e respostas automáticas"
            icon={MessageSquare}
            trend="+8,1% vs. período anterior"
          />
          <StatCard
            title="Taxa de resposta"
            value="94,2%"
            subtitle="Conversas com resposta em menos de 5 min"
            icon={TrendingUp}
            trend="+2,0 p.p. vs. período anterior"
          />
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-2xl border border-zinc-200/80 bg-white p-6 shadow-sm">
          <h3 className="text-base font-semibold text-zinc-900">
            Próximos passos
          </h3>
          <p className="mt-1 text-sm text-zinc-600">
            Sugestões para maximizar seu funil conversacional.
          </p>
          <ul className="mt-5 space-y-3 text-sm text-zinc-700">
            <li className="flex gap-3 rounded-xl bg-zinc-50 px-3 py-2.5 ring-1 ring-zinc-100">
              <span className="mt-0.5 h-2 w-2 shrink-0 rounded-full bg-brand-600" />
              Conecte o Zap Voice a uma fila de atendimento humana.
            </li>
            <li className="flex gap-3 rounded-xl bg-zinc-50 px-3 py-2.5 ring-1 ring-zinc-100">
              <span className="mt-0.5 h-2 w-2 shrink-0 rounded-full bg-brand-green" />
              Crie um fluxo de boas-vindas para novos leads do CRM.
            </li>
            <li className="flex gap-3 rounded-xl bg-zinc-50 px-3 py-2.5 ring-1 ring-zinc-100">
              <span className="mt-0.5 h-2 w-2 shrink-0 rounded-full bg-zinc-400" />
              Revise tags e segmentos antes da próxima campanha.
            </li>
          </ul>
        </div>
        <div className="rounded-2xl border border-zinc-200/80 bg-zinc-900 p-6 text-zinc-100 shadow-sm ring-1 ring-zinc-800">
          <h3 className="text-base font-semibold text-white">
            Estado dos canais
          </h3>
          <p className="mt-1 text-sm text-zinc-400">
            WhatsApp Business API e webhooks.
          </p>
          <dl className="mt-6 space-y-4">
            <div className="flex items-center justify-between gap-4 border-b border-zinc-800 pb-4">
              <dt className="text-sm text-zinc-400">WhatsApp</dt>
              <dd className="text-sm font-medium text-emerald-500">Conectado</dd>
            </div>
            <div className="flex items-center justify-between gap-4 border-b border-zinc-800 pb-4">
              <dt className="text-sm text-zinc-400">Webhooks</dt>
              <dd className="text-sm font-medium text-emerald-500">Ativos</dd>
            </div>
            <div className="flex items-center justify-between gap-4">
              <dt className="text-sm text-zinc-400">Fila de mensagens</dt>
              <dd className="text-sm font-medium text-zinc-200">Normal</dd>
            </div>
          </dl>
        </div>
      </section>
    </div>
  )
}
