import type { LucideIcon } from 'lucide-react'

type StatCardProps = {
  title: string
  value: string
  subtitle?: string
  icon: LucideIcon
  trend?: string
  trendPositive?: boolean
}

export function StatCard({
  title,
  value,
  subtitle,
  icon: Icon,
  trend,
  trendPositive = true,
}: StatCardProps) {
  return (
    <article className="group relative overflow-hidden rounded-2xl border border-zinc-200/80 bg-white p-6 shadow-sm transition hover:border-brand-200/90 hover:shadow-md">
      <div className="pointer-events-none absolute -right-6 -top-6 h-28 w-28 rounded-full bg-gradient-to-br from-brand-600/12 to-brand-green/5 blur-2xl" />
      <div className="relative flex items-start justify-between gap-4">
        <div>
          <p className="text-sm font-medium text-zinc-500">{title}</p>
          <p className="mt-2 text-3xl font-semibold tracking-tight text-zinc-900">
            {value}
          </p>
          {subtitle ? (
            <p className="mt-1 text-sm text-zinc-500">{subtitle}</p>
          ) : null}
        </div>
        <span className="inline-flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-zinc-900 text-white shadow-inner ring-1 ring-white/10 transition group-hover:bg-brand-600">
          <Icon className="h-5 w-5" aria-hidden />
        </span>
      </div>
      {trend ? (
        <p
          className={`relative mt-4 text-sm font-medium ${
            trendPositive ? 'text-emerald-500' : 'text-rose-600'
          }`}
        >
          {trend}
        </p>
      ) : null}
    </article>
  )
}
