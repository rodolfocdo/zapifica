-- Cole este script no SQL Editor do Supabase (Dashboard > SQL > New query) e execute.
-- Cria a tabela de leads do funil Kanban com RLS: cada usuário vê apenas os próprios registros.

create table if not exists public.leads (
  id uuid primary key default gen_random_uuid(),
  nome text not null,
  telefone text not null,
  temperatura text not null
    check (temperatura in ('frio', 'morno', 'quente')),
  status_coluna text not null
    check (status_coluna in ('novo', 'atendimento', 'negociacao', 'fechado')),
  user_id uuid not null references auth.users (id) on delete cascade,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists leads_user_id_idx on public.leads (user_id);
create index if not exists leads_user_status_idx on public.leads (user_id, status_coluna);

alter table public.leads enable row level security;

create policy "leads_select_own"
  on public.leads
  for select
  to authenticated
  using (auth.uid() = user_id);

create policy "leads_insert_own"
  on public.leads
  for insert
  to authenticated
  with check (auth.uid() = user_id);

create policy "leads_update_own"
  on public.leads
  for update
  to authenticated
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

create policy "leads_delete_own"
  on public.leads
  for delete
  to authenticated
  using (auth.uid() = user_id);
