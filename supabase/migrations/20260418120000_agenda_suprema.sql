-- Agenda Suprema: eventos e fila de disparos Evolution API
-- Requer extensão pgcrypto para gen_random_uuid() (padrão Supabase)

create table if not exists public.events (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users (id) on delete cascade,
  title text not null,
  category text not null default 'reuniao',
  client_id uuid references public.leads (id) on delete set null,
  start_at timestamptz not null,
  end_at timestamptz not null,
  sync_kanban boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint events_end_after_start check (end_at > start_at)
);

create index if not exists events_user_start_idx
  on public.events (user_id, start_at);

create index if not exists events_user_time_overlap_idx
  on public.events (user_id, start_at, end_at);

create table if not exists public.scheduled_messages (
  id uuid primary key default gen_random_uuid(),
  event_id uuid not null references public.events (id) on delete cascade,
  user_id uuid not null references auth.users (id) on delete cascade,
  is_active boolean not null default false,
  recipient_type text not null
    check (recipient_type in ('personal', 'segment')),
  content_type text not null
    check (content_type in ('text', 'audio', 'image')),
  message_body text,
  scheduled_at timestamptz,
  status text not null default 'pending'
    check (status in ('pending', 'sent', 'failed', 'cancelled')),
  segment_lead_ids uuid[] not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint scheduled_messages_one_per_event unique (event_id)
);

create index if not exists scheduled_messages_user_scheduled_idx
  on public.scheduled_messages (user_id, scheduled_at)
  where is_active = true;

-- RLS
alter table public.events enable row level security;
alter table public.scheduled_messages enable row level security;

create policy "events_select_own"
  on public.events for select
  using (auth.uid() = user_id);

create policy "events_insert_own"
  on public.events for insert
  with check (auth.uid() = user_id);

create policy "events_update_own"
  on public.events for update
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

create policy "events_delete_own"
  on public.events for delete
  using (auth.uid() = user_id);

create policy "scheduled_messages_select_own"
  on public.scheduled_messages for select
  using (auth.uid() = user_id);

create policy "scheduled_messages_insert_own"
  on public.scheduled_messages for insert
  with check (auth.uid() = user_id);

create policy "scheduled_messages_update_own"
  on public.scheduled_messages for update
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

create policy "scheduled_messages_delete_own"
  on public.scheduled_messages for delete
  using (auth.uid() = user_id);
