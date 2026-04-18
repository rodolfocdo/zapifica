-- Corrige leitura da Agenda no app: GRANT + políticas SELECT explícitas para `authenticated`.
-- (Evita falhas quando as políticas padrão não batem com o JWT ou com embeds.)

grant usage on schema public to authenticated;

grant select on table public.events to authenticated;
grant select on table public.scheduled_messages to authenticated;

drop policy if exists "events_select_own" on public.events;

create policy "events_select_own"
  on public.events
  for select
  to authenticated
  using ( (select auth.uid()) = user_id );

drop policy if exists "scheduled_messages_select_own" on public.scheduled_messages;

-- Permite ler disparos do próprio usuário OU ligados a eventos seus (embed na listagem).
create policy "scheduled_messages_select_own"
  on public.scheduled_messages
  for select
  to authenticated
  using (
    (select auth.uid()) = user_id
    or exists (
      select 1
      from public.events e
      where e.id = scheduled_messages.event_id
        and e.user_id = (select auth.uid())
    )
  );
