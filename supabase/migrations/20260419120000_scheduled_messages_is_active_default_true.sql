-- Lembretes sem is_active explícito no insert herdavam default false e o worker ignorava a fila.
alter table public.scheduled_messages
  alter column is_active set default true;

comment on column public.scheduled_messages.is_active is
  'Quando true, o worker (Edge) processa o agendamento. Padrão true para novos registros.';
