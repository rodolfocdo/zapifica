-- Campos do worker Evolution + status intermediário / erro explícito

alter table public.scheduled_messages
  add column if not exists evolution_message_id text,
  add column if not exists last_error text;

alter table public.scheduled_messages
  drop constraint if exists scheduled_messages_status_check;

alter table public.scheduled_messages
  add constraint scheduled_messages_status_check
  check (
    status in (
      'pending',
      'processing',
      'sent',
      'failed',
      'error',
      'cancelled'
    )
  );

comment on column public.scheduled_messages.evolution_message_id is
  'Identificador da mensagem retornado pela Evolution API (campo key.id).';
comment on column public.scheduled_messages.last_error is
  'Motivo da falha quando status = error ou failed.';
