-- Telefone explícito na fila (evita depender só de auth.users / metadata no worker)
alter table public.scheduled_messages
  add column if not exists recipient_phone text;

comment on column public.scheduled_messages.recipient_phone is
  'E.164 ou dígitos do destinatário para recipient_type = personal; preenchido pelo app no insert.';
