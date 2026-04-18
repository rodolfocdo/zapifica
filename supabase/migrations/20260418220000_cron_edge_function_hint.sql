-- Agendamento do worker (disparo a cada 1 minuto):
--
-- Opção recomendada (sem SQL): no painel Supabase → Edge Functions →
-- `process-scheduled-messages` → Schedules → criar cron com expressão `* * * * *`
-- (a cada minuto) apontando para a URL da função, com o header opcional
-- `x-cron-secret` igual ao segredo CRON_SECRET da função.
--
-- Opção alternativa: extensão pg_cron + pg_net para POST na URL da função
-- (requer URL e service role em vault — documentação Supabase).

comment on table public.scheduled_messages is
  'Fila de lembretes WhatsApp; processada pela Edge Function process-scheduled-messages.';
