import { createClient, type SupabaseClient } from '@supabase/supabase-js'
import type { EvolutionHttpConfig } from './evolution'
import {
  sendAudioMessageWithConfig,
  sendImageMessageWithConfig,
  sendTextMessageWithConfig,
} from './evolution'

export type WorkerDeps = {
  supabase: SupabaseClient
  evolution: EvolutionHttpConfig
}

/**
 * Lê `CHAVE_MESTRA_ZAPIFICA` do ambiente (Node/scripts). Mesmo segredo da Edge Function.
 */
function readChaveMestraZapificaFromEnv(): string {
  const g = globalThis as unknown as {
    process?: { env?: Record<string, string | undefined> }
  }
  return (g.process?.env?.CHAVE_MESTRA_ZAPIFICA ?? '').trim()
}

/**
 * Cliente Supabase com bypass RLS, para rodar o worker fora da Edge (cron local, script).
 * Exige `CHAVE_MESTRA_ZAPIFICA` no ambiente (normalmente a service role do projeto).
 */
export function createSupabaseComChaveMestraZapifica(
  supabaseUrl: string,
): SupabaseClient {
  const key = readChaveMestraZapificaFromEnv()
  if (!key) {
    throw new Error(
      'Defina CHAVE_MESTRA_ZAPIFICA no ambiente (mesmo valor configurado na Edge Function).',
    )
  }
  return createClient(supabaseUrl.trim(), key, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
  })
}

export type WorkerRunSummary = {
  /** Linhas reivindicadas e processadas (tentativa de envio) */
  processed: number
  /** Falhas ao reivindicar ou erros não recuperáveis */
  skipped: number
}

type ScheduledRow = {
  id: string
  user_id: string
  recipient_type: 'personal' | 'segment'
  content_type: 'text' | 'audio' | 'image'
  message_body: string | null
  segment_lead_ids: string[] | null
}

const BATCH_LIMIT = 30

function pickPersonalRawFromUser(user: {
  phone?: string
  user_metadata?: Record<string, unknown>
}): string | null {
  const meta = user.user_metadata ?? {}
  const fromMeta =
    (typeof meta.whatsapp === 'string' && meta.whatsapp) ||
    (typeof meta.phone === 'string' && meta.phone) ||
    null
  if (user.phone?.trim()) return user.phone.trim()
  if (fromMeta?.trim()) return fromMeta.trim()
  return null
}

async function resolveRecipientPhones(
  supabase: SupabaseClient,
  row: ScheduledRow,
): Promise<{ targets: string[]; error: string | null }> {
  if (row.recipient_type === 'personal') {
    const { data, error } = await supabase.auth.admin.getUserById(row.user_id)
    if (error || !data?.user) {
      return {
        targets: [],
        error: error?.message ?? 'Usuário não encontrado para lembrete pessoal.',
      }
    }
    const raw = pickPersonalRawFromUser(data.user)
    if (!raw) {
      return {
        targets: [],
        error:
          'Nenhum telefone no perfil (phone ou user_metadata.whatsapp). Configure antes de agendar.',
      }
    }
    return { targets: [raw], error: null }
  }

  const ids = row.segment_lead_ids ?? []
  if (ids.length === 0) {
    return { targets: [], error: 'Segmento sem clientes selecionados.' }
  }

  const { data: leads, error } = await supabase
    .from('leads')
    .select('id, telefone')
    .eq('user_id', row.user_id)
    .in('id', ids)

  if (error) {
    return { targets: [], error: error.message }
  }

  const targets = (leads ?? [])
    .map((l: { telefone: string | null }) => (l.telefone ?? '').trim())
    .filter(Boolean)

  if (targets.length === 0) {
    return {
      targets: [],
      error: 'Nenhum telefone válido nos leads selecionados.',
    }
  }

  return { targets, error: null }
}

async function sendOne(
  deps: WorkerDeps,
  row: ScheduledRow,
  recipient: string,
): Promise<{ ok: boolean; error: string | null; messageId: string | null }> {
  const { evolution } = deps
  const uid = row.user_id
  const body = row.message_body ?? ''

  switch (row.content_type) {
    case 'text':
      return sendTextMessageWithConfig(uid, recipient, body, evolution)
    case 'audio':
      return sendAudioMessageWithConfig(uid, recipient, body, evolution)
    case 'image':
      return sendImageMessageWithConfig(uid, recipient, body, '', evolution)
    default:
      return { ok: false, error: 'Tipo de conteúdo desconhecido.', messageId: null }
  }
}

/**
 * Processa a fila de `scheduled_messages` prontas para envio.
 * Não interrompe o lote inteiro se uma mensagem falhar — cada linha é isolada.
 */
export async function checkAndSendScheduledMessages(
  deps: WorkerDeps,
): Promise<WorkerRunSummary> {
  const { supabase } = deps
  const nowUtcIso = new Date().toISOString()
  console.log(
    '[worker] Buscando mensagens pending com scheduled_at <=',
    nowUtcIso,
    '| filtros: .eq("status","pending"), .eq("is_active", true boolean)',
  )

  const { data, error } = await supabase
    .from('scheduled_messages')
    .select(
      'id, user_id, recipient_type, content_type, message_body, segment_lead_ids',
    )
    .eq('status', 'pending')
    .eq('is_active', true)
    .lte('scheduled_at', nowUtcIso)
    .order('scheduled_at', { ascending: true })
    .limit(BATCH_LIMIT)

  const candidates = data
  console.log(
    '[worker] Resultado da busca:',
    candidates?.length ?? 0,
    'linhas encontradas. Erro do banco:',
    error,
  )

  if (error) {
    console.error('[worker] Falha ao listar agendamentos:', error.message)
    return { processed: 0, skipped: 1 }
  }

  let processed = 0
  let skipped = 0

  for (const raw of candidates ?? []) {
    const row = raw as ScheduledRow

    const { data: claimed, error: claimErr } = await supabase
      .from('scheduled_messages')
      .update({
        status: 'processing',
        updated_at: new Date().toISOString(),
      })
      .eq('id', row.id)
      .eq('status', 'pending')
      .select('id')
      .maybeSingle()

    if (claimErr || !claimed) {
      skipped += 1
      continue
    }

    try {
      const { targets, error: resolveErr } = await resolveRecipientPhones(
        supabase,
        row,
      )
      if (resolveErr || targets.length === 0) {
        await supabase
          .from('scheduled_messages')
          .update({
            status: 'error',
            last_error: resolveErr ?? 'Sem destinatários.',
            updated_at: new Date().toISOString(),
          })
          .eq('id', row.id)
        processed += 1
        continue
      }

      const sendResults: {
        ok: boolean
        error: string | null
        messageId: string | null
      }[] = []

      for (const recipient of targets) {
        try {
          const r = await sendOne(deps, row, recipient)
          sendResults.push(r)
        } catch (e) {
          const msg = e instanceof Error ? e.message : String(e)
          sendResults.push({ ok: false, error: msg, messageId: null })
        }
      }

      const oks = sendResults.filter((r) => r.ok)
      const fails = sendResults.filter((r) => !r.ok)

      if (oks.length === sendResults.length) {
        const ids = oks
          .map((r) => r.messageId)
          .filter((x): x is string => Boolean(x))
        await supabase
          .from('scheduled_messages')
          .update({
            status: 'sent',
            evolution_message_id: ids.length ? ids.join(',') : null,
            last_error: null,
            updated_at: new Date().toISOString(),
          })
          .eq('id', row.id)
      } else if (oks.length > 0) {
        const failText = fails
          .map((f) => f.error ?? 'Erro desconhecido')
          .join(' | ')
        await supabase
          .from('scheduled_messages')
          .update({
            status: 'error',
            evolution_message_id: oks
              .map((r) => r.messageId)
              .filter(Boolean)
              .join(','),
            last_error: `Envio parcial: ${oks.length} ok, ${fails.length} falha(s). ${failText}`,
            updated_at: new Date().toISOString(),
          })
          .eq('id', row.id)
      } else {
        const failText = fails
          .map((f) => f.error ?? 'Erro desconhecido')
          .join(' | ')
        await supabase
          .from('scheduled_messages')
          .update({
            status: 'error',
            evolution_message_id: null,
            last_error: failText.slice(0, 4000),
            updated_at: new Date().toISOString(),
          })
          .eq('id', row.id)
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      await supabase
        .from('scheduled_messages')
        .update({
          status: 'error',
          last_error: `Exceção no worker: ${msg}`.slice(0, 4000),
          updated_at: new Date().toISOString(),
        })
        .eq('id', row.id)
    }

    processed += 1
  }

  return { processed, skipped }
}
