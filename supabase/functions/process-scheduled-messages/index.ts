/**
 * Edge Function: processa disparos agendados (Evolution API).
 * Agende no Supabase Dashboard → Edge Functions → Schedules (a cada 1 minuto).
 *
 * Variáveis de ambiente (segredos da função):
 * - SUPABASE_URL (preenchido automaticamente)
 * - SUPABASE_SERVICE_ROLE_KEY
 * - EVOLUTION_URL (URL base da Evolution, sem barra final)
 * - EVOLUTION_API_KEY (header apikey)
 * - CRON_SECRET (opcional: envie header x-cron-secret igual ao valor)
 *
 * A lógica espelha `src/services/worker.ts` e `src/services/evolution.ts`.
 */
import 'jsr:@supabase/functions-js/edge-runtime.d.ts'
import {
  createClient,
  type SupabaseClient,
} from 'npm:@supabase/supabase-js@2.103.3'

type EvolutionHttpConfig = { baseUrl: string; apiKey: string }

type WorkerRunSummary = { processed: number; skipped: number }

type SendEvolutionResult = {
  ok: boolean
  error: string | null
  messageId: string | null
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

function trimBaseUrl(raw: string): string {
  return raw.replace(/\/+$/, '')
}

function instanceNameFromUserId(userId: string): string {
  const safe = userId.replace(/[^a-zA-Z0-9-]/g, '_')
  return `zapifica_${safe}`.slice(0, 80)
}

function normalizeEvolutionRecipient(
  raw: string,
): { recipient: string } | { error: string } {
  const t = raw.trim()
  if (!t) return { error: 'Destinatário vazio.' }
  if (t.includes('@g.us')) return { recipient: t }
  if (t.includes('@s.whatsapp.net')) {
    const before = t.split('@')[0] ?? ''
    const digits = before.replace(/\D/g, '')
    if (digits.length < 10) return { error: 'JID individual inválido.' }
    return { recipient: digits }
  }
  if (t.includes('@') && !/^\d/.test(t)) return { recipient: t }
  const digits = t.replace(/\D/g, '')
  if (!digits || digits.length < 10) {
    return {
      error:
        'Número ou grupo inválido (use DDI+DDD+número ou ID @g.us).',
    }
  }
  return { recipient: digits }
}

function formatHttpError(status: number, body: unknown): string {
  if (body && typeof body === 'object') {
    const b = body as Record<string, unknown>
    const msg = b.message
    if (Array.isArray(msg) && msg.length) return String(msg[0])
    if (typeof b.error === 'string') return b.error
    const resp = b.response as Record<string, unknown> | undefined
    if (resp && Array.isArray(resp.message) && resp.message.length) {
      return String(resp.message[0])
    }
  }
  return `Erro HTTP ${status} na Evolution API.`
}

function extractEvolutionMessageId(data: unknown): string | null {
  if (!data || typeof data !== 'object') return null
  const o = data as Record<string, unknown>
  const key = o.key
  if (key && typeof key === 'object') {
    const id = (key as Record<string, unknown>).id
    if (typeof id === 'string' && id.length > 0) return id
  }
  return null
}

async function evolutionFetch(
  cfg: EvolutionHttpConfig,
  path: string,
  init: RequestInit = {},
): Promise<{ ok: boolean; status: number; data: unknown }> {
  const url = `${cfg.baseUrl}${path.startsWith('/') ? path : `/${path}`}`
  const res = await fetch(url, {
    ...init,
    headers: {
      apikey: cfg.apiKey,
      Accept: 'application/json',
      ...(init.headers as Record<string, string>),
    },
  })
  let data: unknown = null
  const text = await res.text()
  if (text) {
    try {
      data = JSON.parse(text) as unknown
    } catch {
      data = { raw: text }
    }
  }
  return { ok: res.ok, status: res.status, data }
}

async function sendTextMessageWithConfig(
  userId: string,
  number: string,
  text: string,
  cfg: EvolutionHttpConfig,
): Promise<SendEvolutionResult> {
  const normalized = normalizeEvolutionRecipient(number)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }
  const trimmedText = text.trim()
  if (!trimmedText) {
    return { ok: false, error: 'Mensagem de texto vazia.', messageId: null }
  }
  const instanceName = instanceNameFromUserId(userId)
  const res = await evolutionFetch(
    cfg,
    `/message/sendText/${encodeURIComponent(instanceName)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        number: normalized.recipient,
        text: trimmedText,
      }),
    },
  )
  if (!res.ok) {
    return {
      ok: false,
      error: formatHttpError(res.status, res.data),
      messageId: null,
    }
  }
  return {
    ok: true,
    error: null,
    messageId: extractEvolutionMessageId(res.data),
  }
}

async function sendAudioMessageWithConfig(
  userId: string,
  recipient: string,
  audio: string,
  cfg: EvolutionHttpConfig,
): Promise<SendEvolutionResult> {
  const normalized = normalizeEvolutionRecipient(recipient)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }
  const trimmed = audio.trim()
  if (!trimmed) {
    return { ok: false, error: 'URL ou base64 do áudio vazio.', messageId: null }
  }
  const instanceName = instanceNameFromUserId(userId)
  const res = await evolutionFetch(
    cfg,
    `/message/sendWhatsAppAudio/${encodeURIComponent(instanceName)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        number: normalized.recipient,
        audio: trimmed,
      }),
    },
  )
  if (!res.ok) {
    return {
      ok: false,
      error: formatHttpError(res.status, res.data),
      messageId: null,
    }
  }
  return {
    ok: true,
    error: null,
    messageId: extractEvolutionMessageId(res.data),
  }
}

async function sendImageMessageWithConfig(
  userId: string,
  recipient: string,
  media: string,
  cfg: EvolutionHttpConfig,
): Promise<SendEvolutionResult> {
  const normalized = normalizeEvolutionRecipient(recipient)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }
  const trimmed = media.trim()
  if (!trimmed) {
    return { ok: false, error: 'URL ou base64 da imagem vazio.', messageId: null }
  }
  const instanceName = instanceNameFromUserId(userId)
  const isData = trimmed.startsWith('data:')
  const mimetype = isData
    ? trimmed.split(';')[0]?.replace('data:', '') || 'image/png'
    : 'image/jpeg'
  const fileName =
    mimetype.includes('png')
      ? 'imagem.png'
      : mimetype.includes('webp')
        ? 'imagem.webp'
        : 'imagem.jpg'

  const res = await evolutionFetch(
    cfg,
    `/message/sendMedia/${encodeURIComponent(instanceName)}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        number: normalized.recipient,
        mediatype: 'image',
        mimetype,
        caption: ' ',
        media: trimmed,
        fileName,
      }),
    },
  )
  if (!res.ok) {
    return {
      ok: false,
      error: formatHttpError(res.status, res.data),
      messageId: null,
    }
  }
  return {
    ok: true,
    error: null,
    messageId: extractEvolutionMessageId(res.data),
  }
}

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
        error: error?.message ?? 'Usuário não encontrado.',
      }
    }
    const raw = pickPersonalRawFromUser(data.user)
    if (!raw) {
      return {
        targets: [],
        error:
          'Telefone não configurado no perfil (phone ou user_metadata.whatsapp).',
      }
    }
    return { targets: [raw], error: null }
  }

  const ids = row.segment_lead_ids ?? []
  if (ids.length === 0) {
    return { targets: [], error: 'Segmento vazio.' }
  }

  const { data: leads, error } = await supabase
    .from('leads')
    .select('id, telefone')
    .eq('user_id', row.user_id)
    .in('id', ids)

  if (error) return { targets: [], error: error.message }

  const targets = (leads ?? [])
    .map((l: { telefone: string | null }) => (l.telefone ?? '').trim())
    .filter(Boolean)

  if (targets.length === 0) {
    return { targets: [], error: 'Nenhum telefone válido nos leads.' }
  }
  return { targets, error: null }
}

async function sendOne(
  evolution: EvolutionHttpConfig,
  row: ScheduledRow,
  recipient: string,
): Promise<SendEvolutionResult> {
  const uid = row.user_id
  const body = row.message_body ?? ''
  switch (row.content_type) {
    case 'text':
      return sendTextMessageWithConfig(uid, recipient, body, evolution)
    case 'audio':
      return sendAudioMessageWithConfig(uid, recipient, body, evolution)
    case 'image':
      return sendImageMessageWithConfig(uid, recipient, body, evolution)
    default:
      return { ok: false, error: 'Tipo de conteúdo desconhecido.', messageId: null }
  }
}

async function checkAndSendScheduledMessages(
  supabase: SupabaseClient,
  evolution: EvolutionHttpConfig,
): Promise<WorkerRunSummary> {
  // `toISOString()` é sempre UTC (sufixo Z); `scheduled_at` no banco é timestamptz.
  const nowUtcIso = new Date().toISOString()
  console.log(
    '[worker] Agora UTC (comparação scheduled_at <=):',
    nowUtcIso,
  )

  const { data: candidates, error: fetchErr } = await supabase
    .from('scheduled_messages')
    .select(
      'id, user_id, recipient_type, content_type, message_body, segment_lead_ids',
    )
    .eq('is_active', true)
    .eq('status', 'pending')
    .lte('scheduled_at', nowUtcIso)
    .order('scheduled_at', { ascending: true })
    .limit(BATCH_LIMIT)

  if (fetchErr) {
    console.error('[worker] listar:', fetchErr.message)
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

      const sendResults: SendEvolutionResult[] = []
      for (const recipient of targets) {
        try {
          const r = await sendOne(evolution, row, recipient)
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
          .map((f) => f.error ?? 'Erro')
          .join(' | ')
        await supabase
          .from('scheduled_messages')
          .update({
            status: 'error',
            evolution_message_id: oks
              .map((r) => r.messageId)
              .filter(Boolean)
              .join(','),
            last_error: `Parcial: ${oks.length} ok, ${fails.length} falha(s). ${failText}`.slice(
              0,
              4000,
            ),
            updated_at: new Date().toISOString(),
          })
          .eq('id', row.id)
      } else {
        const failText = fails
          .map((f) => f.error ?? 'Erro')
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
          last_error: `Exceção: ${msg}`.slice(0, 4000),
          updated_at: new Date().toISOString(),
        })
        .eq('id', row.id)
    }

    processed += 1
  }

  return { processed, skipped }
}

Deno.serve(async (req) => {
  const cronSecret = Deno.env.get('CRON_SECRET')
  if (cronSecret) {
    const sent = req.headers.get('x-cron-secret')
    if (sent !== cronSecret) {
      return new Response(JSON.stringify({ erro: 'Não autorizado.' }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      })
    }
  }

  const supabaseUrl = Deno.env.get('SUPABASE_URL')
  const serviceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')
  const evoUrlRaw =
    Deno.env.get('EVOLUTION_URL') ?? Deno.env.get('VITE_EVOLUTION_URL') ?? ''
  const evoKey =
    Deno.env.get('EVOLUTION_API_KEY') ??
    Deno.env.get('EVOLUTION_GLOBAL_KEY') ??
    Deno.env.get('VITE_EVOLUTION_GLOBAL_KEY') ??
    ''

  if (!supabaseUrl || !serviceKey) {
    return new Response(
      JSON.stringify({
        erro: 'SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausentes.',
      }),
      { status: 500, headers: { 'Content-Type': 'application/json' } },
    )
  }

  if (!evoUrlRaw.trim() || !evoKey.trim()) {
    return new Response(
      JSON.stringify({
        erro:
          'Configure EVOLUTION_URL e EVOLUTION_API_KEY nos segredos da função.',
      }),
      { status: 500, headers: { 'Content-Type': 'application/json' } },
    )
  }

  const evolution: EvolutionHttpConfig = {
    baseUrl: trimBaseUrl(evoUrlRaw),
    apiKey: evoKey.trim(),
  }

  const supabase = createClient(supabaseUrl, serviceKey, {
    auth: { persistSession: false, autoRefreshToken: false },
  })

  try {
    const summary = await checkAndSendScheduledMessages(supabase, evolution)
    return new Response(JSON.stringify({ ok: true, ...summary }), {
      headers: { 'Content-Type': 'application/json' },
    })
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    console.error('[worker]', msg)
    return new Response(JSON.stringify({ ok: false, erro: msg }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    })
  }
})
