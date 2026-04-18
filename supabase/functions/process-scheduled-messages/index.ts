/**
 * Edge Function: processa disparos agendados (Evolution API).
 * Agende no Supabase Dashboard → Edge Functions → Schedules (a cada 1 minuto).
 *
 * Variáveis de ambiente (segredos da função):
 * - SUPABASE_URL (URL do projeto)
 * - CHAVE_MESTRA_ZAPIFICA (chave com bypass RLS — ex.: service role; configure no painel da função)
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
  const nowUtcIso = new Date().toISOString()
  console.log(
    '[worker] Buscando mensagens pending com scheduled_at <=',
    nowUtcIso,
    '| filtros: status eq "pending" (minúsculo), is_active eq boolean true',
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
  if (candidates?.length) {
    console.log(
      '[worker] IDs retornados:',
      candidates.map((r: { id: string }) => r.id).join(', '),
    )
  }

  if (error) {
    console.error('[worker] Falha na query scheduled_messages:', error.message)
    return { processed: 0, skipped: 1 }
  }

  let processed = 0
  let skipped = 0

  async function marcarErroNaLinha(
    id: string,
    erroOriginal: string,
  ): Promise<void> {
    console.error(
      '================================================================================',
    )
    console.error('[worker] ERRO FATAL NO DISPARO (motivo original, ANTES do UPDATE):')
    console.error('[worker]', erroOriginal)
    console.error('[worker] scheduled_messages.id:', id)
    console.error(
      '================================================================================',
    )

    const texto = erroOriginal.slice(0, 4000)
    const { error: upErr } = await supabase
      .from('scheduled_messages')
      .update({
        status: 'error',
        last_error: texto,
        updated_at: new Date().toISOString(),
      })
      .eq('id', id)
      .in('status', ['pending', 'processing'])

    if (upErr) {
      console.error(
        '[worker] UPDATE com last_error falhou — tentando só status=error. Supabase:',
        upErr.message,
        JSON.stringify(upErr),
      )
      const { error: upErrFallback } = await supabase
        .from('scheduled_messages')
        .update({
          status: 'error',
          updated_at: new Date().toISOString(),
        })
        .eq('id', id)
        .in('status', ['pending', 'processing'])
      if (upErrFallback) {
        console.error(
          '[worker] Fallback (só status) também falhou:',
          upErrFallback.message,
          JSON.stringify(upErrFallback),
        )
      }
    }
  }

  for (const raw of candidates ?? []) {
    const row = raw as ScheduledRow
    try {
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

      if (claimErr) {
        console.error('[worker] Claim falhou', row.id, claimErr.message)
        skipped += 1
        processed += 1
        continue
      }
      if (!claimed) {
        skipped += 1
        processed += 1
        continue
      }

      try {
        const { targets, error: resolveErr } = await resolveRecipientPhones(
          supabase,
          row,
        )
        if (resolveErr || targets.length === 0) {
          await marcarErroNaLinha(
            row.id,
            resolveErr ?? 'Sem destinatários.',
          )
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
          const { error: finErr } = await supabase
            .from('scheduled_messages')
            .update({
              status: 'sent',
              evolution_message_id: ids.length ? ids.join(',') : null,
              last_error: null,
              updated_at: new Date().toISOString(),
            })
            .eq('id', row.id)
          if (finErr) {
            console.error('[worker] Update sent falhou', row.id, finErr.message)
            await marcarErroNaLinha(row.id, `Envio ok, mas DB: ${finErr.message}`)
          }
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
          await marcarErroNaLinha(row.id, failText)
        }
      } catch (eProcess) {
        const msg =
          eProcess instanceof Error ? eProcess.message : String(eProcess)
        console.error('[worker] Exceção no envio', row.id, msg)
        await marcarErroNaLinha(row.id, `Exceção no envio: ${msg}`)
      }
    } catch (eLinha) {
      const msg = eLinha instanceof Error ? eLinha.message : String(eLinha)
      console.error('[worker] Exceção ao processar linha', row.id, msg)
      await marcarErroNaLinha(row.id, `Falha geral: ${msg}`)
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

  const supabaseUrl = Deno.env.get('SUPABASE_URL')?.trim()
  const chaveMestra = Deno.env.get('CHAVE_MESTRA_ZAPIFICA')?.trim() ?? ''
  const evoUrlRaw =
    Deno.env.get('EVOLUTION_URL') ?? Deno.env.get('VITE_EVOLUTION_URL') ?? ''
  const evoKey =
    Deno.env.get('EVOLUTION_API_KEY') ??
    Deno.env.get('EVOLUTION_GLOBAL_KEY') ??
    Deno.env.get('VITE_EVOLUTION_GLOBAL_KEY') ??
    ''

  if (!supabaseUrl || !chaveMestra) {
    return new Response(
      JSON.stringify({
        erro:
          'SUPABASE_URL ou CHAVE_MESTRA_ZAPIFICA ausentes. Crie o segredo CHAVE_MESTRA_ZAPIFICA na função (valor = service role do projeto).',
      }),
      { status: 500, headers: { 'Content-Type': 'application/json' } },
    )
  }

  console.log(
    '[worker] createClient com CHAVE_MESTRA_ZAPIFICA (comprimento após trim):',
    chaveMestra.length,
    '| URL:',
    supabaseUrl,
  )

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

  const supabase = createClient(supabaseUrl, chaveMestra, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
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
