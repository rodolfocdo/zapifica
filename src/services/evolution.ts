/**
 * Cliente HTTP para Evolution API (instância + QR Code + envio).
 * Variáveis no browser: VITE_EVOLUTION_URL e VITE_EVOLUTION_GLOBAL_KEY.
 * No servidor (worker/Edge): passe `EvolutionHttpConfig` explicitamente.
 */

export type CreateInstanceQrResult = {
  dataUrl: string | null
  error: string | null
  instanceName?: string
}

export type ConnectionStatusResult = {
  connected: boolean
  state: string | null
  phone: string | null
  error: string | null
}

export type EvolutionHttpConfig = {
  baseUrl: string
  apiKey: string
}

export type SendEvolutionResult = {
  ok: boolean
  error: string | null
  /** `key.id` na resposta da Evolution, quando disponível */
  messageId: string | null
}

function trimBaseUrl(raw: string): string {
  return raw.replace(/\/+$/, '')
}

function evolutionConfigFromVite(): EvolutionHttpConfig | null {
  const baseUrl = trimBaseUrl(import.meta.env.VITE_EVOLUTION_URL?.trim() ?? '')
  const apiKey = import.meta.env.VITE_EVOLUTION_GLOBAL_KEY?.trim() ?? ''
  if (!baseUrl || !apiKey) return null
  return { baseUrl, apiKey }
}

function resolveConfig(
  explicit?: EvolutionHttpConfig | null,
): EvolutionHttpConfig | null {
  if (explicit?.baseUrl && explicit?.apiKey) {
    return { baseUrl: trimBaseUrl(explicit.baseUrl), apiKey: explicit.apiKey }
  }
  return evolutionConfigFromVite()
}

export function instanceNameFromUserId(userId: string): string {
  const safe = userId.replace(/[^a-zA-Z0-9-]/g, '_')
  return `zapifica_${safe}`.slice(0, 80)
}

/**
 * Normaliza destino para Evolution: número (apenas dígitos) ou JID de grupo (@g.us).
 * - Já contém `@g.us` → envia o identificador como veio (grupo).
 * - JID `...@s.whatsapp.net` → extrai a parte numérica antes do `@`.
 * - Começa com dígito (após trim) e não é grupo → só dígitos (DDI + número).
 */
export function normalizeEvolutionRecipient(
  raw: string,
): { recipient: string } | { error: string } {
  const t = raw.trim()
  if (!t) {
    return { error: 'Destinatário vazio.' }
  }
  if (t.includes('@g.us')) {
    return { recipient: t }
  }
  if (t.includes('@s.whatsapp.net')) {
    const before = t.split('@')[0] ?? ''
    const digits = before.replace(/\D/g, '')
    if (digits.length < 10) {
      return { error: 'JID individual inválido.' }
    }
    return { recipient: digits }
  }
  if (t.includes('@') && !/^\d/.test(t)) {
    return { recipient: t }
  }
  const digits = t.replace(/\D/g, '')
  if (!digits || digits.length < 10) {
    return {
      error:
        'Informe um número válido com DDD e código do país (ex.: 5548999999999) ou um ID de grupo (@g.us).',
    }
  }
  return { recipient: digits }
}

function formatPhoneFromApi(raw: string): string {
  const part = raw.split('@')[0] ?? raw
  const digits = part.replace(/\D/g, '')
  if (digits.length < 10) return raw.trim()
  if (digits.length <= 11) {
    const d = digits
    return d.length === 11
      ? `(${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7)}`
      : `(${d.slice(0, 2)}) ${d.slice(2, 6)}-${d.slice(6)}`
  }
  return `+${digits}`
}

function pickPhoneFromPayload(data: unknown): string | null {
  if (!data || typeof data !== 'object') return null
  const root = data as Record<string, unknown>
  const inst = root.instance
  const bag =
    inst && typeof inst === 'object' ? (inst as Record<string, unknown>) : root

  const keys = ['number', 'phoneNumber', 'owner', 'wid', 'user', 'phone']
  for (const key of keys) {
    const v = bag[key]
    if (typeof v === 'string' && v.length >= 8) {
      return formatPhoneFromApi(v)
    }
  }
  return null
}

function formatHttpError(status: number, body: unknown): string {
  if (body && typeof body === 'object') {
    const b = body as Record<string, unknown>
    const msg = b.message
    if (Array.isArray(msg) && msg.length) {
      return String(msg[0])
    }
    if (typeof b.error === 'string') return b.error
    const resp = b.response as Record<string, unknown> | undefined
    if (resp && Array.isArray(resp.message) && resp.message.length) {
      return String(resp.message[0])
    }
  }
  return `Erro HTTP ${status} na Evolution API.`
}

export function extractEvolutionMessageId(data: unknown): string | null {
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
  init: RequestInit & { parseJson?: boolean } = {},
): Promise<{ ok: boolean; status: number; data: unknown }> {
  const url = `${cfg.baseUrl}${path.startsWith('/') ? path : `/${path}`}`

  const { parseJson = true, ...rest } = init
  const res = await fetch(url, {
    ...rest,
    headers: {
      apikey: cfg.apiKey,
      Accept: 'application/json',
      ...(rest.headers as Record<string, string>),
    },
  })

  let data: unknown = null
  if (parseJson) {
    const text = await res.text()
    if (text) {
      try {
        data = JSON.parse(text) as unknown
      } catch {
        data = { raw: text }
      }
    }
  }

  return { ok: res.ok, status: res.status, data }
}

function normalizeQrDataUrl(value: string): string {
  const t = value.trim()
  if (t.startsWith('data:image')) return t
  return `data:image/png;base64,${t}`
}

function extractQrFromPayload(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null
  const o = payload as Record<string, unknown>

  const direct =
    typeof o.base64 === 'string'
      ? o.base64
      : typeof o.code === 'string'
        ? o.code
        : null

  if (direct && direct.length > 80) {
    return normalizeQrDataUrl(direct)
  }

  const qr = o.qrcode
  if (qr && typeof qr === 'object') {
    const q = qr as Record<string, unknown>
    const b =
      typeof q.base64 === 'string'
        ? q.base64
        : typeof q.code === 'string'
          ? q.code
          : null
    if (b && b.length > 80) return normalizeQrDataUrl(b)
  }

  if (typeof qr === 'string' && qr.length > 80) {
    return normalizeQrDataUrl(qr)
  }

  const inst = o.instance
  if (inst && typeof inst === 'object') {
    return extractQrFromPayload(inst)
  }

  return null
}

export async function checkConnectionStatus(
  userId: string,
): Promise<ConnectionStatusResult> {
  const cfg = resolveConfig()
  if (!cfg) {
    return {
      connected: false,
      state: null,
      phone: null,
      error: null,
    }
  }

  const instanceName = instanceNameFromUserId(userId)

  try {
    const res = await evolutionFetch(
      cfg,
      `/instance/connectionState/${encodeURIComponent(instanceName)}`,
      { method: 'GET' },
    )

    if (res.status === 404) {
      return {
        connected: false,
        state: null,
        phone: null,
        error: null,
      }
    }

    if (!res.ok) {
      return {
        connected: false,
        state: null,
        phone: null,
        error: formatHttpError(res.status, res.data),
      }
    }

    const data = res.data as Record<string, unknown>
    const inst = data.instance as Record<string, unknown> | undefined
    const stateRaw = inst?.state
    const state =
      typeof stateRaw === 'string' ? stateRaw.toLowerCase().trim() : null

    const connected =
      state === 'open' ||
      state === 'connected' ||
      state === 'ready' ||
      state === 'online'

    const phone = pickPhoneFromPayload(res.data)

    return {
      connected,
      state: typeof stateRaw === 'string' ? stateRaw : null,
      phone,
      error: null,
    }
  } catch {
    return {
      connected: false,
      state: null,
      phone: null,
      error: null,
    }
  }
}

export async function sendTextMessage(
  userId: string,
  number: string,
  text: string,
  httpConfig?: EvolutionHttpConfig | null,
): Promise<SendEvolutionResult> {
  return sendTextMessageWithConfig(userId, number, text, resolveConfig(httpConfig))
}

export async function sendTextMessageWithConfig(
  userId: string,
  number: string,
  text: string,
  cfg: EvolutionHttpConfig | null,
): Promise<SendEvolutionResult> {
  if (!cfg) {
    return {
      ok: false,
      error:
        'Configure VITE_EVOLUTION_URL e VITE_EVOLUTION_GLOBAL_KEY (ou credenciais no worker).',
      messageId: null,
    }
  }

  const normalized = normalizeEvolutionRecipient(number)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }

  const trimmedText = text.trim()
  if (!trimmedText) {
    return { ok: false, error: 'Digite a mensagem a ser enviada.', messageId: null }
  }

  const instanceName = instanceNameFromUserId(userId)

  try {
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
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    if (msg === 'Failed to fetch') {
      return {
        ok: false,
        error:
          'Não foi possível contatar a Evolution API (rede ou CORS). Verifique a URL.',
        messageId: null,
      }
    }
    return {
      ok: false,
      error: msg || 'Erro inesperado ao enviar a mensagem.',
      messageId: null,
    }
  }
}

/**
 * Áudio (URL ou base64) — endpoint de voz/nota da Evolution.
 */
export async function sendAudioMessageWithConfig(
  userId: string,
  recipient: string,
  audio: string,
  cfg: EvolutionHttpConfig | null,
): Promise<SendEvolutionResult> {
  if (!cfg) {
    return {
      ok: false,
      error: 'Credenciais da Evolution não configuradas.',
      messageId: null,
    }
  }
  const normalized = normalizeEvolutionRecipient(recipient)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }
  const trimmed = audio.trim()
  if (!trimmed) {
    return { ok: false, error: 'Informe a URL ou o base64 do áudio.', messageId: null }
  }

  const instanceName = instanceNameFromUserId(userId)

  try {
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
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    return {
      ok: false,
      error: msg || 'Erro ao enviar áudio.',
      messageId: null,
    }
  }
}

/**
 * Imagem (ou mídia) — URL ou base64.
 */
export async function sendImageMessageWithConfig(
  userId: string,
  recipient: string,
  media: string,
  caption: string,
  cfg: EvolutionHttpConfig | null,
): Promise<SendEvolutionResult> {
  if (!cfg) {
    return {
      ok: false,
      error: 'Credenciais da Evolution não configuradas.',
      messageId: null,
    }
  }
  const normalized = normalizeEvolutionRecipient(recipient)
  if ('error' in normalized) {
    return { ok: false, error: normalized.error, messageId: null }
  }
  const trimmed = media.trim()
  if (!trimmed) {
    return { ok: false, error: 'Informe a URL ou o base64 da imagem.', messageId: null }
  }

  const instanceName = instanceNameFromUserId(userId)
  const captionTrim = caption.trim()
  const isData = trimmed.startsWith('data:')
  const mimetype = isData
    ? trimmed.split(';')[0]?.replace('data:', '') || 'image/png'
    : 'image/jpeg'
  const fileName =
    mimetype.includes('png') ? 'imagem.png' : mimetype.includes('webp') ? 'imagem.webp' : 'imagem.jpg'

  try {
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
          caption: captionTrim || ' ',
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
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    return {
      ok: false,
      error: msg || 'Erro ao enviar imagem.',
      messageId: null,
    }
  }
}

export async function createInstanceAndGetQr(
  userId: string,
): Promise<CreateInstanceQrResult> {
  const cfg = resolveConfig()
  if (!cfg) {
    return {
      dataUrl: null,
      error:
        'Configure VITE_EVOLUTION_URL e VITE_EVOLUTION_GLOBAL_KEY no arquivo .env.local.',
    }
  }

  const instanceName = instanceNameFromUserId(userId)

  try {
    const tryConnect = async (): Promise<string | null> => {
      const connect = await evolutionFetch(
        cfg,
        `/instance/connect/${encodeURIComponent(instanceName)}`,
        { method: 'GET' },
      )
      if (!connect.ok) {
        return null
      }
      return extractQrFromPayload(connect.data)
    }

    const create = await evolutionFetch(cfg, '/instance/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        instanceName,
        integration: 'WHATSAPP-BAILEYS',
        qrcode: true,
      }),
    })

    let qr = extractQrFromPayload(create.data)

    const bodyStr = JSON.stringify(create.data).toLowerCase()
    const duplicate =
      !create.ok &&
      (create.status === 403 ||
        create.status === 409 ||
        bodyStr.includes('already') ||
        bodyStr.includes('already in use') ||
        bodyStr.includes('já existe') ||
        bodyStr.includes('already exists'))

    if (!create.ok && !duplicate) {
      return {
        dataUrl: null,
        error: formatHttpError(create.status, create.data),
        instanceName,
      }
    }

    if (!qr) {
      await new Promise((r) => setTimeout(r, 1200))
      qr = await tryConnect()
    }

    if (!qr) {
      await new Promise((r) => setTimeout(r, 2000))
      qr = await tryConnect()
    }

    if (!qr) {
      return {
        dataUrl: null,
        error:
          'A instância foi criada, mas o QR Code ainda não está disponível. Aguarde alguns segundos e abra de novo.',
        instanceName,
      }
    }

    return { dataUrl: qr, error: null, instanceName }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    if (msg === 'Failed to fetch') {
      return {
        dataUrl: null,
        error:
          'Não foi possível contatar a Evolution API (rede, URL ou bloqueio CORS). Verifique VITE_EVOLUTION_URL e as permissões do servidor.',
        instanceName,
      }
    }
    return {
      dataUrl: null,
      error: msg || 'Erro inesperado ao falar com a Evolution API.',
      instanceName,
    }
  }
}
