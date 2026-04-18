/**
 * Cliente HTTP para Evolution API (instância + QR Code).
 * Variáveis: VITE_EVOLUTION_URL (sem barra final) e VITE_EVOLUTION_GLOBAL_KEY (header `apikey`).
 */

export type CreateInstanceQrResult = {
  /** Pronto para `src` de `<img />` (data URL PNG). */
  dataUrl: string | null
  error: string | null
  /** Nome da instância usado na Evolution (derivado do user_id). */
  instanceName?: string
}

export type ConnectionStatusResult = {
  connected: boolean
  /** Estado bruto retornado pela API (`open`, `close`, `connecting`…). */
  state: string | null
  /** Número / identificador amigável, se a API enviar. */
  phone: string | null
  /** Erro de rede ou HTTP; `null` em checagens silenciosas sem credenciais. */
  error: string | null
}

export type SendTextMessageResult = {
  ok: boolean
  error: string | null
}

function evolutionBaseUrl(): string {
  const raw = import.meta.env.VITE_EVOLUTION_URL?.trim() ?? ''
  return raw.replace(/\/+$/, '')
}

function evolutionApiKey(): string {
  return import.meta.env.VITE_EVOLUTION_GLOBAL_KEY?.trim() ?? ''
}

export function instanceNameFromUserId(userId: string): string {
  const safe = userId.replace(/[^a-zA-Z0-9-]/g, '_')
  return `zapifica_${safe}`.slice(0, 80)
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

/**
 * Consulta o estado da instância WhatsApp na Evolution (`GET /instance/connectionState/{instance}`).
 */
export async function checkConnectionStatus(
  userId: string,
): Promise<ConnectionStatusResult> {
  const base = evolutionBaseUrl()
  const key = evolutionApiKey()

  if (!base || !key) {
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

function normalizeQrDataUrl(value: string): string {
  const t = value.trim()
  if (t.startsWith('data:image')) return t
  return `data:image/png;base64,${t}`
}

function extractQrFromPayload(payload: unknown): string | null {
  if (!payload || typeof payload !== 'object') return null
  const o = payload as Record<string, unknown>

  const direct =
    typeof o.base64 === 'string' ? o.base64 : typeof o.code === 'string' ? o.code : null

  if (direct && direct.length > 80) {
    return normalizeQrDataUrl(direct)
  }

  const qr = o.qrcode
  if (qr && typeof qr === 'object') {
    const q = qr as Record<string, unknown>
    const b = typeof q.base64 === 'string' ? q.base64 : typeof q.code === 'string' ? q.code : null
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

/**
 * Envia mensagem de texto pela instância Evolution (`POST /message/sendText/{instance}`).
 * O nome da instância segue `instanceNameFromUserId` (ex.: `zapifica_{userId}`).
 * Corpo: `{ number, text }` (número só dígitos, com código do país; sem `+`).
 */
export async function sendTextMessage(
  userId: string,
  number: string,
  text: string,
): Promise<SendTextMessageResult> {
  const base = evolutionBaseUrl()
  const key = evolutionApiKey()

  if (!base || !key) {
    return {
      ok: false,
      error:
        'Configure VITE_EVOLUTION_URL e VITE_EVOLUTION_GLOBAL_KEY no arquivo .env.local.',
    }
  }

  const digits = number.replace(/\D/g, '')
  const trimmedText = text.trim()

  if (!digits || digits.length < 10) {
    return {
      ok: false,
      error: 'Informe um número válido com DDD e código do país (ex.: 5548999999999).',
    }
  }

  if (!trimmedText) {
    return {
      ok: false,
      error: 'Digite a mensagem a ser enviada.',
    }
  }

  const instanceName = instanceNameFromUserId(userId)

  try {
    const res = await evolutionFetch(
      `/message/sendText/${encodeURIComponent(instanceName)}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          number: digits,
          text: trimmedText,
        }),
      },
    )

    if (!res.ok) {
      return {
        ok: false,
        error: formatHttpError(res.status, res.data),
      }
    }

    return { ok: true, error: null }
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e)
    if (msg === 'Failed to fetch') {
      return {
        ok: false,
        error:
          'Não foi possível contatar a Evolution API (rede ou CORS). Verifique VITE_EVOLUTION_URL.',
      }
    }
    return {
      ok: false,
      error: msg || 'Erro inesperado ao enviar a mensagem.',
    }
  }
}

async function evolutionFetch(
  path: string,
  init: RequestInit & { parseJson?: boolean } = {},
): Promise<{ ok: boolean; status: number; data: unknown }> {
  const base = evolutionBaseUrl()
  const key = evolutionApiKey()
  const url = `${base}${path.startsWith('/') ? path : `/${path}`}`

  const { parseJson = true, ...rest } = init
  const res = await fetch(url, {
    ...rest,
    headers: {
      apikey: key,
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

/**
 * Cria (ou reutiliza) uma instância WhatsApp na Evolution com nome derivado do `user_id`
 * do Supabase e obtém o QR Code em base64 (via resposta do create e/ou GET connect).
 */
export async function createInstanceAndGetQr(
  userId: string,
): Promise<CreateInstanceQrResult> {
  const base = evolutionBaseUrl()
  const key = evolutionApiKey()

  if (!base || !key) {
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
        `/instance/connect/${encodeURIComponent(instanceName)}`,
        { method: 'GET' },
      )
      if (!connect.ok) {
        return null
      }
      return extractQrFromPayload(connect.data)
    }

    const create = await evolutionFetch('/instance/create', {
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
