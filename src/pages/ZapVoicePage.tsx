import { useCallback, useEffect, useRef, useState } from 'react'
import {
  ImageIcon,
  Megaphone,
  QrCode,
  Radio,
  Smartphone,
  Sparkles,
  X,
} from 'lucide-react'
import { supabase } from '../lib/supabase'
import { NewCampaignModal } from '../components/zapvoice/NewCampaignModal'
import {
  checkConnectionStatus,
  createInstanceAndGetQr,
  sendTextMessage,
} from '../services/evolution'

export function ZapVoicePage() {
  const [floripaHint, setFloripaHint] = useState(false)
  const [campaignModalOpen, setCampaignModalOpen] = useState(false)

  const [whatsappConnected, setWhatsappConnected] = useState(false)
  const [whatsappPhone, setWhatsappPhone] = useState<string | null>(null)

  const [qrModalOpen, setQrModalOpen] = useState(false)
  const [qrLoading, setQrLoading] = useState(false)
  const [qrDataUrl, setQrDataUrl] = useState<string | null>(null)
  const [qrError, setQrError] = useState<string | null>(null)

  const [successToast, setSuccessToast] = useState<string | null>(null)
  const [errorToast, setErrorToast] = useState<string | null>(null)
  const [disconnectHint, setDisconnectHint] = useState(false)

  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null)

  const closeQrModal = useCallback(() => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current)
      pollingRef.current = null
    }
    setQrModalOpen(false)
    setQrLoading(false)
    setQrDataUrl(null)
    setQrError(null)
  }, [])

  /** Checagem silenciosa ao carregar a página */
  useEffect(() => {
    let cancelled = false

    void (async () => {
      const {
        data: { user },
      } = await supabase.auth.getUser()
      if (!user || cancelled) return

      const s = await checkConnectionStatus(user.id)
      if (cancelled) return

      if (!s.error) {
        setWhatsappConnected(s.connected)
        if (s.phone) setWhatsappPhone(s.phone)
      }
    })()

    return () => {
      cancelled = true
    }
  }, [])

  /** Polling a cada 3s enquanto o modal do QR estiver aberto */
  useEffect(() => {
    if (!qrModalOpen) {
      if (pollingRef.current) {
        clearInterval(pollingRef.current)
        pollingRef.current = null
      }
      return
    }

    let active = true

    const setup = async () => {
      const {
        data: { user },
      } = await supabase.auth.getUser()
      if (!user || !active) return

      const tick = async () => {
        if (!active) return
        const s = await checkConnectionStatus(user.id)
        if (!active) return

        if (s.connected) {
          if (pollingRef.current) {
            clearInterval(pollingRef.current)
            pollingRef.current = null
          }
          setWhatsappConnected(true)
          if (s.phone) setWhatsappPhone(s.phone)
          setSuccessToast('WhatsApp conectado com sucesso!')
          window.setTimeout(() => {
            if (active) setSuccessToast(null)
          }, 6000)
          closeQrModal()
          return
        }

        if (s.phone) setWhatsappPhone(s.phone)
      }

      await tick()
      if (!active) return
      pollingRef.current = setInterval(tick, 3000)
    }

    void setup()

    return () => {
      active = false
      if (pollingRef.current) {
        clearInterval(pollingRef.current)
        pollingRef.current = null
      }
    }
  }, [qrModalOpen, closeQrModal])

  const handleCampaignSend = useCallback(
    async (number: string, text: string) => {
      const {
        data: { user },
        error: authErr,
      } = await supabase.auth.getUser()

      if (authErr || !user) {
        const msg =
          'Não foi possível identificar seu usuário. Faça login novamente.'
        setErrorToast(msg)
        window.setTimeout(() => setErrorToast(null), 7000)
        return { ok: false, error: msg }
      }

      const result = await sendTextMessage(user.id, number, text)

      if (!result.ok) {
        setErrorToast(result.error ?? 'Não foi possível enviar a mensagem.')
        window.setTimeout(() => setErrorToast(null), 8000)
        return result
      }

      setSuccessToast('Mensagem disparada com sucesso!')
      window.setTimeout(() => setSuccessToast(null), 6000)
      return result
    },
    [],
  )

  const openConnectFlow = useCallback(() => {
    setQrModalOpen(true)
    setQrLoading(true)
    setQrDataUrl(null)
    setQrError(null)

    void (async () => {
      const {
        data: { user },
        error: authErr,
      } = await supabase.auth.getUser()

      if (authErr || !user) {
        setQrError('Não foi possível identificar seu usuário. Faça login novamente.')
        setQrLoading(false)
        return
      }

      const result = await createInstanceAndGetQr(user.id)
      setQrLoading(false)

      if (result.error) {
        setQrError(result.error)
        return
      }

      if (result.dataUrl) {
        setQrDataUrl(result.dataUrl)
      } else {
        setQrError('A API não retornou um QR Code válido.')
      }
    })()
  }, [])

  return (
    <div className="space-y-8">
      <div className="space-y-3">
        {successToast ? (
          <div
            role="status"
            className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm font-medium text-emerald-900 shadow-sm ring-1 ring-emerald-100"
          >
            {successToast}
          </div>
        ) : null}
        {errorToast ? (
          <div
            role="alert"
            className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-medium text-rose-900 shadow-sm ring-1 ring-rose-100"
          >
            {errorToast}
          </div>
        ) : null}
      </div>

      <div>
        <h2 className="text-xl font-semibold tracking-tight text-zinc-900">
          Zap Voice
        </h2>
        <p className="mt-1 max-w-2xl text-sm leading-relaxed text-zinc-600">
          Conecte seu aparelho, dispare campanhas e enriqueça mensagens com artes
          de alta conversão — tudo no padrão visual Zapifica.
        </p>
      </div>

      <div className="grid gap-6 lg:grid-cols-2 lg:items-start">
        <section className="flex flex-col rounded-2xl border border-zinc-200/90 bg-white p-6 shadow-sm ring-1 ring-zinc-100/80 lg:min-h-[320px]">
          <div className="mb-4 inline-flex w-fit items-center gap-2 rounded-full bg-zinc-100 px-3 py-1 text-xs font-medium text-zinc-600">
            <Radio className="h-3.5 w-3.5 text-zinc-500" aria-hidden />
            Aparelho
          </div>
          <h3 className="text-lg font-semibold tracking-tight text-zinc-900">
            Conexão do WhatsApp
          </h3>
          <p className="mt-1 text-sm text-zinc-500">
            Status da sessão com a Evolution API (QR Code).
          </p>

          <div
            className={`mt-6 flex items-center gap-3 rounded-xl border px-4 py-3 ${
              whatsappConnected
                ? 'border-emerald-200 bg-emerald-50/80'
                : 'border-zinc-200 bg-zinc-50/80'
            }`}
          >
            <span className="relative flex h-2.5 w-2.5 shrink-0" aria-hidden>
              {whatsappConnected ? (
                <>
                  <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-emerald-400/40 opacity-75" />
                  <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-500 ring-2 ring-emerald-200" />
                </>
              ) : (
                <>
                  <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-rose-400/50 opacity-75" />
                  <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-rose-500 ring-2 ring-rose-200" />
                </>
              )}
            </span>
            <div className="min-w-0">
              <p className="text-sm font-semibold text-zinc-800">
                {whatsappConnected ? 'Conectado' : 'Desconectado'}
              </p>
              {whatsappConnected && whatsappPhone ? (
                <p className="mt-0.5 truncate text-xs font-medium text-emerald-800">
                  {whatsappPhone}
                </p>
              ) : (
                <p className="mt-0.5 text-xs text-zinc-500">
                  {whatsappConnected
                    ? 'Sessão ativa na Evolution API.'
                    : 'Nenhum aparelho pareado no momento.'}
                </p>
              )}
            </div>
          </div>

          <div className="mt-6 flex flex-1 flex-col justify-end gap-3">
            {whatsappConnected ? (
              <>
                <button
                  type="button"
                  onClick={() => {
                    setDisconnectHint(true)
                    window.setTimeout(() => setDisconnectHint(false), 4000)
                  }}
                  className="inline-flex w-full items-center justify-center gap-2 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-semibold text-rose-900 shadow-sm transition hover:border-rose-300 hover:bg-rose-100"
                >
                  Desconectar aparelho
                </button>
                {disconnectHint ? (
                  <p className="text-center text-xs text-zinc-500">
                    Em breve: encerramento da instância pela Evolution API.
                  </p>
                ) : null}
              </>
            ) : (
              <button
                type="button"
                onClick={openConnectFlow}
                className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-gradient-to-r from-brand-600 to-brand-700 px-4 py-3.5 text-sm font-semibold text-white shadow-lg shadow-[0_12px_36px_rgba(106,0,184,0.35)] transition hover:from-brand-500 hover:to-brand-600 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand-500"
              >
                <Smartphone className="h-4 w-4 opacity-90" aria-hidden />
                Conectar WhatsApp (QR Code)
              </button>
            )}
          </div>
        </section>

        <div className="flex flex-col gap-6">
          <section className="rounded-2xl border border-zinc-200/90 bg-white p-6 shadow-sm ring-1 ring-zinc-100/80">
            <div className="mb-4 inline-flex w-fit items-center gap-2 rounded-full bg-brand-50 px-3 py-1 text-xs font-medium text-brand-700 ring-1 ring-brand-100">
              <Megaphone className="h-3.5 w-3.5" aria-hidden />
              Disparos
            </div>
            <h3 className="text-lg font-semibold tracking-tight text-zinc-900">
              Campanhas de disparo
            </h3>
            <p className="mt-1 text-sm text-zinc-500">
              Organize envios em massa com segmentação e janelas seguras.
            </p>
            <div className="mt-5">
              <button
                type="button"
                onClick={() => setCampaignModalOpen(true)}
                className="inline-flex items-center gap-2 rounded-xl border border-zinc-200 bg-zinc-50 px-4 py-2.5 text-sm font-semibold text-zinc-800 shadow-sm transition hover:border-zinc-300 hover:bg-white"
              >
                <span className="text-base font-bold text-brand-600">+</span>
                Nova Campanha
              </button>
            </div>
          </section>

          <section className="relative overflow-hidden rounded-2xl border-2 border-brand-green/25 bg-gradient-to-br from-white via-brand-50/40 to-emerald-500/5 p-6 shadow-md ring-1 ring-brand-200/30">
            <div className="pointer-events-none absolute -right-8 -top-8 h-36 w-36 rounded-full bg-brand-green/10 blur-2xl" />
            <div className="pointer-events-none absolute -bottom-10 left-1/3 h-28 w-28 rounded-full bg-brand-600/10 blur-2xl" />

            <div className="relative">
              <div className="mb-3 inline-flex items-center gap-1.5 rounded-full bg-white/90 px-2.5 py-1 text-[10px] font-bold uppercase tracking-widest text-brand-700 shadow-sm ring-1 ring-brand-100">
                <Sparkles className="h-3 w-3 text-brand-600" aria-hidden />
                Jogada de mestre
              </div>
              <h3 className="flex items-center gap-2 text-lg font-semibold tracking-tight text-zinc-900">
                <ImageIcon className="h-5 w-5 text-brand-600" aria-hidden />
                Galeria de artes
              </h3>
              <p className="mt-1 text-sm text-zinc-600">
                Biblioteca curada para elevar o desempenho das suas mensagens.
              </p>

              <div className="mt-6 flex flex-col items-stretch gap-3 sm:items-start">
                <button
                  type="button"
                  onClick={() => {
                    setFloripaHint(true)
                    window.setTimeout(() => setFloripaHint(false), 4000)
                  }}
                  className="group relative inline-flex w-full items-center justify-center gap-2 overflow-hidden rounded-2xl bg-gradient-to-r from-brand-green via-emerald-500 to-emerald-500 px-5 py-4 text-sm font-bold uppercase tracking-wide text-white shadow-[0_0_0_1px_rgba(255,255,255,0.12)_inset,0_12px_40px_rgba(37,211,102,0.45)] transition hover:shadow-[0_0_0_1px_rgba(255,255,255,0.18)_inset,0_16px_48px_rgba(37,211,102,0.55)] sm:w-auto"
                >
                  <span className="absolute inset-0 bg-gradient-to-t from-black/10 to-transparent opacity-0 transition group-hover:opacity-100" />
                  <Sparkles className="relative h-4 w-4" aria-hidden />
                  <span className="relative">Importar Artes da Floripa Web</span>
                </button>
                <p className="max-w-md text-xs leading-relaxed text-zinc-600">
                  Importe artes de alta conversão direto do cofre da agência para
                  suas campanhas.
                </p>
                {floripaHint ? (
                  <p
                    role="status"
                    className="text-xs font-medium text-emerald-500"
                  >
                    Integração em preparação — em breve você sincroniza com um
                    clique.
                  </p>
                ) : null}
              </div>
            </div>
          </section>
        </div>
      </div>

      {qrModalOpen ? (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-zinc-950/65 p-4 backdrop-blur-sm"
          role="dialog"
          aria-modal="true"
          aria-labelledby="qr-modal-title"
        >
          <button
            type="button"
            className="absolute inset-0 cursor-default"
            aria-label="Fechar"
            onClick={closeQrModal}
          />
          <div className="relative w-full max-w-sm rounded-2xl border border-zinc-200/90 bg-white p-8 text-center shadow-2xl ring-1 ring-zinc-100">
            <button
              type="button"
              onClick={closeQrModal}
              className="absolute right-3 top-3 rounded-lg p-2 text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-700"
              aria-label="Fechar"
            >
              <X className="h-5 w-5" />
            </button>

            {qrLoading ? (
              <>
                <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-2xl bg-brand-50 ring-1 ring-brand-100">
                  <div className="h-9 w-9 animate-spin rounded-full border-2 border-brand-600 border-t-transparent" />
                </div>
                <h2
                  id="qr-modal-title"
                  className="mt-5 text-lg font-semibold text-zinc-900"
                >
                  Conectar aparelho
                </h2>
                <p className="mt-2 text-sm leading-relaxed text-zinc-600">
                  Carregando QR Code…
                </p>
                <p className="mt-3 text-xs text-zinc-500">
                  Comunicando com a Evolution API para criar ou recuperar sua
                  instância. A conexão será detectada automaticamente após a
                  leitura do QR.
                </p>
              </>
            ) : qrError ? (
              <>
                <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-2xl bg-rose-50 ring-1 ring-rose-100">
                  <QrCode className="h-9 w-9 text-rose-500" aria-hidden />
                </div>
                <h2
                  id="qr-modal-title"
                  className="mt-5 text-lg font-semibold text-zinc-900"
                >
                  Não foi possível conectar
                </h2>
                <div
                  role="alert"
                  className="mt-3 rounded-xl border border-rose-200 bg-rose-50 px-3 py-2 text-left text-sm text-rose-900"
                >
                  {qrError}
                </div>
                <button
                  type="button"
                  onClick={closeQrModal}
                  className="mt-6 w-full rounded-xl border border-zinc-200 bg-white py-2.5 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50"
                >
                  Fechar
                </button>
              </>
            ) : qrDataUrl ? (
              <>
                <div className="mx-auto flex h-16 w-16 items-center justify-center rounded-2xl bg-gradient-to-br from-brand-600/10 to-brand-green/10 ring-1 ring-brand-200/50">
                  <QrCode className="h-9 w-9 text-brand-600" aria-hidden />
                </div>
                <h2
                  id="qr-modal-title"
                  className="mt-5 text-lg font-semibold text-zinc-900"
                >
                  Escaneie o QR Code
                </h2>
                <p className="mt-2 text-sm text-zinc-600">
                  Abra o WhatsApp no celular e aponte a câmera para a imagem
                  abaixo. Quando a sessão abrir, esta janela fecha sozinha.
                </p>
                <div className="mt-5 rounded-2xl border border-zinc-200 bg-white p-3 shadow-inner ring-1 ring-zinc-100">
                  <img
                    src={qrDataUrl}
                    alt="QR Code para conectar o WhatsApp"
                    className="mx-auto h-auto max-h-64 w-full max-w-[240px] object-contain"
                  />
                </div>
                <button
                  type="button"
                  onClick={closeQrModal}
                  className="mt-6 w-full rounded-xl border border-zinc-200 bg-white py-2.5 text-sm font-medium text-zinc-700 transition hover:bg-zinc-50"
                >
                  Fechar
                </button>
              </>
            ) : null}
          </div>
        </div>
      ) : null}

      <NewCampaignModal
        open={campaignModalOpen}
        onClose={() => setCampaignModalOpen(false)}
        onSend={handleCampaignSend}
      />
    </div>
  )
}
