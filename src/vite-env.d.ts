/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_SUPABASE_URL: string
  readonly VITE_SUPABASE_ANON_KEY: string
  readonly VITE_EVOLUTION_URL: string
  readonly VITE_EVOLUTION_GLOBAL_KEY: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
