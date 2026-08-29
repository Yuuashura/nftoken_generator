export interface CookieUrl {
  label: string;
  url: string;
}

export interface SingleResponse {
  ok: boolean;
  urls?: CookieUrl[];
  expires?: string;
  region?: string;
  plan?: string;
  error?: string;
}

export interface BulkEvent {
  position?: number;
  total?: number;
  line?: string;
  ok?: boolean;
  finished?: boolean;
  cancelled?: boolean;
  fatal?: string;
}

export interface CheckerItem {
  id: string;
  position: number;
  label: string;
  is_live: boolean;
  plan: string;
  billing: string;
  country: string;
  cookie_dict?: Record<string, string> | null;
  error?: string | null;
}

export interface CheckerEvent {
  position?: number;
  total?: number;
  item?: CheckerItem;
  finished?: boolean;
  cancelled?: boolean;
  fatal?: string;
  live_count?: number;
  dead_count?: number;
}

export interface CopyCookieResponse {
  ok: boolean;
  label?: string;
  plan?: string;
  billing?: string;
  country?: string;
  netscape?: string;
  json?: string;
  raw?: string;
  error?: string;
}

export interface GenerateSelectedResponse {
  ok: boolean;
  results?: Array<{
    label: string;
    ok: boolean;
    plan?: string;
    billing?: string;
    country?: string;
    urls?: CookieUrl[];
    expires?: string;
    error?: string;
  }>;
  error?: string;
}
