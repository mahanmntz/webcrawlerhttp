export interface CrawlTarget {
  job_id: string;
  url: string;
  depth: number;
  max_depth: number;
  priority: number;
  stay_in_domain?: boolean;
  scope_host?: string;
  attempts?: number;
  created_at: string;
}

export interface ParsedDocument {
  job_id: string;
  url: string;
  title: string;
  meta_description: string;
  extracted_links: string[];
  text_sample: string;
  markdown?: string;
  estimated_tokens?: number;
  raw_html_bytes?: number;
  markdown_bytes?: number;
  token_savings_pct?: number;
  parsed_at: string;
}

export interface CreateJobRequestBody {
  url: string;
  max_depth?: number;
  priority?: number;
  stay_in_domain?: boolean;
  force?: boolean;
}

export interface CreateBatchJobRequestBody {
  urls: string[];
  max_depth?: number;
  priority?: number;
  stay_in_domain?: boolean;
  force?: boolean;
}

export interface BatchJobResponse {
  total_received: number;
  enqueued_count: number;
  deduplicated_count: number;
  invalid_count: number;
  enqueued_urls: string[];
  deduplicated_urls: string[];
  invalid_urls: string[];
  job_ids: string[];
  message: string;
}

export interface ClusterMetrics {
  pending_queue: number;
  ingest_queue: number;
  scheduled_in_host_queues: number;
  active_hosts: number;
  in_flight_processing: number;
  delayed_retry: number;
  dead_letter: number;
  raw_pages_for_parser: number;
  raw_pages_in_flight: number;
  raw_pages_dead_letter: number;
  parsed_documents_total: number;
  parsed_documents_retained: number;
  unique_urls_seen: number;
  total_markdown_tokens_est?: number;
  token_savings_pct?: number;
  timestamp: string;
}
