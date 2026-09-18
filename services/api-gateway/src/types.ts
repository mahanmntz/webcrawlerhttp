export interface CrawlTarget {
  job_id: string;
  url: string;
  depth: number;
  max_depth: number;
  priority: number;
  created_at: string;
}

export interface ParsedDocument {
  job_id: string;
  url: string;
  title: string;
  meta_description: string;
  extracted_links: string[];
  text_sample: string;
  parsed_at: string;
}

export interface CreateJobRequestBody {
  url: string;
  max_depth?: number;
  priority?: number;
}

export interface CreateBatchJobRequestBody {
  urls: string[];
  max_depth?: number;
  priority?: number;
}

export interface BatchJobResponse {
  total_received: number;
  enqueued_count: number;
  deduplicated_count: number;
  enqueued_urls: string[];
  deduplicated_urls: string[];
  job_ids: string[];
  message: string;
}

export interface ClusterMetrics {
  pending_queue: number;
  in_flight_processing: number;
  raw_pages_for_parser: number;
  parsed_documents_total: number;
  unique_urls_seen: number;
  timestamp: string;
}
