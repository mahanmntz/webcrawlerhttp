package models

import "time"

// RawPage represents a successfully or failed crawled web page payload.
// Maps strictly to shared/contracts/raw_page.json.
type RawPage struct {
	JobID        string    `json:"job_id"`
	URL          string    `json:"url"`
	StatusCode   int       `json:"status_code"`
	ContentType  string    `json:"content_type"`
	Depth        int       `json:"depth"`
	MaxDepth     int       `json:"max_depth"`
	StayInDomain bool      `json:"stay_in_domain"`
	HTML         string    `json:"html"`
	DurationMs   int64     `json:"duration_ms"`
	FetchedAt    time.Time `json:"fetched_at"`
}
