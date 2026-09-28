package models

import "time"

// CrawlTarget represents a crawl unit enqueued in the URL Frontier.
// Maps strictly to shared/contracts/crawl_target.json.
type CrawlTarget struct {
	JobID        string    `json:"job_id"`
	URL          string    `json:"url"`
	Depth        int       `json:"depth"`
	MaxDepth     int       `json:"max_depth"`
	Priority     int       `json:"priority"`
	StayInDomain bool      `json:"stay_in_domain"`
	CreatedAt    time.Time `json:"created_at"`
}
