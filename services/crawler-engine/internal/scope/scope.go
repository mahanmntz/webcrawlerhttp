// Package scope decides whether a host belongs to a crawl's domain boundary.
// Rules and test vectors live in shared/contracts/url_canonicalization.json;
// parser-scraper/app/urls.py implements the same check.
package scope

import "strings"

func stripWWW(host string) string {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return strings.TrimPrefix(host, "www.")
}

// InScope reports whether host is scopeHost or one of its subdomains,
// ignoring a leading "www." on either side.
func InScope(host, scopeHost string) bool {
	host, scopeHost = stripWWW(host), stripWWW(scopeHost)
	if host == "" || scopeHost == "" {
		return false
	}
	return host == scopeHost || strings.HasSuffix(host, "."+scopeHost)
}
