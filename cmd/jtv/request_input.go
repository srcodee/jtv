package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

type requestInput struct {
	method  string
	url     string
	headers http.Header
	body    string
}

type requestResponseInfo struct {
	status      string
	statusCode  int
	contentType string
	bytes       int
	preview     string
}

func parseRequestInput(input string) (requestInput, bool, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return requestInput{}, false, nil
	}
	lower := strings.ToLower(trimmed)
	switch {
	case strings.HasPrefix(lower, "curl ") || strings.HasPrefix(lower, "curl.exe "):
		return parseCurlRequest(trimmed)
	case strings.Contains(lower, "invoke-webrequest") || strings.Contains(lower, "invoke-restmethod"):
		return parsePowerShellRequest(trimmed)
	case looksLikeRawHTTPRequest(trimmed):
		return parseRawHTTPRequest(trimmed)
	default:
		return requestInput{}, false, nil
	}
}

func directURLRequest(rawURL string) (requestInput, bool) {
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		return requestInput{method: http.MethodGet, url: rawURL, headers: http.Header{}}, true
	}
	return requestInput{}, false
}

func fetchRequestInput(spec requestInput) ([]byte, requestResponseInfo, error) {
	if spec.method == "" {
		spec.method = http.MethodGet
	}
	var body io.Reader
	if spec.body != "" {
		body = strings.NewReader(spec.body)
	}
	httpReq, err := http.NewRequest(spec.method, spec.url, body)
	if err != nil {
		return nil, requestResponseInfo{}, err
	}
	for key, values := range spec.headers {
		for _, value := range values {
			httpReq.Header.Add(key, value)
		}
	}
	// Let Go negotiate/decode gzip itself. Browser-copied request files often
	// include br/zstd, which net/http does not transparently decode.
	httpReq.Header.Del("Accept-Encoding")
	client := http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, requestResponseInfo{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, requestResponseInfo{}, err
	}
	info := requestResponseInfo{
		status:      resp.Status,
		statusCode:  resp.StatusCode,
		contentType: resp.Header.Get("Content-Type"),
		bytes:       len(respBody),
		preview:     responseSnippet(bytes.TrimSpace(respBody)),
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, info, fmt.Errorf("HTTP %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	if err := validateFetchedBody(resp.Header.Get("Content-Type"), respBody); err != nil {
		return nil, info, err
	}
	return respBody, info, nil
}

func validateFetchedBody(contentType string, body []byte) error {
	lowerType := strings.ToLower(contentType)
	trimmed := bytes.TrimSpace(body)
	if strings.Contains(lowerType, "text/html") || strings.Contains(lowerType, "javascript") {
		return fmt.Errorf("request returned %s, expected JSON/NDJSON/CSV response: %s", contentType, responseSnippet(trimmed))
	}
	if len(trimmed) > 0 && trimmed[0] == '<' {
		return fmt.Errorf("request returned an HTML-like response, expected JSON/NDJSON/CSV response: %s", responseSnippet(trimmed))
	}
	return nil
}

func responseSnippet(body []byte) string {
	if len(body) > 180 {
		body = body[:180]
	}
	snippet := strings.Join(strings.Fields(string(body)), " ")
	if snippet == "" {
		return "(empty body)"
	}
	return snippet
}

func parseCurlRequest(input string) (requestInput, bool, error) {
	normalized := normalizeCurlInput(input)
	tokens, err := shellFields(normalized)
	if err != nil {
		return requestInput{}, true, err
	}
	if len(tokens) == 0 || !strings.HasPrefix(strings.ToLower(tokens[0]), "curl") {
		return requestInput{}, false, nil
	}
	req := requestInput{method: http.MethodGet, headers: http.Header{}}
	var cookies []string
	for i := 1; i < len(tokens); i++ {
		token := tokens[i]
		switch token {
		case "-H", "--header":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl header is missing a value")
			}
			addHeaderLine(req.headers, tokens[i])
		case "-b", "--cookie", "--cookie-jar":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl cookie is missing a value")
			}
			cookies = append(cookies, tokens[i])
		case "-X", "--request":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl request method is missing a value")
			}
			req.method = strings.ToUpper(tokens[i])
		case "-A", "--user-agent":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl user-agent is missing a value")
			}
			req.headers.Set("User-Agent", tokens[i])
		case "-d", "--data", "--data-raw", "--data-binary", "--data-ascii":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl data is missing a value")
			}
			req.body = tokens[i]
			if req.method == http.MethodGet {
				req.method = http.MethodPost
			}
		case "--url":
			i++
			if i >= len(tokens) {
				return requestInput{}, true, errors.New("curl url is missing a value")
			}
			req.url = tokens[i]
		default:
			if strings.HasPrefix(token, "http://") || strings.HasPrefix(token, "https://") {
				req.url = token
			}
		}
	}
	if len(cookies) > 0 {
		req.headers.Set("Cookie", strings.Join(cookies, "; "))
	}
	if req.url == "" {
		return requestInput{}, true, errors.New("curl request is missing URL")
	}
	return req, true, nil
}

func normalizeCurlInput(input string) string {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	input = regexp.MustCompile(`\\\s*\n`).ReplaceAllString(input, " ")
	input = regexp.MustCompile(`\^\s*\n`).ReplaceAllString(input, " ")
	var b strings.Builder
	for i := 0; i < len(input); i++ {
		if input[i] == '^' && i+1 < len(input) {
			i++
			b.WriteByte(input[i])
			continue
		}
		b.WriteByte(input[i])
	}
	return b.String()
}

func parsePowerShellRequest(input string) (requestInput, bool, error) {
	normalized := strings.ReplaceAll(input, "\r\n", "\n")
	normalized = regexp.MustCompile("`\\s*\\n").ReplaceAllString(normalized, " ")
	normalized = unescapePowerShell(normalized)
	req := requestInput{method: http.MethodGet, headers: http.Header{}}
	if uri := firstPowerShellArg(normalized, "Uri"); uri != "" {
		req.url = uri
	}
	if method := firstPowerShellArg(normalized, "Method"); method != "" {
		req.method = strings.ToUpper(method)
	}
	if contentType := firstPowerShellArg(normalized, "ContentType"); contentType != "" {
		req.headers.Set("Content-Type", contentType)
	}
	if body := firstPowerShellArg(normalized, "Body"); body != "" {
		req.body = body
		if req.method == http.MethodGet {
			req.method = http.MethodPost
		}
	}
	if userAgent := firstAssignmentString(normalized, `(?i)\$session\.UserAgent\s*=`); userAgent != "" {
		req.headers.Set("User-Agent", userAgent)
	}
	if headersBlock := powerShellHashtable(normalized, "Headers"); headersBlock != "" {
		for key, value := range parsePowerShellHashtable(headersBlock) {
			req.headers.Set(key, value)
		}
	}
	cookies := parsePowerShellCookies(normalized)
	if len(cookies) > 0 {
		req.headers.Set("Cookie", strings.Join(cookies, "; "))
	}
	if req.url == "" {
		return requestInput{}, true, errors.New("PowerShell request is missing -Uri")
	}
	return req, true, nil
}

func parseRawHTTPRequest(input string) (requestInput, bool, error) {
	input = strings.ReplaceAll(input, "\r\n", "\n")
	parts := strings.SplitN(input, "\n\n", 2)
	headerLines := strings.Split(parts[0], "\n")
	requestLine := strings.Fields(headerLines[0])
	if len(requestLine) < 2 {
		return requestInput{}, false, nil
	}
	req := requestInput{method: strings.ToUpper(requestLine[0]), headers: http.Header{}}
	for _, line := range headerLines[1:] {
		addHeaderLine(req.headers, line)
	}
	host := req.headers.Get("Host")
	if host == "" {
		return requestInput{}, true, errors.New("raw HTTP request is missing Host header")
	}
	scheme := "https"
	if strings.HasPrefix(strings.ToLower(host), "localhost") || strings.HasPrefix(strings.ToLower(host), "127.0.0.1") {
		scheme = "http"
	}
	target := requestLine[1]
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
		req.url = target
	} else {
		req.url = scheme + "://" + host + target
	}
	if len(parts) == 2 {
		req.body = parts[1]
	}
	return req, true, nil
}

func looksLikeRawHTTPRequest(input string) bool {
	line := input
	if index := strings.IndexAny(input, "\r\n"); index != -1 {
		line = input[:index]
	}
	fields := strings.Fields(line)
	if len(fields) < 3 || !strings.HasPrefix(strings.ToUpper(fields[2]), "HTTP/") {
		return false
	}
	switch strings.ToUpper(fields[0]) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodHead, http.MethodOptions:
		return true
	default:
		return false
	}
}

func addHeaderLine(headers http.Header, line string) {
	key, value, ok := strings.Cut(line, ":")
	if !ok {
		return
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" {
		return
	}
	headers.Set(key, value)
}

func firstPowerShellArg(input, name string) string {
	re := regexp.MustCompile(`(?is)-` + regexp.QuoteMeta(name) + `\s+("[^"]*"|'[^']*'|[^\s` + "`" + `]+)`)
	match := re.FindStringSubmatch(input)
	if len(match) < 2 {
		return ""
	}
	return trimQuotes(match[1])
}

func firstAssignmentString(input, prefixPattern string) string {
	re := regexp.MustCompile(prefixPattern + `\s*("[^"]*"|'[^']*')`)
	match := re.FindStringSubmatch(input)
	if len(match) < 2 {
		return ""
	}
	return trimQuotes(match[1])
}

func powerShellHashtable(input, name string) string {
	re := regexp.MustCompile(`(?is)-` + regexp.QuoteMeta(name) + `\s*@\{(.*?)\}`)
	match := re.FindStringSubmatch(input)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func parsePowerShellHashtable(block string) map[string]string {
	out := map[string]string{}
	re := regexp.MustCompile(`(?m)["']([^"']+)["']\s*=\s*("[^"]*"|'[^']*')`)
	for _, match := range re.FindAllStringSubmatch(block, -1) {
		out[match[1]] = trimQuotes(match[2])
	}
	return out
}

func parsePowerShellCookies(input string) []string {
	var cookies []string
	re := regexp.MustCompile(`(?is)System\.Net\.Cookie\((.*?)\)`)
	for _, match := range re.FindAllStringSubmatch(input, -1) {
		args := quotedStrings(match[1])
		if len(args) >= 2 {
			cookies = append(cookies, args[0]+"="+args[1])
		}
	}
	return cookies
}

func quotedStrings(input string) []string {
	var out []string
	for _, quote := range []byte{'"', '\''} {
		for _, part := range splitQuoted(input, quote) {
			out = append(out, part)
		}
	}
	return out
}

func splitQuoted(input string, quote byte) []string {
	var out []string
	for i := 0; i < len(input); i++ {
		if input[i] != quote {
			continue
		}
		var b strings.Builder
		for j := i + 1; j < len(input); j++ {
			if input[j] == quote {
				out = append(out, b.String())
				i = j
				break
			}
			b.WriteByte(input[j])
		}
	}
	return out
}

func shellFields(input string) ([]string, error) {
	var fields []string
	var b bytes.Buffer
	quote := byte(0)
	escaped := false
	for i := 0; i < len(input); i++ {
		ch := input[i]
		if escaped {
			b.WriteByte(ch)
			escaped = false
			continue
		}
		if quote == 0 && ch == '\\' {
			escaped = true
			continue
		}
		if quote != 0 {
			if ch == quote {
				quote = 0
				continue
			}
			if quote == '"' && ch == '\\' && i+1 < len(input) {
				i++
				b.WriteByte(input[i])
				continue
			}
			b.WriteByte(ch)
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case ' ', '\t', '\n':
			if b.Len() > 0 {
				fields = append(fields, b.String())
				b.Reset()
			}
		default:
			b.WriteByte(ch)
		}
	}
	if escaped {
		b.WriteByte('\\')
	}
	if quote != 0 {
		return nil, errors.New("unterminated quote in request file")
	}
	if b.Len() > 0 {
		fields = append(fields, b.String())
	}
	return fields, nil
}

func unescapePowerShell(value string) string {
	replacements := map[string]string{
		"`\"": "\"",
		"``":  "`",
		"`$":  "$",
		"`%":  "%",
	}
	for old, replacement := range replacements {
		value = strings.ReplaceAll(value, old, replacement)
	}
	return value
}

func trimQuotes(value string) string {
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			return value[1 : len(value)-1]
		}
	}
	return value
}

func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return raw
	}
	return u.Scheme + "://" + u.Host + u.Path
}

func requestHeaderNames(headers http.Header) []string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		if isSensitiveHeader(name) {
			names = append(names, name+"(redacted)")
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func isSensitiveHeader(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "cookie", "x-xsrf-token", "x-csrf-token":
		return true
	default:
		return false
	}
}
