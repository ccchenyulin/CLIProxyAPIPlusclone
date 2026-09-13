package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/auth/codebuddy"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	codeBuddyChatPath = "/v2/chat/completions"
	codeBuddyAuthType = "codebuddy"

	// CodeBuddy CLI version aligned with the latest public CodeBuddy Code package
	// (verified against community reverse-engineered proxies in 2026-09: orangeboyChen/
	// codebuddy2api uses 2.137.1, JobinBai/codebuddycli-proxy uses 2.130.0). Only used
	// by the international executor path.
	codeBuddyCLIVersion = "2.137.1"
	codeBuddyIntlUserAgent = "CLI/" + codeBuddyCLIVersion + " CodeBuddy/" + codeBuddyCLIVersion
)

// randomHex32 returns 32 lowercase hex chars (used for 32-hex request/conversation IDs).
func randomHex32() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// randomHex16 returns 16 lowercase hex chars (used for OTel span IDs).
func randomHex16() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// randomUUIDCompact returns a UUID stripped of hyphens (32 hex chars).
func randomUUIDCompact() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// codeBuddyBaseURLForDomain picks the correct CodeBuddy API base URL based on
// the account's domain field. International accounts use www.codebuddy.ai;
// everything else (CN or empty) defaults to the CN API at copilot.tencent.com.
func codeBuddyBaseURLForDomain(domain string) string {
	if strings.HasSuffix(domain, "codebuddy.ai") {
		return codebuddy.IntlBaseURL
	}
	return codebuddy.BaseURL
}

// CodeBuddyExecutor handles requests to the CodeBuddy API.
type CodeBuddyExecutor struct {
	cfg *config.Config
}

func sendStreamChunk(ctx context.Context, out chan<- cliproxyexecutor.StreamChunk, chunk cliproxyexecutor.StreamChunk) bool {
	select {
	case out <- chunk:
		return true
	case <-ctx.Done():
		return false
	}
}

// NewCodeBuddyExecutor creates a new CodeBuddy executor instance.
func NewCodeBuddyExecutor(cfg *config.Config) *CodeBuddyExecutor {
	return &CodeBuddyExecutor{cfg: cfg}
}

// Identifier returns the unique identifier for this executor.
func (e *CodeBuddyExecutor) Identifier() string { return codeBuddyAuthType }

// codeBuddyCredentials extracts the access token and domain from auth metadata.
func codeBuddyCredentials(auth *cliproxyauth.Auth) (accessToken, userID, domain string) {
	if auth == nil {
		return "", "", ""
	}
	accessToken = metaStringValue(auth.Metadata, "access_token")
	userID = metaStringValue(auth.Metadata, "user_id")
	domain = metaStringValue(auth.Metadata, "domain")
	if domain == "" {
		domain = codebuddy.DefaultDomain
	}
	return
}

// PrepareRequest prepares the HTTP request before execution.
func (e *CodeBuddyExecutor) PrepareRequest(req *http.Request, auth *cliproxyauth.Auth) error {
	if req == nil {
		return nil
	}
	accessToken, userID, domain := codeBuddyCredentials(auth)
	if accessToken == "" {
		return fmt.Errorf("codebuddy: missing access token")
	}
	e.applyHeaders(req, accessToken, userID, domain)
	return nil
}

// HttpRequest executes a raw HTTP request.
func (e *CodeBuddyExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, fmt.Errorf("codebuddy executor: request is nil")
	}
	if ctx == nil {
		ctx = req.Context()
	}
	httpReq := req.WithContext(ctx)
	if err := e.PrepareRequest(httpReq, auth); err != nil {
		return nil, err
	}
	httpClient := newProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	return httpClient.Do(httpReq)
}

// Execute performs a non-streaming request.
func (e *CodeBuddyExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	reporter := newUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.trackFailure(ctx, &err)

	accessToken, userID, domain := codeBuddyCredentials(auth)
	if accessToken == "" {
		return resp, fmt.Errorf("codebuddy: missing access token")
	}

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai")

	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayloadSource, true)
	translated := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, true)
	requestedModel := payloadRequestedModel(opts, req.Model)
	translated = applyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", translated, originalTranslated, requestedModel)
	translated, _ = sjson.SetBytes(translated, "stream", true)
	translated, _ = sjson.SetBytes(translated, "stream_options.include_usage", true)

	translated, err = thinking.ApplyThinking(translated, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return resp, err
	}

	url := codeBuddyBaseURLForDomain(domain) + codeBuddyChatPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(translated))
	if err != nil {
		return resp, err
	}
	e.applyHeaders(httpReq, accessToken, userID, domain)
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Cache-Control", "no-cache")

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	recordAPIRequest(ctx, e.cfg, upstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      translated,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})

	httpClient := newProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		recordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	defer func() {
		if errClose := httpResp.Body.Close(); errClose != nil {
			log.Errorf("codebuddy executor: close response body error: %v", errClose)
		}
	}()

	recordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if !isHTTPSuccess(httpResp.StatusCode) {
		b, _ := io.ReadAll(httpResp.Body)
		appendAPIResponseChunk(ctx, e.cfg, b)
		log.Debugf("codebuddy executor: upstream error status: %d, body: %s", httpResp.StatusCode, summarizeErrorBody(httpResp.Header.Get("Content-Type"), b))
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return resp, err
	}

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		recordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	appendAPIResponseChunk(ctx, e.cfg, body)
	aggregatedBody, usageDetail, err := aggregateOpenAIChatCompletionStream(body)
	if err != nil {
		recordAPIResponseError(ctx, e.cfg, err)
		return resp, err
	}
	reporter.publish(ctx, usageDetail)
	reporter.ensurePublished(ctx)

	var param any
	out := sdktranslator.TranslateNonStream(ctx, to, from, req.Model, opts.OriginalRequest, translated, aggregatedBody, &param)
	resp = cliproxyexecutor.Response{Payload: []byte(out), Headers: httpResp.Header.Clone()}
	return resp, nil
}

// ExecuteStream performs a streaming request.
func (e *CodeBuddyExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	baseModel := thinking.ParseSuffix(req.Model).ModelName

	reporter := newUsageReporter(ctx, e.Identifier(), baseModel, auth)
	defer reporter.trackFailure(ctx, &err)

	accessToken, userID, domain := codeBuddyCredentials(auth)
	if accessToken == "" {
		return nil, fmt.Errorf("codebuddy: missing access token")
	}

	from := opts.SourceFormat
	to := sdktranslator.FromString("openai")

	originalPayloadSource := req.Payload
	if len(opts.OriginalRequest) > 0 {
		originalPayloadSource = opts.OriginalRequest
	}
	originalTranslated := sdktranslator.TranslateRequest(from, to, baseModel, originalPayloadSource, true)
	translated := sdktranslator.TranslateRequest(from, to, baseModel, req.Payload, true)
	requestedModel := payloadRequestedModel(opts, req.Model)
	translated = applyPayloadConfigWithRoot(e.cfg, baseModel, to.String(), "", translated, originalTranslated, requestedModel)

	translated, err = thinking.ApplyThinking(translated, req.Model, from.String(), to.String(), e.Identifier())
	if err != nil {
		return nil, err
	}

	url := codeBuddyBaseURLForDomain(domain) + codeBuddyChatPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(translated))
	if err != nil {
		return nil, err
	}
	e.applyHeaders(httpReq, accessToken, userID, domain)
	httpReq.Header.Set("Accept", "text/event-stream")
	httpReq.Header.Set("Cache-Control", "no-cache")

	var authID, authLabel, authType, authValue string
	if auth != nil {
		authID = auth.ID
		authLabel = auth.Label
		authType, authValue = auth.AccountInfo()
	}
	recordAPIRequest(ctx, e.cfg, upstreamRequestLog{
		URL:       url,
		Method:    http.MethodPost,
		Headers:   httpReq.Header.Clone(),
		Body:      translated,
		Provider:  e.Identifier(),
		AuthID:    authID,
		AuthLabel: authLabel,
		AuthType:  authType,
		AuthValue: authValue,
	})

	httpClient := newProxyAwareHTTPClient(ctx, e.cfg, auth, 0)
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		recordAPIResponseError(ctx, e.cfg, err)
		return nil, err
	}

	recordAPIResponseMetadata(ctx, e.cfg, httpResp.StatusCode, httpResp.Header.Clone())
	if !isHTTPSuccess(httpResp.StatusCode) {
		b, _ := io.ReadAll(httpResp.Body)
		appendAPIResponseChunk(ctx, e.cfg, b)
		httpResp.Body.Close()
		log.Debugf("codebuddy executor: upstream error status: %d, body: %s", httpResp.StatusCode, summarizeErrorBody(httpResp.Header.Get("Content-Type"), b))
		err = statusErr{code: httpResp.StatusCode, msg: string(b)}
		return nil, err
	}

	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer func() {
			if errClose := httpResp.Body.Close(); errClose != nil {
				log.Errorf("codebuddy executor: close stream body error: %v", errClose)
			}
		}()

		scanner := bufio.NewScanner(httpResp.Body)
		scanner.Buffer(nil, maxScannerBufferSize)
		var param any
		for scanner.Scan() {
			line := scanner.Bytes()
			appendAPIResponseChunk(ctx, e.cfg, line)
			if detail, ok := parseOpenAIStreamUsage(line); ok {
				reporter.publish(ctx, detail)
			}
			if len(line) == 0 {
				continue
			}
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			chunks := sdktranslator.TranslateStream(ctx, to, from, req.Model, opts.OriginalRequest, translated, bytes.Clone(line), &param)
			for i := range chunks {
				if !sendStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Payload: []byte(chunks[i])}) {
					return
				}
			}
		}
		if errScan := scanner.Err(); errScan != nil {
			recordAPIResponseError(ctx, e.cfg, errScan)
			reporter.publishFailure(ctx)
			if !sendStreamChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: errScan}) {
				return
			}
		}
		reporter.ensurePublished(ctx)
	}()

	return &cliproxyexecutor.StreamResult{
		Headers: httpResp.Header.Clone(),
		Chunks:  out,
	}, nil
}

// Refresh exchanges the CodeBuddy refresh token for a new access token.
func (e *CodeBuddyExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	if auth == nil {
		return nil, fmt.Errorf("codebuddy: missing auth")
	}

	refreshToken := metaStringValue(auth.Metadata, "refresh_token")
	if refreshToken == "" {
		log.Debugf("codebuddy executor: no refresh token available, skipping refresh")
		return auth, nil
	}

	accessToken, userID, domain := codeBuddyCredentials(auth)

	// Pick the auth service that matches the account's region: international
	// accounts must refresh against www.codebuddy.ai, CN accounts against
	// copilot.tencent.com. Using the wrong host makes the refresh call fail.
	authSvc := codebuddy.NewCodeBuddyAuth(e.cfg)
	if strings.HasSuffix(domain, "codebuddy.ai") {
		authSvc = codebuddy.NewCodeBuddyIntlAuth(e.cfg)
	}
	storage, err := authSvc.RefreshToken(ctx, accessToken, refreshToken, userID, domain)
	if err != nil {
		return nil, fmt.Errorf("codebuddy: token refresh failed: %w", err)
	}

	updated := auth.Clone()
	updated.Metadata["access_token"] = storage.AccessToken
	if storage.RefreshToken != "" {
		updated.Metadata["refresh_token"] = storage.RefreshToken
	}
	updated.Metadata["expires_in"] = storage.ExpiresIn
	updated.Metadata["domain"] = storage.Domain
	if storage.UserID != "" {
		updated.Metadata["user_id"] = storage.UserID
	}
	now := time.Now()
	updated.UpdatedAt = now
	updated.LastRefreshedAt = now

	return updated, nil
}

// CountTokens is not supported for CodeBuddy.
func (e *CodeBuddyExecutor) CountTokens(_ context.Context, _ *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, fmt.Errorf("codebuddy: count tokens not supported")
}

// applyHeaders sets required headers for CodeBuddy API requests.
// CN accounts keep the historical header set (stable for months, no risk of
// regression); international accounts use a full reverse-engineered CLI header
// profile aligned with community projects (orangeboyChen/codebuddy2api 2.137.1).
func (e *CodeBuddyExecutor) applyHeaders(req *http.Request, accessToken, userID, domain string) {
	if strings.HasSuffix(domain, "codebuddy.ai") {
		e.applyHeadersIntl(req, accessToken, userID, domain)
		return
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", codebuddy.UserAgent)
	req.Header.Set("X-User-Id", userID)
	req.Header.Set("X-Domain", domain)
	req.Header.Set("X-Product", "SaaS")
	req.Header.Set("X-IDE-Type", "vscode")
	req.Header.Set("X-IDE-Name", "CodeBuddy CN")
	req.Header.Set("X-IDE-Version", "2.63.2")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("X-Request-Trace-Id", uuid.New().String()[:16])
	req.Header.Set("Origin", "https://"+domain)
	req.Header.Set("Referer", "https://"+domain+"/")
}

// applyHeadersIntl builds the full CLI-fingerprint header set for international
// CodeBuddy accounts (www.codebuddy.ai). Aligned with the packet-captured request
// headers observed by community proxies orangeboyChen/codebuddy2api and
// JobinBai/codebuddycli-proxy. Includes: OpenAI Stainless SDK fingerprint,
// three-tier session IDs, agent intent/purpose, and OTel + Zipkin B3 tracing.
func (e *CodeBuddyExecutor) applyHeadersIntl(req *http.Request, accessToken, userID, domain string) {
	// Single-request ID bundle. Conversation IDs are generated per HTTP request
	// here because CLIProxyAPI does not maintain cross-request session state; the
	// invariants that matter for upstream risk control are per-request consistency
	// (same message-id for X-Request-ID and X-Conversation-Message-ID, etc.).
	conversationID := uuid.New().String()   // UUID with hyphens, session-level
	conversationReqID := randomUUIDCompact() // 32 hex, turn-level
	messageID := randomUUIDCompact()          // 32 hex, request-level (also used as X-Request-ID)

	// OTel + Zipkin B3 trace context.
	traceID := randomUUIDCompact()
	spanID := randomHex16()
	parentSpanID := randomHex16()

	// --- OpenAI SDK / Chromium-ish basics ---
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	// --- OpenAI Stainless client fingerprint ---
	req.Header.Set("x-stainless-arch", runtime.GOARCH)
	req.Header.Set("x-stainless-lang", "js")
	req.Header.Set("x-stainless-os", "Linux")
	req.Header.Set("x-stainless-package-version", codeBuddyCLIVersion)
	req.Header.Set("x-stainless-retry-count", "0")
	req.Header.Set("x-stainless-timeout", "600")
	req.Header.Set("x-stainless-runtime", "node")
	req.Header.Set("x-stainless-runtime-version", "v18.0.0")

	// --- Session identifiers (three-tier) ---
	req.Header.Set("X-Conversation-ID", conversationID)
	req.Header.Set("X-Conversation-Request-ID", conversationReqID)
	req.Header.Set("X-Conversation-Message-ID", messageID)
	req.Header.Set("X-Agent-Intent", "craft")
	req.Header.Set("X-Agent-Purpose", "conversation")

	// --- IDE / product identity (CLI, not desktop IDE) ---
	req.Header.Set("X-IDE-Type", "CLI")
	req.Header.Set("X-IDE-Name", "CLI")
	req.Header.Set("X-IDE-Version", codeBuddyCLIVersion)
	req.Header.Set("X-Product", "SaaS")
	req.Header.Set("X-Product-Version", codeBuddyCLIVersion)
	req.Header.Set("X-Client-Platform", "web")
	req.Header.Set("X-Private-Data", "false")

	// --- Request-level IDs (X-Request-ID and X-Conversation-Message-ID share value) ---
	req.Header.Set("X-Request-ID", messageID)

	// --- Distributed tracing: W3C traceparent + Zipkin B3 ---
	req.Header.Set("traceparent", fmt.Sprintf("00-%s-%s-01", traceID, spanID))
	req.Header.Set("b3", fmt.Sprintf("%s-%s-1-%s", traceID, spanID, parentSpanID))
	req.Header.Set("x-b3-traceid", traceID)
	req.Header.Set("x-b3-parentspanid", parentSpanID)
	req.Header.Set("x-b3-spanid", spanID)
	req.Header.Set("x-b3-sampled", "1")
	req.Header.Set("x-trace-id", traceID)

	// --- Auth / identity ---
	req.Header.Set("Authorization", "Bearer "+accessToken)
	if userID != "" {
		req.Header.Set("X-User-Id", userID)
	}
	req.Header.Set("X-Domain", domain)

	// --- User-Agent (real CLI uses `axios/1.18.1` after OpenAI SDK UA is stripped;
	// we keep the CLI/<version> CodeBuddy/<version> form used by
	// orangeboyChen/codebuddy2api, which mirrors the CodeBuddy Code npm package)
	req.Header.Set("User-Agent", codeBuddyIntlUserAgent)

	// --- Origin / Referer ---
	req.Header.Set("Origin", "https://"+domain)
	req.Header.Set("Referer", "https://"+domain+"/")
}

type openAIChatStreamChoiceAccumulator struct {
	Role               string
	ContentParts       []string
	ReasoningParts     []string
	FinishReason       string
	ToolCalls          map[int]*openAIChatStreamToolCallAccumulator
	ToolCallOrder      []int
	NativeFinishReason any
}

type openAIChatStreamToolCallAccumulator struct {
	ID        string
	Type      string
	Name      string
	Arguments strings.Builder
}

func aggregateOpenAIChatCompletionStream(raw []byte) ([]byte, usage.Detail, error) {
	lines := bytes.Split(raw, []byte("\n"))
	var (
		responseID  string
		model       string
		created     int64
		serviceTier string
		systemFP    string
		usageDetail usage.Detail
		choices     = map[int]*openAIChatStreamChoiceAccumulator{}
		choiceOrder []int
	)

	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[5:])
		if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		if !gjson.ValidBytes(payload) {
			continue
		}

		root := gjson.ParseBytes(payload)
		if responseID == "" {
			responseID = root.Get("id").String()
		}
		if model == "" {
			model = root.Get("model").String()
		}
		if created == 0 {
			created = root.Get("created").Int()
		}
		if serviceTier == "" {
			serviceTier = root.Get("service_tier").String()
		}
		if systemFP == "" {
			systemFP = root.Get("system_fingerprint").String()
		}
		if detail, ok := parseOpenAIStreamUsage(line); ok {
			usageDetail = detail
		}

		for _, choiceResult := range root.Get("choices").Array() {
			idx := int(choiceResult.Get("index").Int())
			choice := choices[idx]
			if choice == nil {
				choice = &openAIChatStreamChoiceAccumulator{ToolCalls: map[int]*openAIChatStreamToolCallAccumulator{}}
				choices[idx] = choice
				choiceOrder = append(choiceOrder, idx)
			}

			delta := choiceResult.Get("delta")
			if role := delta.Get("role").String(); role != "" {
				choice.Role = role
			}
			if content := delta.Get("content").String(); content != "" {
				choice.ContentParts = append(choice.ContentParts, content)
			}
			if reasoning := delta.Get("reasoning_content").String(); reasoning != "" {
				choice.ReasoningParts = append(choice.ReasoningParts, reasoning)
			}
			if finishReason := choiceResult.Get("finish_reason").String(); finishReason != "" {
				choice.FinishReason = finishReason
			}
			if nativeFinishReason := choiceResult.Get("native_finish_reason"); nativeFinishReason.Exists() {
				choice.NativeFinishReason = nativeFinishReason.Value()
			}

			for _, toolCallResult := range delta.Get("tool_calls").Array() {
				toolIdx := int(toolCallResult.Get("index").Int())
				toolCall := choice.ToolCalls[toolIdx]
				if toolCall == nil {
					toolCall = &openAIChatStreamToolCallAccumulator{}
					choice.ToolCalls[toolIdx] = toolCall
					choice.ToolCallOrder = append(choice.ToolCallOrder, toolIdx)
				}
				if id := toolCallResult.Get("id").String(); id != "" {
					toolCall.ID = id
				}
				if typ := toolCallResult.Get("type").String(); typ != "" {
					toolCall.Type = typ
				}
				if name := toolCallResult.Get("function.name").String(); name != "" {
					toolCall.Name = name
				}
				if args := toolCallResult.Get("function.arguments").String(); args != "" {
					toolCall.Arguments.WriteString(args)
				}
			}
		}
	}

	if responseID == "" && model == "" && len(choiceOrder) == 0 {
		return nil, usageDetail, fmt.Errorf("codebuddy: streaming response did not contain any chat completion chunks")
	}

	response := map[string]any{
		"id":      responseID,
		"object":  "chat.completion",
		"created": created,
		"model":   model,
		"choices": make([]map[string]any, 0, len(choiceOrder)),
		"usage": map[string]any{
			"prompt_tokens":     usageDetail.InputTokens,
			"completion_tokens": usageDetail.OutputTokens,
			"total_tokens":      usageDetail.TotalTokens,
		},
	}
	if serviceTier != "" {
		response["service_tier"] = serviceTier
	}
	if systemFP != "" {
		response["system_fingerprint"] = systemFP
	}

	for _, idx := range choiceOrder {
		choice := choices[idx]
		message := map[string]any{
			"role":    choice.Role,
			"content": strings.Join(choice.ContentParts, ""),
		}
		if message["role"] == "" {
			message["role"] = "assistant"
		}
		if len(choice.ReasoningParts) > 0 {
			message["reasoning_content"] = strings.Join(choice.ReasoningParts, "")
		}
		if len(choice.ToolCallOrder) > 0 {
			toolCalls := make([]map[string]any, 0, len(choice.ToolCallOrder))
			for _, toolIdx := range choice.ToolCallOrder {
				toolCall := choice.ToolCalls[toolIdx]
				toolCallType := toolCall.Type
				if toolCallType == "" {
					toolCallType = "function"
				}
				arguments := toolCall.Arguments.String()
				if arguments == "" {
					arguments = "{}"
				}
				toolCalls = append(toolCalls, map[string]any{
					"id":   toolCall.ID,
					"type": toolCallType,
					"function": map[string]any{
						"name":      toolCall.Name,
						"arguments": arguments,
					},
				})
			}
			message["tool_calls"] = toolCalls
		}

		finishReason := choice.FinishReason
		if finishReason == "" {
			finishReason = "stop"
		}
		choicePayload := map[string]any{
			"index":         idx,
			"message":       message,
			"finish_reason": finishReason,
		}
		if choice.NativeFinishReason != nil {
			choicePayload["native_finish_reason"] = choice.NativeFinishReason
		}
		response["choices"] = append(response["choices"].([]map[string]any), choicePayload)
	}

	out, err := json.Marshal(response)
	if err != nil {
		return nil, usageDetail, fmt.Errorf("codebuddy: failed to encode aggregated response: %w", err)
	}
	return out, usageDetail, nil
}
