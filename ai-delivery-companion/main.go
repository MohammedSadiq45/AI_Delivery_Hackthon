package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	v2 "github.com/amikos-tech/chroma-go/pkg/api/v2"

	"github.com/gorilla/websocket"
	openai "github.com/sashabaranov/go-openai"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string

	OpenAIAPIKey string
	OpenAIModel  string

	ChromaURL        string
	ChromaCollection string

	WebsocketAddr string
}

func LoadConfig() Config {
	return Config{
		KafkaBrokers:     getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "events"),
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "ai-delivery-companion"),
		OpenAIAPIKey:     getEnv("OPENAI_API_KEY", ""),
		OpenAIModel:      getEnv("OPENAI_MODEL", "gpt-4o-mini"),
		ChromaURL:        getEnv("CHROMA_URL", "http://localhost:8000"),
		ChromaCollection: getEnv("CHROMA_COLLECTION", "ai_delivery_events"),
		WebsocketAddr:    getEnv("WS_ADDR", ":8080"),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type Event struct {
	EventID       string                 `json:"event_id"`
	EventName     string                 `json:"event_name"`
	EventType     string                 `json:"event_type"`
	Source        string                 `json:"source"`
	ProjectID     string                 `json:"project_id"`
	RolesAffected []string               `json:"roles_affected"`
	Payload       map[string]interface{} `json:"payload"`
	RagQuery      string                 `json:"rag_query"`
	Timestamp     time.Time              `json:"timestamp"`
}

type RoleView struct {
	Role               string             `json:"role"`
	Perspective        string             `json:"perspective"`
	RecommendedActions []string           `json:"recommended_actions"`
	Metrics            map[string]float64 `json:"metrics,omitempty"`
}

type ActionItem struct {
	ID               string `json:"id"`
	Title            string `json:"title"`
	Description      string `json:"description"`
	OwnerRole        string `json:"owner_role"`
	Priority         string `json:"priority"`
	SuggestedDue     string `json:"suggested_due_date"`
	RequiresApproval bool   `json:"requires_approval"`
}

type ExplanationContext struct {
	Source      string `json:"source"`
	Snippet     string `json:"snippet"`
	ReferenceID string `json:"reference_id"`
}

type Explanation struct {
	KeySignals        []string             `json:"key_signals"`
	SupportingContext []ExplanationContext `json:"supporting_context"`
}

type EventInsight struct {
	EventID   string `json:"event_id"`
	EventName string `json:"event_name"`

	Summary   string  `json:"summary"`
	Reasoning string  `json:"reasoning"`
	Impact    string  `json:"impact"`
	Sentiment string  `json:"sentiment"`
	RiskScore float64 `json:"risk_score"`

	Severity        string   `json:"severity"`
	Category        string   `json:"category"`
	Status          string   `json:"status"`
	ConfidenceScore float64  `json:"confidence_score"`
	Tags            []string `json:"tags"`

	Metrics        map[string]float64 `json:"metrics"`
	RoleViews      []RoleView         `json:"role_views"`
	ActionSequence []ActionItem       `json:"action_sequence"`

	Explanation Explanation `json:"explanation"`
}

type ChatTurn struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type ChatRequest struct {
	ChatID       string     `json:"chat_id"`
	EventID      string     `json:"event_id"`
	UserQuestion string     `json:"user_question"`
	History      []ChatTurn `json:"history"`
	Timestamp    time.Time  `json:"timestamp"`
}

type ChatResponse struct {
	ChatID    string     `json:"chat_id"`
	EventID   string     `json:"event_id"`
	Answer    string     `json:"answer"`
	CreatedAt time.Time  `json:"created_at"`
	History   []ChatTurn `json:"history"`
}

type RagClient struct {
	client         v2.Client
	collection     v2.Collection
	collectionID   string
	baseURL        string
	collectionName string
	httpClient     *http.Client
}

// func NewRagClient(ctx context.Context, cfg Config) (*RagClient, error) {
// 	client, err := v2.NewHTTPClient(v2.WithBaseURL(cfg.ChromaURL))
// 	if err != nil {
// 		return nil, err
// 	}

// 	col, err := client.GetOrCreateCollection(ctx, cfg.ChromaCollection)
// 	if err != nil {
// 		return nil, err
// 	}

// 	// ✔ Correct: return a *RagClient struct
// 	return &RagClient{
// 		client:         client,
// 		collection:     col,
// 		baseURL:        strings.TrimRight(cfg.ChromaURL, "/"),
// 		collectionName: cfg.ChromaCollection,
// 		httpClient:     &http.Client{Timeout: 10 * time.Second},
// 	}, nil
// }

func NewRagClient(ctx context.Context, cfg Config) (*RagClient, error) {
	client, err := v2.NewHTTPClient(v2.WithBaseURL(cfg.ChromaURL))
	if err != nil {
		return nil, err
	}

	col, err := client.GetOrCreateCollection(ctx, cfg.ChromaCollection)
	if err != nil {
		return nil, err
	}

	return &RagClient{
		client:         client,
		collection:     col,
		collectionID:   col.ID(), // ← FIX: call the method
		baseURL:        strings.TrimRight(cfg.ChromaURL, "/"),
		collectionName: cfg.ChromaCollection,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (r *RagClient) AddDocumentToChroma(ctx context.Context, id, text string, metadata map[string]interface{}) error {

	url := fmt.Sprintf(
		"%s/api/v2/tenants/default/databases/default/collections/%s/upsert",
		r.baseURL,
		r.collectionID, // UUID not name
	)

	payload := map[string]interface{}{
		"ids":       []string{id},
		"documents": []string{text},
		"metadatas": []map[string]interface{}{metadata},
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("chroma upsert failed %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (r *RagClient) FetchContext(ctx context.Context, evt *Event) ([]string, error) {
	query := evt.RagQuery
	if query == "" {
		query = evt.EventName + " " + evt.EventType
		if desc, ok := evt.Payload["description"].(string); ok {
			query += " " + desc
		}
	}

	log.Printf("[RAG] Constructed query for EventID=%s: %s", evt.EventID, query)

	qr, err := r.collection.Query(ctx,
		v2.WithQueryTexts(query),
		v2.WithNResults(5),
	)
	if err != nil {
		return nil, err
	}

	dGroups := qr.GetDocumentsGroups()
	if len(dGroups) == 0 {
		log.Printf("[RAG] No documents found for EventID=%s", evt.EventID)
		return []string{}, nil
	}

	docs := dGroups[0]
	texts := make([]string, len(docs))

	for i, doc := range docs {
		if d, ok := doc.(interface{ GetText() string }); ok {
			texts[i] = d.GetText()
		} else {
			texts[i] = ""
		}
	}

	log.Printf("[RAG] Texts retrieved for EventID=%s: %+v", evt.EventID, texts)
	return texts, nil
}

// SearchContext: NEW – used for chat when EventID is empty; semantic search on user question
// func (r *RagClient) SearchContext(ctx context.Context, query string) ([]string, error) {
// 	q := strings.TrimSpace(query)
// 	if q == "" {
// 		return []string{}, nil
// 	}

// 	log.Printf("[RAG] Chat search query: %s", q)

// 	qr, err := r.collection.Query(ctx,
// 		v2.WithQueryTexts(q),
// 		v2.WithNResults(5),
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	dGroups := qr.GetDocumentsGroups()
// 	if len(dGroups) == 0 {
// 		log.Printf("[RAG] Chat search: no documents for query=%q", q)
// 		return []string{}, nil
// 	}

// 	docs := dGroups[0]
// 	texts := make([]string, len(docs))
// 	for i, doc := range docs {
// 		if d, ok := doc.(interface{ GetText() string }); ok {
// 			texts[i] = d.GetText()
// 		} else {
// 			texts[i] = ""
// 		}
// 	}

// 	log.Printf("[RAG] Chat search retrieved %d docs for query=%q", len(texts), q)
// 	return texts, nil
// }

// type chromaAddPayload struct {
// 	IDs       []string                 `json:"ids"`
// 	Documents []string                 `json:"documents"`
// 	Metadatas []map[string]interface{} `json:"metadatas,omitempty"`
// }

type chromaGetPayload struct {
	IDs     []string `json:"ids"`
	Include []string `json:"include,omitempty"`
}

type chromaGetResponse struct {
	IDs       []string                 `json:"ids"`
	Documents []string                 `json:"documents"`
	Metadatas []map[string]interface{} `json:"metadatas"`
}

// func (r *RagClient) AddDocumentToChroma(ctx context.Context, id string, text string, metadata map[string]interface{}) error {
// 	payload := chromaAddPayload{
// 		IDs:       []string{id},
// 		Documents: []string{text},
// 	}
// 	if metadata != nil {
// 		payload.Metadatas = []map[string]interface{}{metadata}
// 	}

// 	body, _ := json.Marshal(payload)
// 	url := fmt.Sprintf("%s/collections/%s/add", r.baseURL, r.collectionName)

// 	log.Printf("[Chroma] add URL=%s", url)

// 	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
// 	if err != nil {
// 		return err
// 	}
// 	req.Header.Set("Content-Type", "application/json")

// 	resp, err := r.httpClient.Do(req)
// 	if err != nil {
// 		return err
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode >= 400 {
// 		data, _ := io.ReadAll(resp.Body)
// 		return fmt.Errorf("chroma add status %d: %s", resp.StatusCode, string(data))
// 	}

// 	return nil
// }

// GetEventFromChroma – used when ChatRequest has EventID
func (r *RagClient) GetEventFromChroma(ctx context.Context, eventID string) (*Event, []string, error) {
	payload := chromaGetPayload{
		IDs:     []string{eventID},
		Include: []string{"metadatas", "documents"},
	}

	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/collections/%s/get", r.baseURL, r.collectionName)

	log.Printf("[Chroma] get URL=%s event_id=%s", url, eventID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		data, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("chroma get status %d: %s", resp.StatusCode, string(data))
	}

	var out chromaGetResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, nil, err
	}

	if len(out.IDs) == 0 {
		log.Printf("[Chroma] no records for event_id=%s", eventID)
		return nil, out.Documents, nil
	}

	var evt Event
	if len(out.Metadatas) > 0 {
		md := out.Metadatas[0]

		if raw, ok := md["event_json"]; ok {
			if s, ok := raw.(string); ok {
				if err := json.Unmarshal([]byte(s), &evt); err == nil {
					return &evt, out.Documents, nil
				}
			}
		}

		if id, ok := md["event_id"].(string); ok {
			evt.EventID = id
		} else {
			evt.EventID = eventID
		}
		if name, ok := md["event_name"].(string); ok {
			evt.EventName = name
		}
		if et, ok := md["event_type"].(string); ok {
			evt.EventType = et
		}
		if pid, ok := md["project_id"].(string); ok {
			evt.ProjectID = pid
		}
		if ts, ok := md["timestamp"].(string); ok {
			if t, err := time.Parse(time.RFC3339, ts); err == nil {
				evt.Timestamp = t
			}
		}
		return &evt, out.Documents, nil
	}

	return nil, out.Documents, nil
}

func (r *RagClient) Close() error {
	return r.client.Close()
}

type AIEngine struct {
	client *openai.Client
	model  string
	rag    *RagClient
}

func NewAIEngine(cfg Config, rag *RagClient) *AIEngine {
	client := openai.NewClient(cfg.OpenAIAPIKey)
	return &AIEngine{
		client: client,
		model:  cfg.OpenAIModel,
		rag:    rag,
	}
}

type AppError struct {
	Msg string
}

func (e *AppError) Error() string { return e.Msg }

var ErrNoChoices = &AppError{"openai returned no choices"}

func (e *AIEngine) GenerateInsights(ctx context.Context, evt *Event) (*EventInsight, error) {
	start := time.Now()
	log.Printf("[AI] ⇢ GenerateInsights START for event_id=%s event_name=%s type=%s",
		evt.EventID, evt.EventName, evt.EventType)

	ragDocs, err := e.rag.FetchContext(ctx, evt)
	if err != nil {
		log.Printf("[AI] RAG error (non-fatal): %v", err)
	} else {
		log.Printf("[AI] RAG retrieved %d documents", len(ragDocs))
	}

	prompt := buildPrompt(evt, ragDocs)
	log.Printf("[AI] Prompt generated (len=%d). Preview:\n%s\n---", len(prompt), safePreview(prompt, 300))

	resp, err := e.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: e.model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleSystem,
				Content: "You are an AI Delivery Companion. Respond ONLY with valid JSON matching EventInsight struct.",
			},
			{
				Role:    openai.ChatMessageRoleUser,
				Content: prompt,
			},
		},
		Temperature: 0.2,
	})
	if err != nil {
		log.Printf("[AI] ERROR calling OpenAI: %v", err)
		return nil, err
	}
	if len(resp.Choices) == 0 {
		log.Printf("[AI] ERROR: No choices returned from OpenAI")
		return nil, ErrNoChoices
	}

	content := resp.Choices[0].Message.Content
	log.Printf("[AI] Raw LLM response received (len=%d)", len(content))
	log.Printf("[AI] RAW LLM RESPONSE BODY:\n%s\n---", content)

	clean := strings.TrimSpace(content)
	clean = strings.TrimPrefix(clean, "```json")
	clean = strings.TrimPrefix(clean, "```")
	clean = strings.TrimSuffix(clean, "```")

	var insight EventInsight
	if err := json.Unmarshal([]byte(clean), &insight); err != nil {
		log.Printf("[AI] ERROR parsing JSON. Raw response:\n%s\n---", content)
		return nil, err
	}

	if insight.EventID == "" {
		insight.EventID = evt.EventID
	}
	if insight.EventName == "" {
		insight.EventName = evt.EventName
	}

	log.Printf("[AI] ✓ Insight generated for event_id=%s severity=%s category=%s risk=%.2f",
		insight.EventID, insight.Severity, insight.Category, insight.RiskScore)

	log.Printf("[AI] INSIGHT SUMMARY: %s", insight.Summary)
	log.Printf("[AI] GenerateInsights END (duration=%s)", time.Since(start))

	return &insight, nil
}

func buildPrompt(evt *Event, ragDocs []string) string {
	evtJSON, _ := json.Marshal(evt)
	ragJSON, _ := json.Marshal(ragDocs)

	example := EventInsight{
		EventID:   evt.EventID,
		EventName: evt.EventName,

		Summary:   "Short, executive summary of the event.",
		Reasoning: "Why this event is happening, based on signals and context.",
		Impact:    "What this event means for the project and stakeholders.",
		Sentiment: "neutral",
		RiskScore: 0.5,

		Severity:        "Medium",
		Category:        "Delivery",
		Status:          "Open",
		ConfidenceScore: 0.75,
		Tags:            []string{"example", "demo"},

		Metrics: map[string]float64{
			"quality_risk":       0.2,
			"timeline_slip_days": 1,
		},
		RoleViews: []RoleView{
			{
				Role:        "Project Manager",
				Perspective: "Focus on delivery risk and timeline impact.",
				RecommendedActions: []string{
					"Review current sprint commitments.",
					"Communicate risk to stakeholders.",
				},
				Metrics: map[string]float64{
					"timeline_slip_days": 1,
				},
			},
			{
				Role:        "Developer",
				Perspective: "Focus on implementation complexity and technical risk.",
				RecommendedActions: []string{
					"Investigate failing components.",
					"Refactor high-risk modules.",
				},
				Metrics: map[string]float64{
					"bug_count": 2,
				},
			},
		},
		ActionSequence: []ActionItem{
			{
				ID:               "act-1",
				Title:            "Example action",
				Description:      "Update JIRA story priority based on new risk.",
				OwnerRole:        "Project Manager",
				Priority:         "High",
				SuggestedDue:     "2025-11-21",
				RequiresApproval: true,
			},
		},
		Explanation: Explanation{
			KeySignals: []string{
				"Recent increase in related bug reports.",
				"Negative sentiment in team communication.",
			},
			SupportingContext: []ExplanationContext{
				{
					Source:      "RAG",
					Snippet:     "In previous checkout incidents, missing retries led to similar failures.",
					ReferenceID: "doc-123",
				},
			},
		},
	}

	exampleJSON, _ := json.Marshal(example)

	return `
You are an AI assistant generating JSON for an "AI Delivery Companion" dashboard.

Your task:
- Read the EVENT and RAG_CONTEXT.
- Produce a SINGLE JSON object that matches the TARGET_JSON_EXAMPLE structure.
- Customize the values based on the event and context, but keep the same shape and field names.

Important rules:
- Output ONLY valid JSON. Do NOT include any markdown, backticks, or extra text.
- Always include all top-level fields: event_id, event_name, summary, reasoning, impact,
  sentiment, risk_score, severity, category, status, confidence_score, tags, metrics,
  role_views, action_sequence, explanation.
- For unknown numeric values, make a reasonable estimate (e.g., 0.0 or small numbers).
- For role_views, include at least the roles mentioned in the EVENT roles_affected when possible.
- For explanation, briefly list key signals and at least one supporting context if RAG_CONTEXT is not empty.

EVENT (JSON):
` + string(evtJSON) + `

RAG_CONTEXT (array of strings):
` + string(ragJSON) + `

TARGET_JSON_EXAMPLE (follow this shape, but adapt values):
` + string(exampleJSON)
}

func safePreview(input string, max int) string {
	if len(input) <= max {
		return input
	}
	return input[:max] + "...(truncated)"
}

func (e *AIEngine) GenerateChatResponse(ctx context.Context, req *ChatRequest) (*ChatResponse, error) {
	start := time.Now()
	log.Printf("[CHAT] ⇢ GenerateChatResponse START chat_id=%s event_id=%s",
		req.ChatID, req.EventID)

	var evt *Event
	var docs []string
	var err error

	if strings.TrimSpace(req.EventID) != "" {
		evt, docs, err = e.rag.GetEventFromChroma(ctx, req.EventID)
		if err != nil {
			log.Printf("[CHAT] RAG get error (non-fatal): %v", err)
		} else {
			if evt != nil {
				log.Printf("[CHAT] RAG retrieved event for event_id=%s", req.EventID)
			} else {
				log.Printf("[CHAT] RAG returned no event metadata for event_id=%s", req.EventID)
			}
			log.Printf("[CHAT] RAG returned %d documents for event_id=%s", len(docs), req.EventID)
		}
	} else {
		log.Printf("[CHAT] No EventID supplied → Running semantic search on user_question")
		docs, err = e.rag.SearchContext(ctx, req.UserQuestion)
		if err != nil {
			log.Printf("[CHAT] RAG search error (non-fatal): %v", err)
			docs = []string{}
		}
		log.Printf("[CHAT] RAG search returned %d documents for question", len(docs))
		evt = nil
	}

	ctxText := buildChatContext(evt, docs)

	var messages []openai.ChatCompletionMessage

	// STRICT system prompt (no disclaimers)
	messages = append(messages, openai.ChatCompletionMessage{
		Role: openai.ChatMessageRoleSystem,
		Content: `
You are an AI Delivery Companion Chat assistant.

STRICT RULES:
- Never mention missing data, lack of details, incomplete context, or uncertainty.
- Never generate disclaimers or meta commentary.
- Never ask the user for more details.
- Always respond with confidence.
- If EVENT or RAG documents are empty, infer context using common industry patterns.
- Provide practical, role-aware insights (PM, Developer, QA, Architect, SRE).
- Use actionable, concise delivery language.
- Always give structured, high-value reasoning and practical guidance.
- Assume plausible scenarios even with limited context.
`,
	})

	messages = append(messages, openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleSystem,
		Content: ctxText,
	})

	for _, turn := range req.History {
		clean := strings.TrimSpace(turn.Content)
		if clean == "" {
			continue
		}

		role := openai.ChatMessageRoleUser
		switch strings.ToLower(turn.Role) {
		case "assistant":
			role = openai.ChatMessageRoleAssistant
		case "system":
			role = openai.ChatMessageRoleSystem
		}

		messages = append(messages, openai.ChatCompletionMessage{
			Role:    role,
			Content: clean,
		})
	}

	userQ := strings.TrimSpace(req.UserQuestion)
	if userQ == "" {
		userQ = "(empty user question)"
	}
	messages = append(messages, openai.ChatCompletionMessage{
		Role:    openai.ChatMessageRoleUser,
		Content: userQ,
	})

	log.Printf("[CHAT] Sending chat prompt to OpenAI model=%s", e.model)

	resp, err := e.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model:       e.model,
		Messages:    messages,
		Temperature: 0.3,
	})
	if err != nil {
		log.Printf("[CHAT] ERROR calling OpenAI: %v", err)
		return nil, err
	}
	if len(resp.Choices) == 0 {
		log.Printf("[CHAT] ERROR: No choices returned from OpenAI")
		return nil, ErrNoChoices
	}

	answer := strings.TrimSpace(resp.Choices[0].Message.Content)
	now := time.Now().UTC()

	if req.Timestamp.IsZero() {
		req.Timestamp = now
	}

	extendedHistory := append(req.History, ChatTurn{
		Role:      "user",
		Content:   userQ,
		Timestamp: req.Timestamp,
	})
	extendedHistory = append(extendedHistory, ChatTurn{
		Role:      "assistant",
		Content:   answer,
		Timestamp: now,
	})

	respObj := &ChatResponse{
		ChatID:    req.ChatID,
		EventID:   req.EventID,
		Answer:    answer,
		CreatedAt: now,
		History:   extendedHistory,
	}

	log.Printf("[CHAT] ✓ Chat response generated chat_id=%s event_id=%s duration=%s",
		req.ChatID, req.EventID, time.Since(start))

	return respObj, nil
}

func buildChatContext(evt *Event, docs []string) string {
	var evtJSON string
	if evt != nil {
		b, _ := json.Marshal(evt)
		evtJSON = string(b)
	} else {
		evtJSON = "{}"
	}
	docsJSON, _ := json.Marshal(docs)

	return fmt.Sprintf(
		"EVENT JSON:\n%s\n\nRAG_CONTEXT_DOCS (array of strings):\n%s\n",
		evtJSON,
		docsJSON,
	)
}

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 8192
)

type WSClient struct {
	hub  *WebsocketHub
	conn *websocket.Conn
	send chan []byte
}

type WebsocketHub struct {
	clients    map[*WSClient]bool
	register   chan *WSClient
	unregister chan *WSClient
	broadcast  chan []byte
	upgrader   websocket.Upgrader

	engine *AIEngine
}

func NewWebsocketHub() *WebsocketHub {
	return &WebsocketHub{
		clients:    make(map[*WSClient]bool),
		register:   make(chan *WSClient),
		unregister: make(chan *WSClient),
		broadcast:  make(chan []byte, 256),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *WebsocketHub) Run() {
	for {
		select {
		case c := <-h.register:
			h.clients[c] = true
			log.Printf("WebSocket client registered (%d total)", len(h.clients))
		case c := <-h.unregister:
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
				log.Printf("WebSocket client unregistered (%d total)", len(h.clients))
			}
		case msg := <-h.broadcast:
			for c := range h.clients {
				select {
				case c.send <- msg:
				default:
					close(c.send)
					delete(h.clients, c)
					log.Printf("WebSocket client dropped (send buffer full)")
				}
			}
		}
	}
}

func (h *WebsocketHub) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}

	client := &WSClient{
		hub:  h,
		conn: conn,
		send: make(chan []byte, 256),
	}

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	h.register <- client

	go client.writePump()
	go client.readPump()
}

func (h *WebsocketHub) Broadcast(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("broadcast marshal error: %v", err)
		return
	}
	select {
	case h.broadcast <- data:
	default:
		log.Printf("hub broadcast channel full; dropping message")
	}
}

func (c *WSClient) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()

	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket unexpected close: %v", err)
			}
			return
		}

		var req ChatRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp := map[string]string{"error": "invalid chat payload"}
			b, _ := json.Marshal(resp)
			c.sendNonBlocking(b)
			continue
		}

		go func(r ChatRequest, client *WSClient) {
			respObj, err := client.hub.handleChatRequestWS(r)
			if err != nil {
				errResp := map[string]string{"error": "AI generation failed"}
				b, _ := json.Marshal(errResp)
				client.sendNonBlocking(b)
				return
			}
			b, _ := json.Marshal(respObj)

			client.sendNonBlocking(b)
			client.hub.Broadcast(respObj)
		}(req, c)
	}
}

func (c *WSClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			if _, err := w.Write(msg); err != nil {
				_ = w.Close()
				return
			}

			n := len(c.send)
			for i := 0; i < n; i++ {
				more := <-c.send
				if _, err := w.Write([]byte("\n")); err != nil {
					break
				}
				if _, err := w.Write(more); err != nil {
					break
				}
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *WSClient) sendNonBlocking(b []byte) {
	select {
	case c.send <- b:
	default:
		log.Printf("client send buffer full, dropping message")
	}
}

func (h *WebsocketHub) handleChatRequestWS(req ChatRequest) (*ChatResponse, error) {
	if h.engine == nil {
		return nil, fmt.Errorf("AI engine unavailable")
	}
	return h.engine.GenerateChatResponse(context.Background(), &req)
}

func StartKafkaConsumer(ctx context.Context, cfg Config, engine *AIEngine, hub *WebsocketHub) error {
	brokers := strings.Split(cfg.KafkaBrokers, ",")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    cfg.KafkaTopic,
		GroupID:  cfg.KafkaGroupID,
		MinBytes: 1e3,
		MaxBytes: 10e6,
	})

	go func() {
		defer reader.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Kafka read error: %v", err)
				continue
			}

			var evt Event
			if err := json.Unmarshal(msg.Value, &evt); err != nil {
				log.Printf("Failed to parse event JSON: %v, payload=%s", err, string(msg.Value))
				continue
			}

			docText := evt.EventName + " " + evt.EventType
			if desc, ok := evt.Payload["description"].(string); ok && desc != "" {
				docText += " " + desc
			}

			evtJSON, _ := json.Marshal(evt)
			meta := map[string]interface{}{
				"event_id":   evt.EventID,
				"event_name": evt.EventName,
				"event_type": evt.EventType,
				"project_id": evt.ProjectID,
				"timestamp":  evt.Timestamp.Format(time.RFC3339),
				"event_json": string(evtJSON),
			}

			if err := engine.rag.AddDocumentToChroma(ctx, evt.EventID, docText, meta); err != nil {
				log.Printf("[Chroma] failed to add event to Chroma (non-fatal): %v", err)
			} else {
				log.Printf("[Chroma] added event to collection=%s id=%s", engine.rag.collectionName, evt.EventID)
			}

			insight, err := engine.GenerateInsights(ctx, &evt)
			if err != nil {
				log.Printf("AI engine error: %v", err)
				continue
			}

			hub.Broadcast(insight)
			log.Printf("Broadcast insight for event %s", evt.EventID)
		}
	}()

	return nil
}

func (h *WebsocketHub) HandleChatHTTP(engine *AIEngine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"})
			return
		}

		defer r.Body.Close()

		var req ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("[HTTP /chat] decode error: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
			return
		}

		if strings.TrimSpace(req.UserQuestion) == "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "user_question is required"})
			return
		}

		if req.Timestamp.IsZero() {
			req.Timestamp = time.Now().UTC()
		}

		respObj, err := engine.GenerateChatResponse(r.Context(), &req)
		if err != nil {
			log.Printf("[HTTP /chat] AI error: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "failed to generate chat response"})
			return
		}

		h.Broadcast(respObj)

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(respObj); err != nil {
			log.Printf("[HTTP /chat] encode error: %v", err)
		}
	}
}

func main() {
	cfg := LoadConfig()
	if cfg.OpenAIAPIKey == "" {
		log.Fatal("OPENAI_API_KEY is not set")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rag, err := NewRagClient(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to init Chroma RAG client: %v", err)
	}
	defer func() {
		if err := rag.Close(); err != nil {
			log.Printf("Error closing RAG client: %v", err)
		}
	}()

	engine := NewAIEngine(cfg, rag)

	hub := NewWebsocketHub()
	hub.engine = engine

	go hub.Run()

	http.Handle("/ws", withCORS(http.HandlerFunc(hub.ServeWS)))
	http.Handle("/chat", withCORS(hub.HandleChatHTTP(engine)))

	go func() {
		log.Printf("HTTP + WebSocket server listening on %s", cfg.WebsocketAddr)
		if err := http.ListenAndServe(cfg.WebsocketAddr, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	if err := StartKafkaConsumer(ctx, cfg, engine, hub); err != nil {
		log.Fatalf("Failed to start Kafka event consumer: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	cancel()
	time.Sleep(1 * time.Second)
	log.Println("Exiting cleanly. Bye 👋")
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (r *RagClient) SearchContext(ctx context.Context, q string) ([]string, error) {

	url := fmt.Sprintf(
		"%s/api/v2/tenants/default/databases/default/collections/%s/query",
		r.baseURL,
		r.collectionID,
	)

	payload := map[string]interface{}{
		"query_texts": []string{q},
		"n_results":   5,
	}

	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed %d: %s", resp.StatusCode, b)
	}

	var out struct {
		Documents [][]string `json:"documents"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}

	if len(out.Documents) == 0 {
		return []string{}, nil
	}

	return out.Documents[0], nil
}
