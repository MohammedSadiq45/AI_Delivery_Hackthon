package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	chroma "github.com/amikos-tech/chroma-go/pkg/api/v2"
	// v2 "github.com/amikos-tech/chroma-go/pkg/api/v2"
	"github.com/gorilla/websocket"
	openai "github.com/sashabaranov/go-openai"
	"github.com/segmentio/kafka-go"
)

///////////////////////////
// CONFIG
///////////////////////////

type Config struct {
	KafkaBrokers string
	KafkaTopic   string
	KafkaGroupID string

	OpenAIAPIKey string
	OpenAIModel  string

	ChromaURL        string
	ChromaCollection string

	WebsocketAddr string // e.g. ":8080"
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

///////////////////////////
// EVENT + AI SCHEMAS
///////////////////////////

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

type EventInsight struct {
	EventID   string `json:"event_id"`
	EventName string `json:"event_name"`

	Summary   string  `json:"summary"`
	Reasoning string  `json:"reasoning"`
	Impact    string  `json:"impact"`
	Sentiment string  `json:"sentiment"`
	RiskScore float64 `json:"risk_score"`

	Metrics        map[string]float64 `json:"metrics"`
	RoleViews      []RoleView         `json:"role_views"`
	ActionSequence []ActionItem       `json:"action_sequence"`
}

///////////////////////////
// RAG CLIENT (CHROMA)
///////////////////////////

type RagClient struct {
	client     chroma.Client
	collection chroma.Collection
}

func NewRagClient(ctx context.Context, cfg Config) (*RagClient, error) {
	client, err := chroma.NewHTTPClient(chroma.WithBaseURL(cfg.ChromaURL))
	if err != nil {
		return nil, err
	}

	col, err := client.GetOrCreateCollection(ctx, cfg.ChromaCollection)
	if err != nil {
		return nil, err
	}

	return &RagClient{
		client:     client,
		collection: col,
	}, nil
}

func (r *RagClient) FetchContext(ctx context.Context, evt *Event) ([]string, error) {
	query := evt.RagQuery
	if query == "" {
		query = evt.EventName + " " + evt.EventType
		if desc, ok := evt.Payload["description"].(string); ok {
			query += " " + desc
		}
	}

	qr, err := r.collection.Query(ctx,
		chroma.WithQueryTexts(query),
		chroma.WithNResults(5),
	)
	if err != nil {
		return nil, err
	}

	dGroups := qr.GetDocumentsGroups()
	if len(dGroups) == 0 {
		return []string{}, nil
	}

	docs := dGroups[0]
	texts := make([]string, len(docs))

	for i, doc := range docs {
		// Cast to concrete struct if needed
		if d, ok := doc.(interface{ GetText() string }); ok {
			texts[i] = d.GetText()
		} else {
			texts[i] = "" // fallback
		}
	}

	return texts, nil
}

func (r *RagClient) Close() error {
	return r.client.Close()
}

///////////////////////////
// OPENAI AI ENGINE
///////////////////////////

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

var ErrNoChoices = &AppError{"openai returned no choices"}

func (e *AIEngine) GenerateInsights(ctx context.Context, evt *Event) (*EventInsight, error) {
	ragDocs, err := e.rag.FetchContext(ctx, evt)
	if err != nil {
		log.Printf("RAG error (non-fatal), continuing without context: %v", err)
	}

	prompt := buildPrompt(evt, ragDocs)

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
		return nil, err
	}

	if len(resp.Choices) == 0 {
		return nil, ErrNoChoices
	}

	content := resp.Choices[0].Message.Content
	var insight EventInsight
	if err := json.Unmarshal([]byte(content), &insight); err != nil {
		log.Printf("Failed to parse JSON from LLM: %s", content)
		return nil, err
	}

	if insight.EventID == "" {
		insight.EventID = evt.EventID
	}
	if insight.EventName == "" {
		insight.EventName = evt.EventName
	}

	return &insight, nil
}

// Prompt builder
func buildPrompt(evt *Event, ragDocs []string) string {
	evtJSON, _ := json.Marshal(evt)
	ragJSON, _ := json.Marshal(ragDocs)

	example := EventInsight{
		EventID:   evt.EventID,
		EventName: evt.EventName,
		Summary:   "Short summary of the event.",
		Reasoning: "Why this event happened.",
		Impact:    "Impact explanation.",
		Sentiment: "neutral",
		RiskScore: 0.5,
		Metrics:   map[string]float64{"quality_risk": 0.2},
		RoleViews: []RoleView{
			{
				Role:               "Project Manager",
				Perspective:        "Timeline impact",
				RecommendedActions: []string{"Example action 1", "Example action 2"},
				Metrics:            map[string]float64{"timeline_slip_days": 2},
			},
		},
		ActionSequence: []ActionItem{
			{
				ID:               "act-1",
				Title:            "Example action",
				Description:      "Update JIRA story priority",
				OwnerRole:        "Project Manager",
				Priority:         "High",
				SuggestedDue:     "2025-11-21",
				RequiresApproval: true,
			},
		},
	}
	exampleJSON, _ := json.Marshal(example)

	return `
You are generating JSON for an AI Delivery Companion.
EVENT (JSON):
` + string(evtJSON) + `
RAG_CONTEXT:
` + string(ragJSON) + `
TARGET_JSON_EXAMPLE:
` + string(exampleJSON)
}

///////////////////////////
// WEBSOCKET HUB
///////////////////////////

type WebsocketHub struct {
	mu       sync.Mutex
	clients  map[*websocket.Conn]bool
	upgrader websocket.Upgrader
}

func NewWebsocketHub() *WebsocketHub {
	return &WebsocketHub{
		clients: make(map[*websocket.Conn]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (h *WebsocketHub) HandleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade error: %v", err)
		return
	}

	h.mu.Lock()
	h.clients[conn] = true
	h.mu.Unlock()
	log.Printf("WebSocket client connected (%d total)", len(h.clients))

	go func() {
		defer func() {
			h.mu.Lock()
			delete(h.clients, conn)
			h.mu.Unlock()
			conn.Close()
			log.Printf("WebSocket client disconnected (%d total)", len(h.clients))
		}()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

func (h *WebsocketHub) Broadcast(v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("broadcast marshal error: %v", err)
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for conn := range h.clients {
		if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
			conn.Close()
			delete(h.clients, conn)
		}
	}
}

///////////////////////////
// KAFKA CONSUMER LOOP
///////////////////////////

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
				log.Printf("Failed to parse event JSON: %v", err)
				continue
			}

			insight, err := engine.GenerateInsights(ctx, &evt)
			if err != nil {
				log.Printf("AI engine error: %v", err)
				continue
			}

			hub.Broadcast(insight)
		}
	}()

	return nil
}

///////////////////////////
// ERROR TYPE
///////////////////////////

type AppError struct {
	Msg string
}

func (e *AppError) Error() string { return e.Msg }

///////////////////////////
// MAIN
///////////////////////////

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
	defer rag.Close()

	engine := NewAIEngine(cfg, rag)

	hub := NewWebsocketHub()
	http.HandleFunc("/ws", hub.HandleWS)

	go func() {
		log.Printf("WebSocket server listening on %s", cfg.WebsocketAddr)
		if err := http.ListenAndServe(cfg.WebsocketAddr, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	if err := StartKafkaConsumer(ctx, cfg, engine, hub); err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	cancel()
	time.Sleep(1 * time.Second)
	log.Println("Exiting cleanly. Bye 👋")
}
