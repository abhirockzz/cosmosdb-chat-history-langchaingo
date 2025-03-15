package cosmosdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/schema"
)

type CosmosDBChatMessageHistory struct {
	databaseID   string
	containerID  string
	sessionID    string
	userID       string
	container    *azcosmos.ContainerClient
	messages     []llms.ChatMessage
}

// Pre-reqs: 
// - database and container should be created in advance
// - container should have partition key as /userid
// - (optional) container should have TTL set on either the container or item level

func NewCosmosDBChatMessageHistory(client *azcosmos.Client, databaseID, containerID, sessionID, userID string) (*CosmosDBChatMessageHistory, error) {
	// Input validation
	if client == nil {
		return nil, fmt.Errorf("cosmos DB client cannot be nil")
	}
	if databaseID == "" || containerID == "" || sessionID == "" || userID == "" {
		return nil, fmt.Errorf("databaseID, containerID, sessionID and userID are mandatory")
	}

	history := &CosmosDBChatMessageHistory{
		databaseID:  databaseID,
		containerID: containerID,
		sessionID:   sessionID,
		userID:      userID,
		messages:   []llms.ChatMessage{},
	}

	database, err := client.NewDatabase(databaseID)
	if err != nil {
		return nil, fmt.Errorf("failed to create database client: %w", err)
	}

	container, err := database.NewContainer(containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to create container client: %w", err)
	}

	history.container = container

	return history, nil
}

var _ schema.ChatMessageHistory = &CosmosDBChatMessageHistory{}

func (h *CosmosDBChatMessageHistory) AddMessage(ctx context.Context, message llms.ChatMessage) error {
	if message == nil {
		return fmt.Errorf("cannot add nil message")
	}

	// Add to in-memory cache
	h.messages = append(h.messages, message)

	var chatMessages []llms.ChatMessageModel
	for _, msg := range h.messages {
		chatMessages = append(chatMessages, llms.ConvertChatMessageToModel(msg))
	}

	// Create history document
	history := History{
		SessionId:    h.sessionID,
		UserID:       h.userID,
		ChatMessages: chatMessages,
	}

	historyItem, err := json.Marshal(history)
	if err != nil {
		return fmt.Errorf("failed to marshal chat history: %w", err)
	}

	// Save to Cosmos DB
	_, err = h.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(h.userID), historyItem, nil)
	if err != nil {
		return fmt.Errorf("failed to upsert chat history to Cosmos DB: %w", err)
	}

	return nil
}

func (h *CosmosDBChatMessageHistory) AddUserMessage(ctx context.Context, text string) error {
	return h.AddMessage(ctx, llms.HumanChatMessage{Content: text})
}

func (h *CosmosDBChatMessageHistory) AddAIMessage(ctx context.Context, text string) error {
	return h.AddMessage(ctx, llms.AIChatMessage{Content: text})
}

func (h *CosmosDBChatMessageHistory) Clear(ctx context.Context) error {
	// Reset in-memory messages
	h.messages = make([]llms.ChatMessage, 0)
	
	// Try to delete from the database
	_, err := h.container.DeleteItem(ctx, azcosmos.NewPartitionKeyString(h.userID), h.sessionID, nil)
	
	// If the error is a 404 Not Found, it's not really an error in this context
	if err != nil {
		if cosmosErr, ok := err.(*azcore.ResponseError); ok && cosmosErr.StatusCode == 404 {
			// Item didn't exist, which is fine for a Clear operation
			return nil
		}
		return fmt.Errorf("failed to clear chat history: %w", err)
	}
	
	return nil
}

func (h *CosmosDBChatMessageHistory) SetMessages(ctx context.Context, messages []llms.ChatMessage) error {
	// Validate input
	if messages == nil {
		messages = make([]llms.ChatMessage, 0)
	}

	// Clear existing messages first
	err := h.Clear(ctx)
	if err != nil {
		return fmt.Errorf("failed to clear existing messages: %w", err)
	}

	// If we have no messages to add, we're done
	if len(messages) == 0 {
		return nil
	}

	// Convert messages to model format
	var chatMessages []llms.ChatMessageModel
	for _, message := range messages {
		chatMessages = append(chatMessages, llms.ConvertChatMessageToModel(message))
	}

	// Create history document
	history := History{
		UserID:       h.userID,
		SessionId:    h.sessionID,
		ChatMessages: chatMessages,
	}

	// Marshal to JSON
	historyItem, err := json.Marshal(history)
	if err != nil {
		return fmt.Errorf("failed to marshal chat history: %w", err)
	}

	// Save to Cosmos DB
	_, err = h.container.UpsertItem(ctx, azcosmos.NewPartitionKeyString(h.userID), historyItem, nil)
	if err != nil {
		return fmt.Errorf("failed to upsert chat history: %w", err)
	}

	// Update in-memory cache
	h.messages = make([]llms.ChatMessage, len(messages))
	copy(h.messages, messages)
	
	return nil
}

func (h *CosmosDBChatMessageHistory) Messages(ctx context.Context) ([]llms.ChatMessage, error) {
	// Attempt to read the item from Cosmos DB
	item, err := h.container.ReadItem(ctx, azcosmos.NewPartitionKeyString(h.userID), h.sessionID, nil)
	if err != nil {
		if cosmosErr, ok := err.(*azcore.ResponseError); ok && cosmosErr.StatusCode == 404 {
			// Return an empty slice if the item is not found
			h.messages = make([]llms.ChatMessage, 0)
			return h.messages, nil
		}
		return nil, fmt.Errorf("failed to read item with sessionID %s: %w", h.sessionID, err)
	}

	// Parse the retrieved JSON item
	var history History
	err = json.Unmarshal(item.Value, &history)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal history data: %w", err)
	}

	// Convert message models back to chat messages
	var messages []llms.ChatMessage
	for _, message := range history.ChatMessages {
		messages = append(messages, message.ToChatMessage())
	}

	// Update the in-memory cache
	h.messages = messages

	return messages, nil
}

type History struct {
	SessionId   string `json:"id"` //unique id
	UserID      string `json:"userid"` //partition key
	ChatMessages []llms.ChatMessageModel `json:"messages"`
}
