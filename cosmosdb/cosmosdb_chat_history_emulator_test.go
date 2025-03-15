package cosmosdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tmc/langchaingo/llms"
)

const (
	testOperationDBName   = "testDatabase"
	testOperationContainerName  = "testContainer"
	testPartitionKey   = "/userid"
	emulatorImage      = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"
	emulatorPort       = "8081"
	emulatorEndpoint   = "http://localhost:8081"
	emulatorKey        = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
)

var (
	emulator testcontainers.Container
	client   *azcosmos.Client
)

// setupCosmosEmulator creates a CosmosDB emulator container for testing
func setupCosmosEmulator(ctx context.Context) (testcontainers.Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        emulatorImage,
		ExposedPorts: []string{emulatorPort + ":8081", "1234:1234"},
		WaitingFor:   wait.ForListeningPort(nat.Port(emulatorPort)),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to start container: %w", err)
	}

	// Give the emulator a bit more time to fully initialize
	time.Sleep(5 * time.Second)

	return container, nil
}

// setupCosmosClient creates a Cosmos DB client for the emulator
func setupCosmosClient() (*azcosmos.Client, error) {
	// Create credential with the emulator key
	cred, err := azcosmos.NewKeyCredential(emulatorKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create key credential: %w", err)
	}

	// Create the client
	client, err := azcosmos.NewClientWithKey(emulatorEndpoint, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cosmos client: %w", err)
	}

	return client, nil
}

// setupDatabaseAndContainer ensures the test database and container exist
func setupDatabaseAndContainer(ctx context.Context, client *azcosmos.Client) error {
	// Try to create the test database
	databaseProps := azcosmos.DatabaseProperties{ID: testOperationDBName}
	_, err := client.CreateDatabase(ctx, databaseProps, nil)
	if err != nil && !isResourceExistsError(err) {
		return fmt.Errorf("failed to create test database: %w", err)
	}

	// Create container if it doesn't exist
	database, err := client.NewDatabase(testOperationDBName)
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	containerProps := azcosmos.ContainerProperties{
		ID: testOperationContainerName,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{"/userid"},
		},
		DefaultTimeToLive: to.Ptr[int32](60), // Short TTL for test data (60 seconds)
	}

	_, err = database.CreateContainer(ctx, containerProps, nil)
	if err != nil && !isResourceExistsError(err) {
		return fmt.Errorf("failed to create test container: %w", err)
	}

	return nil
}

// isResourceExistsError checks if error is because resource already exists (status code 409)
func isResourceExistsError(err error) bool {
	var responseErr *azcore.ResponseError
	if errors.As(err, &responseErr) {
		return responseErr.StatusCode == 409
	}
	return false
}

// cleanupTestData removes test data after tests
func cleanupTestData(ctx context.Context, t *testing.T, client *azcosmos.Client, userID, sessionID string) {
	t.Helper()
	database, err := client.NewDatabase(testOperationDBName)
	if err != nil {
		return
	}

	container, err := database.NewContainer(testOperationContainerName)
	if err != nil {
		return
	}

	// Delete the test item
	_, _ = container.DeleteItem(ctx, azcosmos.NewPartitionKeyString(userID), sessionID, nil)
}

func TestMain(m *testing.M) {
	// Set up the CosmosDB emulator container
	ctx := context.Background()
	var err error
	emulator, err = setupCosmosEmulator(ctx)
	if err != nil {
		fmt.Printf("Failed to set up CosmosDB emulator: %v\n", err)
		os.Exit(1)
	}

	// Set up the CosmosDB client
	client, err = setupCosmosClient()
	if err != nil {
		fmt.Printf("Failed to set up CosmosDB client: %v\n", err)
		os.Exit(1)
	}

	// Set up the database and container
	err = setupDatabaseAndContainer(ctx, client)
	if err != nil {
		fmt.Printf("Failed to set up database and container: %v\n", err)
		os.Exit(1)
	}

	// Run the tests
	code := m.Run()

	// Tear down the CosmosDB emulator container
	if emulator != nil {
		_ = emulator.Terminate(ctx)
	}

	os.Exit(code)
}


// verifyMessages is a helper function to verify message content and type
func verifyMessages(t *testing.T, actualMessages []llms.ChatMessage, expectedContents []string, expectedTypes []llms.ChatMessageType) {
	t.Helper()
	require.Equal(t, len(expectedContents), len(actualMessages), "Message count should match expected count")
	
	for i, msg := range actualMessages {
		assert.Equal(t, expectedContents[i], msg.GetContent(), "Message content should match at index %d", i)
		if i < len(expectedTypes) {
			assert.Equal(t, expectedTypes[i], msg.GetType(), "Message type should match at index %d", i)
		}
	}
}

func TestScenario_NewUser_FirstInteraction(t *testing.T) {
	// Setup
	ctx := context.Background()
	userID := "user123"
	sessionID := "session_" + time.Now().Format("20060102150405")
	
	defer cleanupTestData(ctx, t, client, userID, sessionID)

	// Test scenario: New user starts a conversation with the AI
	history, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// First, check that there are no messages for a new user
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Empty(t, messages, "A new user should have no message history")
	
	// User sends first message
	err = history.AddUserMessage(ctx, "Hello, I need help with my project")
	require.NoError(t, err)
	
	// AI responds to first message
	err = history.AddAIMessage(ctx, "Hi there! I'd be happy to help with your project. What specifically do you need assistance with?")
	require.NoError(t, err)
	
	// Retrieve the messages to verify they were stored correctly
	messages, err = history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages), "Should have 2 messages in history")
	assert.Equal(t, "Hello, I need help with my project", messages[0].GetContent())
	assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].GetType())
	assert.Equal(t, "Hi there! I'd be happy to help with your project. What specifically do you need assistance with?", messages[1].GetContent())
	assert.Equal(t, llms.ChatMessageTypeAI, messages[1].GetType())
}

func TestScenario_ReturningUser_ContinuingConversation(t *testing.T) {
	// Setup
	ctx := context.Background()
	userID := "user456"
	sessionID := "session_" + time.Now().Format("20060102150405")
	
	defer cleanupTestData(ctx, t, client, userID, sessionID)

	// Initial conversation
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Setup initial conversation
	err = history1.AddUserMessage(ctx, "I'm trying to learn Go programming")
	require.NoError(t, err)
	err = history1.AddAIMessage(ctx, "That's great! Go is an excellent language. What aspects are you interested in learning?")
	require.NoError(t, err)
	
	// User comes back later (simulating a new session with the same history)
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Check that previous conversation is loaded
	messages, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages), "Returning user should see previous conversation history")
	
	// User continues the conversation
	err = history2.AddUserMessage(ctx, "I'm particularly interested in concurrency patterns")
	require.NoError(t, err)
	err = history2.AddAIMessage(ctx, "Go has excellent concurrency primitives with goroutines and channels. Let me explain how they work...")
	require.NoError(t, err)
	
	// Verify the full conversation
	messages, err = history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, len(messages), "Should have 4 messages in history after continuing conversation")
}

func TestScenario_LongConversation_SetMessages(t *testing.T) {
	// Setup
	ctx := context.Background()
	userID := "user789"
	sessionID := "session_" + time.Now().Format("20060102150405")
	
	defer cleanupTestData(ctx, t, client, userID, sessionID)

	// Create a history with a long conversation
	history, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Add many messages to simulate a long conversation
	for i := 0; i < 5; i++ {
		err = history.AddUserMessage(ctx, "Question "+strconv.Itoa(i))
		require.NoError(t, err)
		err = history.AddAIMessage(ctx, "Answer "+strconv.Itoa(i))
		require.NoError(t, err)
	}
	
	// Verify the conversation length
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, len(messages), "Should have 10 messages after adding 5 Q&A pairs")
	
	// Scenario: Replace conversation with a summarized version
	summarizedMessages := []llms.ChatMessage{
		llms.HumanChatMessage{Content: "I've asked several questions about programming"},
		llms.AIChatMessage{Content: "I've provided detailed answers on various programming topics"},
	}
	
	// Set the summarized messages
	err = history.SetMessages(ctx, summarizedMessages)
	require.NoError(t, err)
	
	// Verify the conversation was replaced
	messages, err = history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages), "Should have 2 messages after summarization")
	assert.Equal(t, "I've provided detailed answers on various programming topics", messages[1].GetContent())
	
	// Continue the conversation after summarization
	err = history.AddUserMessage(ctx, "Let's continue with more advanced topics")
	require.NoError(t, err)
	
	messages, err = history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, len(messages), "Should have 3 messages after adding to the summarized conversation")
}

func TestScenario_ClearConversation_StartFresh(t *testing.T) {
	// Setup
	ctx := context.Background()
	userID := "user101"
	sessionID := "session_" + time.Now().Format("20060102150405")
	
	defer cleanupTestData(ctx, t, client, userID, sessionID)

	// Start a conversation
	history, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	err = history.AddUserMessage(ctx, "Can we discuss machine learning?")
	require.NoError(t, err)
	err = history.AddAIMessage(ctx, "Certainly! Machine learning is a fascinating field. What would you like to know?")
	require.NoError(t, err)
	
	// User decides to start a new topic and clears history
	err = history.Clear(ctx)
	require.NoError(t, err)
	
	// Verify conversation was cleared
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Empty(t, messages, "Conversation should be empty after clearing")
	
	// Start a fresh conversation
	err = history.AddUserMessage(ctx, "Let's talk about cloud computing instead")
	require.NoError(t, err)
	err = history.AddAIMessage(ctx, "Great choice! Cloud computing offers many advantages for modern applications.")
	require.NoError(t, err)
	
	// Verify only the new conversation exists
	messages, err = history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages), "Should have only 2 messages from the new conversation")
	assert.Equal(t, "Let's talk about cloud computing instead", messages[0].GetContent())
}

func TestScenario_MultipleUsersSeparateSessions(t *testing.T) {
	// Setup
	ctx := context.Background()
	userID1 := "user_alice"
	userID2 := "user_bob"
	sessionID1 := "session_" + time.Now().Format("20060102150405") + "_alice"
	sessionID2 := "session_" + time.Now().Format("20060102150405") + "_bob"
	
	defer cleanupTestData(ctx, t, client, userID1, sessionID1)
	defer cleanupTestData(ctx, t, client, userID2, sessionID2)

	// Alice's conversation
	aliceHistory, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID1, userID1)
	require.NoError(t, err)
	
	err = aliceHistory.AddUserMessage(ctx, "I'm working on a data analysis project")
	require.NoError(t, err)
	err = aliceHistory.AddAIMessage(ctx, "What kind of data are you analyzing?")
	require.NoError(t, err)
	
	// Bob's separate conversation
	bobHistory, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID2, userID2)
	require.NoError(t, err)
	
	err = bobHistory.AddUserMessage(ctx, "I need help with a web application")
	require.NoError(t, err)
	err = bobHistory.AddAIMessage(ctx, "What framework are you using for your web app?")
	require.NoError(t, err)
	
	// Verify Alice's conversation is separate
	aliceMessages, err := aliceHistory.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(aliceMessages))
	assert.Equal(t, "I'm working on a data analysis project", aliceMessages[0].GetContent())
	
	// Verify Bob's conversation is separate
	bobMessages, err := bobHistory.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(bobMessages))
	assert.Equal(t, "I need help with a web application", bobMessages[0].GetContent())
}

func TestScenario_InvalidInputs(t *testing.T) {
	// Setup
	// Test with missing parameters
	_, err := NewCosmosDBChatMessageHistory(client, "", testOperationContainerName, "session123", "user123")
	assert.Error(t, err, "Should error with empty database ID")
	
	_, err = NewCosmosDBChatMessageHistory(client, testOperationDBName, "", "session123", "user123")
	assert.Error(t, err, "Should error with empty container ID")
	
	_, err = NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, "", "user123")
	assert.Error(t, err, "Should error with empty session ID")
	
	_, err = NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, "session123", "")
	assert.Error(t, err, "Should error with empty user ID")
}

//=====

// createTestHistory creates a test history instance with unique IDs
func createTestHistory(t *testing.T, client *azcosmos.Client) (*CosmosDBChatMessageHistory, string, string) {
	t.Helper()
	
	userID := fmt.Sprintf("user_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
	
	history, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	return history, userID, sessionID
}


func TestOperation_Constructor(t *testing.T) {
	
	t.Run("Valid parameters", func(t *testing.T) {
		userID := fmt.Sprintf("user_%d", time.Now().UnixNano())
		sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
		
		history, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
		require.NoError(t, err)
		assert.NotNil(t, history)
		assert.Equal(t, userID, history.userID)
		assert.Equal(t, sessionID, history.sessionID)
		assert.Empty(t, history.messages)
	})
	
	t.Run("Missing database", func(t *testing.T) {
		_, err := NewCosmosDBChatMessageHistory(client, "", testOperationContainerName, "session", "user")
		assert.Error(t, err)
	})
	
	t.Run("Missing container", func(t *testing.T) {
		_, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, "", "session", "user")
		assert.Error(t, err)
	})
	
	t.Run("Missing session ID", func(t *testing.T) {
		_, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, "", "user")
		assert.Error(t, err)
	})
	
	t.Run("Missing user ID", func(t *testing.T) {
		_, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, "session", "")
		assert.Error(t, err)
	})
	
	t.Run("Non-existent database", func(t *testing.T) {
		// Creating the history with non-existent database succeeds because it just creates a client reference
		history, err := NewCosmosDBChatMessageHistory(client, "nonexistentdb", "nonexistentcontainer", "session", "user")
		require.NoError(t, err)
		
		// But operations on this history should fail
		ctx := context.Background()
		err = history.AddUserMessage(ctx, "Test message")
		assert.Error(t, err, "Operations on non-existent database should fail")
	})
}

func TestOperation_Constructor_TableDriven(t *testing.T) {
	testCases := []struct {
		name        string
		databaseID  string
		containerID string
		sessionID   string
		userID      string
		expectError bool
	}{
		{
			name:        "All valid parameters",
			databaseID:  testOperationDBName,
			containerID: testOperationContainerName,
			sessionID:   "valid-session",
			userID:      "valid-user",
			expectError: false,
		},
		{
			name:        "Empty database ID",
			databaseID:  "",
			containerID: testOperationContainerName,
			sessionID:   "valid-session",
			userID:      "valid-user",
			expectError: true,
		},
		{
			name:        "Empty container ID",
			databaseID:  testOperationDBName,
			containerID: "",
			sessionID:   "valid-session",
			userID:      "valid-user",
			expectError: true,
		},
		{
			name:        "Empty session ID",
			databaseID:  testOperationDBName,
			containerID: testOperationContainerName,
			sessionID:   "",
			userID:      "valid-user",
			expectError: true,
		},
		{
			name:        "Empty user ID",
			databaseID:  testOperationDBName,
			containerID: testOperationContainerName,
			sessionID:   "valid-session",
			userID:      "",
			expectError: true,
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			history, err := NewCosmosDBChatMessageHistory(client, tc.databaseID, tc.containerID, tc.sessionID, tc.userID)
			
			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, history)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, history)
				assert.Equal(t, tc.userID, history.userID)
				assert.Equal(t, tc.sessionID, history.sessionID)
			}
		})
	}
}

func TestOperation_AddMessages(t *testing.T) {
	ctx := context.Background()
	
	t.Run("Add user message", func(t *testing.T) {
		history, userID, sessionID := createTestHistory(t, client)
		defer cleanupTestData(ctx, t, client, userID, sessionID)
		
		// Add a user message
		err := history.AddUserMessage(ctx, "Test user message")
		require.NoError(t, err)
		
		// Verify message properties
		messages, err := history.Messages(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, len(messages))
		assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].GetType())
		assert.Equal(t, "Test user message", messages[0].GetContent())
	})
	
	t.Run("Add AI message", func(t *testing.T) {
		history, userID, sessionID := createTestHistory(t, client)
		defer cleanupTestData(ctx, t, client, userID, sessionID)
		
		// Add an AI message
		err := history.AddAIMessage(ctx, "Test AI response")
		require.NoError(t, err)
		
		// Verify message properties
		messages, err := history.Messages(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, len(messages))
		assert.Equal(t, llms.ChatMessageTypeAI, messages[0].GetType())
		assert.Equal(t, "Test AI response", messages[0].GetContent())
	})
	
	t.Run("Add multiple messages", func(t *testing.T) {
		history, userID, sessionID := createTestHistory(t, client)
		defer cleanupTestData(ctx, t, client, userID, sessionID)
		
		// Add alternating messages
		expectedContents := []string{"User msg 1", "AI response 1", "User msg 2", "AI response 2"}
		expectedTypes := []llms.ChatMessageType{llms.ChatMessageTypeHuman, llms.ChatMessageTypeAI, llms.ChatMessageTypeHuman, llms.ChatMessageTypeAI}
		
		for i, content := range expectedContents {
			var err error
			if i % 2 == 0 {
				err = history.AddUserMessage(ctx, content)
			} else {
				err = history.AddAIMessage(ctx, content)
			}
			require.NoError(t, err)
		}
		
		// Verify all messages
		messages, err := history.Messages(ctx)
		require.NoError(t, err)
		verifyMessages(t, messages, expectedContents, expectedTypes)
	})
}

func TestOperation_AddMessage(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Add messages of different types
	err := history.AddMessage(ctx, llms.HumanChatMessage{Content: "Human message"})
	require.NoError(t, err)
	
	err = history.AddMessage(ctx, llms.AIChatMessage{Content: "AI message"})
	require.NoError(t, err)
	
	// Verify messages were stored correctly
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages))
	assert.Equal(t, "Human message", messages[0].GetContent())
	assert.Equal(t, llms.ChatMessageTypeHuman, messages[0].GetType())
	assert.Equal(t, "AI message", messages[1].GetContent())
	assert.Equal(t, llms.ChatMessageTypeAI, messages[1].GetType())
}

func TestOperation_AddMessageNil(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Test adding nil message
	err := history.AddMessage(ctx, nil)
	assert.Error(t, err, "Adding nil message should return an error")
	assert.Contains(t, err.Error(), "cannot add nil message")
}

func TestOperation_Clear(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Add some messages
	err := history.AddUserMessage(ctx, "Message 1")
	require.NoError(t, err)
	err = history.AddAIMessage(ctx, "Response 1")
	require.NoError(t, err)
	
	// Verify messages exist
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages))
	
	// Clear the history
	err = history.Clear(ctx)
	require.NoError(t, err)
	
	// Verify in-memory messages are cleared
	assert.Empty(t, history.messages)
	
	// Verify stored messages are cleared
	messages, err = history.Messages(ctx)
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestOperation_SetMessages(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Add initial messages
	err := history.AddUserMessage(ctx, "Original message 1")
	require.NoError(t, err)
	err = history.AddAIMessage(ctx, "Original response 1")
	require.NoError(t, err)
	
	// Create new message set
	newMessages := []llms.ChatMessage{
		llms.HumanChatMessage{Content: "New message 1"},
		llms.AIChatMessage{Content: "New response 1"},
		llms.HumanChatMessage{Content: "New message 2"},
	}
	
	// Set the new messages
	err = history.SetMessages(ctx, newMessages)
	require.NoError(t, err)
	
	// Verify in-memory messages are updated
	assert.Equal(t, 3, len(history.messages))
	
	// Verify stored messages are updated
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, len(messages))
	assert.Equal(t, "New message 1", messages[0].GetContent())
	assert.Equal(t, "New response 1", messages[1].GetContent())
	assert.Equal(t, "New message 2", messages[2].GetContent())
}

func TestOperation_SetMessages_EdgeCases(t *testing.T) {
	ctx := context.Background()
	
	t.Run("Empty message array", func(t *testing.T) {
		history, userID, sessionID := createTestHistory(t, client)
		defer cleanupTestData(ctx, t, client, userID, sessionID)
		
		// Add initial message
		err := history.AddUserMessage(ctx, "Original message")
		require.NoError(t, err)
		
		// Set empty message array
		err = history.SetMessages(ctx, []llms.ChatMessage{})
		require.NoError(t, err)
		
		// Verify messages are cleared
		messages, err := history.Messages(ctx)
		require.NoError(t, err)
		assert.Empty(t, messages, "Messages should be empty after setting empty array")
	})
	
	t.Run("Nil message array", func(t *testing.T) {
		history, userID, sessionID := createTestHistory(t, client)
		defer cleanupTestData(ctx, t, client, userID, sessionID)
		
		// Add initial message
		err := history.AddUserMessage(ctx, "Original message")
		require.NoError(t, err)
		
		// Set nil message array
		err = history.SetMessages(ctx, nil)
		require.NoError(t, err)
		
		// Verify messages are cleared
		messages, err := history.Messages(ctx)
		require.NoError(t, err)
		assert.Empty(t, messages, "Messages should be empty after setting nil array")
	})
}

func TestOperation_Messages_EmptyHistory(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Get messages for a new history (should be empty)
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Empty(t, messages)
}

func TestOperation_Messages_UpdateBetweenInstances(t *testing.T) {
	ctx := context.Background()
	
	// Create first history instance
	userID := fmt.Sprintf("user_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Add messages to first instance
	err = history1.AddUserMessage(ctx, "Hello from instance 1")
	require.NoError(t, err)
	
	// Create second history instance with same session/user IDs
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Verify second instance can read the message added by first instance
	messages, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, "Hello from instance 1", messages[0].GetContent())
	
	// Add message from second instance
	err = history2.AddAIMessage(ctx, "Hello from instance 2")
	require.NoError(t, err)
	
	// Verify first instance can see both messages
	messages, err = history1.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages))
	assert.Equal(t, "Hello from instance 1", messages[0].GetContent())
	assert.Equal(t, "Hello from instance 2", messages[1].GetContent())
}


func TestOperation_Persistence(t *testing.T) {
	ctx := context.Background()
	
	userID := fmt.Sprintf("user_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Create first history instance and add messages
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	err = history1.AddUserMessage(ctx, "Message from original session")
	require.NoError(t, err)
	
	// "Close" the first instance by discarding the reference
	history1 = nil
	
	// Create a completely new instance with the same session/user IDs
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Verify the new instance can load the previous message
	messages, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, "Message from original session", messages[0].GetContent())
}

func TestOperation_ConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	
	userID := fmt.Sprintf("user_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Create two history instances for the same session
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Perform operations from both instances
	err = history1.AddUserMessage(ctx, "Message 1 from instance 1")
	require.NoError(t, err)
	
	// Force refresh of the second instance to see the first message
	_, err = history2.Messages(ctx)
	require.NoError(t, err)
	
	err = history2.AddAIMessage(ctx, "Message 2 from instance 2")
	require.NoError(t, err)
	
	// Force refresh of the first instance to see the second message
	_, err = history1.Messages(ctx)
	require.NoError(t, err)
	
	err = history1.AddUserMessage(ctx, "Message 3 from instance 1")
	require.NoError(t, err)
	
	// Verify both instances can see all messages
	messages1, err := history1.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, len(messages1), "Instance 1 should see all 3 messages")
	
	messages2, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, len(messages2), "Instance 2 should see all 3 messages")
	
	// Verify the content of the messages is correct
	expectedContents := []string{
		"Message 1 from instance 1",
		"Message 2 from instance 2",
		"Message 3 from instance 1",
	}
	
	for i, msg := range messages1 {
		assert.Equal(t, expectedContents[i], msg.GetContent())
	}

	for i, msg := range messages2 {
		assert.Equal(t, expectedContents[i], msg.GetContent())
	}
}

func TestOperation_MessageOrder(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Add messages in a specific order
	messageContents := []string{
		"First message",
		"Second message",
		"Third message",
		"Fourth message", 
		"Fifth message",
	}
	
	for i, content := range messageContents {
		if i%2 == 0 {
			err := history.AddUserMessage(ctx, content)
			require.NoError(t, err)
		} else {
			err := history.AddAIMessage(ctx, content)
			require.NoError(t, err)
		}
	}
	
	// Verify messages are retrieved in the correct order
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(messageContents), len(messages))
	
	for i, message := range messages {
		assert.Equal(t, messageContents[i], message.GetContent())
	}
}

func TestOperation_EmptyMessages(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Test empty string messages
	err := history.AddUserMessage(ctx, "")
	require.NoError(t, err)
	
	err = history.AddAIMessage(ctx, "")
	require.NoError(t, err)
	
	// Verify empty messages are stored
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages))
	assert.Equal(t, "", messages[0].GetContent())
	assert.Equal(t, "", messages[1].GetContent())
}

func TestOperation_LargeMessages(t *testing.T) {
	ctx := context.Background()
	
	history, userID, sessionID := createTestHistory(t, client)
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Create a large message (100KB)
	largeMessage := ""
	for i := 0; i < 1024*100; i++ {
		largeMessage += "A"
	}
	
	// Add the large message
	err := history.AddUserMessage(ctx, largeMessage)
	require.NoError(t, err)
	
	// Verify it can be retrieved correctly
	messages, err := history.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, len(largeMessage), len(messages[0].GetContent()))
	assert.Equal(t, largeMessage, messages[0].GetContent())
}

func TestOperation_MultiUserConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	
	// Create histories for different users
	userID1 := fmt.Sprintf("user_multi1_%d", time.Now().UnixNano())
	userID2 := fmt.Sprintf("user_multi2_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_multi_%d", time.Now().UnixNano()) // Same session ID
	
	defer cleanupTestData(ctx, t, client, userID1, sessionID)
	defer cleanupTestData(ctx, t, client, userID2, sessionID)
	
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID1)
	require.NoError(t, err)
	
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID2)
	require.NoError(t, err)
	
	// Add messages from both users to the same session ID (but different partition keys)
	err = history1.AddUserMessage(ctx, "Message from user 1")
	require.NoError(t, err)
	
	err = history2.AddUserMessage(ctx, "Message from user 2")
	require.NoError(t, err)
	
	// Verify each history only sees its own messages
	messages1, err := history1.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(messages1))
	assert.Equal(t, "Message from user 1", messages1[0].GetContent())
	
	messages2, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(messages2))
	assert.Equal(t, "Message from user 2", messages2[0].GetContent())
	
	// Add more messages
	err = history1.AddAIMessage(ctx, "Response to user 1")
	require.NoError(t, err)
	
	err = history2.AddAIMessage(ctx, "Response to user 2")
	require.NoError(t, err)
	
	// Verify histories remain separate
	messages1, err = history1.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages1))
	assert.Equal(t, "Message from user 1", messages1[0].GetContent())
	assert.Equal(t, "Response to user 1", messages1[1].GetContent())
	
	messages2, err = history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, len(messages2))
	assert.Equal(t, "Message from user 2", messages2[0].GetContent())
	assert.Equal(t, "Response to user 2", messages2[1].GetContent())
}

func TestOperation_MessageOrderConsistency(t *testing.T) {
	ctx := context.Background()
	
	// Test that message order remains consistent even when accessing from multiple instances
	userID := fmt.Sprintf("user_order_%d", time.Now().UnixNano())
	sessionID := fmt.Sprintf("session_order_%d", time.Now().UnixNano())
	defer cleanupTestData(ctx, t, client, userID, sessionID)
	
	// Create first history instance
	history1, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Add alternating messages with distinct patterns to test order preservation
	messages := []struct {
		content string
		isUser  bool
	}{
		{content: "First user message", isUser: true},
		{content: "First AI response", isUser: false},
		{content: "Second user message", isUser: true},
		{content: "Second AI response", isUser: false},
		{content: "Third user message", isUser: true},
	}
	
	// Add messages
	for _, msg := range messages {
		if msg.isUser {
			err = history1.AddUserMessage(ctx, msg.content)
		} else {
			err = history1.AddAIMessage(ctx, msg.content)
		}
		require.NoError(t, err)
	}
	
	// Create a separate instance and verify order
	history2, err := NewCosmosDBChatMessageHistory(client, testOperationDBName, testOperationContainerName, sessionID, userID)
	require.NoError(t, err)
	
	// Get messages from second instance
	retrievedMessages, err := history2.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(messages), len(retrievedMessages))
	
	// Verify order is preserved
	for i, expected := range messages {
		assert.Equal(t, expected.content, retrievedMessages[i].GetContent())
		
		expectedType := llms.ChatMessageTypeHuman
		if !expected.isUser {
			expectedType = llms.ChatMessageTypeAI
		}
		assert.Equal(t, expectedType, retrievedMessages[i].GetType())
	}
	
	// Add more messages to second instance
	additionalMessages := []struct {
		content string
		isUser  bool
	}{
		{content: "Fourth message from second instance", isUser: true},
		{content: "Fifth message from second instance", isUser: false},
	}
	
	for _, msg := range additionalMessages {
		if msg.isUser {
			err = history2.AddUserMessage(ctx, msg.content)
		} else {
			err = history2.AddAIMessage(ctx, msg.content)
		}
		require.NoError(t, err)
	}
	
	// Verify from first instance that all messages are in correct order
	allMessages, err := history1.Messages(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(messages)+len(additionalMessages), len(allMessages))
	
	// Check first batch
	for i, expected := range messages {
		assert.Equal(t, expected.content, allMessages[i].GetContent())
	}
	
	// Check second batch
	for i, expected := range additionalMessages {
		assert.Equal(t, expected.content, allMessages[i+len(messages)].GetContent())
	}
}
