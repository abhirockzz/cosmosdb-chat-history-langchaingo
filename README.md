# Azure Cosmos DB Chat History implementation for `langchaingo`

This package provides an Azure Cosmos DB NoSQL API based implementation for chat history in `langchaingo` using [Go SDK for Azure Cosmos DB](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-go).

Below is an example of using Azure Cosmos DB chat history with a `LLMChain`:

```go
// Create a chat history instance
cosmosChatHistory, err := cosmosdb.NewCosmosDBChatMessageHistory(cosmosClient, databaseName, containerName, req.SessionID, req.UserID)
if err != nil {
	log.Printf("Error creating chat history: %v", err)
	sendErrorResponse(w, "Failed to create chat session", http.StatusInternalServerError)
	return
}

// Create a memory with the chat history
chatMemory := memory.NewConversationBuffer(
	memory.WithMemoryKey("chat_history"),
	memory.WithChatHistory(cosmosChatHistory),
)

// Create an LLM chain
chain := chains.LLMChain{
	Prompt:       promptsTemplate,
	LLM:          llm,
	Memory:       chatMemory,
	OutputParser: outputparser.NewSimple(),
	OutputKey:    "text",
}
```

Check out the blog post [Implementing Chat History for AI Applications Using Azure Cosmos DB Go SDK](https://devblogs.microsoft.com/cosmosdb/implementing-chat-history-for-ai-applications-using-azure-cosmos-db-go-sdk) and the sample chatbot application that demonstrates how to use this package.

![App](https://raw.githubusercontent.com/AzureCosmosDB/cosmosdb-chat-history-langchaingo/refs/heads/main/images/app.png)

## Run test cases

This repository includes simple test cases for the chat history component. It demonstrates an example of how to use the [Azure Cosmos DB Linux-based emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator-linux) (in *preview* at the time of writing) for integration tests with [Testcontainers for Go](https://golang.testcontainers.org/).

To run the tests for the Azure Cosmos DB chat history component, use the following command:

```bash
go test -v github.com/abhirockzz/cosmosdb-chat-history-langchaingo/cosmosdb
```