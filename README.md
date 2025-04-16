# A2A-Go

Go implementation of the A2A protocol, enabling different AI agents to discover and collaborate with each other.

## Core Components

Implemented core components include:

### 1. Type System (`taskmanager` and `jsonrpc`)

- Complete implementation of data types defined in the A2A protocol specification
- Includes JSON-RPC message structures (`jsonrpc`), task states, and more (`taskmanager`)
- Provides convenient constructors and interfaces

### 2. Client (`client`)

- Implements interaction with A2A servers
- Supports sending tasks, getting task status, and canceling tasks
- Handles JSON-RPC request/response formatting
- Supports streaming task updates (SSE)

### 3. Server (`server`)

- Provides an HTTP server based on the standard library
- Handles JSON-RPC request routing
- Supports agent card discovery (`/.well-known/agent.json`)
- Implements Server-Sent Events (SSE) for streaming updates
- Includes authentication middleware support

### 4. Task Management (`taskmanager`)

- Defines interface for task lifecycle management
- Provides default in-memory implementation (`MemoryTaskManager`)
- Supports task status updates and notifications
- Provides `TaskProcessor` interface for custom processing logic

### 5. Authentication (`auth`)

- Flexible authentication middleware
- Supports JWT and API key authentication methods
- Chain authentication provider for multiple auth methods
- JWT-based push notification authentication
- JWKS endpoint for public key distribution

## Examples

### 1. Basic Agent (`examples/agents/basic`)

A simple text-reversing agent example that:
- Handles text input
- Reverses text and returns the result
- Supports streaming responses
- Implements the `TaskProcessor` interface for task handling

### 2. CLI Client (`examples/hosts/cli`)

An interactive command-line client that:
- Connects to any A2A agent
- Sends text requests
- Displays streaming responses and artifacts
- Maintains session context

### 3. Streaming Client (`examples/streaming`)

A streaming data processing client example that:
- Connects to an A2A agent
- Sends streaming data requests
- Handles streaming response events

### 4. Authentication Server (`examples/auth_server`)

A complete example of an A2A server with authentication that:
- Demonstrates JWT and API key authentication
- Includes a mock task manager to showcase functionality
- Shows how to secure A2A endpoints

## Usage

### Running Example Agents

```bash
# Start the example agent on default port 8080
go run examples/agents/basic/main.go

# Specify different host and port
go run examples/agents/basic/main.go --host 0.0.0.0 --port 9000

# Disable CORS headers
go run examples/agents/basic/main.go --no-cors
```

### Using the CLI Client

```bash
# Connect to a local agent
go run examples/hosts/cli/main.go

# Connect to a specific agent
go run examples/hosts/cli/main.go --agent http://localhost:9000/

# Specify request timeout
go run examples/hosts/cli/main.go --timeout 30s
```

### Running the Streaming Client Example

```bash
# Connect to a local agent for streaming
go run examples/streaming/main.go
```

### Running the Authentication Server Example

```bash
# Start the authentication example server
go run examples/auth_server/main.go
```

## Creating Your Own Agent

1. Implement the `TaskProcessor` interface:

```go
import (
    "context"

    "trpc.group/trpc-go/a2a-go/taskmanager"
)

// Implement the TaskProcessor interface
type myTaskProcessor struct {
    // Optional: add your custom fields
}

func (p *myTaskProcessor) Process(
	ctx context.Context,
	taskID string,
	message taskmanager.Message,
	handle taskmanager.TaskHandle,
) error {
	// 1. Extract input data from message
	// 2. Process data, generate results
	// 3. Use handle to update task status and add artifacts

    // Processing complete, return nil for success
    return nil
}
```

2. Create an agent card, describing your agent's capabilities:

```go
import (
    "trpc.group/trpc-go/a2a-go/server"
    "trpc.group/trpc-go/a2a-go/taskmanager"
)

// Helper function to create string pointers
func stringPtr(s string) *string {
    return &s
}

agentCard := server.AgentCard{
    Name: "My Agent",
    Description: stringPtr("Agent description"),
    URL: "http://localhost:8080/",
    Version: "1.0.0",
    Provider: &server.AgentProvider{
        Name: "Provider name",
    },
    Capabilities: server.AgentCapabilities{
        Streaming: true,
        StateTransitionHistory: true,
    },
    DefaultInputModes: []string{string(taskmanager.PartTypeText)},
    DefaultOutputModes: []string{string(taskmanager.PartTypeText)},
    Skills: []server.AgentSkill{
        {
            ID: "my_skill",
            Name: "Skill name",
            Description: stringPtr("Skill description"),
            Tags: []string{"tag1", "tag2"},
            Examples: []string{"Example input"},
            InputModes: []string{string(taskmanager.PartTypeText)},
            OutputModes: []string{string(taskmanager.PartTypeText)},
        },
    },
}
```

3. Create and start the server:

```go
import (
    "log"

    "trpc.group/trpc-go/a2a-go/server"
    "trpc.group/trpc-go/a2a-go/taskmanager"
)

// Create the task processor
processor := &myTaskProcessor{}

// Create task manager, inject processor
taskManager, err := taskmanager.NewMemoryTaskManager(processor)
if err != nil {
    log.Fatalf("Failed to create task manager: %v", err)
}

// Create the server
srv, err := server.NewA2AServer(agentCard, taskManager)
if err != nil {
    log.Fatalf("Failed to create server: %v", err)
}

// Start the server
log.Printf("Agent server started on :8080")
if err := srv.Start(":8080"); err != nil {
    log.Fatalf("Server start failed: %v", err)
}
```

## Adding Authentication

To secure your A2A server, you can use the authentication providers:

```go
import (
    "trpc.group/trpc-go/a2a-go/auth"
    "trpc.group/trpc-go/a2a-go/server"
)

// Create a JWT authentication provider
jwtSecret := []byte("your-secret-key")
jwtProvider := auth.NewJWTAuthProvider(
    jwtSecret,
    "your-audience",
    "your-issuer",
    24*time.Hour, // token lifetime
)

// Or create an API key authentication provider
apiKeys := map[string]string{
    "api-key-1": "user1",
    "api-key-2": "user2",
}
apiKeyProvider := auth.NewAPIKeyAuthProvider(apiKeys, "X-API-Key")

// Create the server with authentication
srv, err := server.NewA2AServer(
    agentCard,
    taskManager,
    server.WithAuthProvider(jwtProvider), // or apiKeyProvider
)
```

## Push Notification Authentication

A2A supports authenticated push notifications:

```go
// Enable JWKS endpoint for push notifications
srv, err := server.NewA2AServer(
    agentCard,
    taskManager,
    server.WithJWKSEndpoint(true, "/.well-known/jwks.json"),
)
```

## Future Enhancements

- Persistent storage
- More utilities and helper functions
- Metrics and logging
- Comprehensive test suite

## Contributing

Contributions and improvement suggestions are welcome! Please ensure your code follows Go coding standards and includes appropriate tests. 
