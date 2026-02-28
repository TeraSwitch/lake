package chat

import (
	"github.com/malbeclabs/lake/agent/pkg/workflow"
)

// ChatWorkflowInput is the input to the ChatWorkflow Temporal workflow.
type ChatWorkflowInput struct {
	WorkflowRunID string
	SessionID     string
	Question      string
	History       []workflow.ConversationMessage
	Format        string // "slack" or ""
	Env           string // "mainnet-beta", "devnet", "testnet"
}

// ChatWorkflowOutput is the output of the ChatWorkflow Temporal workflow.
type ChatWorkflowOutput struct {
	Answer            string
	FollowUpQuestions []string
	LLMCalls          int
	InputTokens       int
	OutputTokens      int
}

// FinalizeInput is the input to the FinalizeChat activity.
type FinalizeInput struct {
	WorkflowRunID string
	SessionID     string
	Question      string
	Env           string
	Result        ChatWorkflowOutput
}
