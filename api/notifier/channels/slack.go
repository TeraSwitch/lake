package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/slack-go/slack"

	"github.com/malbeclabs/lake/api/notifier"
)

const (
	ChannelTypeSlack = "slack"

	// maxIndividualNotifications is the threshold above which we send a
	// summary message instead of individual notifications.
	maxIndividualNotifications = 10
)

// SlackDestination is the JSON schema for Slack channel destinations.
type SlackDestination struct {
	TeamID    string `json:"team_id"`
	ChannelID string `json:"channel_id"`
}

// SlackChannel delivers notifications to Slack channels using the installed bot.
type SlackChannel struct {
	PgPool *pgxpool.Pool
}

func (c *SlackChannel) Type() string {
	return ChannelTypeSlack
}

func (c *SlackChannel) Send(ctx context.Context, destination json.RawMessage, groups []notifier.EventGroup) error {
	var dest SlackDestination
	if err := json.Unmarshal(destination, &dest); err != nil {
		return fmt.Errorf("invalid slack destination: %w", err)
	}
	if dest.TeamID == "" || dest.ChannelID == "" {
		return fmt.Errorf("slack destination requires team_id and channel_id")
	}

	botToken, err := c.getBotToken(ctx, dest.TeamID)
	if err != nil {
		return fmt.Errorf("failed to get bot token for team %s: %w", dest.TeamID, err)
	}

	api := slack.New(botToken)

	if len(groups) > maxIndividualNotifications {
		blocks := formatSummary(groups)
		_, _, err := api.PostMessageContext(ctx, dest.ChannelID,
			slack.MsgOptionBlocks(blocks...),
			slack.MsgOptionDisableLinkUnfurl(),
		)
		if err != nil {
			return fmt.Errorf("failed to post summary to %s: %w", dest.ChannelID, err)
		}
		return nil
	}

	for _, group := range groups {
		blocks := formatEventGroup(group)
		_, _, err := api.PostMessageContext(ctx, dest.ChannelID,
			slack.MsgOptionBlocks(blocks...),
			slack.MsgOptionDisableLinkUnfurl(),
		)
		if err != nil {
			return fmt.Errorf("failed to post message to %s: %w", dest.ChannelID, err)
		}
	}

	return nil
}

func (c *SlackChannel) getBotToken(ctx context.Context, teamID string) (string, error) {
	var token string
	err := c.PgPool.QueryRow(ctx,
		`SELECT bot_token FROM slack_installations WHERE team_id = $1 AND is_active = true`,
		teamID,
	).Scan(&token)
	if err != nil {
		return "", err
	}
	return token, nil
}

// formatEventGroup builds Slack Block Kit blocks for a notification.
func formatEventGroup(group notifier.EventGroup) []slack.Block {
	var blocks []slack.Block

	// Header with summary.
	headerText := slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*%s*", group.Summary), false, false)
	blocks = append(blocks, slack.NewSectionBlock(headerText, nil, nil))

	// Event details.
	var details []string
	for _, event := range group.Events {
		line := formatEventLine(event)
		if line != "" {
			details = append(details, line)
		}
	}

	if len(details) > 0 {
		detailText := slack.NewTextBlockObject(slack.MarkdownType, strings.Join(details, "\n"), false, false)
		blocks = append(blocks, slack.NewSectionBlock(detailText, nil, nil))
	}

	// Transaction context line.
	if len(group.Events) > 0 {
		first := group.Events[0]
		var contextParts []string
		if signer, ok := first.Details["signer"].(string); ok && signer != "" {
			short := signer
			if len(short) > 12 {
				short = short[:8] + "..." + short[len(short)-4:]
			}
			contextParts = append(contextParts, fmt.Sprintf("signer: `%s`", short))
		}
		if txSig, ok := first.Details["tx_signature"].(string); ok && txSig != "" {
			short := txSig
			if len(short) > 8 {
				short = short[:8]
			}
			contextParts = append(contextParts, fmt.Sprintf("`tx: %s...`", short))
		}
		if slot, ok := first.Details["slot"]; ok {
			contextParts = append(contextParts, fmt.Sprintf("slot %v", slot))
		}
		if len(contextParts) > 0 {
			ctxText := slack.NewTextBlockObject(slack.MarkdownType, strings.Join(contextParts, " · "), false, false)
			blocks = append(blocks, slack.NewContextBlock("", ctxText))
		}
	}

	// Divider between groups if we ever batch multiple into one message.
	blocks = append(blocks, slack.NewDividerBlock())

	return blocks
}

// formatEventLine returns a human-readable line for a single event.
func formatEventLine(event notifier.Event) string {
	switch event.Type {
	case "initialize_seat":
		return formatWithSeatContext(event, "Seat initialized")
	case "initialize_escrow":
		return formatWithSeatContext(event, "Escrow initialized")
	case "fund":
		amount := formatUSDC(event.Details["amount_usdc"])
		balance := formatUSDC(event.Details["balance_after_usdc"])
		if amount != "" && balance != "" {
			return fmt.Sprintf("Funded %s USDC (balance: %s USDC)", amount, balance)
		}
		if amount != "" {
			return fmt.Sprintf("Funded %s USDC", amount)
		}
		return "Funded"
	case "allocate_seat":
		if epoch, ok := event.Details["epoch"]; ok {
			return fmt.Sprintf("Instant allocated (epoch %v)", epoch)
		}
		return "Instant allocated"
	case "batch_allocate":
		if epoch, ok := event.Details["epoch"]; ok {
			return fmt.Sprintf("Batch allocated (epoch %v)", epoch)
		}
		return "Batch allocated"
	case "ack_allocate":
		return "Allocation confirmed"
	case "reject_allocate":
		return "Allocation rejected"
	case "withdraw_seat":
		return "Withdrawal requested"
	case "ack_withdraw":
		return "Withdrawal confirmed"
	case "close":
		amount := formatUSDC(event.Details["amount_usdc"])
		if amount != "" {
			return fmt.Sprintf("Escrow closed (withdrew %s USDC)", amount)
		}
		return "Escrow closed"
	case "batch_settle":
		return "Devices settled"
	case "set_price_override":
		return "Price override set"
	case "connected":
		return formatUserActivityLine(event, "Connected")
	case "disconnected":
		return formatUserActivityLine(event, "Disconnected")
	default:
		return event.Type
	}
}

// formatWithSeatContext adds client seat PK context to a line if available.
func formatWithSeatContext(event notifier.Event, prefix string) string {
	if pk, ok := event.Details["client_seat_pk"].(string); ok && pk != "" {
		short := pk
		if len(short) > 8 {
			short = short[:8]
		}
		return fmt.Sprintf("%s (`%s...`)", prefix, short)
	}
	return prefix
}

// formatUserActivityLine formats a user connected/disconnected event line.
func formatUserActivityLine(event notifier.Event, action string) string {
	var parts []string
	if kind, ok := event.Details["kind"].(string); ok && kind != "" {
		parts = append(parts, kind)
	}
	if clientIP, ok := event.Details["client_ip"].(string); ok && clientIP != "" {
		parts = append(parts, fmt.Sprintf("IP: %s", clientIP))
	}
	if devicePK, ok := event.Details["device_pk"].(string); ok && devicePK != "" {
		short := devicePK
		if len(short) > 8 {
			short = short[:8]
		}
		parts = append(parts, fmt.Sprintf("device: `%s...`", short))
	}
	if len(parts) > 0 {
		return fmt.Sprintf("%s (%s)", action, strings.Join(parts, ", "))
	}
	return action
}

// formatSummary builds a single Slack message summarizing a large batch of
// event groups, used when too many events accumulated to send individually.
func formatSummary(groups []notifier.EventGroup) []slack.Block {
	// Count events by summary type.
	counts := make(map[string]int)
	for _, g := range groups {
		counts[g.Summary]++
	}

	headerText := slack.NewTextBlockObject(slack.MarkdownType,
		fmt.Sprintf("*%d events while notifications were batched*", len(groups)), false, false)
	blocks := []slack.Block{slack.NewSectionBlock(headerText, nil, nil)}

	var lines []string
	for summary, count := range counts {
		if count == 1 {
			lines = append(lines, fmt.Sprintf("- %s", summary))
		} else {
			lines = append(lines, fmt.Sprintf("- %s (%d)", summary, count))
		}
	}

	detailText := slack.NewTextBlockObject(slack.MarkdownType, strings.Join(lines, "\n"), false, false)
	blocks = append(blocks, slack.NewSectionBlock(detailText, nil, nil))
	blocks = append(blocks, slack.NewDividerBlock())

	return blocks
}

// formatUSDC formats a raw USDC lamport value (int64) to a human-readable dollar string.
// USDC has 6 decimal places, so 1_000_000 = $1.00.
func formatUSDC(v any) string {
	switch n := v.(type) {
	case int64:
		dollars := float64(n) / 1_000_000
		return fmt.Sprintf("%.2f", dollars)
	case float64:
		dollars := n / 1_000_000
		return fmt.Sprintf("%.2f", dollars)
	default:
		return ""
	}
}
