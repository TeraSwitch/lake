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

func (c *SlackChannel) Send(ctx context.Context, destination json.RawMessage, groups []notifier.EventGroup, outputFormat string) error {
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

	// Summary mode for large backlogs.
	if len(groups) > maxIndividualNotifications {
		return c.postText(ctx, api, dest.ChannelID, notifier.RenderSummaryMarkdown(groups))
	}

	switch outputFormat {
	case notifier.FormatMarkdown, notifier.FormatPlaintext:
		return c.sendAsText(ctx, api, dest.ChannelID, groups, outputFormat)
	default:
		return c.sendAsBlocks(ctx, api, dest.ChannelID, groups)
	}
}

// sendAsText renders all groups as text and posts each as a message.
func (c *SlackChannel) sendAsText(ctx context.Context, api *slack.Client, channelID string, groups []notifier.EventGroup, format string) error {
	for _, group := range groups {
		var text string
		if format == notifier.FormatPlaintext {
			text = notifier.RenderPlaintext([]notifier.EventGroup{group})
		} else {
			text = notifier.RenderMarkdown([]notifier.EventGroup{group})
		}
		if err := c.postText(ctx, api, channelID, text); err != nil {
			return err
		}
	}
	return nil
}

// sendAsBlocks renders groups as Slack Block Kit and posts each as a message.
func (c *SlackChannel) sendAsBlocks(ctx context.Context, api *slack.Client, channelID string, groups []notifier.EventGroup) error {
	for _, group := range groups {
		blocks := buildBlocks(group)
		_, _, err := api.PostMessageContext(ctx, channelID,
			slack.MsgOptionBlocks(blocks...),
			slack.MsgOptionDisableLinkUnfurl(),
		)
		if err != nil {
			return fmt.Errorf("failed to post message to %s: %w", channelID, err)
		}
	}
	return nil
}

func (c *SlackChannel) postText(ctx context.Context, api *slack.Client, channelID, text string) error {
	_, _, err := api.PostMessageContext(ctx, channelID,
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	if err != nil {
		return fmt.Errorf("failed to post message to %s: %w", channelID, err)
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

// buildBlocks creates Slack Block Kit blocks for a single event group,
// using the shared formatters for event lines and context.
func buildBlocks(group notifier.EventGroup) []slack.Block {
	var blocks []slack.Block

	headerText := slack.NewTextBlockObject(slack.MarkdownType, fmt.Sprintf("*%s*", group.Summary), false, false)
	blocks = append(blocks, slack.NewSectionBlock(headerText, nil, nil))

	var details []string
	for _, event := range group.Events {
		line := notifier.FormatEventLine(event)
		if line != "" {
			details = append(details, line)
		}
	}
	if len(details) > 0 {
		detailText := slack.NewTextBlockObject(slack.MarkdownType, strings.Join(details, "\n"), false, false)
		blocks = append(blocks, slack.NewSectionBlock(detailText, nil, nil))
	}

	// Context line (signer, tx, slot, owner, etc.)
	if len(group.Events) > 0 {
		first := group.Events[0]
		var contextParts []string
		if signer, ok := first.Details["signer"].(string); ok && signer != "" {
			contextParts = append(contextParts, fmt.Sprintf("signer: `%s`", truncateKey(signer)))
		}
		if owner, ok := first.Details["owner_pubkey"].(string); ok && owner != "" {
			contextParts = append(contextParts, fmt.Sprintf("owner: `%s`", truncateKey(owner)))
		}
		if txSig, ok := first.Details["tx_signature"].(string); ok && txSig != "" {
			contextParts = append(contextParts, fmt.Sprintf("tx: `%s`", truncateKey(txSig)))
		}
		if slot, ok := first.Details["slot"]; ok {
			contextParts = append(contextParts, fmt.Sprintf("slot %v", slot))
		}
		if len(contextParts) > 0 {
			ctxText := slack.NewTextBlockObject(slack.MarkdownType, strings.Join(contextParts, " · "), false, false)
			blocks = append(blocks, slack.NewContextBlock("", ctxText))
		}
	}

	blocks = append(blocks, slack.NewDividerBlock())
	return blocks
}

func truncateKey(key string) string {
	if len(key) <= 12 {
		return key
	}
	return key[:8] + "..." + key[len(key)-4:]
}
