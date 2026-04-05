package notifier

import (
	"fmt"
	"strings"
	"time"
)

// RenderMarkdown renders event groups as a markdown string.
func RenderMarkdown(groups []EventGroup) string {
	var sb strings.Builder
	for i, g := range groups {
		if i > 0 {
			sb.WriteString("\n---\n\n")
		}
		sb.WriteString(fmt.Sprintf("**%s**\n", g.Summary))

		for _, e := range g.Events {
			line := FormatEventLine(e)
			if line != "" {
				sb.WriteString(line + "\n")
			}
		}

		ctx := formatContextLine(g)
		if ctx != "" {
			sb.WriteString(ctx + "\n")
		}
	}
	return sb.String()
}

// RenderPlaintext renders event groups as plain text (no markdown formatting).
func RenderPlaintext(groups []EventGroup) string {
	var sb strings.Builder
	for i, g := range groups {
		if i > 0 {
			sb.WriteString("\n---\n\n")
		}
		sb.WriteString(g.Summary + "\n")

		for _, e := range g.Events {
			line := FormatEventLine(e)
			if line != "" {
				sb.WriteString("  " + line + "\n")
			}
		}

		ctx := formatContextLinePlain(g)
		if ctx != "" {
			sb.WriteString("  " + ctx + "\n")
		}
	}
	return sb.String()
}

// RenderSummaryMarkdown renders a summary for a large batch of event groups.
func RenderSummaryMarkdown(groups []EventGroup) string {
	counts := make(map[string]int)
	for _, g := range groups {
		counts[g.Summary]++
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("**%d events while notifications were batched**\n\n", len(groups)))
	for summary, count := range counts {
		if count == 1 {
			sb.WriteString(fmt.Sprintf("- %s\n", summary))
		} else {
			sb.WriteString(fmt.Sprintf("- %s (%d)\n", summary, count))
		}
	}
	return sb.String()
}

// FormatEventLine returns a human-readable line for a single event.
func FormatEventLine(event Event) string {
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

func formatWithSeatContext(event Event, prefix string) string {
	if pk, ok := event.Details["client_seat_pk"].(string); ok && pk != "" {
		return fmt.Sprintf("%s (`%s`)", prefix, truncateKey(pk))
	}
	return prefix
}

func formatUserActivityLine(event Event, action string) string {
	var parts []string
	if kind, ok := event.Details["kind"].(string); ok && kind != "" {
		parts = append(parts, kind)
	}
	if clientIP, ok := event.Details["client_ip"].(string); ok && clientIP != "" {
		parts = append(parts, fmt.Sprintf("IP: %s", clientIP))
	}
	if dzIP, ok := event.Details["dz_ip"].(string); ok && dzIP != "" {
		parts = append(parts, fmt.Sprintf("DZ: %s", dzIP))
	}
	if devicePK, ok := event.Details["device_pk"].(string); ok && devicePK != "" {
		parts = append(parts, fmt.Sprintf("device: `%s`", truncateKey(devicePK)))
	}
	if len(parts) > 0 {
		return fmt.Sprintf("%s (%s)", action, strings.Join(parts, ", "))
	}
	return action
}

func formatContextLine(g EventGroup) string {
	if len(g.Events) == 0 {
		return ""
	}
	first := g.Events[0]
	var parts []string

	if signer, ok := first.Details["signer"].(string); ok && signer != "" {
		parts = append(parts, fmt.Sprintf("signer: `%s`", truncateKey(signer)))
	}
	if owner, ok := first.Details["owner_pubkey"].(string); ok && owner != "" {
		parts = append(parts, fmt.Sprintf("owner: `%s`", truncateKey(owner)))
	}
	if txSig, ok := first.Details["tx_signature"].(string); ok && txSig != "" {
		parts = append(parts, fmt.Sprintf("tx: `%s`", truncateKey(txSig)))
	}
	if slot, ok := first.Details["slot"]; ok {
		parts = append(parts, fmt.Sprintf("slot %v", slot))
	}
	if ts, ok := first.Details["event_ts"].(time.Time); ok {
		parts = append(parts, ts.UTC().Format("15:04:05 UTC"))
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " · ")
}

func formatContextLinePlain(g EventGroup) string {
	// Same content, just without backticks
	line := formatContextLine(g)
	return strings.ReplaceAll(line, "`", "")
}

func truncateKey(key string) string {
	if len(key) <= 12 {
		return key
	}
	return key[:8] + "..." + key[len(key)-4:]
}

func formatUSDC(v any) string {
	switch n := v.(type) {
	case int64:
		return fmt.Sprintf("%.2f", float64(n)/1_000_000)
	case float64:
		return fmt.Sprintf("%.2f", n/1_000_000)
	default:
		return ""
	}
}
