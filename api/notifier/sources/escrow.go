package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/malbeclabs/lake/api/notifier"
)

// escrowFilters is the JSON schema for escrow event filters.
type escrowFilters struct {
	ExcludeSigners []string `json:"exclude_signers"`
}

const (
	SourceTypeEscrowEvents = "escrow_events"

	// maxEventsPerPoll limits how many events we fetch per poll to avoid
	// overwhelming delivery channels on first run or after long downtime.
	maxEventsPerPoll = 500
)

// EscrowEventsSource polls ClickHouse for new shred escrow events.
type EscrowEventsSource struct {
	DB       driver.Conn
	Database string
}

func (s *EscrowEventsSource) Type() string {
	return SourceTypeEscrowEvents
}

type escrowEventRow struct {
	EventTS      time.Time
	EscrowPK     string
	ClientSeatPK string
	TxSignature  string
	Slot         uint64
	EventType    string
	AmountUSDC   *int64
	BalanceUSDC  *int64
	Epoch        *uint64
	Status       string
	Signer       string
}

func (s *EscrowEventsSource) Poll(ctx context.Context, cp notifier.Checkpoint) ([]notifier.EventGroup, notifier.Checkpoint, error) {
	query := fmt.Sprintf(`
		SELECT event_ts, escrow_pk, client_seat_pk, tx_signature, slot,
		       event_type, amount_usdc, balance_after_usdc, epoch, status, signer
		FROM %s.fact_dz_shred_escrow_events FINAL
		WHERE (slot > $1 OR (slot = $1 AND event_ts > $2))
		  AND status = 'ok'
		ORDER BY slot ASC, event_ts ASC
		LIMIT %d`, s.Database, maxEventsPerPoll)

	rows, err := s.DB.Query(ctx, query, cp.LastSlot, cp.LastEventTS)
	if err != nil {
		return nil, cp, fmt.Errorf("escrow events query: %w", err)
	}
	defer rows.Close()

	var events []escrowEventRow
	for rows.Next() {
		var e escrowEventRow
		if err := rows.Scan(&e.EventTS, &e.EscrowPK, &e.ClientSeatPK, &e.TxSignature,
			&e.Slot, &e.EventType, &e.AmountUSDC, &e.BalanceUSDC, &e.Epoch, &e.Status, &e.Signer); err != nil {
			return nil, cp, fmt.Errorf("escrow events scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, cp, fmt.Errorf("escrow events rows: %w", err)
	}

	if len(events) == 0 {
		return nil, cp, nil
	}

	// Group events by tx_signature.
	groups := groupByTransaction(events)

	// Advance checkpoint to the last event seen.
	last := events[len(events)-1]
	newCP := notifier.Checkpoint{
		LastEventTS: last.EventTS,
		LastSlot:    last.Slot,
	}

	return groups, newCP, nil
}

// Filter excludes event groups where any event's signer is in the exclude list.
func (s *EscrowEventsSource) Filter(groups []notifier.EventGroup, filtersRaw json.RawMessage) []notifier.EventGroup {
	if len(filtersRaw) == 0 || string(filtersRaw) == "{}" {
		return groups
	}

	var f escrowFilters
	if err := json.Unmarshal(filtersRaw, &f); err != nil || len(f.ExcludeSigners) == 0 {
		return groups
	}

	excluded := make(map[string]bool, len(f.ExcludeSigners))
	for _, s := range f.ExcludeSigners {
		excluded[s] = true
	}

	var filtered []notifier.EventGroup
	for _, g := range groups {
		exclude := false
		for _, e := range g.Events {
			if signer, ok := e.Details["signer"].(string); ok && excluded[signer] {
				exclude = true
				break
			}
		}
		if !exclude {
			filtered = append(filtered, g)
		}
	}
	return filtered
}

// groupByTransaction groups escrow events by tx_signature and produces
// human-readable summaries.
func groupByTransaction(events []escrowEventRow) []notifier.EventGroup {
	// Preserve order by using a slice + map for grouping.
	var keys []string
	grouped := make(map[string][]escrowEventRow)

	for _, e := range events {
		if _, exists := grouped[e.TxSignature]; !exists {
			keys = append(keys, e.TxSignature)
		}
		grouped[e.TxSignature] = append(grouped[e.TxSignature], e)
	}

	groups := make([]notifier.EventGroup, 0, len(keys))
	for _, txSig := range keys {
		txEvents := grouped[txSig]
		group := notifier.EventGroup{
			Key:    txSig,
			Events: make([]notifier.Event, 0, len(txEvents)),
		}

		for _, e := range txEvents {
			details := map[string]any{
				"escrow_pk":      e.EscrowPK,
				"client_seat_pk": e.ClientSeatPK,
				"tx_signature":   e.TxSignature,
				"slot":           e.Slot,
				"event_type":     e.EventType,
				"signer":         e.Signer,
				"event_ts":       e.EventTS,
			}
			if e.AmountUSDC != nil {
				details["amount_usdc"] = *e.AmountUSDC
			}
			if e.BalanceUSDC != nil {
				details["balance_after_usdc"] = *e.BalanceUSDC
			}
			if e.Epoch != nil {
				details["epoch"] = *e.Epoch
			}

			group.Events = append(group.Events, notifier.Event{
				Type:    e.EventType,
				Details: details,
			})
		}

		group.Summary = buildTransactionSummary(txEvents)
		groups = append(groups, group)
	}

	return groups
}

// buildTransactionSummary produces a human-readable summary for a group of
// events in the same transaction.
func buildTransactionSummary(events []escrowEventRow) string {
	types := make(map[string]bool, len(events))
	for _, e := range events {
		types[e.EventType] = true
	}

	// Detect common transaction patterns and produce concise summaries.
	switch {
	case types["initialize_seat"] && types["fund"]:
		return "Seat Initialized & Funded"
	case types["initialize_seat"]:
		return "Seat Initialized"
	case types["initialize_escrow"]:
		return "Escrow Initialized"
	case types["fund"] && types["allocate_seat"]:
		return "Seat Funded & Allocated"
	case types["fund"]:
		return "Seat Funded"
	case types["allocate_seat"]:
		return "Seat Instant Allocated"
	case types["batch_allocate"]:
		return "Seats Batch Allocated"
	case types["ack_allocate"]:
		return "Seat Allocation Confirmed"
	case types["reject_allocate"]:
		return "Seat Allocation Rejected"
	case types["withdraw_seat"]:
		return "Seat Withdrawal Requested"
	case types["ack_withdraw"]:
		return "Seat Withdrawal Confirmed"
	case types["close"]:
		return "Escrow Closed"
	case types["batch_settle"]:
		return "Devices Settled"
	case types["set_price_override"]:
		return "Price Override Set"
	default:
		return "Escrow Activity"
	}
}
