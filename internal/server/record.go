package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/agynio/metering/internal/db"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type usageRecord struct {
	OrgID          uuid.UUID
	IdempotencyKey string
	Producer       string
	Timestamp      time.Time
	Unit           string
	Value          int64
	Labels         labelValues
}

func (s *Server) Record(ctx context.Context, req *meteringv1.RecordRequest) (*meteringv1.RecordResponse, error) {
	if req == nil || len(req.GetRecords()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "records must not be empty")
	}

	records := make([]usageRecord, 0, len(req.Records))
	for i, record := range req.Records {
		parsed, err := parseUsageRecord(record)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "records[%d]: %v", i, err)
		}
		records = append(records, parsed)
	}

	if err := s.recordUsage(ctx, records); err != nil {
		return nil, status.Errorf(codes.Internal, "record usage: %v", err)
	}

	s.logger.Debugf("recorded %d usage records", len(records))
	return &meteringv1.RecordResponse{}, nil
}

func parseUsageRecord(record *meteringv1.UsageRecord) (usageRecord, error) {
	if record == nil {
		return usageRecord{}, fmt.Errorf("record is required")
	}
	orgID, err := parseUUID(record.GetOrgId())
	if err != nil {
		return usageRecord{}, fmt.Errorf("org_id: %w", err)
	}
	idempotencyKey := strings.TrimSpace(record.GetIdempotencyKey())
	if idempotencyKey == "" {
		return usageRecord{}, fmt.Errorf("idempotency_key must not be empty")
	}
	producer := strings.TrimSpace(record.GetProducer())
	if producer == "" {
		return usageRecord{}, fmt.Errorf("producer must not be empty")
	}
	if record.Timestamp == nil {
		return usageRecord{}, fmt.Errorf("timestamp is required")
	}
	if err := record.GetTimestamp().CheckValid(); err != nil {
		return usageRecord{}, fmt.Errorf("timestamp: %w", err)
	}
	timestamp := record.GetTimestamp().AsTime().UTC()
	unit, err := parseUnit(record.GetUnit())
	if err != nil {
		return usageRecord{}, fmt.Errorf("unit: %w", err)
	}
	labels, err := parseLabelValues(record.GetLabels())
	if err != nil {
		return usageRecord{}, err
	}

	return usageRecord{
		OrgID:          orgID,
		IdempotencyKey: idempotencyKey,
		Producer:       producer,
		Timestamp:      timestamp,
		Unit:           unit,
		Value:          record.GetValue(),
		Labels:         labels,
	}, nil
}

func parseUnit(unit meteringv1.Unit) (string, error) {
	switch unit {
	case meteringv1.Unit_UNIT_TOKENS,
		meteringv1.Unit_UNIT_CORE_SECONDS,
		meteringv1.Unit_UNIT_GB_SECONDS,
		meteringv1.Unit_UNIT_COUNT:
		return unit.String(), nil
	default:
		return "", fmt.Errorf("invalid unit")
	}
}

func (s *Server) recordUsage(ctx context.Context, records []usageRecord) (err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if err = db.EnsureMonthlyPartitions(ctx, tx, time.Now()); err != nil {
		return fmt.Errorf("ensure partitions: %w", err)
	}
	if err = insertUsageRecords(ctx, tx, records); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func insertUsageRecords(ctx context.Context, tx pgx.Tx, records []usageRecord) error {
	const columns = "org_id, idempotency_key, producer, timestamp, unit, value, resource_id, resource, identity_id, identity_type, thread_id, kind, status"
	args := make([]any, 0, len(records)*13)
	var builder strings.Builder
	builder.WriteString("INSERT INTO usage_events (")
	builder.WriteString(columns)
	builder.WriteString(") VALUES ")

	for i, record := range records {
		if i > 0 {
			builder.WriteString(", ")
		}
		start := i*13 + 1
		builder.WriteString("(")
		if _, err := fmt.Fprintf(
			&builder,
			"$%d, $%d, $%d, $%d, $%d, ($%d::numeric / 1000000), $%d, $%d, $%d, $%d, $%d, $%d, $%d",
			start,
			start+1,
			start+2,
			start+3,
			start+4,
			start+5,
			start+6,
			start+7,
			start+8,
			start+9,
			start+10,
			start+11,
			start+12,
		); err != nil {
			return fmt.Errorf("build insert row: %w", err)
		}
		builder.WriteString(")")

		args = append(args,
			record.OrgID,
			record.IdempotencyKey,
			record.Producer,
			record.Timestamp,
			record.Unit,
			record.Value,
			nullableUUID(record.Labels.ResourceID),
			nullableString(record.Labels.Resource),
			nullableUUID(record.Labels.IdentityID),
			nullableString(record.Labels.IdentityType),
			nullableUUID(record.Labels.ThreadID),
			nullableString(record.Labels.Kind),
			nullableString(record.Labels.Status),
		)
	}

	builder.WriteString(" ON CONFLICT (idempotency_key, month) DO UPDATE SET value = EXCLUDED.value")

	if _, err := tx.Exec(ctx, builder.String(), args...); err != nil {
		return fmt.Errorf("insert usage records: %w", err)
	}
	return nil
}

func nullableUUID(value *uuid.UUID) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
