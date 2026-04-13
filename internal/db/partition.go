package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Execer describes the subset of database interfaces used for partition creation.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// EnsureMonthlyPartitions creates partitions for the current and next month if missing.
func EnsureMonthlyPartitions(ctx context.Context, execer Execer, now time.Time) error {
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	if err := ensurePartition(ctx, execer, monthStart); err != nil {
		return err
	}
	nextMonth := monthStart.AddDate(0, 1, 0)
	if err := ensurePartition(ctx, execer, nextMonth); err != nil {
		return err
	}
	return nil
}

func ensurePartition(ctx context.Context, execer Execer, monthStart time.Time) error {
	normalized := time.Date(monthStart.Year(), monthStart.Month(), 1, 0, 0, 0, 0, time.UTC)
	nextMonth := normalized.AddDate(0, 1, 0)
	partitionName := fmt.Sprintf("usage_events_%04d_%02d", normalized.Year(), normalized.Month())
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF usage_events FOR VALUES FROM ('%s') TO ('%s')",
		partitionName,
		normalized.Format("2006-01-02"),
		nextMonth.Format("2006-01-02"),
	)
	if _, err := execer.Exec(ctx, query); err != nil {
		return fmt.Errorf("create partition %s: %w", partitionName, err)
	}
	return nil
}
