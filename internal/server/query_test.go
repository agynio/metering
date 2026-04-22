package server

import (
	"strings"
	"testing"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/google/uuid"
)

func TestBuildUsageQueryFiveMinuteBuckets(t *testing.T) {
	start := time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)
	query := usageQuery{
		OrgID:       uuid.New(),
		Unit:        meteringv1.Unit_UNIT_TOKENS.String(),
		Start:       start,
		End:         end,
		Granularity: meteringv1.Granularity_GRANULARITY_FIVE_MINUTES,
		TimeZone:    "UTC",
	}

	sqlQuery, args := buildUsageQuery(query)

	expectedBucket := "date_bin('5 minutes'::interval, timestamp, timezone($5, date_trunc('day', timezone($5, timestamp))))"
	if !strings.Contains(sqlQuery, expectedBucket) {
		t.Fatalf("expected bucket expression %q in query: %s", expectedBucket, sqlQuery)
	}
	if len(args) != 5 {
		t.Fatalf("expected 5 arguments, got %d", len(args))
	}
	if args[4] != "UTC" {
		t.Fatalf("expected time_zone arg UTC, got %v", args[4])
	}
}

func TestBuildUsageQueryDayBucketsTimeZoneAlignment(t *testing.T) {
	start := time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC)
	end := start.Add(12 * time.Hour)
	query := usageQuery{
		OrgID:       uuid.New(),
		Unit:        meteringv1.Unit_UNIT_TOKENS.String(),
		Start:       start,
		End:         end,
		Granularity: meteringv1.Granularity_GRANULARITY_DAY,
		TimeZone:    "America/Los_Angeles",
	}

	sqlQuery, args := buildUsageQuery(query)

	expectedBucket := "timezone($5, date_trunc('day', timezone($5, timestamp)))"
	if !strings.Contains(sqlQuery, expectedBucket) {
		t.Fatalf("expected timezone bucket expression %q in query: %s", expectedBucket, sqlQuery)
	}
	if len(args) != 5 {
		t.Fatalf("expected 5 arguments, got %d", len(args))
	}
	if args[4] != "America/Los_Angeles" {
		t.Fatalf("expected time_zone arg America/Los_Angeles, got %v", args[4])
	}
}
