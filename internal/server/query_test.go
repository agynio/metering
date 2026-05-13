package server

import (
	"fmt"
	"strings"
	"testing"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/google/uuid"
)

func TestBuildUsageQueryBucketedGranularities(t *testing.T) {
	start := time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)
	cases := []struct {
		name        string
		granularity meteringv1.Granularity
		interval    string
	}{
		{
			name:        "five-minutes",
			granularity: meteringv1.Granularity_GRANULARITY_FIVE_MINUTES,
			interval:    "5 minutes",
		},
		{
			name:        "hour",
			granularity: meteringv1.Granularity_GRANULARITY_HOUR,
			interval:    "1 hour",
		},
		{
			name:        "six-hours",
			granularity: meteringv1.Granularity_GRANULARITY_SIX_HOURS,
			interval:    "6 hours",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			query := usageQuery{
				OrgID:       uuid.New(),
				Unit:        meteringv1.Unit_UNIT_TOKENS.String(),
				Start:       start,
				End:         end,
				Granularity: testCase.granularity,
				TimeZone:    "UTC",
			}

			sqlQuery, args := buildUsageQuery(query)

			expectedBucket := fmt.Sprintf(
				"date_bin('%s'::interval, timestamp, timezone($5, date_trunc('day', timezone($5, timestamp))))",
				testCase.interval,
			)
			if !strings.Contains(sqlQuery, expectedBucket) {
				t.Fatalf("expected bucket expression %q in query: %s", expectedBucket, sqlQuery)
			}
			if len(args) != 5 {
				t.Fatalf("expected 5 arguments, got %d", len(args))
			}
			if args[4] != "UTC" {
				t.Fatalf("expected time_zone arg UTC, got %v", args[4])
			}
		})
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

func TestBuildUsageQueryTotalDoesNotIncludeTimeZone(t *testing.T) {
	start := time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC)
	end := start.Add(30 * time.Minute)
	query := usageQuery{
		OrgID:       uuid.New(),
		Unit:        meteringv1.Unit_UNIT_TOKENS.String(),
		Start:       start,
		End:         end,
		Granularity: meteringv1.Granularity_GRANULARITY_TOTAL,
		TimeZone:    "UTC",
	}

	sqlQuery, args := buildUsageQuery(query)

	if strings.Contains(sqlQuery, "timezone($5") {
		t.Fatalf("expected total query to omit time_zone bucket expression: %s", sqlQuery)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 arguments, got %d", len(args))
	}
}

func TestBuildUsageQueryReturnsMicroUnits(t *testing.T) {
	query := usageQuery{
		OrgID:       uuid.New(),
		Unit:        meteringv1.Unit_UNIT_COUNT.String(),
		Start:       time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC),
		End:         time.Date(2026, 4, 22, 10, 33, 0, 0, time.UTC),
		Granularity: meteringv1.Granularity_GRANULARITY_TOTAL,
		TimeZone:    "UTC",
	}

	sqlQuery, _ := buildUsageQuery(query)

	if !strings.Contains(sqlQuery, "(SUM(value) * 1000000)::bigint AS value") {
		t.Fatalf("expected query to return micro-unit values: %s", sqlQuery)
	}
}

func TestUsageQueryUsesSameScaleAsInsert(t *testing.T) {
	if usageValueScale != 1000000 {
		t.Fatalf("expected usage value scale 1000000, got %d", usageValueScale)
	}

	query := usageQuery{
		OrgID:       uuid.New(),
		Unit:        meteringv1.Unit_UNIT_TOKENS.String(),
		Start:       time.Date(2026, 4, 22, 10, 3, 0, 0, time.UTC),
		End:         time.Date(2026, 4, 22, 10, 33, 0, 0, time.UTC),
		Granularity: meteringv1.Granularity_GRANULARITY_TOTAL,
		TimeZone:    "UTC",
	}

	sqlQuery, _ := buildUsageQuery(query)

	if !strings.Contains(sqlQuery, fmt.Sprintf("SUM(value) * %d", usageValueScale)) {
		t.Fatalf("expected query to use usage value scale %d: %s", usageValueScale, sqlQuery)
	}
}
