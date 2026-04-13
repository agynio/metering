package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type usageQuery struct {
	OrgID       uuid.UUID
	Unit        string
	Start       time.Time
	End         time.Time
	Filters     []labelFilter
	GroupBy     string
	Granularity meteringv1.Granularity
}

func (s *Server) QueryUsage(ctx context.Context, req *meteringv1.QueryUsageRequest) (*meteringv1.QueryUsageResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	orgID, err := parseUUID(req.GetOrgId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "org_id: %v", err)
	}
	if req.Start == nil {
		return nil, status.Error(codes.InvalidArgument, "start is required")
	}
	if err := req.GetStart().CheckValid(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "start: %v", err)
	}
	if req.End == nil {
		return nil, status.Error(codes.InvalidArgument, "end is required")
	}
	if err := req.GetEnd().CheckValid(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "end: %v", err)
	}
	start := req.GetStart().AsTime().UTC()
	end := req.GetEnd().AsTime().UTC()
	if end.Before(start) {
		return nil, status.Error(codes.InvalidArgument, "end must be after start")
	}
	unit, err := parseUnit(req.GetUnit())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unit: %v", err)
	}
	filters, err := parseLabelFilters(req.GetLabelFilters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "label_filters: %v", err)
	}
	groupBy, err := parseGroupBy(req.GetGroupBy())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "group_by: %v", err)
	}
	granularity := req.GetGranularity()
	switch granularity {
	case meteringv1.Granularity_GRANULARITY_TOTAL, meteringv1.Granularity_GRANULARITY_DAY:
	default:
		return nil, status.Error(codes.InvalidArgument, "granularity must be total or day")
	}

	result, err := s.queryUsage(ctx, usageQuery{
		OrgID:       orgID,
		Unit:        unit,
		Start:       start,
		End:         end,
		Filters:     filters,
		GroupBy:     groupBy,
		Granularity: granularity,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query usage: %v", err)
	}

	return result, nil
}

func (s *Server) queryUsage(ctx context.Context, query usageQuery) (*meteringv1.QueryUsageResponse, error) {
	sqlQuery, args := buildUsageQuery(query)
	rows, err := s.pool.Query(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	buckets := make([]*meteringv1.UsageBucket, 0)
	for rows.Next() {
		var bucketTime pgtype.Timestamptz
		var groupValue string
		var value int64
		if err := rows.Scan(&bucketTime, &groupValue, &value); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		var timestamp *timestamppb.Timestamp
		if bucketTime.Valid {
			timestamp = timestamppb.New(bucketTime.Time)
		}

		buckets = append(buckets, &meteringv1.UsageBucket{
			Timestamp:  timestamp,
			GroupValue: groupValue,
			Value:      value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rows: %w", err)
	}

	s.logger.Debugf("query usage buckets=%d", len(buckets))
	return &meteringv1.QueryUsageResponse{Buckets: buckets}, nil
}

func buildUsageQuery(query usageQuery) (string, []any) {
	selectParts := make([]string, 0, 3)
	groupByParts := make([]string, 0, 2)
	orderParts := make([]string, 0, 2)

	if query.Granularity == meteringv1.Granularity_GRANULARITY_DAY {
		selectParts = append(selectParts, "date_trunc('day', timestamp) AS bucket_ts")
		groupByParts = append(groupByParts, "date_trunc('day', timestamp)")
		orderParts = append(orderParts, "bucket_ts")
	} else {
		selectParts = append(selectParts, "NULL::timestamptz AS bucket_ts")
	}

	if query.GroupBy != "" {
		selectParts = append(selectParts, fmt.Sprintf("COALESCE(%s::text, '') AS group_value", query.GroupBy))
		groupByParts = append(groupByParts, query.GroupBy)
		orderParts = append(orderParts, "group_value")
	} else {
		selectParts = append(selectParts, "'' AS group_value")
	}

	selectParts = append(selectParts, "(SUM(value) * 1000000)::bigint AS value")

	whereParts := []string{
		"org_id = $1",
		"unit = $2",
		"timestamp >= $3",
		"timestamp <= $4",
	}
	args := []any{query.OrgID, query.Unit, query.Start, query.End}

	for _, filter := range query.Filters {
		args = append(args, filter.Value)
		whereParts = append(whereParts, fmt.Sprintf("%s = $%d", filter.Column, len(args)))
	}

	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(strings.Join(selectParts, ", "))
	builder.WriteString(" FROM usage_events WHERE ")
	builder.WriteString(strings.Join(whereParts, " AND "))
	if len(groupByParts) > 0 {
		builder.WriteString(" GROUP BY ")
		builder.WriteString(strings.Join(groupByParts, ", "))
	}
	if query.GroupBy == "" && query.Granularity == meteringv1.Granularity_GRANULARITY_TOTAL {
		builder.WriteString(" HAVING COUNT(*) > 0")
	}
	if len(orderParts) > 0 {
		builder.WriteString(" ORDER BY ")
		builder.WriteString(strings.Join(orderParts, ", "))
	}

	return builder.String(), args
}
