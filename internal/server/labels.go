package server

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

const (
	labelResourceIDKey   = "resource_id"
	labelResourceKey     = "resource"
	labelIdentityIDKey   = "identity_id"
	labelIdentityTypeKey = "identity_type"
	labelThreadIDKey     = "thread_id"
	labelKindKey         = "kind"
	labelStatusKey       = "status"
)

type labelKind int

const (
	labelKindString labelKind = iota
	labelKindUUID
)

type labelDefinition struct {
	Column string
	Kind   labelKind
}

var labelDefinitions = map[string]labelDefinition{
	labelResourceIDKey:   {Column: "resource_id", Kind: labelKindUUID},
	labelResourceKey:     {Column: "resource", Kind: labelKindString},
	labelIdentityIDKey:   {Column: "identity_id", Kind: labelKindUUID},
	labelIdentityTypeKey: {Column: "identity_type", Kind: labelKindString},
	labelThreadIDKey:     {Column: "thread_id", Kind: labelKindUUID},
	labelKindKey:         {Column: "kind", Kind: labelKindString},
	labelStatusKey:       {Column: "status", Kind: labelKindString},
}

type labelValues struct {
	ResourceID   *uuid.UUID
	Resource     *string
	IdentityID   *uuid.UUID
	IdentityType *string
	ThreadID     *uuid.UUID
	Kind         *string
	Status       *string
}

type labelFilter struct {
	Column string
	Value  any
}

func parseLabelValues(labels map[string]string) (labelValues, error) {
	var values labelValues
	for key, raw := range labels {
		definition, ok := labelDefinitions[key]
		if !ok {
			continue
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		switch definition.Kind {
		case labelKindUUID:
			parsed, err := parseUUID(trimmed)
			if err != nil {
				return labelValues{}, fmt.Errorf("%s: %w", key, err)
			}
			assignUUIDLabel(&values, key, parsed)
		case labelKindString:
			assignStringLabel(&values, key, trimmed)
		}
	}
	return values, nil
}

func parseLabelFilters(filters map[string]string) ([]labelFilter, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	result := make([]labelFilter, 0, len(filters))
	for key, raw := range filters {
		definition, ok := labelDefinitions[key]
		if !ok {
			return nil, fmt.Errorf("unsupported label filter: %s", key)
		}
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil, fmt.Errorf("label filter %s must not be empty", key)
		}
		if definition.Kind == labelKindUUID {
			parsed, err := parseUUID(trimmed)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			result = append(result, labelFilter{Column: definition.Column, Value: parsed})
			continue
		}
		result = append(result, labelFilter{Column: definition.Column, Value: trimmed})
	}
	return result, nil
}

func parseGroupBy(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", nil
	}
	definition, ok := labelDefinitions[trimmed]
	if !ok {
		return "", fmt.Errorf("unsupported group_by: %s", trimmed)
	}
	return definition.Column, nil
}

func assignUUIDLabel(values *labelValues, key string, parsed uuid.UUID) {
	switch key {
	case labelResourceIDKey:
		values.ResourceID = &parsed
	case labelIdentityIDKey:
		values.IdentityID = &parsed
	case labelThreadIDKey:
		values.ThreadID = &parsed
	}
}

func assignStringLabel(values *labelValues, key, parsed string) {
	switch key {
	case labelResourceKey:
		values.Resource = &parsed
	case labelIdentityTypeKey:
		values.IdentityType = &parsed
	case labelKindKey:
		values.Kind = &parsed
	case labelStatusKey:
		values.Status = &parsed
	}
}
