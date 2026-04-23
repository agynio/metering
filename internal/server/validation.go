package server

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

func parseUUID(value string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return uuid.UUID{}, fmt.Errorf("value is empty")
	}
	id, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.UUID{}, err
	}
	return id, nil
}

func parseTimeZone(value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "UTC", nil
	}
	location, err := time.LoadLocation(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid time zone")
	}
	return location.String(), nil
}
