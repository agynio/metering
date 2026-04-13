package server

import (
	"fmt"
	"strings"

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
