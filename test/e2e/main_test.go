//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMeteringAddress = "metering:50051"
	dialTimeout            = 20 * time.Second
)

var (
	meteringClient meteringv1.MeteringServiceClient
	meteringConn   *grpc.ClientConn
)

func envOrDefault(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func TestMain(m *testing.M) {
	addr := envOrDefault("METERING_ADDRESS", defaultMeteringAddress)

	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", addr, err)
		os.Exit(1)
	}

	meteringConn = conn
	meteringClient = meteringv1.NewMeteringServiceClient(conn)

	exitCode := m.Run()
	if err := conn.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close gRPC connection: %v\n", err)
	}
	os.Exit(exitCode)
}
