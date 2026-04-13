package server

import (
	"context"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/agynio/metering/internal/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server implements the MeteringService gRPC API.
type Server struct {
	meteringv1.UnimplementedMeteringServiceServer
	pool   dbPool
	logger *logging.Logger
}

// Options defines required inputs for constructing a Server.
type Options struct {
	Pool   *pgxpool.Pool
	Logger *logging.Logger
}

type dbPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// New constructs a MeteringService server.
func New(options Options) *Server {
	return &Server{
		pool:   options.Pool,
		logger: options.Logger,
	}
}
