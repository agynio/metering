package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	meteringv1 "github.com/agynio/metering/.gen/go/agynio/api/metering/v1"
	"github.com/agynio/metering/internal/config"
	"github.com/agynio/metering/internal/db"
	"github.com/agynio/metering/internal/logging"
	"github.com/agynio/metering/internal/server"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("metering: %v", err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := logging.New(os.Stdout, cfg.LogLevel)

	poolCfg, err := pgxpool.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	if err := db.EnsureMonthlyPartitions(ctx, pool, time.Now()); err != nil {
		return fmt.Errorf("ensure partitions: %w", err)
	}

	grpcServer := grpc.NewServer()
	meteringv1.RegisterMeteringServiceServer(grpcServer, server.New(server.Options{
		Pool:   pool,
		Logger: logger,
	}))

	listener, err := net.Listen("tcp", cfg.GRPCAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.GRPCAddress, err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			logger.Errorf("close listener: %v", err)
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Infof("metering: ready")
		if err := grpcServer.Serve(listener); err != nil {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	case <-ctx.Done():
		grpcServer.GracefulStop()
		return nil
	}
}
