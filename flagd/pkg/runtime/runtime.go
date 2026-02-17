package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	msync "sync"
	"syscall"

	"github.com/open-feature/flagd/core/pkg/evaluator"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/service"
	"github.com/open-feature/flagd/core/pkg/sync"
	"github.com/open-feature/flagd/flagd/pkg/service/flag-evaluation/ofrep"
	flagsync "github.com/open-feature/flagd/flagd/pkg/service/flag-sync"
	"golang.org/x/sync/errgroup"
)

type Runtime struct {
	Evaluator         evaluator.IEvaluator
	Logger            *logger.Logger
	SyncService       flagsync.ISyncService
	OfrepService      ofrep.IOfrepService
	EvaluationService service.IFlagEvaluationService
	ServiceConfig     service.Configuration
	Syncs             []sync.ISync

	mu msync.Mutex
}

//nolint:funlen
func (r *Runtime) Start() error {
	if r.EvaluationService == nil {
		return errors.New("no service set")
	}
	if len(r.Syncs) == 0 {
		return errors.New("no sync implementation set")
	}
	if r.Evaluator == nil {
		return errors.New("no evaluator set")
	}

	// Create operation context (NOT signal-bound) for two-phase shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Separate signal handling for coordinated shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	g, gCtx := errgroup.WithContext(ctx)
	dataSync := make(chan sync.DataSync, len(r.Syncs))

	// Initialize DataSync channel watcher
	g.Go(func() error {
		for {
			select {
			case data := <-dataSync:
				r.updateAndEmit(data)
			case <-gCtx.Done():
				return nil
			}
		}
	})

	// Init sync providers
	for _, s := range r.Syncs {
		if err := s.Init(gCtx); err != nil {
			return fmt.Errorf("sync provider Init returned error: %w", err)
		}
	}

	// Start sync provider
	for _, s := range r.Syncs {
		p := s
		g.Go(func() error {
			if err := p.Sync(gCtx, dataSync); err != nil {
				return fmt.Errorf("sync provider returned error: %w", err)
			}
			return nil
		})
	}

	// Shutdown coordinator goroutine - implements event-driven graceful shutdown
	g.Go(func() error {
		<-sigChan // Wait for shutdown signal

		// PHASE 1: Send graceful shutdown notifications and get completion channel
		r.Logger.Info("Initiating graceful shutdown...")
		shutdownComplete := r.EvaluationService.Shutdown()

		// PHASE 2: Wait for handlers to actually complete (or timeout)
		r.Logger.Info("Waiting for active handlers to complete...")
		<-shutdownComplete // Block until all handlers unsubscribed or timeout
		r.Logger.Info("All handlers completed, canceling contexts")

		// PHASE 3: Cancel contexts to trigger cleanup
		cancel() // This triggers all goroutines to exit via gCtx.Done()
		return nil
	})

	g.Go(func() error {
		// Readiness probe rely on the runtime
		r.ServiceConfig.ReadinessProbe = r.isReady
		if err := r.EvaluationService.Serve(gCtx, r.ServiceConfig); err != nil {
			return fmt.Errorf("error returned from serving flag evaluation service: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		err := r.OfrepService.Start(gCtx)
		if err != nil {
			return fmt.Errorf("error from ofrep server: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		err := r.SyncService.Start(gCtx)
		if err != nil {
			return fmt.Errorf("error from sync server: %w", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return fmt.Errorf("errgroup closed with error: %w", err)
	}

	r.Logger.Info("Server successfully shutdown.")
	return nil
}

func (r *Runtime) isReady() bool {
	// if all providers can watch for flag changes, we are ready.
	for _, p := range r.Syncs {
		if !p.IsReady() {
			return false
		}
	}
	return true
}

// updateAndEmit helps to update state, notify changes and trigger sync updates
func (r *Runtime) updateAndEmit(payload sync.DataSync) {
	r.mu.Lock()
	defer r.mu.Unlock()

	err := r.Evaluator.SetState(payload)
	if err != nil {
		r.Logger.Error(fmt.Sprintf("error setting state: %v", err))
		return
	}
	r.SyncService.Emit(payload.Source)
}
