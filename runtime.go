package flow

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/store"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
)

const defaultMaxCommandsPerExecution = 1000

// Option is a sealed runtime configuration option.
type Option interface {
	applyRuntime(*runtimeOptions)
}

type runtimeOptions struct {
	schema            string
	maxCommands       int
	workerConcurrency int
	planConcurrency   int
	queueConcurrency  map[string]int
	commandLease      time.Duration
	pollInterval      time.Duration
	shutdownGrace     time.Duration
	planVerification  bool
	observer          Observer
	faults            fault.Hook
	errs              []error
}

type runtimeOptionFunc func(*runtimeOptions)

func (f runtimeOptionFunc) applyRuntime(options *runtimeOptions) { f(options) }

func (o schemaOption) applyRuntime(options *runtimeOptions) { options.schema = o.schema }

// WithMaxCommandsPerExecution sets the command ceiling copied into each newly
// created execution. Zero explicitly disables the ceiling.
func WithMaxCommandsPerExecution(max int) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if max < 0 {
			options.errs = append(options.errs, errors.New("maximum commands must not be negative"))
			return
		}
		options.maxCommands = max
	})
}

// WithObserver installs the optional no-op-by-default operational observer.
func WithObserver(observer Observer) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if observer == nil {
			options.errs = append(options.errs, errors.New("observer must not be nil"))
			return
		}
		options.observer = observer
	})
}

// WithWorkerConcurrency bounds command handlers running in this process.
func WithWorkerConcurrency(concurrency int) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if concurrency <= 0 {
			options.errs = append(options.errs, errors.New("worker concurrency must be positive"))
			return
		}
		options.workerConcurrency = concurrency
	})
}

// WithPlanConcurrency bounds plan reconciliation transactions in this
// process. Different executions may reconcile concurrently; one execution is
// still serialized by its PostgreSQL execution-row lock.
func WithPlanConcurrency(concurrency int) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if concurrency <= 0 {
			options.errs = append(options.errs, errors.New("plan concurrency must be positive"))
			return
		}
		options.planConcurrency = concurrency
	})
}

// WithQueueConcurrency optionally gives one queue lane a smaller process-local
// handler limit. The lane still shares the runtime's global worker capacity.
func WithQueueConcurrency(queue string, concurrency int) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if err := definition.ValidateName(queue); err != nil {
			options.errs = append(options.errs, errors.New("queue concurrency requires a valid queue name"))
			return
		}
		if concurrency <= 0 {
			options.errs = append(options.errs, errors.New("queue concurrency must be positive"))
			return
		}
		if options.queueConcurrency == nil {
			options.queueConcurrency = make(map[string]int)
		}
		if _, exists := options.queueConcurrency[queue]; exists {
			options.errs = append(options.errs, errors.New("queue concurrency configured more than once"))
			return
		}
		options.queueConcurrency[queue] = concurrency
	})
}

// WithCommandLease configures the renewable ownership lease for command attempts.
func WithCommandLease(lease time.Duration) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if lease < 30*time.Millisecond {
			options.errs = append(options.errs, errors.New("command lease must be at least 30 milliseconds"))
			return
		}
		options.commandLease = lease
	})
}

// WithPollInterval configures the fallback scheduler and maintenance poll.
func WithPollInterval(interval time.Duration) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if interval <= 0 {
			options.errs = append(options.errs, errors.New("poll interval must be positive"))
			return
		}
		options.pollInterval = interval
	})
}

// WithShutdownGrace configures how long Run waits before interrupting handlers.
func WithShutdownGrace(grace time.Duration) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if grace < 0 {
			options.errs = append(options.errs, errors.New("shutdown grace must not be negative"))
			return
		}
		options.shutdownGrace = grace
	})
}

// WithPlanVerification enables development-time double evaluation of every
// complete pure-plan snapshot. It detects nondeterministic declarations or
// consulted reads before a reconciliation is committed.
func WithPlanVerification(enabled bool) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) { options.planVerification = enabled })
}

// Runtime is a configured PostgreSQL-backed Flow client. New starts no
// goroutines; execution operations are usable before background processing is
// started.
type Runtime struct {
	db                *pgkit.DB
	store             *store.Store
	schema            string
	maxCommands       int
	workerConcurrency int
	planConcurrency   int
	queueConcurrency  map[string]int
	commandLease      time.Duration
	pollInterval      time.Duration
	shutdownGrace     time.Duration
	planVerification  bool
	instanceID        uuid.UUID
	observer          Observer
	faults            fault.Hook

	mu          sync.RWMutex
	closed      bool
	lifecycle   runtimeLifecycle
	registry    *runtimeRegistry
	runCancel   context.CancelFunc
	runDone     chan struct{}
	wake        *wakeHub
	active      *activeCommands
	workerGroup sync.WaitGroup
}

// New validates configuration and schema compatibility without migrating or
// starting background work.
func New(db *pgkit.DB, opts ...Option) (*Runtime, error) {
	options := runtimeOptions{
		schema: defaultSchema, maxCommands: defaultMaxCommandsPerExecution,
		workerConcurrency: max(1, runtime.GOMAXPROCS(0)), commandLease: 60 * time.Second,
		planConcurrency: 1,
		pollInterval:    time.Second, shutdownGrace: 30 * time.Second,
		observer: NopObserver{}, faults: fault.None{},
	}
	for _, option := range opts {
		if option == nil {
			options.errs = append(options.errs, errors.New("nil runtime option"))
			continue
		}
		option.applyRuntime(&options)
	}
	if err := errors.Join(options.errs...); err != nil {
		return nil, newError(ErrInvalid, "new", "runtime", "", err.Error())
	}
	if _, err := CheckSchema(context.Background(), db, WithSchema(options.schema)); err != nil {
		return nil, err
	}
	repository, err := store.New(db, options.schema)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		db: db, store: repository, schema: options.schema, maxCommands: options.maxCommands,
		workerConcurrency: options.workerConcurrency, commandLease: options.commandLease,
		planConcurrency:  options.planConcurrency,
		queueConcurrency: cloneIntMap(options.queueConcurrency),
		pollInterval:     options.pollInterval, shutdownGrace: options.shutdownGrace,
		planVerification: options.planVerification,
		instanceID:       uuid.New(),
		observer:         options.observer, faults: options.faults, lifecycle: runtimeCreated,
		registry: newRuntimeRegistry(), wake: newWakeHub(), active: newActiveCommands(),
	}, nil
}

func cloneIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func (*Runtime) flowClient() {}

type transactionClient struct {
	runtime *Runtime
	tx      pgx.Tx
	order   store.LockOrder
}

func (*transactionClient) flowClient() {}

// InTx returns a client whose writes participate in the supplied caller-owned
// transaction. Flow never commits or rolls back that transaction.
func (r *Runtime) InTx(tx pgx.Tx) Client {
	return &transactionClient{runtime: r, tx: tx}
}

type resolvedClient struct {
	runtime *Runtime
	tx      pgx.Tx
	order   *store.LockOrder
}

func resolveClient(client Client) (resolvedClient, error) {
	switch value := client.(type) {
	case *Runtime:
		if value == nil {
			return resolvedClient{}, newError(ErrInvalid, "use", "client", "", "runtime is nil")
		}
		value.mu.RLock()
		closed := value.closed
		value.mu.RUnlock()
		if closed {
			return resolvedClient{}, newError(ErrClosed, "use", "runtime", "", "runtime is closed")
		}
		return resolvedClient{runtime: value}, nil
	case *transactionClient:
		if value == nil || value.runtime == nil || value.tx == nil {
			return resolvedClient{}, newError(ErrInvalid, "use", "client", "", "transaction client is incomplete")
		}
		value.runtime.mu.RLock()
		closed := value.runtime.closed
		value.runtime.mu.RUnlock()
		if closed {
			return resolvedClient{}, newError(ErrClosed, "use", "runtime", "", "runtime is closed")
		}
		return resolvedClient{runtime: value.runtime, tx: value.tx, order: &value.order}, nil
	default:
		return resolvedClient{}, newError(ErrInvalid, "use", "client", "", "unsupported or nil client")
	}
}

func (c resolvedClient) inTransaction(ctx context.Context, operation func(pgx.Tx) error) (resultErr error) {
	if c.tx != nil {
		return operation(c.tx)
	}
	tx, err := c.runtime.db.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return store.MapError("begin Flow operation", err)
	}
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback(context.WithoutCancel(ctx))
		}
	}()
	if err := operation(tx); err != nil {
		return err
	}
	if err := c.runtime.faults.Hit(ctx, fault.IngressBeforeCommit); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return store.MapError("commit Flow operation", err)
	}
	if err := c.runtime.faults.Hit(ctx, fault.IngressCommitAmbiguous); err != nil {
		return err
	}
	return nil
}

func (c resolvedClient) semantic(ctx context.Context, id uuid.UUID, operation func(*store.SemanticTx) error) error {
	if c.order != nil {
		if err := c.order.BeforeExecution(id); err != nil {
			return err
		}
	}
	return c.inTransaction(ctx, func(tx pgx.Tx) error {
		semantic, err := c.runtime.store.AttachSemantic(ctx, tx, id, store.LockBlocking)
		if err != nil {
			return err
		}
		return operation(semantic)
	})
}

func (r *Runtime) observe(ctx context.Context, observation Observation) {
	defer func() { _ = recover() }()
	r.observer.Observe(ctx, observation)
}

func parseExecutionID(id ExecutionID) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.Nil, newError(ErrInvalid, "parse", "execution", string(id), "invalid identifier")
	}
	return parsed, nil
}

func parseCommandID(id CommandID) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.Nil, newError(ErrInvalid, "parse", "command", string(id), "invalid identifier")
	}
	return parsed, nil
}
