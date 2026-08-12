package flow

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/goware/flow/internal/definition"
	"github.com/goware/flow/internal/durable"
	"github.com/goware/flow/internal/fault"
	"github.com/goware/flow/internal/store"
	"github.com/goware/flow/internal/uuid"
	"github.com/goware/pgkit/v2"
	"github.com/jackc/pgx/v5"
)

const defaultMaxCommandsPerRun = 1000

// Option is a sealed runtime configuration option.
type Option interface {
	applyRuntime(*runtimeOptions)
}

type runtimeOptions struct {
	schema            string
	maxCommands       int
	workerConcurrency int
	queueConcurrency  map[string]int
	commandLease      time.Duration
	pollInterval      time.Duration
	shutdownGrace     time.Duration
	notifications     bool
	observer          Observer
	faults            fault.Hook
	errs              []error
}

type runtimeOptionFunc func(*runtimeOptions)

func (f runtimeOptionFunc) applyRuntime(options *runtimeOptions) { f(options) }

func (o schemaOption) applyRuntime(options *runtimeOptions) { options.schema = o.schema }

// WithMaxCommandsPerRun sets the command ceiling copied into each newly
// created run. Zero explicitly disables the ceiling.
func WithMaxCommandsPerRun(max int) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if max < 0 {
			options.errs = append(options.errs, errors.New("maximum commands must not be negative"))
			return
		}
		if err := durable.PostgresInteger("maximum commands", max, 0, durable.PostgresIntegerMax); err != nil {
			options.errs = append(options.errs, err)
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

// withCommandLeaseForTest is an unexported seam for in-package lease and
// takeover tests. Production callers always use the fixed 60-second lease.
func withCommandLeaseForTest(lease time.Duration) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) {
		if lease < 30*time.Millisecond {
			options.errs = append(options.errs, errors.New("command lease must be at least 30 milliseconds"))
			return
		}
		if _, err := durable.ExactMilliseconds("command lease", lease); err != nil {
			options.errs = append(options.errs, err)
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

// WithNotifications enables or disables transactional PostgreSQL wake hints.
// It defaults to enabled. Polling always remains active and is the correctness
// path, so disabling notifications is suitable for transaction-pooling
// proxies and deliberately poll-only deployments.
func WithNotifications(enabled bool) Option {
	return runtimeOptionFunc(func(options *runtimeOptions) { options.notifications = enabled })
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

// Runtime is a configured PostgreSQL-backed Flow client. New starts no
// goroutines; run operations are usable before background processing is
// started.
type Runtime struct {
	db                *pgkit.DB
	store             *store.Store
	schema            string
	maxCommands       int
	workerConcurrency int
	queueConcurrency  map[string]int
	commandLease      time.Duration
	pollInterval      time.Duration
	shutdownGrace     time.Duration
	notifications     bool
	instanceID        uuid.UUID
	observer          Observer
	observations      *observerAdapter
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
		schema: defaultSchema, maxCommands: defaultMaxCommandsPerRun,
		workerConcurrency: max(1, runtime.GOMAXPROCS(0)), commandLease: 60 * time.Second,
		pollInterval: time.Second, shutdownGrace: 30 * time.Second,
		notifications: true,
		observer:      noOpObserver{}, faults: fault.None{},
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
	repository, err := store.New(db, options.schema, options.notifications)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		db: db, store: repository, schema: options.schema, maxCommands: options.maxCommands,
		workerConcurrency: options.workerConcurrency, commandLease: options.commandLease,
		queueConcurrency: cloneIntMap(options.queueConcurrency),
		pollInterval:     options.pollInterval, shutdownGrace: options.shutdownGrace,
		notifications: options.notifications,
		instanceID:    uuid.New(),
		observer:      options.observer, observations: newObserverAdapter(options.observer),
		faults: options.faults, lifecycle: runtimeCreated,
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

// TransactionClient joins Flow operations to one caller-owned PostgreSQL
// transaction. Create it exactly once per pgx.Tx, use Flow operations before
// application row locks/writes, and do not use it concurrently or after the
// transaction ends. Flow never commits or rolls back the transaction.
type TransactionClient struct {
	runtime *Runtime
	tx      pgx.Tx
	order   store.LockOrder
}

func (*TransactionClient) flowClient() {}

// InTx returns a transaction-scoped client. Call it once at the transaction
// boundary and thread the returned value through every Flow operation in that
// transaction; repeated calls for the same pgx.Tx create independent order
// guards and are invalid usage.
func (r *Runtime) InTx(tx pgx.Tx) *TransactionClient {
	return &TransactionClient{runtime: r, tx: tx}
}

// BeginApplicationWrites marks the irreversible boundary after which this
// client rejects every Flow write or run-locking operation before issuing SQL.
// It does not execute SQL or prove that the caller has not already taken
// application locks.
func (c *TransactionClient) BeginApplicationWrites() error {
	if c == nil || c.runtime == nil || c.tx == nil {
		return newError(ErrInvalid, "begin", "application writes", "", "transaction client is incomplete")
	}
	if err := c.order.BeginApplicationPhase(); err != nil {
		return newError(ErrInvalidState, "begin", "application writes", "", err.Error())
	}
	return nil
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
	case *TransactionClient:
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

func (c resolvedClient) beforeFlowWrite() error {
	if c.order == nil {
		return nil
	}
	return c.order.BeforeFlowOperation()
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
		if err := c.order.BeforeRun(id); err != nil {
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
	_ = ctx
	if r == nil {
		return
	}
	r.observations.emit(observation)
}

func parseRunID(id RunID) (uuid.UUID, error) {
	parsed, err := uuid.Parse(string(id))
	if err != nil {
		return uuid.Nil, newError(ErrInvalid, "parse", "run", string(id), "invalid identifier")
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
