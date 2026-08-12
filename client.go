package flow

// Client is a sealed capability implemented by Runtime and transaction-scoped
// clients. Applications pass it directly to durable operations; they cannot
// construct another implementation that bypasses Flow's durable operations.
type Client interface {
	flowClient()
}

var _ Client = (*TransactionClient)(nil)
