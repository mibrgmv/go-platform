package inbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/inbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupInboxDB(t *testing.T) (*pgxpool.Pool, func()) {
	ctx := context.Background()

	pgContainer, err := postgrestest.Run(ctx,
		"postgres:16-alpine",
		postgrestest.WithDatabase("testdb"),
		postgrestest.WithUsername("testuser"),
		postgrestest.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second)),
	)
	require.NoError(t, err)

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.New(ctx, connStr)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		create table if not exists processed_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			source_service varchar(100) not null,
			processed_at timestamptz not null default now()
		);
	`)
	require.NoError(t, err)

	cleanup := func() {
		pool.Close()
		if err := pgContainer.Terminate(ctx); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}

	return pool, cleanup
}

func TestMarkEventProcessedTx(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupInboxDB(t)
	defer cleanup()

	repo := inbox.NewPostgresRepository(pool, "test_service")
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = repo.MarkEventProcessedTx(ctx, tx, "evt-1", "order_created")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	processed, err := repo.IsEventProcessed(ctx, "evt-1")
	require.NoError(t, err)
	assert.True(t, processed)
}

func TestMarkEventProcessedTx_Duplicate(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupInboxDB(t)
	defer cleanup()

	repo := inbox.NewPostgresRepository(pool, "test_service")
	ctx := context.Background()

	tx1, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.MarkEventProcessedTx(ctx, tx1, "evt-dup", "order_created")
	require.NoError(t, err)
	require.NoError(t, tx1.Commit(ctx))

	tx2, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.MarkEventProcessedTx(ctx, tx2, "evt-dup", "order_created")
	assert.ErrorIs(t, err, inbox.ErrEventAlreadyProcessed)
	require.NoError(t, tx2.Rollback(ctx))
}

func TestIsEventProcessed_NotFound(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupInboxDB(t)
	defer cleanup()

	repo := inbox.NewPostgresRepository(pool, "test_service")
	ctx := context.Background()

	processed, err := repo.IsEventProcessed(ctx, "nonexistent")
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestMarkEventProcessedTx_UsesServiceName(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupInboxDB(t)
	defer cleanup()

	repo := inbox.NewPostgresRepository(pool, "my_service")
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = repo.MarkEventProcessedTx(ctx, tx, "evt-svc", "order_created")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var sourceSvc string
	err = pool.QueryRow(ctx, `select source_service from processed_events where event_id = 'evt-svc'`).Scan(&sourceSvc)
	require.NoError(t, err)
	assert.Equal(t, "my_service", sourceSvc)
}

func TestCleanupOldEvents(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupInboxDB(t)
	defer cleanup()

	repo := inbox.NewPostgresRepository(pool, "test_service")
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into processed_events (event_id, event_type, source_service, processed_at)
		values ('evt-old', 'type_a', 'test_service', now() - interval '48 hours')
	`)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	err = repo.MarkEventProcessedTx(ctx, tx, "evt-recent", "type_a")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	err = repo.CleanupOldEvents(ctx, 1)
	require.NoError(t, err)

	processed, err := repo.IsEventProcessed(ctx, "evt-old")
	require.NoError(t, err)
	assert.False(t, processed)

	processed, err = repo.IsEventProcessed(ctx, "evt-recent")
	require.NoError(t, err)
	assert.True(t, processed)
}
