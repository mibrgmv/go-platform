package outbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mibrgmv/go-platform/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	postgrestest "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupOutboxDB(t *testing.T) (*pgxpool.Pool, func()) {
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
		create table if not exists outbox_events (
			event_id varchar(255) primary key,
			event_type varchar(100) not null,
			topic varchar(255) not null,
			payload jsonb not null,
			status varchar(50) not null default 'pending',
			retry_count integer not null default 0,
			max_retries integer not null default 5,
			error_message text,
			created_at timestamptz not null default now(),
			published_at timestamptz,
			updated_at timestamptz not null default now(),
			next_retry_at timestamptz
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

func addEvent(t *testing.T, pool *pgxpool.Pool, id, eventType, topic string, payload []byte) {
	_, err := pool.Exec(context.Background(), `
		insert into outbox_events (event_id, event_type, topic, payload, max_retries)
		values ($1, $2, $3, $4, 5)
	`, id, eventType, topic, payload)
	require.NoError(t, err)
}

func TestGetPendingEvents(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	addEvent(t, pool, "evt-1", "type_a", "topic.a", []byte(`{"k":"v"}`))
	addEvent(t, pool, "evt-2", "type_b", "topic.b", []byte(`{"k":"v2"}`))

	events, err := repo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 2)

	assert.Equal(t, "evt-1", events[0].EventID)
	assert.Equal(t, outbox.EventStatusPending, events[0].Status)
}

func TestGetPendingEvents_RespectsLimit(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		addEvent(t, pool, "evt-"+string(rune('a'+i)), "type_a", "topic.a", []byte(`{}`))
	}

	events, err := repo.GetPendingEvents(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, events, 2)
}

func TestGetPendingEvents_ExcludesNonPending(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	addEvent(t, pool, "evt-pending", "type_a", "topic.a", []byte(`{}`))

	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status)
		values ('evt-published', 'type_a', 'topic.a', '{}', 'published')
	`)
	require.NoError(t, err)

	events, err := repo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "evt-pending", events[0].EventID)
}

func TestLockEventForProcessing(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	addEvent(t, pool, "evt-lock", "type_a", "topic.a", []byte(`{"data":1}`))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	locked, err := repo.LockEventForProcessing(ctx, tx, "evt-lock")
	require.NoError(t, err)
	require.NotNil(t, locked)

	assert.Equal(t, "evt-lock", locked.EventID)
	assert.Equal(t, "type_a", locked.EventType)
	assert.JSONEq(t, `{"data":1}`, string(locked.RawPayload))
}

func TestLockEventForProcessing_ReturnsNilForPublished(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status)
		values ('evt-done', 'type_a', 'topic.a', '{}', 'published')
	`)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	locked, err := repo.LockEventForProcessing(ctx, tx, "evt-done")
	require.NoError(t, err)
	assert.Nil(t, locked)
}

func TestLockEventForProcessing_ReturnsNilForExhaustedRetries(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, retry_count, max_retries)
		values ('evt-exhausted', 'type_a', 'topic.a', '{}', 'failed', 5, 5)
	`)
	require.NoError(t, err)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	locked, err := repo.LockEventForProcessing(ctx, tx, "evt-exhausted")
	require.NoError(t, err)
	assert.Nil(t, locked)
}

func TestMarkEventAsPublishedTx(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	addEvent(t, pool, "evt-pub", "type_a", "topic.a", []byte(`{}`))

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = repo.MarkEventAsPublishedTx(ctx, tx, "evt-pub")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	events, err := repo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 0)
}

func TestMarkEventAsFailedTx(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	addEvent(t, pool, "evt-fail", "type_a", "topic.a", []byte(`{}`))

	nextRetry := time.Now().Add(time.Minute)

	tx, err := pool.Begin(ctx)
	require.NoError(t, err)

	err = repo.MarkEventAsFailedTx(ctx, tx, "evt-fail", "something broke", nextRetry)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Should not be in pending anymore
	pending, err := repo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 0)

	// Should not be in retry yet (next_retry_at is in the future)
	retry, err := repo.GetRetryEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, retry, 0)
}

func TestGetRetryEvents(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	pastTime := time.Now().Add(-time.Minute)
	futureTime := time.Now().Add(time.Hour)

	// Ready for retry: failed, next_retry_at in the past, retries left
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, retry_count, max_retries, next_retry_at)
		values ('evt-ready', 'type_a', 'topic.a', '{}', 'failed', 1, 5, $1)
	`, pastTime)
	require.NoError(t, err)

	// Not ready: next_retry_at in the future
	_, err = pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, retry_count, max_retries, next_retry_at)
		values ('evt-future', 'type_a', 'topic.a', '{}', 'failed', 1, 5, $1)
	`, futureTime)
	require.NoError(t, err)

	// Not ready: retries exhausted
	_, err = pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, retry_count, max_retries, next_retry_at)
		values ('evt-exhausted', 'type_a', 'topic.a', '{}', 'failed', 5, 5, $1)
	`, pastTime)
	require.NoError(t, err)

	events, err := repo.GetRetryEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "evt-ready", events[0].EventID)
}

func TestCleanupOldEvents(t *testing.T) {
	t.Parallel()

	pool, cleanup := setupOutboxDB(t)
	defer cleanup()

	repo := outbox.NewPostgresRepository(pool)
	ctx := context.Background()

	oldTime := time.Now().Add(-48 * time.Hour)

	// Old published event — should be deleted
	_, err := pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, published_at)
		values ('evt-old-pub', 'type_a', 'topic.a', '{}', 'published', $1)
	`, oldTime)
	require.NoError(t, err)

	// Old failed event with exhausted retries — should be deleted
	_, err = pool.Exec(ctx, `
		insert into outbox_events (event_id, event_type, topic, payload, status, retry_count, max_retries, created_at)
		values ('evt-old-fail', 'type_a', 'topic.a', '{}', 'failed', 5, 5, $1)
	`, oldTime)
	require.NoError(t, err)

	// Recent pending event — should remain
	addEvent(t, pool, "evt-recent", "type_a", "topic.a", []byte(`{}`))

	err = repo.CleanupOldEvents(ctx, 1)
	require.NoError(t, err)

	pending, err := repo.GetPendingEvents(ctx, 10)
	require.NoError(t, err)
	assert.Len(t, pending, 1)
	assert.Equal(t, "evt-recent", pending[0].EventID)
}
