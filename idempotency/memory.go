package idempotency

import (
	"context"
	"sync"
	"time"

	"github.com/Ocean-Gaming/platform-go/tenant"
)

// MemoryStore is an in-process Store. It exists so unit tests can prove the
// replay and fingerprint rules with no database and no Docker.
//
// It deliberately does NOT model transactionality — that is the one property
// that cannot be proven without a real database. See the integration tests.
type MemoryStore struct {
	mu   sync.Mutex
	recs map[tenant.ID]map[Key]Record
	now  func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{recs: map[tenant.ID]map[Key]Record{}, now: time.Now}
}

func (s *MemoryStore) Claim(ctx context.Context, key Key, fingerprint string) (Record, bool, error) {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return Record{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	byTenant, ok := s.recs[tid]
	if !ok {
		byTenant = map[Key]Record{}
		s.recs[tid] = byTenant
	}

	if existing, ok := byTenant[key]; ok {
		if existing.Fingerprint != fingerprint {
			return Record{}, false, ErrFingerprintMismatch
		}
		if existing.State == StateInFlight {
			return Record{}, false, ErrInFlight
		}
		return existing, true, nil
	}

	rec := Record{
		Tenant:      tid,
		Key:         key,
		Fingerprint: fingerprint,
		State:       StateInFlight,
		CreatedAt:   s.now(),
	}
	byTenant[key] = rec
	return rec, false, nil
}

func (s *MemoryStore) Complete(ctx context.Context, key Key, response []byte) error {
	tid, err := tenant.Require(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	byTenant, ok := s.recs[tid]
	if !ok {
		return ErrNotClaimed
	}
	rec, ok := byTenant[key]
	if !ok {
		return ErrNotClaimed
	}
	rec.State = StateCompleted
	rec.Response = append([]byte(nil), response...)
	rec.CompletedAt = s.now()
	byTenant[key] = rec
	return nil
}
