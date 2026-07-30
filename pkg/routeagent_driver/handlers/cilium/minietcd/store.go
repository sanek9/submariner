/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package minietcd is a tiny in-memory etcd v3 gRPC peer for Cilium ClusterMesh.
// It implements the KV (Range/Put/DeleteRange) and Watch services needed by
// Cilium's ListAndWatch path, without raft, WAL, or SQL.
package minietcd

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const (
	clusterID = uint64(1)
	memberID  = uint64(1)
)

type kvEntry struct {
	value          []byte
	createRevision int64
	modRevision    int64
	version        int64
}

// Store is an in-memory revisioned key-value map with prefix watches.
type Store struct {
	mu       sync.RWMutex
	data     map[string]kvEntry
	rev      atomic.Int64
	watchers map[int64]*watcher
	nextWID  atomic.Int64
}

type watcher struct {
	id        int64
	key       []byte
	end       []byte
	startRev  int64
	ch        chan []*mvccpb.Event
	done      chan struct{}
	closeOnce sync.Once
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		data:     make(map[string]kvEntry),
		watchers: make(map[int64]*watcher),
	}
}

func (s *Store) header(rev int64) *etcdserverpb.ResponseHeader {
	if rev == 0 {
		rev = s.rev.Load()
	}

	return &etcdserverpb.ResponseHeader{
		ClusterId: clusterID,
		MemberId:  memberID,
		Revision:  rev,
		RaftTerm:  1,
	}
}

func (s *Store) nextRev() int64 {
	return s.rev.Add(1)
}

// Put writes a key and notifies watchers.
func (s *Store) Put(key string, value []byte) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	rev := s.nextRev()
	prev, had := s.data[key]

	ent := kvEntry{
		value:          append([]byte(nil), value...),
		createRevision: rev,
		modRevision:    rev,
		version:        1,
	}
	if had {
		ent.createRevision = prev.createRevision
		ent.version = prev.version + 1
	}

	s.data[key] = ent

	ev := &mvccpb.Event{
		Type: mvccpb.PUT,
		Kv:   entryToKV(key, ent),
	}
	if had {
		ev.PrevKv = entryToKV(key, prev)
	}

	s.broadcastLocked([]*mvccpb.Event{ev})

	return rev
}

// Delete removes a key (no-op if absent) and notifies watchers.
func (s *Store) Delete(key string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prev, had := s.data[key]
	if !had {
		return s.rev.Load(), false
	}

	rev := s.nextRev()
	delete(s.data, key)

	ev := &mvccpb.Event{
		Type:   mvccpb.DELETE,
		Kv:     &mvccpb.KeyValue{Key: []byte(key), ModRevision: rev},
		PrevKv: entryToKV(key, prev),
	}
	s.broadcastLocked([]*mvccpb.Event{ev})

	return rev, true
}

// Get returns the value for key, or nil if missing.
func (s *Store) Get(key string) ([]byte, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rev := s.rev.Load()

	ent, ok := s.data[key]
	if !ok {
		return nil, rev, false
	}

	return append([]byte(nil), ent.value...), rev, true
}

// Range lists keys in [key, end). Empty end means a single-key get.
func (s *Store) Range(key, end []byte, limit int64) ([]*mvccpb.KeyValue, int64, int64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rev := s.rev.Load()
	keyStr := string(key)

	if len(end) == 0 {
		if ent, ok := s.data[keyStr]; ok {
			return []*mvccpb.KeyValue{entryToKV(keyStr, ent)}, rev, 1
		}

		return nil, rev, 0
	}

	var (
		kvs   []*mvccpb.KeyValue
		count int64
	)

	for k, ent := range s.data {
		if !keyInRange(k, key, end) {
			continue
		}

		count++

		if limit > 0 && int64(len(kvs)) >= limit {
			continue
		}

		kvs = append(kvs, entryToKV(k, ent))
	}

	sort.Slice(kvs, func(i, j int) bool {
		return bytes.Compare(kvs[i].Key, kvs[j].Key) < 0
	})

	return kvs, rev, count
}

func (s *Store) broadcastLocked(events []*mvccpb.Event) {
	for _, w := range s.watchers {
		matched := filterEvents(events, w.key, w.end)
		if len(matched) == 0 {
			continue
		}

		select {
		case w.ch <- matched:
		default:
			// Slow consumer: drop; Cilium will relist on disruption.
		}
	}
}

func (s *Store) addWatcher(key, end []byte, startRev int64) *watcher {
	w := &watcher{
		id:       s.nextWID.Add(1),
		key:      append([]byte(nil), key...),
		end:      append([]byte(nil), end...),
		startRev: startRev,
		ch:       make(chan []*mvccpb.Event, 64),
		done:     make(chan struct{}),
	}

	s.mu.Lock()
	s.watchers[w.id] = w
	s.mu.Unlock()

	return w
}

func (s *Store) removeWatcher(id int64) {
	s.mu.Lock()

	w, ok := s.watchers[id]
	if ok {
		delete(s.watchers, id)
	}

	s.mu.Unlock()

	if ok {
		w.closeOnce.Do(func() { close(w.done) })
	}
}

func entryToKV(key string, ent kvEntry) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:            []byte(key),
		Value:          append([]byte(nil), ent.value...),
		CreateRevision: ent.createRevision,
		ModRevision:    ent.modRevision,
		Version:        ent.version,
	}
}

func keyInRange(key string, start, end []byte) bool {
	kb := []byte(key)
	if bytes.Compare(kb, start) < 0 {
		return false
	}

	if len(end) == 0 {
		return bytes.Equal(kb, start)
	}

	return bytes.Compare(kb, end) < 0
}

func filterEvents(events []*mvccpb.Event, start, end []byte) []*mvccpb.Event {
	var out []*mvccpb.Event

	for _, ev := range events {
		if ev == nil || ev.Kv == nil {
			continue
		}

		if keyInRange(string(ev.Kv.Key), start, end) {
			out = append(out, ev)
		}
	}

	return out
}
