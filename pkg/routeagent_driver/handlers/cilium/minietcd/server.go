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

package minietcd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Server is a minimal etcd-compatible gRPC peer backed by Store.
type Server struct {
	etcdserverpb.UnimplementedKVServer
	etcdserverpb.UnimplementedWatchServer

	store   *Store
	grpcSrv *grpc.Server
	wg      sync.WaitGroup
}

// TLSConfig holds PEM paths for mTLS (client cert required).
type TLSConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

// ListenAndServe starts the gRPC server on listenAddr (host:port).
func ListenAndServe(listenAddr string, tlsCfg *TLSConfig) (*Server, *Store, error) {
	store := NewStore()
	s := &Server{store: store}

	opts, err := grpcServerOptions(tlsCfg)
	if err != nil {
		return nil, nil, err
	}

	s.grpcSrv = grpc.NewServer(opts...)
	etcdserverpb.RegisterKVServer(s.grpcSrv, s)
	etcdserverpb.RegisterWatchServer(s.grpcSrv, s)

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, errors.Wrap(err, "listen")
	}

	s.wg.Go(func() {
		_ = s.grpcSrv.Serve(lis)
	})

	return s, store, nil
}

// Store returns the in-process backend used by the local publisher.
func (s *Server) Store() *Store {
	return s.store
}

// Close stops the gRPC server.
func (s *Server) Close() error {
	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
	}

	s.wg.Wait()

	return nil
}

func grpcServerOptions(tlsCfg *TLSConfig) ([]grpc.ServerOption, error) {
	opts := make([]grpc.ServerOption, 0, 2)
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: false,
	}))

	if tlsCfg == nil || tlsCfg.CertFile == "" || tlsCfg.KeyFile == "" {
		return opts, nil
	}

	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load server keypair")
	}

	clientCAs := x509.NewCertPool()

	if tlsCfg.CAFile != "" {
		pemBytes, err := os.ReadFile(tlsCfg.CAFile)
		if err != nil {
			return nil, errors.Wrap(err, "read CA")
		}

		if !clientCAs.AppendCertsFromPEM(pemBytes) {
			return nil, errors.New("append CA certs")
		}
	}

	creds := credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    clientCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	})

	return append(opts, grpc.Creds(creds)), nil
}

// Range implements etcd KV.Range.
func (s *Server) Range(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	kvs, rev, count := s.store.Range(req.Key, req.RangeEnd, req.Limit)

	return &etcdserverpb.RangeResponse{
		Header: s.store.header(rev),
		Kvs:    kvs,
		Count:  count,
	}, nil
}

// Put implements etcd KV.Put.
func (s *Server) Put(_ context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	rev := s.store.Put(string(req.Key), req.Value)

	return &etcdserverpb.PutResponse{
		Header: s.store.header(rev),
	}, nil
}

// DeleteRange implements etcd KV.DeleteRange (single key or prefix range).
func (s *Server) DeleteRange(_ context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if len(req.RangeEnd) == 0 {
		rev, ok := s.store.Delete(string(req.Key))

		deleted := int64(0)
		if ok {
			deleted = 1
		}

		return &etcdserverpb.DeleteRangeResponse{
			Header:  s.store.header(rev),
			Deleted: deleted,
		}, nil
	}

	kvs, _, _ := s.store.Range(req.Key, req.RangeEnd, 0)

	var (
		deleted int64
		rev     int64
	)

	for _, kv := range kvs {
		r, ok := s.store.Delete(string(kv.Key))
		rev = r

		if ok {
			deleted++
		}
	}

	if rev == 0 {
		rev = s.store.rev.Load()
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header:  s.store.header(rev),
		Deleted: deleted,
	}, nil
}

// Txn implements a small subset used by clientv3 deletes and simple writes.
func (s *Server) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	ops := req.Success
	resps := make([]*etcdserverpb.ResponseOp, 0, len(ops))

	var rev int64

	for _, op := range ops {
		respOp, r, err := s.applyTxnOp(ctx, op)
		if err != nil {
			return nil, err
		}

		rev = r

		resps = append(resps, respOp)
	}

	return &etcdserverpb.TxnResponse{
		Header:    s.store.header(rev),
		Succeeded: true,
		Responses: resps,
	}, nil
}

func (s *Server) applyTxnOp(ctx context.Context, op *etcdserverpb.RequestOp) (*etcdserverpb.ResponseOp, int64, error) {
	switch {
	case op.GetRequestRange() != nil:
		r, err := s.Range(ctx, op.GetRequestRange())
		if err != nil {
			return nil, 0, err
		}

		return &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: r},
		}, r.Header.Revision, nil

	case op.GetRequestDeleteRange() != nil:
		r, err := s.DeleteRange(ctx, op.GetRequestDeleteRange())
		if err != nil {
			return nil, 0, err
		}

		return &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: r},
		}, r.Header.Revision, nil

	case op.GetRequestPut() != nil:
		r, err := s.Put(ctx, op.GetRequestPut())
		if err != nil {
			return nil, 0, err
		}

		return &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: r},
		}, r.Header.Revision, nil
	}

	return &etcdserverpb.ResponseOp{}, s.store.rev.Load(), nil
}

// Compact acknowledges compaction without retaining history beyond the live map.
func (s *Server) Compact(_ context.Context, req *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	return &etcdserverpb.CompactionResponse{
		Header: s.store.header(req.Revision),
	}, nil
}

// Watch implements etcd Watch bi-di stream.
func (s *Server) Watch(stream etcdserverpb.Watch_WatchServer) error {
	actives := map[int64]*watcher{}
	ctx := stream.Context()

	defer func() {
		for _, w := range actives {
			s.store.removeWatcher(w.id)
		}
	}()

	var sendMu sync.Mutex

	send := func(resp *etcdserverpb.WatchResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()

		return errors.Wrap(stream.Send(resp), "watch send")
	}

	for {
		req, err := stream.Recv()
		if err != nil {
			return errors.Wrap(err, "watch recv")
		}

		if err := s.handleWatchRequest(ctx, req, actives, send); err != nil {
			return err
		}
	}
}

func (s *Server) handleWatchRequest(
	ctx context.Context,
	req *etcdserverpb.WatchRequest,
	actives map[int64]*watcher,
	send func(*etcdserverpb.WatchResponse) error,
) error {
	if cr := req.GetCancelRequest(); cr != nil {
		if w, ok := actives[cr.WatchId]; ok {
			s.store.removeWatcher(w.id)
			delete(actives, cr.WatchId)
		}

		return nil
	}

	cr := req.GetCreateRequest()
	if cr == nil {
		return nil
	}

	w := s.store.addWatcher(cr.Key, cr.RangeEnd, cr.StartRevision)

	watchID := cr.WatchId
	if watchID == 0 {
		watchID = w.id
	}

	actives[watchID] = w

	if err := send(&etcdserverpb.WatchResponse{
		Header:  s.store.header(0),
		WatchId: watchID,
		Created: true,
	}); err != nil {
		return err
	}

	go s.forwardWatch(ctx, watchID, w, send)

	return nil
}

func (s *Server) forwardWatch(
	ctx context.Context,
	watchID int64,
	w *watcher,
	send func(*etcdserverpb.WatchResponse) error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.done:
			return
		case batch, ok := <-w.ch:
			if !ok {
				return
			}

			events := filterWatchBatch(batch, w.startRev)
			if len(events) == 0 {
				continue
			}

			_ = send(&etcdserverpb.WatchResponse{
				Header:  s.store.header(0),
				WatchId: watchID,
				Events:  events,
			})
		}
	}
}

func filterWatchBatch(batch []*mvccpb.Event, startRev int64) []*mvccpb.Event {
	var events []*mvccpb.Event

	for _, ev := range batch {
		if startRev > 0 && ev.Kv != nil && ev.Kv.ModRevision < startRev {
			continue
		}

		events = append(events, ev)
	}

	return events
}
