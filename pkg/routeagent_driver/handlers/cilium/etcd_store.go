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

package cilium

import (
	"context"
	"crypto/tls"
	stderrors "errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	_ "github.com/k3s-io/kine/pkg/drivers/memory" // register memory:// backend
	"github.com/k3s-io/kine/pkg/server"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

const (
	defaultEtcdDialTimeout = 5 * time.Second
	// watchProgressInterval matches kine's default so progress notify jitter is valid.
	watchProgressInterval = 5 * time.Second
	grpcGracefulStopWait  = 2 * time.Second
	loopbackHost          = "127.0.0.1"
)

// EtcdStoreConfig configures an in-memory etcd-compatible ClusterMesh peer (kine).
type EtcdStoreConfig struct {
	ListenClientURL    string
	AdvertiseClientURL string
	CertFile           string
	KeyFile            string
	CAFile             string
}

type etcdStore struct {
	client    EtcdClient
	grpcSrv   *grpc.Server
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func newEtcdStoreWithClient(client EtcdClient) *etcdStore {
	return &etcdStore{client: client}
}

func startEtcdStore(ctx context.Context, cfg *EtcdStoreConfig) (*etcdStore, error) {
	setEtcdStoreDefaults(cfg)

	listenHostPort, endpointScheme, err := listenAddrAndScheme(cfg.ListenClientURL, cfg)
	if err != nil {
		return nil, err
	}

	// Detach from the caller's cancel/deadline: the kvstore outlives Init.
	srvCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	store := &etcdStore{cancel: cancel}

	_, backend, err := drivers.New(srvCtx, &store.wg, &drivers.Config{
		Endpoint: "memory://",
	})
	if err != nil {
		cancel()
		return nil, errors.Wrap(err, "create kine memory backend")
	}

	if err := backend.Start(srvCtx); err != nil {
		cancel()
		return nil, errors.Wrap(err, "start kine memory backend")
	}

	grpcSrv, err := newKineGRPCServer(cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	store.grpcSrv = grpcSrv
	server.New(backend, endpointScheme, watchProgressInterval, "").Register(grpcSrv)

	listener, err := net.Listen("tcp", listenHostPort)
	if err != nil {
		cancel()
		grpcSrv.Stop()

		return nil, errors.Wrap(err, "listen ClusterMesh kvstore")
	}

	store.wg.Go(func() {
		if serveErr := grpcSrv.Serve(listener); serveErr != nil && !stderrors.Is(serveErr, grpc.ErrServerStopped) {
			logger.Errorf(serveErr, "ClusterMesh kvstore gRPC server exited")
		}
	})

	cli, err := newLocalEtcdClient(cfg)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	store.client = newKineEtcdClient(cli)

	// Ensure the server accepts connections before returning.
	readyCtx, readyCancel := context.WithTimeout(ctx, defaultEtcdDialTimeout)
	defer readyCancel()

	if _, err := cli.Get(readyCtx, cmHeartbeatKey); err != nil {
		_ = store.Close()
		return nil, errors.Wrap(err, "ClusterMesh kvstore not ready")
	}

	return store, nil
}

func newKineGRPCServer(cfg *EtcdStoreConfig) (*grpc.Server, error) {
	opts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Hour,
			Timeout: 20 * time.Second,
		}),
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsCfg, err := serverTLSConfig(cfg)
		if err != nil {
			return nil, err
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}

	return grpc.NewServer(opts...), nil
}

func serverTLSConfig(cfg *EtcdStoreConfig) (*tls.Config, error) {
	// Always require client certs: Cilium agents present the ClusterMesh
	// client cert, and the publisher's local client uses the same bundle.
	tlsInfo := transport.TLSInfo{
		CertFile:       cfg.CertFile,
		KeyFile:        cfg.KeyFile,
		TrustedCAFile:  cfg.CAFile,
		ClientCertAuth: true,
	}

	tlsCfg, err := tlsInfo.ServerConfig()
	if err != nil {
		return nil, errors.Wrap(err, "ClusterMesh kvstore server TLS")
	}

	return tlsCfg, nil
}

func newLocalEtcdClient(cfg *EtcdStoreConfig) (*clientv3.Client, error) {
	cCfg := &clientv3.Config{
		Endpoints:   []string{localClientEndpoint(cfg)},
		DialTimeout: defaultEtcdDialTimeout,
	}

	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      cfg.CertFile,
			KeyFile:       cfg.KeyFile,
			TrustedCAFile: cfg.CAFile,
		}

		tlsCfg, err := tlsInfo.ClientConfig()
		if err != nil {
			return nil, errors.Wrap(err, "ClusterMesh kvstore client TLS")
		}

		// Prefer loopback SAN used by publisher certs (127.0.0.1 / localhost).
		tlsCfg.ServerName = "localhost"
		cCfg.TLS = tlsCfg
	}

	cli, err := clientv3.New(*cCfg)
	if err != nil {
		return nil, errors.Wrap(err, "create ClusterMesh kvstore client")
	}

	return cli, nil
}

func localClientEndpoint(cfg *EtcdStoreConfig) string {
	u, err := url.Parse(cfg.ListenClientURL)
	if err != nil {
		return cfg.AdvertiseClientURL
	}

	scheme := u.Scheme
	host := u.Hostname()
	port := u.Port()

	switch host {
	case "0.0.0.0", "", loopbackHost, "localhost":
		host = loopbackHost
	}

	return fmt.Sprintf("%s://%s:%s", scheme, host, port)
}

func listenAddrAndScheme(listenURL string, cfg *EtcdStoreConfig) (string, string, error) {
	u, err := url.Parse(listenURL)
	if err != nil {
		return "", "", errors.Wrap(err, "parse listen client URL")
	}

	if u.Port() == "" {
		return "", "", errors.New("listen client URL must include a port")
	}

	host := u.Hostname()
	if host == "" {
		host = loopbackHost
	}

	scheme := "http"
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		scheme = "https"
	}

	return net.JoinHostPort(host, u.Port()), scheme, nil
}

func setEtcdStoreDefaults(cfg *EtcdStoreConfig) {
	if cfg.ListenClientURL == "" {
		cfg.ListenClientURL = "http://" + loopbackHost + ":12379"
	}

	if cfg.AdvertiseClientURL == "" {
		cfg.AdvertiseClientURL = cfg.ListenClientURL
	}
}

func (s *etcdStore) Bootstrap(ctx context.Context, remoteName string, clusterID uint32) error {
	b, err := marshalClusterConfig(defaultClusterConfig(clusterID))
	if err != nil {
		return errors.Wrap(err, "marshal cluster-config")
	}

	key := clusterConfigKey(remoteName)
	if _, err := s.client.Put(ctx, key, string(b)); err != nil {
		return errors.Wrapf(err, "put cluster-config %q", key)
	}

	return nil
}

func (s *etcdStore) UpsertRoute(ctx context.Context, cidrStr, hostIP string, clusterID uint32) error {
	pair, key, err := buildIPIdentityPair(cidrStr, hostIP, clusterID)
	if err != nil {
		return err
	}

	b, err := marshalIPIdentityPair(pair)
	if err != nil {
		return errors.Wrap(err, "marshal IPIdentityPair")
	}

	if _, err := s.client.Put(ctx, key, string(b)); err != nil {
		return errors.Wrapf(err, "put route %q", key)
	}

	return nil
}

func (s *etcdStore) DeleteRoute(ctx context.Context, cidrStr string) error {
	ip, mask, err := parseCIDR(cidrStr)
	if err != nil {
		return errors.Wrap(err, "parse CIDR")
	}

	key := ipIdentityKey(prefixString(&ipIdentityPair{IP: ip, Mask: mask}))
	if _, err := s.client.Delete(ctx, key); err != nil {
		return errors.Wrapf(err, "delete route %q", key)
	}

	return nil
}

func (s *etcdStore) DeleteClusterConfig(ctx context.Context, remoteName string) error {
	key := clusterConfigKey(remoteName)
	if _, err := s.client.Delete(ctx, key); err != nil {
		return errors.Wrapf(err, "delete cluster-config %q", key)
	}

	return nil
}

func (s *etcdStore) TouchHeartbeat(ctx context.Context) error {
	value := time.Now().UTC().Format(time.RFC3339)
	if _, err := s.client.Put(ctx, cmHeartbeatKey, value); err != nil {
		return errors.Wrapf(err, "put heartbeat %q", cmHeartbeatKey)
	}

	return nil
}

func (s *etcdStore) Close() error {
	var err error

	s.closeOnce.Do(func() {
		err = s.close()
	})

	return err
}

func (s *etcdStore) close() error {
	var errs []error

	if s.client != nil {
		if closeErr := s.client.Close(); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}

	if s.grpcSrv != nil {
		stopped := make(chan struct{})

		go func() {
			s.grpcSrv.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
		case <-time.After(grpcGracefulStopWait):
			s.grpcSrv.Stop()
		}
	}

	if s.cancel != nil {
		s.cancel()
	}

	s.wg.Wait()

	return stderrors.Join(errs...)
}
