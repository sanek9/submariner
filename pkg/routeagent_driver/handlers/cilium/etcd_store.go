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
	stderrors "errors"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/submariner-io/submariner/pkg/routeagent_driver/handlers/cilium/minietcd"
)

const (
	grpcGracefulStopWait = 2 * time.Second
	loopbackHost         = "127.0.0.1"
)

// EtcdStoreConfig configures an in-memory etcd-compatible ClusterMesh peer.
type EtcdStoreConfig struct {
	ListenClientURL string
	CertFile        string
	KeyFile         string
	CAFile          string
}

type etcdStore struct {
	client    EtcdClient
	mem       *minietcd.Store
	server    *minietcd.Server
	closeOnce sync.Once
}

func newEtcdStoreWithClient(client EtcdClient) *etcdStore {
	return &etcdStore{client: client}
}

func startEtcdStore(_ context.Context, cfg *EtcdStoreConfig) (*etcdStore, error) {
	setEtcdStoreDefaults(cfg)

	listenHostPort, err := listenHostPort(cfg.ListenClientURL)
	if err != nil {
		return nil, err
	}

	var tlsCfg *minietcd.TLSConfig
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsCfg = &minietcd.TLSConfig{
			CertFile: cfg.CertFile,
			KeyFile:  cfg.KeyFile,
			CAFile:   cfg.CAFile,
		}
	}

	srv, mem, err := minietcd.ListenAndServe(listenHostPort, tlsCfg)
	if err != nil {
		return nil, errors.Wrap(err, "start ClusterMesh kvstore")
	}

	return &etcdStore{
		mem:    mem,
		server: srv,
	}, nil
}

func listenHostPort(listenURL string) (string, error) {
	u, err := url.Parse(listenURL)
	if err != nil {
		return "", errors.Wrap(err, "parse listen client URL")
	}

	if u.Port() == "" {
		return "", errors.New("listen client URL must include a port")
	}

	host := u.Hostname()
	if host == "" {
		host = loopbackHost
	}

	return net.JoinHostPort(host, u.Port()), nil
}

func setEtcdStoreDefaults(cfg *EtcdStoreConfig) {
	if cfg.ListenClientURL == "" {
		cfg.ListenClientURL = "http://" + loopbackHost + ":12379"
	}
}

func (s *etcdStore) put(ctx context.Context, key, val string) error {
	if s.client != nil {
		return errors.Wrap(s.client.Put(ctx, key, val), "put")
	}

	s.mem.Put(key, []byte(val))

	return nil
}

func (s *etcdStore) get(ctx context.Context, key string) ([]byte, error) {
	if s.client != nil {
		v, err := s.client.Get(ctx, key)

		return v, errors.Wrap(err, "get")
	}

	v, _, ok := s.mem.Get(key)
	if !ok {
		return nil, nil
	}

	return v, nil
}

func (s *etcdStore) delete(ctx context.Context, key string) error {
	if s.client != nil {
		return errors.Wrap(s.client.Delete(ctx, key), "delete")
	}

	s.mem.Delete(key)

	return nil
}

func (s *etcdStore) Bootstrap(ctx context.Context, remoteName string, clusterID uint32) error {
	b, err := marshalClusterConfig(defaultClusterConfig(clusterID))
	if err != nil {
		return errors.Wrap(err, "marshal cluster-config")
	}

	key := clusterConfigKey(remoteName)
	if err := s.put(ctx, key, string(b)); err != nil {
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

	if err := s.put(ctx, key, string(b)); err != nil {
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
	if err := s.delete(ctx, key); err != nil {
		return errors.Wrapf(err, "delete route %q", key)
	}

	return nil
}

func (s *etcdStore) DeleteClusterConfig(ctx context.Context, remoteName string) error {
	key := clusterConfigKey(remoteName)
	if err := s.delete(ctx, key); err != nil {
		return errors.Wrapf(err, "delete cluster-config %q", key)
	}

	return nil
}

func (s *etcdStore) TouchHeartbeat(ctx context.Context) error {
	value := time.Now().UTC().Format(time.RFC3339)
	if err := s.put(ctx, cmHeartbeatKey, value); err != nil {
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

	if s.server != nil {
		stopped := make(chan struct{})

		go func() {
			_ = s.server.Close()
			close(stopped)
		}()

		select {
		case <-stopped:
		case <-time.After(grpcGracefulStopWait):
		}
	}

	return stderrors.Join(errs...)
}
