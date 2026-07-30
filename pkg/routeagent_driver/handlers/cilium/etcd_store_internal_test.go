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
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/submariner-io/submariner/pkg/routeagent_driver/handlers/cilium/fake"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var _ = Describe("etcdStore", func() {
	It("should bootstrap and upsert/delete routes with correct data", func(ctx context.Context) {
		etcdClient := fake.NewEtcdClient()
		store := newEtcdStoreWithClient(etcdClient)

		Expect(store.Bootstrap(ctx, "submariner", 255)).To(Succeed())
		Expect(store.UpsertRoute(ctx, "10.151.0.0/16", "10.0.0.2", 255)).To(Succeed())
		Expect(store.TouchHeartbeat(ctx)).To(Succeed())

		Expect(etcdClient.HasKey(clusterConfigKey("submariner"))).To(BeTrue())

		raw := etcdClient.Value(ipIdentityKey("10.151.0.0/16"))
		Expect(raw).NotTo(BeNil())

		var pair ipIdentityPair
		Expect(json.Unmarshal(raw, &pair)).To(Succeed())
		Expect(pair.HostIP.String()).To(Equal("10.0.0.2"))

		hb := etcdClient.Value(cmHeartbeatKey)
		Expect(hb).NotTo(BeNil())
		_, err := time.Parse(time.RFC3339, string(hb))
		Expect(err).NotTo(HaveOccurred())

		Expect(store.DeleteRoute(ctx, "10.151.0.0/16")).To(Succeed())
		Expect(etcdClient.Value(ipIdentityKey("10.151.0.0/16"))).To(BeNil())
	})

	It("should propagate Put errors from the client", func(ctx context.Context) {
		etcdClient := fake.NewEtcdClient()
		etcdClient.SetPutError(errors.New("put failed"))
		store := newEtcdStoreWithClient(etcdClient)

		Expect(store.Bootstrap(ctx, "submariner", 255)).To(MatchError(ContainSubstring("put failed")))
	})

	It("should release listen ports on Close", func(ctx context.Context) {
		clientPort := freeTCPPort()
		clientURL := fmt.Sprintf("http://127.0.0.1:%d", clientPort)

		store, err := startEtcdStore(ctx, &EtcdStoreConfig{
			ListenClientURL: clientURL,
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(store.Bootstrap(ctx, "submariner", 255)).To(Succeed())
		Expect(store.UpsertRoute(ctx, "10.151.0.0/16", "10.0.0.2", 255)).To(Succeed())

		Expect(store.Close()).To(Succeed())
		Expect(store.Close()).To(Succeed()) // sync.Once — idempotent

		Eventually(func() error {
			return canListen(clientPort)
		}).WithTimeout(5 * time.Second).Should(Succeed())
	})

	It("should fail cleanly when the listen port is already taken", func(ctx context.Context) {
		clientPort := freeTCPPort()

		blocker, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", clientPort))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { _ = blocker.Close() })

		_, err = startEtcdStore(ctx, &EtcdStoreConfig{
			ListenClientURL: fmt.Sprintf("http://127.0.0.1:%d", clientPort),
		})
		Expect(err).To(HaveOccurred())
	})

	It("should bootstrap against a real in-memory kvstore", func(ctx context.Context) {
		clientPort := freeTCPPort()

		store, err := startEtcdStore(ctx, &EtcdStoreConfig{
			ListenClientURL: "http://127.0.0.1:" + strconv.Itoa(clientPort),
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(store.Close()).To(Succeed())
		})

		Expect(store.Bootstrap(ctx, "submariner", 255)).To(Succeed())
		Expect(store.UpsertRoute(ctx, "10.151.0.0/16", "10.0.0.2", 255)).To(Succeed())
		Expect(store.TouchHeartbeat(ctx)).To(Succeed())

		raw, err := store.get(ctx, ipIdentityKey("10.151.0.0/16"))
		Expect(err).NotTo(HaveOccurred())
		Expect(raw).NotTo(BeNil())

		var pair ipIdentityPair
		Expect(json.Unmarshal(raw, &pair)).To(Succeed())
		Expect(pair.HostIP.String()).To(Equal("10.0.0.2"))
	})

	// Cilium ClusterMesh syncs ipcache via ListAndWatch; a Get-only peer is not enough.
	It("should deliver prefix Watch events for route upsert and delete", func(ctx context.Context) {
		clientPort := freeTCPPort()
		clientURL := fmt.Sprintf("http://127.0.0.1:%d", clientPort)

		store, err := startEtcdStore(ctx, &EtcdStoreConfig{
			ListenClientURL: clientURL,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			Expect(store.Close()).To(Succeed())
		})

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{clientURL},
			DialTimeout: 5 * time.Second,
		})
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() {
			_ = cli.Close()
		})

		watchCtx, cancelWatch := context.WithCancel(ctx)
		DeferCleanup(cancelWatch)

		wch := cli.Watch(watchCtx, cmIPStatePrefix+"/", clientv3.WithPrefix())

		var (
			mu     sync.Mutex
			events []*clientv3.Event
		)

		go func() {
			for wr := range wch {
				if wr.Err() != nil {
					return
				}

				mu.Lock()

				events = append(events, wr.Events...)
				mu.Unlock()
			}
		}()

		// Allow the watch stream to attach before mutating keys.
		time.Sleep(200 * time.Millisecond)

		const (
			routeCIDR = "10.151.0.0/16"
			hostIP    = "10.0.0.2"
		)

		routeKey := ipIdentityKey(routeCIDR)

		Expect(store.UpsertRoute(ctx, routeCIDR, hostIP, 255)).To(Succeed())

		Eventually(func(g Gomega) {
			mu.Lock()
			defer mu.Unlock()

			g.Expect(findWatchEvent(events, mvccpb.PUT, routeKey)).NotTo(BeNil())
		}).WithTimeout(5 * time.Second).Should(Succeed())

		Expect(store.DeleteRoute(ctx, routeCIDR)).To(Succeed())

		Eventually(func(g Gomega) {
			mu.Lock()
			defer mu.Unlock()

			g.Expect(findWatchEvent(events, mvccpb.DELETE, routeKey)).NotTo(BeNil())
		}).WithTimeout(5 * time.Second).Should(Succeed())
	})
})

func findWatchEvent(events []*clientv3.Event, typ mvccpb.Event_EventType, key string) *clientv3.Event {
	for _, ev := range events {
		if ev == nil || ev.Type != typ {
			continue
		}

		if ev.Kv != nil && string(ev.Kv.Key) == key {
			return ev
		}

		if ev.PrevKv != nil && string(ev.PrevKv.Key) == key {
			return ev
		}
	}

	return nil
}

func freeTCPPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func canListen(port int) error {
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}

	return l.Close()
}
