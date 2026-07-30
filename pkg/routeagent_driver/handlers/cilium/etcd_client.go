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

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EtcdClient is the subset of clientv3.Client used by etcdStore.
// *clientv3.Client does not satisfy Delete against kine (DeleteRange is
// unimplemented); use newKineEtcdClient for the live kvstore.
type EtcdClient interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Close() error
}

// kineEtcdClient adapts clientv3 to kine's etcd API subset. Kine rejects
// DeleteRange and expects deletes as a Txn of [Range, DeleteRange] (see
// kine pkg/server/delete.go isDelete).
type kineEtcdClient struct {
	cli *clientv3.Client
}

func newKineEtcdClient(cli *clientv3.Client) *kineEtcdClient {
	return &kineEtcdClient{cli: cli}
}

func (c *kineEtcdClient) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	resp, err := c.cli.Put(ctx, key, val, opts...)
	return resp, errors.Wrap(err, "kine put")
}

func (c *kineEtcdClient) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := c.cli.Get(ctx, key, opts...)
	return resp, errors.Wrap(err, "kine get")
}

func (c *kineEtcdClient) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	txn, err := c.cli.Txn(ctx).Then(clientv3.OpGet(key), clientv3.OpDelete(key, opts...)).Commit()
	if err != nil {
		return nil, errors.Wrap(err, "kine delete txn")
	}

	if !txn.Succeeded {
		// Key already absent — treat like a successful no-op delete.
		return &clientv3.DeleteResponse{Header: txn.Header}, nil
	}

	for _, resp := range txn.Responses {
		if del := resp.GetResponseDeleteRange(); del != nil {
			return &clientv3.DeleteResponse{
				Header:  del.Header,
				Deleted: del.Deleted,
				PrevKvs: del.PrevKvs,
			}, nil
		}
	}

	return &clientv3.DeleteResponse{Header: txn.Header}, nil
}

func (c *kineEtcdClient) Close() error {
	return errors.Wrap(c.cli.Close(), "close kine client")
}
