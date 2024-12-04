/*
Copyright 2014 The Kubernetes Authors.

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

package rest

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/runtime"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/pod"

	// ensure types are installed
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// CheckpointREST implements the checkpoint subresource for a Container in a Pod
type CheckpointREST struct {
	Store       *genericregistry.Store
	KubeletConn client.ConnectionInfoGetter
}

// Implement Connecter
var _ = rest.Connecter(&CheckpointREST{})

// New returns an empty ContainerCheckpointOptions object
func (r *CheckpointREST) New() runtime.Object {
	return &api.ContainerCheckpointOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *CheckpointREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// NewConnectOptions returns the versioned object that represents exec parameters
func (r *CheckpointREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.ContainerCheckpointOptions{}, false, ""
}

// ConnectMethods returns the methods supported by exec
func (r *CheckpointREST) ConnectMethods() []string {
	return upgradeableMethods
}

// Connect returns a handler for the pod exec proxy
func (r *CheckpointREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	execOpts, ok := opts.(*api.ContainerCheckpointOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}
	location, transport, err := pod.CheckpointLocation(ctx, r.Store, r.KubeletConn, name, execOpts)
	if err != nil {
		return nil, err
	}
	handler := newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder)
	return handler, nil
}

// NewGetOptions creates a new options object
func (r *CheckpointREST) NewGetOptions() (runtime.Object, bool, string) {
	return &api.ContainerCheckpointOptions{}, false, ""
}
