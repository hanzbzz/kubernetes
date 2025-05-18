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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/kubernetes"
	clientRest "k8s.io/client-go/rest"
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
var _ = rest.NamedCreater(&CheckpointREST{})

// New returns an empty ContainerCheckpointOptions object
func (r *CheckpointREST) New() runtime.Object {
	return &api.ContainerCheckpointOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *CheckpointREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// get secret via API
func getSecret(namespace string, name string) (*v1.Secret, error) {
	config := clientRest.Config{
		Host: "https://127.0.0.1:6443",
		TLSClientConfig: clientRest.TLSClientConfig{
			Insecure: false,
			CertFile: "/etc/kubernetes/pki/apiserver-kubelet-client.crt",
			KeyFile:  "/etc/kubernetes/pki/apiserver-kubelet-client.key",
			CAFile:   "/etc/kubernetes/pki/ca.crt",
		},
	}
	kubeClient, err := kubernetes.NewForConfig(&config)
	if err != nil {
		return nil, err
	}
	secret, err := kubeClient.CoreV1().Secrets(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return secret, nil
}

func (r *CheckpointREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	opts := obj.(*api.ContainerCheckpointOptions)
	if opts.LeaveRunning == nil {
		defaultTrue := true
		opts.LeaveRunning = &defaultTrue
	}
	if opts.Encrypt == nil {
		defaultTrue := true
		opts.Encrypt = &defaultTrue
	}
	location, transport, container, namespace, err := pod.CheckpointLocation(ctx, r.Store, r.KubeletConn, name, opts)
	if err != nil {
		return nil, err
	}

	if *opts.Encrypt {
		if opts.EncryptionSecret == "" {
			return &metav1.Status{
				Status:  metav1.StatusFailure,
				Message: "encryptionSecret must be provided",
				Code:    http.StatusBadRequest,
			}, nil
		}
		secret, err := getSecret(namespace, opts.EncryptionSecret)
		if err != nil {
			return &metav1.Status{
				Status:  metav1.StatusFailure,
				Message: fmt.Sprintf("Error while trying to get secret %v: %v", opts.EncryptionSecret, err.Error()),
				Code:    http.StatusNotFound,
			}, nil
		}
		if secret.Type != v1.SecretTypeTLS {
			return &metav1.Status{
				Status:  metav1.StatusFailure,
				Message: fmt.Sprintf("encryptionSecret expected type: TLS, got: %v", secret.Type),
				Code:    http.StatusBadRequest,
			}, nil
		}
		opts.EncryptionCert = string(secret.Data["tls.crt"])
	}

	// Marshal the object into JSON
	jsonData, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", location.String(), bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Transport: transport,
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var responseData *api.ContainerCheckpointResponse
	if err := json.NewDecoder(resp.Body).Decode(&responseData); err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusCreated {
		return &metav1.Status{
			Status:  metav1.StatusFailure,
			Message: responseData.Message,
			Code:    int32(resp.StatusCode),
		}, nil
	}
	details := metav1.StatusDetails{Kind: responseData.Location, Name: responseData.Node}
	if !*opts.LeaveRunning {
		r.Store.Delete(ctx, name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
	}
	return &metav1.Status{
		Status:  metav1.StatusSuccess,
		Message: fmt.Sprintf("Checkpoint of container %s succesfull", container),
		Details: &details,
		Code:    http.StatusCreated,
	}, nil
}
