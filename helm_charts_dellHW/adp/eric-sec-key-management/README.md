# Key Management Helm Chart

The Key Management Helm chart deploys Vault to the Kubernetes cluster
and by default automatically initializes and unseals it for application to use.

## Prerequisites Details

* Kubernetes API, from version 1.9
* Helm and Tiller, from version 2.9
* Service Identity Provider for TLS, from version 1.0.0
* Distributed Coordinator ED, from version 1.0.0

## Chart Details

The Key Management Helm chart will do the following:

* Install a Vault deployment
* Initialization and configuration of Vault
* Automatic unseal, which also monitors Vault unseal status during operation

## Execution of the Chart

    helm repo add kms https://repo_containing_key_management_service
    helm repo update
    helm install kms/eric-sec-key-management <options>

When selecting the options, carefully read the below sub-chapters.

### Secure connection

Vault needs to be started with TLS certificates in order to be operational.

If Service Identity Provider for TLS is part of the deployment flow,
it can handle the certificates for you. Use the following flag:

    --set service.tls.enabled=true

Alternatively, for quick testing (DO NOT USE IN PRODUCTION):

Use the flag:

    --set service.tls.enabled=false

This will disable TLS and expose Vault under insecure http interface.

### Persistence

(Not recommended; not production-grade; high-availability not possible)
To deploy the Key Management for test or demo purposes with
file-based backend using the default storage class, use:

    --set persistence.type=pvc

(Recommended)
To deploy the Key Management with Distributed Coordinator ED, then
configure Key Management to use it as a backend:

    --set persistence.type=etcd, this is set by default
    --set persistence.etcd.serviceName=<etcd-service-name, default: eric-data-distributed-coordinator-ed>
    --set persistence.etcd.servicePort=<etcd-service-port, default: 2379>

## Configuration

Please see values.yaml

