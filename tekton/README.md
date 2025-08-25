# Tekton Pipeline for Streams E2E Tests

This directory contains Tekton resources for running the streams e2e tests using the containerized test suite.

## Resources

- **pipeline.yaml**: Main Tekton Pipeline definition
- **task.yaml**: Tekton Task that runs the tests using the container image
- **pipelinerun.yaml**: Example PipelineRun to execute the pipeline

## Prerequisites

1. Tekton Pipelines installed in your Kubernetes cluster
2. Access to the container image `quay.io/streamshub/streams-e2e:latest`
3. Sufficient cluster resources (CPU, memory, storage)
4. Kubeconfig secret created for cluster access during tests

## Usage

### 1. Create the kubeconfig secret

```bash
# Create secret from your kubeconfig file
kubectl create secret generic kubeconfig-secret --from-file=config=$HOME/.kube/config

# Or apply the template and edit it with your kubeconfig
kubectl apply -f tekton/kubeconfig-secret.yaml
```

### 2. Install the Tekton resources

```bash
kubectl apply -f tekton/task.yaml
kubectl apply -f tekton/pipeline.yaml
```

### 3. Run the pipeline

(Optional) Update and apply configmap with test configuration.

```bash
kubectl apply -f tekton/config-configmap.yaml
```

Run pipeline

```bash
kubectl apply -f tekton/pipelinerun.yaml
```

Or use tkn cli

```bash
  tkn pipeline start streams-e2e-tests \
    --name streams-e2e-tests-run \
    --param config-configmap=streams-e2e-config \
    --param kubeconfig-secret=your-kubeconfig-secret
```

### 4. Monitor the pipeline run

```bash
# Watch the pipeline run status
kubectl get pipelinerun streams-e2e-tests-run -w

# Get logs from the pipeline run
tkn pipelinerun logs streams-e2e-tests-run -f
```

## Parameters

The pipeline accepts the following parameters:

- **test-image**: Container image containing the test suite (default: `quay.io/streamshub/streams-e2e:latest`)
- **test-namespace**: Kubernetes namespace for running tests (default: `default`)
- **kubeconfig-secret**: Name of the secret containing kubeconfig for cluster access
- **streams-e2e-config**: Name of the configmap containing config.yaml (optional)
- **test**: Specific integration test class or method to run (optional, for -Dit.test parameter)
- **groups**: Test groups to run (optional, for -Dgroups parameter)

## Customization

### Running specific tests

To run specific test classes or methods:

```yaml
params:
  - name: test
    value: "SqlExampleST"
```

To run specific test groups:

```yaml
params:
  - name: groups
    value: "smoke"
```

### Custom container image

```yaml
params:
  - name: test-image
    value: "my-registry/streams-e2e:my-tag"
```

### Using a different kubeconfig secret

```yaml
params:
  - name: kubeconfig-secret
    value: "my-kubeconfig-secret"
```

### Using a test config configmap

```yaml
params:
  - name: config-configmap
    value: "streams-e2e-config"
```

## Kubeconfig Requirements

The kubeconfig secret must contain a valid Kubernetes configuration that allows the test container to:

- Access the target Kubernetes cluster where tests will be executed
- Have appropriate permissions for creating/managing test resources
- Connect to the cluster (network access, authentication, etc.)

The kubeconfig is mounted at `/opt/kubeconfig/config` inside the test container and used by the test suite to interact with the Kubernetes cluster.

## Test Results

Test results and logs are stored in the workspace volume and can be retrieved after the pipeline completes.

## Troubleshooting

- Check pipeline run status: `kubectl describe pipelinerun streams-e2e-tests-run`
- View task logs: `tkn taskrun logs <taskrun-name> -f`
- Check resource usage: `kubectl top pods`
- Verify kubeconfig secret: `kubectl get secret kubeconfig-secret -o yaml`
- Check if kubeconfig is valid: `kubectl --kubeconfig=<your-config> cluster-info`