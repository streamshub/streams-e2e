FROM registry.access.redhat.com/ubi9/openjdk-21:latest

LABEL org.opencontainers.image.source='https://github.com/streamshub/streams-e2e'

LABEL name='streams-e2e' \
    vendor='streamshub' \
    summary='Container image with streams-e2e test suite.' \
    description='Streamshub streams-e2e test suite for running integration test within streams portfolio.'

ENV STREAMS_HOME=/opt/streams-e2e
ENV KUBECONFIG=/opt/kubeconfig/config
ENV OPERATOR_SDK_VERSION=1.41.1
ENV HELM_VERSION=3.17.3

USER root
RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y unzip git bsdtar && microdnf clean all

# Install kubectl, oc, operator-sdk and helm clients
RUN export ARCH=$(case $(uname -m) in x86_64) echo -n amd64 ;; aarch64) echo -n arm64 ;; *) echo -n $(uname -m) ;; esac) && \
    export OS=$(uname | awk '{print tolower($0)}') && \
    export OPERATOR_SDK_DL_URL=https://github.com/operator-framework/operator-sdk/releases/download/v${OPERATOR_SDK_VERSION} && \
    curl -L "https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/openshift-client-linux-${ARCH}-rhel9.tar.gz" -o openshift-client-linux.tar.gz && \
    bsdtar -xzf openshift-client-linux.tar.gz && \
    chmod +x oc kubectl && \
    mv oc /usr/local/bin/ && \
    mv kubectl /usr/local/bin/ && \
    rm -f openshift-client-linux.tar.gz README.md && \
    curl -LO ${OPERATOR_SDK_DL_URL}/operator-sdk_${OS}_${ARCH} && \
    chmod +x operator-sdk_${OS}_${ARCH} && \
    mv operator-sdk_${OS}_${ARCH} /usr/local/bin/operator-sdk && \
    curl -L "https://get.helm.sh/helm-v${HELM_VERSION}-${OS}-${ARCH}.tar.gz" -o helm.tar.gz && \
    bsdtar -xzf helm.tar.gz && \
    mv ${OS}-${ARCH}/helm /usr/local/bin/helm && \
    chmod +x /usr/local/bin/helm && \
    rm -rf helm.tar.gz ${OS}-${ARCH}

RUN mkdir -p /opt/kubeconfig && chown 185:0 /opt/kubeconfig && \
    mkdir -p /opt/streams-e2e && chown -R 185:0 /opt/streams-e2e

# Copy only build definition files first to cache dependency resolution
COPY --chown=185:0 pom.xml mvnw /opt/streams-e2e/
COPY --chown=185:0 .mvn /opt/streams-e2e/.mvn

USER 185

WORKDIR $STREAMS_HOME

# Cache dependencies - only re-runs when pom.xml or wrapper changes
RUN ./mvnw dependency:go-offline -B -q

# Copy full source
COPY --chown=185:0 . /opt/streams-e2e

VOLUME ["/opt/kubeconfig"]
VOLUME ["${STREAMS_HOME}/operator-install-files"]

# Download operator files (generate-sources) + compile main and test in one pass
RUN ./mvnw test-compile -Pget-operator-files -B -q -Dcheckstyle.skip=true

CMD ["./mvnw", "verify", "-Ptest"]
