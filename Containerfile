FROM registry.access.redhat.com/ubi9/openjdk-17:1.22

LABEL org.opencontainers.image.source='https://github.com/streamshub/streams-e2e'

LABEL name='streams-e2e' \
    vendor='streamshub' \
    summary='Container image with streams-e2e test suite.' \
    description='Streamshub streams-e2e test suite for running integration test within streams portfolio.'

ENV STREAMS_HOME=/opt/streams-e2e
ENV KUBECONFIG=/opt/kubeconfig/config
ENV OPERATOR_SDK_VERSION=1.41.1

COPY . /opt/streams-e2e

USER root
RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y unzip git && microdnf clean all

# Install kubectl, oc, operator-sdk and helm3 clients
RUN export ARCH=$(case $(uname -m) in x86_64) echo -n amd64 ;; aarch64) echo -n arm64 ;; *) echo -n $(uname -m) ;; esac) && \
    export OS=$(uname | awk '{print tolower($0)}') && \
    export OPERATOR_SDK_DL_URL=https://github.com/operator-framework/operator-sdk/releases/download/v${OPERATOR_SDK_VERSION} && \
    curl -L "https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/openshift-client-linux-${ARCH}-rhel9.tar.gz" -o openshift-client-linux.tar.gz && \
    tar -xzf openshift-client-linux.tar.gz && \
    chmod +x oc kubectl && \
    mv oc /usr/local/bin/ && \
    mv kubectl /usr/local/bin/ && \
    rm -f openshift-client-linux.tar.gz README.md && \
    curl -LO ${OPERATOR_SDK_DL_URL}/operator-sdk_${OS}_${ARCH} && \
    chmod +x operator-sdk_${OS}_${ARCH} && \
    mv operator-sdk_${OS}_${ARCH} /usr/local/bin/operator-sdk  && \
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh


RUN mkdir -p /opt/kubeconfig && chown 185:0 /opt/kubeconfig && \
    chown -R 185:0 /opt/streams-e2e && chmod +x /opt/streams-e2e/mvnw

USER 185

WORKDIR $STREAMS_HOME

VOLUME ["/opt/kubeconfig"]
VOLUME ["${STREAMS_HOME}/operator-install-files"]
V

RUN ./mvnw dependency:go-offline -B -q \
    && ./mvnw install -Pget-operator-files \
    && ./mvnw compile test-compile -B -q -Dcheckstyle.skip=true

CMD ["./mvnw", "verify", "-Ptest"]
