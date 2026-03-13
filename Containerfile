FROM --platform=$BUILDPLATFORM registry.access.redhat.com/ubi9/ubi-minimal:latest AS tools
ARG TARGETARCH
ARG OPERATOR_SDK_VERSION=1.41.1

RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y tar gzip && microdnf clean all

RUN curl -L "https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/openshift-client-linux-${TARGETARCH}-rhel9.tar.gz" -o oc.tar.gz && \
    tar -xzf oc.tar.gz oc kubectl && \
    chmod +x oc kubectl && \
    curl -LO "https://github.com/operator-framework/operator-sdk/releases/download/v${OPERATOR_SDK_VERSION}/operator-sdk_linux_${TARGETARCH}" && \
    chmod +x operator-sdk_linux_${TARGETARCH} && \
    mv operator-sdk_linux_${TARGETARCH} operator-sdk && \
    curl -fsSL "https://get.helm.sh/helm-v3.17.1-linux-${TARGETARCH}.tar.gz" -o helm.tar.gz && \
    tar -xzf helm.tar.gz "linux-${TARGETARCH}/helm" && \
    mv "linux-${TARGETARCH}/helm" helm

FROM registry.access.redhat.com/ubi9/openjdk-21:latest

LABEL org.opencontainers.image.source='https://github.com/streamshub/streams-e2e'

LABEL name='streams-e2e' \
    vendor='streamshub' \
    summary='Container image with streams-e2e test suite.' \
    description='Streamshub streams-e2e test suite for running integration test within streams portfolio.'

ENV STREAMS_HOME=/opt/streams-e2e
ENV KUBECONFIG=/opt/kubeconfig/config

COPY . /opt/streams-e2e

USER root
RUN microdnf --setopt=install_weak_deps=0 --setopt=tsflags=nodocs install -y unzip git && microdnf clean all

COPY --from=tools /oc /kubectl /operator-sdk /helm /usr/local/bin/

RUN mkdir -p /opt/kubeconfig && chown 185:0 /opt/kubeconfig && \
    chown -R 185:0 /opt/streams-e2e && chmod +x /opt/streams-e2e/mvnw

USER 185

WORKDIR $STREAMS_HOME

VOLUME ["/opt/kubeconfig"]
VOLUME ["${STREAMS_HOME}/operator-install-files"]

RUN ./mvnw dependency:go-offline -B -q \
    && ./mvnw install -Pget-operator-files \
    && ./mvnw compile test-compile -B -q -Dcheckstyle.skip=true

CMD ["./mvnw", "verify", "-Ptest"]