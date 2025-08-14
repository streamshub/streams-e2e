FROM registry.access.redhat.com/ubi9/openjdk-17:1.22

LABEL org.opencontainers.image.source='https://github.com/streamshub/streams-e2e'

LABEL name='streams-e2e' \
    vendor='streamshub' \
    summary='Container image with streams-e2e test suite.' \
    description='Streamshub streams-e2e test suite for running integration test within streams portfolio.'

ENV STREAMS_HOME=/opt/streams-e2e
ENV KUBECONFIG=/opt/kubeconfig/config

COPY . /opt/streams-e2e

USER root
RUN microdnf install -y unzip git && microdnf clean all
RUN mkdir -p /opt/kubeconfig && chown 185:0 /opt/kubeconfig
RUN chown -R 185:0 /opt/streams-e2e && chmod +x /opt/streams-e2e/mvnw

USER 185

WORKDIR $STREAMS_HOME

VOLUME ["/opt/kubeconfig"]

RUN ./mvnw dependency:go-offline -B -q \
    && ./mvnw compile test-compile -B -q -Dcheckstyle.skip=true

CMD ["./mvnw", "verify", "-Ptest"]
