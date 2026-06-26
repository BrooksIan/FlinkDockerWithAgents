# Build: apemosyne build [git-ref]
# Produces agent_flink_image:latest with PyFlink + Flink Agents Python wheel.
ARG FLINK_AGENTS_VERSION=release-0.3

FROM flink:1.20-java11

ARG FLINK_AGENTS_VERSION
ENV FLINK_AGENTS_VERSION=${FLINK_AGENTS_VERSION}
ENV PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv git curl bash maven openjdk-11-jdk \
    && ln -sfn "$(ls -d /usr/lib/jvm/java-11-openjdk-* | head -1)" /opt/java-11 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink
ENV JAVA_HOME=/opt/java-11
ENV SKIP_SPOTLESS_CHECK=true
RUN git clone --depth 1 --branch "${FLINK_AGENTS_VERSION}" https://github.com/apache/flink-agents.git /tmp/flink-agents \
    && cd /tmp/flink-agents \
    && bash ./tools/build.sh \
    && python3 -m pip install --break-system-packages ./python/dist/*.whl \
    && rm -rf /tmp/flink-agents

COPY examples/demo_datastream.py examples/demo_table.py examples/demo_datastream_local.py /opt/flink/
COPY examples/agents /opt/flink/examples/agents
COPY apemosyne/__init__.py apemosyne/constants.py apemosyne/paths.py apemosyne/docker_utils.py apemosyne/kafka_sources.py /opt/flink/apemosyne/
COPY apemosyne/runtime /opt/flink/apemosyne/runtime

USER flink
