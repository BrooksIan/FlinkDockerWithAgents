# Build: apemosyne build [git-ref]
# Produces agent_flink_image:latest with PyFlink + Flink Agents Python wheel.
ARG FLINK_AGENTS_VERSION=release-0.2.1

FROM flink:1.20-java11

ARG FLINK_AGENTS_VERSION
ENV FLINK_AGENTS_VERSION=${FLINK_AGENTS_VERSION}
ENV PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-src.zip

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv git curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink
RUN git clone --depth 1 --branch "${FLINK_AGENTS_VERSION}" https://github.com/apache/flink-agents.git /tmp/flink-agents \
    && cd /tmp/flink-agents \
    && ./tools/build.sh \
    && python3 -m pip install --break-system-packages ./python/dist/*.whl \
    && rm -rf /tmp/flink-agents

COPY examples/demo_datastream.py examples/demo_table.py examples/demo_datastream_local.py /opt/flink/
COPY examples/agents /opt/flink/examples/agents
COPY apemosyne/runtime /opt/flink/apemosyne/runtime
COPY apemosyne/__init__.py /opt/flink/apemosyne/__init__.py

USER flink
