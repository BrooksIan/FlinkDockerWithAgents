# Build: apemosyne build [git-ref]
# Produces agent_flink_image:latest with PyFlink + Flink Agents Python wheel.
ARG FLINK_AGENTS_VERSION=release-0.3

FROM flink:1.20-java11

ARG FLINK_AGENTS_VERSION
ENV FLINK_AGENTS_VERSION=${FLINK_AGENTS_VERSION}
ENV PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-pip python3-venv python3-dev \
        libpython3.10 libpython3.10-dev \
        g++ gcc git curl bash maven openjdk-11-jdk unzip \
    && ln -sfn "$(ls -d /usr/lib/jvm/java-11-openjdk-* | head -1)" /opt/java-11 \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && mkdir -p /opt/flink/opt/python/pyflink /opt/flink/opt/python/py4j \
    && unzip -q -o /opt/flink/opt/python/pyflink.zip -d /opt/flink/opt/python/pyflink \
    && unzip -q -o /opt/flink/opt/python/py4j-0.10.9.7-src.zip -d /opt/flink/opt/python/py4j \
    && ln -sf /opt/flink/opt/python/py4j-0.10.9.7-src.zip /opt/flink/opt/python/py4j-src.zip \
    && chmod +x /opt/flink/opt/python/pyflink/pyflink/bin/pyflink-udf-runner.sh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink
ENV JAVA_HOME=/opt/java-11
ENV SKIP_SPOTLESS_CHECK=true
RUN git clone --depth 1 --branch "${FLINK_AGENTS_VERSION}" https://github.com/apache/flink-agents.git /tmp/flink-agents \
    && cd /tmp/flink-agents \
    && bash ./tools/build.sh \
    && mkdir -p /opt/flink/pythonpath/agent-site-packages \
    && python3 -m pip install --target=/opt/flink/pythonpath/agent-site-packages \
        'numpy>=1.22.4,<2' \
        'pyarrow>=5.0.0,<16.0.0' \
        'apache-beam>=2.43.0,<2.49.0' \
        'grpcio-tools>=1.29.0,<=1.51.3' \
        'pemja>=0.6.0,<0.7.0' \
        'setuptools>=75.3,<82' \
        'avro-python3>=1.10.0,<1.12.0' \
        ./python/dist/*.whl \
        ruamel.yaml \
    && cp dist/common/target/flink-agents-dist-common-*.jar /opt/flink/lib/ \
    && rm -rf /tmp/flink-agents

COPY examples/demo_datastream.py examples/demo_table.py examples/demo_datastream_local.py /opt/flink/
COPY examples/agents /opt/flink/examples/agents
COPY apemosyne/__init__.py apemosyne/constants.py apemosyne/paths.py apemosyne/docker_utils.py apemosyne/kafka_sources.py /opt/flink/apemosyne/
COPY apemosyne/agents/__init__.py apemosyne/agents/published_copy.py /opt/flink/apemosyne/agents/
COPY apemosyne/runtime /opt/flink/apemosyne/runtime
RUN chown -R flink:flink /opt/flink/examples /opt/flink/apemosyne /opt/flink/pythonpath

USER flink
