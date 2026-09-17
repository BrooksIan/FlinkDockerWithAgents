# Build: ratatoskr build [git-ref]
# Produces agent_flink_image:latest with PyFlink + Flink Agents Python wheel.
# Python 3.12 requires Flink 2.1+ (Flink Agents 0.3+). See:
# https://nightlies.apache.org/flink/flink-agents-docs-release-0.3/docs/get-started/installation/
ARG FLINK_AGENTS_VERSION=release-0.3
ARG FLINK_BASE_IMAGE=flink:2.1-java11
ARG FLINK_MAJOR_MINOR=2.1

FROM ${FLINK_BASE_IMAGE}

ARG FLINK_AGENTS_VERSION
ARG FLINK_MAJOR_MINOR
ENV FLINK_AGENTS_VERSION=${FLINK_AGENTS_VERSION}
ENV FLINK_MAJOR_MINOR=${FLINK_MAJOR_MINOR}
ENV PYTHONPATH=/opt/flink:/opt/flink/pythonpath/agent-site-packages:/opt/flink/opt/python/pyflink:/opt/flink/opt/python/py4j

USER root
# Ubuntu 22.04 (Jammy) Flink images ship without Python 3.12 — install via deadsnakes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        software-properties-common gnupg ca-certificates \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        python3.12 \
        python3.12-venv \
        python3.12-dev \
        libpython3.12 \
        libpython3.12-dev \
        g++ gcc git curl bash maven openjdk-11-jdk unzip \
    && ln -sfn "$(ls -d /usr/lib/jvm/java-11-openjdk-* | head -1)" /opt/java-11 \
    && ln -sfn /usr/bin/python3.12 /usr/bin/python3 \
    && ln -sfn /usr/bin/python3.12 /usr/bin/python \
    && curl -fsSL https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3.12 /tmp/get-pip.py \
    && rm -f /tmp/get-pip.py \
    && python3 --version \
    && mkdir -p /opt/flink/opt/python/pyflink /opt/flink/opt/python/py4j \
    && unzip -q -o /opt/flink/opt/python/pyflink.zip -d /opt/flink/opt/python/pyflink \
    && unzip -q -o /opt/flink/opt/python/py4j-0.10.9.7-src.zip -d /opt/flink/opt/python/py4j \
    && ln -sf /opt/flink/opt/python/py4j-0.10.9.7-src.zip /opt/flink/opt/python/py4j-src.zip \
    && chmod +x /opt/flink/opt/python/pyflink/pyflink/bin/pyflink-udf-runner.sh \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/flink
ENV JAVA_HOME=/opt/java-11
ENV SKIP_SPOTLESS_CHECK=true
# Python 3.12: constrain setuptools in install + build isolation (setuptools>=82 drops pkg_resources).
RUN git clone --depth 1 --branch "${FLINK_AGENTS_VERSION}" https://github.com/apache/flink-agents.git /tmp/flink-agents \
    && cd /tmp/flink-agents \
    && bash ./tools/build.sh \
    && mkdir -p /opt/flink/pythonpath/agent-site-packages \
    && printf 'setuptools>=75.3,<82\n' > /tmp/pip-constraint.txt \
    && export PIP_CONSTRAINT=/tmp/pip-constraint.txt \
    && export PIP_BUILD_CONSTRAINT=/tmp/pip-constraint.txt \
    && python3 -m pip install --upgrade 'pip' 'setuptools>=75.3,<82' 'wheel' \
    && python3 -m pip install --target=/opt/flink/pythonpath/agent-site-packages \
        'numpy>=1.22.4,<2' \
        'pyarrow>=5.0.0,<21.0.0' \
        'apache-beam>=2.54.0,<=2.61.0' \
        'pemja>=0.6.0,<0.7.0' \
        'setuptools>=75.3,<82' \
        'avro-python3>=1.10.0,<1.12.0' \
        'requests>=2.32.0' \
        'kafka-python>=2.0.2' \
        'streamlit>=1.28.0' \
        'pandas>=2.0.0' \
        'plotly>=5.17.0' \
        'openai>=1.0.0' \
        'ipwhois>=1.2.0' \
        'python-dotenv>=1.0.0' \
        ./python/dist/*.whl \
        ruamel.yaml \
    && cp dist/common/target/flink-agents-dist-common-*.jar /opt/flink/lib/ \
    && cp dist/flink-${FLINK_MAJOR_MINOR}/target/flink-agents-dist-flink-${FLINK_MAJOR_MINOR}-*-thin.jar /opt/flink/lib/ \
    && rm -rf /tmp/flink-agents /tmp/pip-constraint.txt

# Pemja (pemja.core.object.PyObject/PyIterator) lives in flink-python. It must be
# loaded by a single, stable parent classloader; otherwise concurrent user-code
# ChildFirstClassLoaders (PyFlink task + flink-agents action operator) each load
# their own copy and casts fail with ClassCastException (FLINK-39226). Placing the
# jar in lib/ + classloader.parent-first-patterns.additional=pemja gives one identity.
RUN cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/

COPY examples/demo_datastream.py examples/demo_table.py examples/demo_datastream_local.py /opt/flink/
COPY examples/agents /opt/flink/examples/agents
COPY ratatoskr/__init__.py ratatoskr/constants.py ratatoskr/paths.py ratatoskr/docker_utils.py ratatoskr/kafka_sources.py /opt/flink/ratatoskr/
COPY ratatoskr/agents/__init__.py ratatoskr/agents/published_copy.py /opt/flink/ratatoskr/agents/
COPY ratatoskr/nifi /opt/flink/ratatoskr/nifi
COPY ratatoskr/kafka /opt/flink/ratatoskr/kafka
COPY ratatoskr/runtime /opt/flink/ratatoskr/runtime
RUN chown -R flink:flink /opt/flink/examples /opt/flink/ratatoskr /opt/flink/pythonpath

USER flink
