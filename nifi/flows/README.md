# Sample NiFi flow notes for Ratatoskr monitoring demos.
#
# ## Ratatoskr Sample (synthetic)
#   ./scripts/nifi_load_sample_flow.sh
#   GenerateFlowFile → UpdateAttribute → LogAttribute
#
#   Heal examples (docs/NIFI_MONITOR.md):
#     python3 scripts/nifi_fault_inject.py --stop-generate
#     export NIFI_HEAL_PHASE=safe
#     python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
#
#     python3 scripts/nifi_fault_inject.py --invalid-log
#     export NIFI_HEAL_PHASE=lab
#     python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
#
#     python3 scripts/nifi_fault_inject.py --queue-backlog --settle-sec 5
#     export NIFI_HEAL_PHASE=lab NIFI_HEAL_ALLOW_EMPTY_QUEUE=1
#     python3 examples/agents/run_workflow_nifi_monitor_local.py --count 1
#
# ## Ratatoskr Kafka Demo (shared NiFi + Kafka base)
#   Prerequisites: `ratatoskr kafka up` then `ratatoskr up --profile nifi`
#   (NiFi joins `ratatoskr-kafka_kafka-network` so bootstrap `kafka:9092` works.)
#
#   ./scripts/nifi_load_kafka_flow.sh
#
#   Flow:
#     Kafka3ConnectionService "Studio Kafka"
#     ConsumeKafka (topic nifi.kafka.demo, group ratatoskr-nifi-kafka-demo)
#       → UpdateAttribute (ratatoskr.demo=nifi-kafka)
#       → LogAttribute
#
#   Smoke:
#     python3 test/test_nifi_kafka_demo.py          # offline (CI-safe)
#     python3 scripts/smoke_nifi_kafka_demo.py      # live publish → ConsumeKafka
#
#   Heal demo catalog (break → monitor → heal):
#     python3 scripts/demo_nifi_kafka_heal.py --list
#     python3 scripts/demo_nifi_kafka_heal.py --scenario stop-consume
#     python3 scripts/demo_nifi_kafka_heal.py --scenario disable-cs
#     python3 scripts/demo_nifi_kafka_heal.py --scenario invalid-log
#     python3 scripts/demo_nifi_kafka_heal.py --scenario queue-backlog
#     python3 scripts/demo_nifi_kafka_heal.py --scenario delete-topic
#     python3 scripts/demo_nifi_kafka_heal.py --scenario increase-partitions
#     python3 scripts/demo_nifi_kafka_heal.py --scenario lag-group
#     python3 scripts/demo_nifi_kafka_heal.py --scenario lag-earliest
#     python3 scripts/demo_nifi_kafka_heal.py --scenario cross-topic
#     python3 scripts/demo_nifi_kafka_heal.py --scenario cross-lag
#     python3 scripts/demo_nifi_kafka_heal.py --all
#
#   Fault inject (kafka target):
#     python3 scripts/nifi_fault_inject.py --target kafka --stop-consume
#     python3 scripts/nifi_fault_inject.py --target kafka --disable-cs
#     python3 scripts/nifi_fault_inject.py --target kafka --kafka-invalid-log
#     python3 scripts/nifi_fault_inject.py --target kafka --stop-log
#     python3 scripts/nifi_fault_inject.py --target kafka --restore
#
#   Publish a test message from the host:
#     python3 -c "
#     from kafka import KafkaProducer
#     p=KafkaProducer(bootstrap_servers='localhost:9094')
#     p.send('nifi.kafka.demo', b'{\"hello\":\"nifi\"}'); p.flush()
#     "
#
#   Guides:
#     docs/NIFI_MONITOR.md — NiFi + orchestrated catalog
#     docs/KAFKA_MONITOR.md — Kafka heals
#     docs/SIGNAL_CORRELATE.md — cross-topic / cross-lag
#
# Fault injection (sample flow):
#   python3 scripts/nifi_fault_inject.py --stop-generate
#   python3 scripts/nifi_fault_inject.py --restore
