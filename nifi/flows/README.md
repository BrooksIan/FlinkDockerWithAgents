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
#   Runbook HITL (docs/NIFI_RUNBOOK.md):
#     python3 scripts/demo_nifi_runbook.py --list
#     python3 scripts/demo_nifi_runbook.py --scenario stop-generate --heal --approve --restore
#     python examples/agents/run_react_nifi_runbook_local.py --fixture stop-generate
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
#   Cross runbook (+ optional HITL → cross-stack heal):
#     python3 scripts/demo_cross_runbook.py --scenario topic-missing
#     python3 scripts/demo_cross_runbook.py --scenario topic-missing --heal --approve
#     python3 scripts/demo_cross_runbook.py --live --inject --heal --approve
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
# ## Ratatoskr Data Plane (schema / route / replay)
#   ./scripts/nifi_load_dataplane_flow.sh
#   Customer POC (propose → ack → apply): docs/CUSTOMER_POC.md
#     python3 scripts/demo_customer_poc.py
#   Guides: docs/SCHEMA_GATE.md · docs/ROUTE_ENRICH.md · docs/REPLAY.md · docs/DATAPLANE_APPROVAL.md
#
#   Guides:
#     docs/NIFI_MONITOR.md — NiFi + orchestrated catalog
#     docs/NIFI_RUNBOOK.md — react_nifi_runbook + HITL
#     docs/KAFKA_MONITOR.md — Kafka heals
#     docs/SIGNAL_CORRELATE.md — cross-topic / cross-lag / cross runbook HITL
#
# Fault injection (sample flow):
#   python3 scripts/nifi_fault_inject.py --stop-generate
#   python3 scripts/nifi_fault_inject.py --restore
