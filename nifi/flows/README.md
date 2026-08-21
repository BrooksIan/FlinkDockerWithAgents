# Sample NiFi flow notes for Ratatoskr monitoring demos.
#
# ## Ratatoskr Sample (synthetic)
#   ./scripts/nifi_load_sample_flow.sh
#   GenerateFlowFile → UpdateAttribute → LogAttribute
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
#   Publish a test message from the host:
#     python3 -c "
#     from kafka import KafkaProducer
#     p=KafkaProducer(bootstrap_servers='localhost:9094')
#     p.send('nifi.kafka.demo', b'{\"hello\":\"nifi\"}'); p.flush()
#     "
#
#   Future monitor hooks:
#     - Stop ConsumeKafka → NiFi STOPPED
#     - Delete nifi.kafka.demo → Kafka TOPIC_MISSING + NiFi consumer errors
#     - Stop LogAttribute → connection backlog (NiFi BACKPRESSURE)
#     - Group ratatoskr-nifi-kafka-demo lag → Kafka LAG_*
#
# Fault injection (sample flow):
#   python3 scripts/nifi_fault_inject.py --stop-generate
#   python3 scripts/nifi_fault_inject.py --restore
