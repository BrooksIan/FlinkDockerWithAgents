# Sample NiFi flow notes for Ratatoskr monitoring demos.
#
# Preferred bootstrap (creates processors via REST):
#   ./scripts/nifi_load_sample_flow.sh
#
# Flow shape:
#   GenerateFlowFile → UpdateAttribute (ratatoskr.demo=true) → LogAttribute
#
# Fault injection (after sample exists):
#   python3 scripts/nifi_fault_inject.py --stop-generate
#   python3 scripts/nifi_fault_inject.py --restore
