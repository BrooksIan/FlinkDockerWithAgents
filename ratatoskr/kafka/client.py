"""Kafka admin client for monitoring / healing agents."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Optional

from ratatoskr.kafka.env import (
    catalog_mode,
    default_partitions,
    default_replication_factor,
    flag_unexpected_topics,
    lag_crit_threshold,
    lag_warn_threshold,
    matches_watch,
    probe_slow_ms,
)
from ratatoskr.kafka_sources import (
    STUDIO_CATALOG_TOPICS,
    _STATIC_TOPICS,
    kafka_bootstrap_servers,
    known_pipeline_topics,
    resolve_host_bootstrap,
    topic_description,
)


def canonical_topic_catalog() -> dict[str, dict[str, Any]]:
    """Expected topics → partitions / RF / description.

    Default ``KAFKA_CATALOG=studio`` matches ``ratatoskr kafka up`` (no cowrie.*).
    Set ``KAFKA_CATALOG=full`` when the honeypot broker / full profile is in use.
    """
    parts = default_partitions()
    rf = default_replication_factor()
    mode = catalog_mode()
    if mode == "studio":
        names = sorted(STUDIO_CATALOG_TOPICS)
    else:
        names = list(dict.fromkeys([*known_pipeline_topics(), *_STATIC_TOPICS.keys()]))
    catalog: dict[str, dict[str, Any]] = {}
    for name in names:
        if not name or name.startswith("__"):
            continue
        catalog[name] = {
            "partitions": parts,
            "replication_factor": rf,
            "description": topic_description(name),
        }
    return catalog


@dataclass
class KafkaClient:
    """Thin admin wrapper — bootstrap via Studio Kafka / env."""

    bootstrap: str = ""
    client_id: str = "ratatoskr-kafka-monitor"
    request_timeout_ms: int = 5000
    mutations: list[dict[str, Any]] = field(default_factory=list, repr=False)
    _admin: Any = field(default=None, repr=False)
    _last_metadata_ms: float = field(default=0.0, repr=False)

    def __post_init__(self) -> None:
        if not self.bootstrap:
            self.bootstrap = (
                resolve_host_bootstrap(timeout_ms=min(self.request_timeout_ms, 2000))
                or kafka_bootstrap_servers()
            )

    def _get_admin(self) -> Any:
        if self._admin is not None:
            return self._admin
        from kafka.admin import KafkaAdminClient

        self._admin = KafkaAdminClient(
            bootstrap_servers=self.bootstrap,
            client_id=self.client_id,
            request_timeout_ms=self.request_timeout_ms,
        )
        return self._admin

    def close(self) -> None:
        if self._admin is not None:
            try:
                self._admin.close()
            except Exception:  # noqa: BLE001
                pass
            self._admin = None

    def probe(self) -> dict[str, Any]:
        """Metadata round-trip timing."""
        t0 = time.perf_counter()
        try:
            admin = self._get_admin()
            topics = set(admin.list_topics())
            ms = (time.perf_counter() - t0) * 1000.0
            self._last_metadata_ms = ms
            return {
                "ok": True,
                "bootstrap": self.bootstrap,
                "metadata_ms": round(ms, 2),
                "topic_count": len(topics),
            }
        except Exception as exc:  # noqa: BLE001
            ms = (time.perf_counter() - t0) * 1000.0
            self._last_metadata_ms = ms
            return {
                "ok": False,
                "bootstrap": self.bootstrap,
                "metadata_ms": round(ms, 2),
                "error": str(exc),
            }

    def list_topics(self) -> set[str]:
        admin = self._get_admin()
        return {t for t in admin.list_topics() if t and not str(t).startswith("__")}

    def describe_topics(self, names: list[str] | None = None) -> list[dict[str, Any]]:
        admin = self._get_admin()
        try:
            metas = admin.describe_topics(names)
        except Exception:  # noqa: BLE001
            return []
        out: list[dict[str, Any]] = []
        for meta in metas or []:
            topic = getattr(meta, "topic", None) or getattr(meta, "name", None)
            if topic is None and isinstance(meta, dict):
                topic = meta.get("topic") or meta.get("name")
            partitions_raw = getattr(meta, "partitions", None)
            if partitions_raw is None and isinstance(meta, dict):
                partitions_raw = meta.get("partitions") or []
            partitions: list[dict[str, Any]] = []
            under_replicated = 0
            offline = 0
            no_leader = 0
            for p in partitions_raw or []:
                if isinstance(p, dict):
                    pid = p.get("partition")
                    leader = p.get("leader")
                    replicas = list(p.get("replicas") or [])
                    isr = list(p.get("isr") or [])
                else:
                    pid = getattr(p, "partition", getattr(p, "partition_id", None))
                    leader = getattr(p, "leader", -1)
                    replicas = list(getattr(p, "replicas", []) or [])
                    isr = list(getattr(p, "isr", []) or [])
                if leader is None or int(leader) < 0:
                    no_leader += 1
                    offline += 1
                if replicas and len(isr) < len(replicas):
                    under_replicated += 1
                partitions.append(
                    {
                        "partition": pid,
                        "leader": leader,
                        "replicas": replicas,
                        "isr": isr,
                    }
                )
            out.append(
                {
                    "name": topic,
                    "partitions": partitions,
                    "partition_count": len(partitions),
                    "under_replicated": under_replicated,
                    "offline_partitions": offline,
                    "no_leader": no_leader,
                }
            )
        return out

    def list_consumer_groups(self) -> list[str]:
        admin = self._get_admin()
        try:
            groups = admin.list_consumer_groups()
        except Exception:  # noqa: BLE001
            return []
        names: list[str] = []
        for g in groups or []:
            if isinstance(g, tuple):
                names.append(str(g[0]))
            elif isinstance(g, str):
                names.append(g)
            else:
                gid = getattr(g, "group_id", None) or getattr(g, "group", None)
                if gid:
                    names.append(str(gid))
        return names

    def describe_consumer_groups(self, group_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not group_ids:
            return {}
        admin = self._get_admin()
        try:
            descs = admin.describe_consumer_groups(group_ids)
        except Exception:  # noqa: BLE001
            return {}
        out: dict[str, dict[str, Any]] = {}
        for d in descs or []:
            if isinstance(d, dict):
                gid = str(d.get("group_id") or d.get("group") or "")
                members = d.get("members") or []
            else:
                gid = str(getattr(d, "group_id", "") or getattr(d, "group", "") or "")
                members = list(getattr(d, "members", []) or [])
            if gid:
                out[gid] = {"group_id": gid, "members": len(members)}
        return out

    def consumer_group_lag(self, group_id: str) -> dict[str, Any]:
        """Total lag for a group (committed vs log end)."""
        from kafka import KafkaConsumer

        admin = self._get_admin()
        try:
            committed = admin.list_consumer_group_offsets(group_id) or {}
        except Exception as exc:  # noqa: BLE001
            return {
                "group_id": group_id,
                "lag": 0,
                "partitions": [],
                "error": str(exc),
            }

        if not committed:
            return {"group_id": group_id, "lag": 0, "partitions": []}

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            client_id=f"{self.client_id}-lag",
            enable_auto_commit=False,
            consumer_timeout_ms=1000,
        )
        try:
            tps = list(committed.keys())
            end_offsets = consumer.end_offsets(tps)
            parts: list[dict[str, Any]] = []
            total = 0
            for tp in tps:
                meta = committed[tp]
                committed_off = int(getattr(meta, "offset", meta) if not isinstance(meta, int) else meta)
                end = int(end_offsets.get(tp, committed_off))
                lag = max(0, end - committed_off)
                total += lag
                parts.append(
                    {
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "committed": committed_off,
                        "end": end,
                        "lag": lag,
                    }
                )
            return {"group_id": group_id, "lag": total, "partitions": parts}
        finally:
            consumer.close()

    def get_cluster_health_status(self) -> dict[str, Any]:
        """Comprehensive broker health snapshot for monitoring agents."""
        poll_t0 = time.perf_counter()
        probe = self.probe()
        if not probe.get("ok"):
            return {
                "bootstrap": self.bootstrap,
                "healthy": False,
                "severities": ["BROKER_UNREACHABLE"],
                "probe": probe,
                "missing_topics": [],
                "unexpected_topics": [],
                "topic_details": [],
                "under_replicated_topics": [],
                "offline_partitions": [],
                "undersized_topics": [],
                "oversized_topics": [],
                "consumer_groups": [],
                "lag_warn_groups": [],
                "lag_crit_groups": [],
                "stalled_groups": [],
                "empty_lagging_groups": [],
                "catalog": {},
                "counts": {},
            }

        catalog = canonical_topic_catalog()
        live = self.list_topics()
        catalog_names = set(catalog.keys())

        missing = sorted(
            n for n in catalog_names if n not in live and matches_watch(n)
        )
        unexpected = sorted(
            n for n in live if n not in catalog_names and matches_watch(n)
        )

        # Describe watched live + missing catalog (present only)
        describe_names = sorted(
            n for n in live if matches_watch(n) or n in catalog_names
        )
        details = self.describe_topics(describe_names) if describe_names else []
        under: list[dict[str, Any]] = []
        offline: list[dict[str, Any]] = []
        for d in details:
            if not matches_watch(str(d.get("name") or "")) and str(d.get("name")) not in catalog_names:
                continue
            if int(d.get("under_replicated") or 0) > 0:
                under.append(d)
            if int(d.get("offline_partitions") or 0) > 0 or int(d.get("no_leader") or 0) > 0:
                offline.append(d)

        groups = [g for g in self.list_consumer_groups() if matches_watch(g)]
        group_meta = self.describe_consumer_groups(groups) if groups else {}
        lag_warn_th = lag_warn_threshold()
        lag_crit_th = lag_crit_threshold()
        lag_warn: list[dict[str, Any]] = []
        lag_crit: list[dict[str, Any]] = []
        stalled: list[dict[str, Any]] = []
        empty_lagging: list[dict[str, Any]] = []
        group_summaries: list[dict[str, Any]] = []

        for gid in groups:
            lag_info = self.consumer_group_lag(gid)
            members = int((group_meta.get(gid) or {}).get("members") or 0)
            lag = int(lag_info.get("lag") or 0)
            summary = {
                "group_id": gid,
                "lag": lag,
                "members": members,
                "partitions": lag_info.get("partitions") or [],
            }
            group_summaries.append(summary)
            if lag >= lag_crit_th:
                lag_crit.append(summary)
            elif lag >= lag_warn_th:
                lag_warn.append(summary)
            if members == 0 and lag > 0:
                empty_lagging.append(summary)
                stalled.append(summary)

        poll_ms = (time.perf_counter() - poll_t0) * 1000.0
        probe = {
            **probe,
            "poll_ms": round(poll_ms, 2),
        }

        # Catalog partition drift (live topics only).
        detail_by_name = {
            str(d.get("name")): d for d in details if d.get("name")
        }
        undersized: list[dict[str, Any]] = []
        oversized: list[dict[str, Any]] = []
        for name, meta in catalog.items():
            if name not in live or not matches_watch(name):
                continue
            want = int(meta.get("partitions") or default_partitions())
            have = int((detail_by_name.get(name) or {}).get("partition_count") or 0)
            if have <= 0:
                continue
            if have < want:
                undersized.append(
                    {
                        "name": name,
                        "partition_count": have,
                        "desired_partitions": want,
                        "partitions": want,  # create/increase target
                        "replication_factor": meta.get("replication_factor"),
                        "description": meta.get("description"),
                    }
                )
            elif have > want:
                oversized.append(
                    {
                        "name": name,
                        "partition_count": have,
                        "desired_partitions": want,
                        "partitions": want,
                        "replication_factor": meta.get("replication_factor"),
                        "description": meta.get("description"),
                    }
                )

        severities: list[str] = []
        if missing:
            severities.append("TOPIC_MISSING")
        if unexpected and flag_unexpected_topics():
            severities.append("TOPIC_UNEXPECTED")
        if under:
            severities.append("UNDER_REPLICATED")
        if offline:
            severities.append("OFFLINE_PARTITION")
        if undersized:
            severities.append("TOPIC_PARTITIONS_LOW")
        if oversized:
            severities.append("TOPIC_PARTITIONS_HIGH")
        if lag_warn:
            severities.append("LAG_WARN")
        if lag_crit:
            severities.append("LAG_CRIT")
        if stalled:
            severities.append("CONSUMER_STALLED")
        if empty_lagging and "CONSUMER_STALLED" not in severities:
            severities.append("GROUP_EMPTY")
        if poll_ms >= probe_slow_ms() or float(probe.get("metadata_ms") or 0) >= probe_slow_ms():
            severities.append("BROKER_SLOW")

        return {
            "bootstrap": self.bootstrap,
            "healthy": not severities,
            "severities": severities,
            "probe": probe,
            "missing_topics": [
                {"name": n, **catalog[n]} for n in missing if n in catalog
            ],
            "unexpected_topics": [{"name": n} for n in unexpected],
            "topic_details": details,
            "under_replicated_topics": under,
            "offline_partitions": offline,
            "undersized_topics": undersized,
            "oversized_topics": oversized,
            "consumer_groups": group_summaries,
            "lag_warn_groups": lag_warn,
            "lag_crit_groups": lag_crit,
            "stalled_groups": stalled,
            "empty_lagging_groups": empty_lagging,
            "catalog": catalog,
            "counts": {
                "live_topics": len(live),
                "catalog_topics": len(catalog),
                "missing": len(missing),
                "undersized": len(undersized),
                "oversized": len(oversized),
                "consumer_groups": len(groups),
            },
        }

    def delete_topic(self, name: str) -> dict[str, Any]:
        """Delete a topic (lab / fault-inject only)."""
        admin = self._get_admin()
        try:
            admin.delete_topics([name])
            self.mutations.append({"op": "delete_topic", "target": name, "ok": True})
            return {"ok": True, "name": name}
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {"op": "delete_topic", "target": name, "ok": False, "error": str(exc)}
            )
            raise

    def create_topic(
        self,
        name: str,
        *,
        partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> dict[str, Any]:
        from kafka.admin import NewTopic

        parts = partitions if partitions is not None else default_partitions()
        rf = (
            replication_factor
            if replication_factor is not None
            else default_replication_factor()
        )
        admin = self._get_admin()
        topic = NewTopic(name=name, num_partitions=parts, replication_factor=rf)
        try:
            admin.create_topics([topic], validate_only=False)
            self.mutations.append(
                {
                    "op": "create_topic",
                    "target": name,
                    "partitions": parts,
                    "replication_factor": rf,
                    "ok": True,
                }
            )
            return {"ok": True, "name": name, "partitions": parts, "replication_factor": rf}
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {
                    "op": "create_topic",
                    "target": name,
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise

    def reset_offsets_to_end(self, group_id: str, topic: str) -> dict[str, Any]:
        """Lab-only: commit group offsets to log end (skip backlog)."""
        return self.reset_offsets(group_id, topic, strategy="latest")

    def reset_offsets_to_beginning(self, group_id: str, topic: str) -> dict[str, Any]:
        """Lab-only: commit group offsets to log start (replay)."""
        return self.reset_offsets(group_id, topic, strategy="earliest")

    def reset_offsets(
        self, group_id: str, topic: str, *, strategy: str = "latest"
    ) -> dict[str, Any]:
        """Commit group offsets to latest or earliest for all partitions of topic."""
        from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

        strat = (strategy or "latest").strip().lower()
        if strat not in ("latest", "earliest"):
            raise ValueError(f"unknown offset strategy {strategy!r}")

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
            client_id=f"{self.client_id}-reset",
        )
        try:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                raise RuntimeError(f"topic {topic!r} not found or has no partitions")
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            consumer.assign(tps)
            targets = (
                consumer.end_offsets(tps)
                if strat == "latest"
                else consumer.beginning_offsets(tps)
            )
            to_commit = {
                tp: OffsetAndMetadata(targets[tp], "", -1) for tp in tps
            }
            consumer.commit(to_commit)
            self.mutations.append(
                {
                    "op": "reset_offsets",
                    "target": group_id,
                    "topic": topic,
                    "strategy": strat,
                    "ok": True,
                }
            )
            return {
                "ok": True,
                "group_id": group_id,
                "topic": topic,
                "strategy": strat,
                "offsets": {
                    f"{tp.topic}:{tp.partition}": targets[tp] for tp in tps
                },
            }
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {
                    "op": "reset_offsets",
                    "target": group_id,
                    "topic": topic,
                    "strategy": strat,
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise
        finally:
            consumer.close()

    def reset_offsets_by_timestamp(
        self, group_id: str, topic: str, timestamp_ms: int
    ) -> dict[str, Any]:
        """Commit group offsets to the first message at/after ``timestamp_ms`` (epoch ms).

        Partitions with no match fall back to the log end (skip empty history).
        """
        from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
            client_id=f"{self.client_id}-reset-ts",
        )
        try:
            parts = consumer.partitions_for_topic(topic)
            if not parts:
                raise RuntimeError(f"topic {topic!r} not found or has no partitions")
            tps = [TopicPartition(topic, p) for p in sorted(parts)]
            consumer.assign(tps)
            ts_map = {tp: int(timestamp_ms) for tp in tps}
            found = consumer.offsets_for_times(ts_map)
            end = consumer.end_offsets(tps)
            to_commit: dict = {}
            offsets_out: dict[str, int] = {}
            for tp in tps:
                meta = found.get(tp) if found else None
                if meta is not None and getattr(meta, "offset", None) is not None:
                    off = int(meta.offset)
                else:
                    off = int(end[tp])
                to_commit[tp] = OffsetAndMetadata(off, "", -1)
                offsets_out[f"{tp.topic}:{tp.partition}"] = off
            consumer.commit(to_commit)
            self.mutations.append(
                {
                    "op": "reset_offsets_by_timestamp",
                    "target": group_id,
                    "topic": topic,
                    "timestamp_ms": int(timestamp_ms),
                    "ok": True,
                }
            )
            return {
                "ok": True,
                "group_id": group_id,
                "topic": topic,
                "timestamp_ms": int(timestamp_ms),
                "offsets": offsets_out,
            }
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {
                    "op": "reset_offsets_by_timestamp",
                    "target": group_id,
                    "topic": topic,
                    "timestamp_ms": int(timestamp_ms),
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise
        finally:
            consumer.close()

    def increase_partitions(self, name: str, total_count: int) -> dict[str, Any]:
        """Raise partition count to ``total_count`` (can only increase)."""
        from kafka.admin import NewPartitions

        want = max(1, int(total_count))
        admin = self._get_admin()
        try:
            admin.create_partitions({name: NewPartitions(want)})
            self.mutations.append(
                {
                    "op": "increase_partitions",
                    "target": name,
                    "partitions": want,
                    "ok": True,
                }
            )
            return {"ok": True, "name": name, "partitions": want}
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {
                    "op": "increase_partitions",
                    "target": name,
                    "partitions": want,
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise

    def recreate_topic(
        self,
        name: str,
        *,
        partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> dict[str, Any]:
        """Delete then create a topic (lab — destructive; data loss)."""
        parts = partitions if partitions is not None else default_partitions()
        rf = (
            replication_factor
            if replication_factor is not None
            else default_replication_factor()
        )
        if name in self.list_topics():
            self.delete_topic(name)
            for _ in range(60):
                if name not in self.list_topics():
                    break
                time.sleep(0.5)
        last_err: Exception | None = None
        created: dict[str, Any] = {}
        for _ in range(10):
            try:
                created = self.create_topic(
                    name, partitions=parts, replication_factor=rf
                )
                last_err = None
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(0.75)
        if last_err is not None:
            raise RuntimeError(f"recreate_topic create failed: {last_err}") from last_err
        self.mutations.append(
            {
                "op": "recreate_topic",
                "target": name,
                "partitions": parts,
                "replication_factor": rf,
                "ok": True,
            }
        )
        return {"ok": True, "name": name, "recreated": True, **created}

    def delete_consumer_group(self, group_id: str) -> dict[str, Any]:
        admin = self._get_admin()
        try:
            admin.delete_consumer_groups([group_id])
            self.mutations.append(
                {"op": "delete_group", "target": group_id, "ok": True}
            )
            return {"ok": True, "group_id": group_id}
        except Exception as exc:  # noqa: BLE001
            self.mutations.append(
                {
                    "op": "delete_group",
                    "target": group_id,
                    "ok": False,
                    "error": str(exc),
                }
            )
            raise
