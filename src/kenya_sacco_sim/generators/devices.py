from __future__ import annotations

import random
from datetime import datetime

from kenya_sacco_sim.core.config import WorldConfig, start_timestamp
from kenya_sacco_sim.core.id_factory import IdFactory


def generate_devices(config: WorldConfig, members: list[dict[str, object]]) -> list[dict[str, object]]:
    rng = random.Random(config.seed + 606)
    ids = IdFactory()
    devices: list[dict[str, object]] = []
    shared_group_by_index: dict[int, str] = {}
    share_stride = 50
    for index, member in enumerate(members):
        shared_group = None
        stride_position = index % share_stride
        if stride_position == 0 and index + 1 < len(members):
            group_index = index // share_stride
            shared_group = shared_group_by_index.setdefault(group_index, f"SHARED_DEVICE_GROUP_{group_index + 1:05d}")
        elif stride_position == 1:
            group_index = (index - 1) // share_stride
            shared_group = shared_group_by_index.setdefault(group_index, f"SHARED_DEVICE_GROUP_{group_index + 1:05d}")
        devices.append(
            {
                "device_id": ids.next("DEVICE"),
                "member_id": member["member_id"],
                "institution_id": member["institution_id"],
                "first_seen": member["join_date"],
                "last_seen": member["join_date"],
                "os_family": rng.choices(["ANDROID", "IOS", "FEATURE_PHONE"], weights=[0.78, 0.12, 0.10], k=1)[0],
                "app_user_flag": rng.random() < 0.88,
                "shared_device_group": shared_group,
                "created_at": start_timestamp(config),
            }
        )
    return devices


def update_device_last_seen(devices: list[dict[str, object]], transactions: list[dict[str, object]]) -> None:
    latest_by_device: dict[str, str] = {}
    for txn in transactions:
        device_id = str(txn.get("device_id") or "")
        if not device_id:
            continue
        try:
            seen_date = datetime.fromisoformat(str(txn["timestamp"])).date().isoformat()
        except (KeyError, ValueError):
            continue
        if seen_date > latest_by_device.get(device_id, ""):
            latest_by_device[device_id] = seen_date

    for device in devices:
        device_id = str(device["device_id"])
        if device_id in latest_by_device:
            device["last_seen"] = latest_by_device[device_id]
