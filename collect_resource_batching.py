import csv
import psutil
import time
import os
from datetime import datetime

RUN_DURATION = 1800
BATCH_SIZE = 1000
CSV_FILENAME = "pi_metrics_batched.csv"


def flatten_dict(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, val in enumerate(v):
                list_key = f"{new_key}{sep}{i}"
                if isinstance(val, dict):
                    items.extend(flatten_dict(val, list_key, sep=sep).items())
                else:
                    items.append((list_key, val))
        else:
            items.append((new_key, v))
    return dict(items)


def collect_resource_once():
    timestamp = datetime.now().isoformat()

    # CPU Metrics
    cpu_percent_system = psutil.cpu_percent(interval=None, percpu=False)
    cpu_percent_cores = psutil.cpu_percent(interval=None, percpu=True)
    cpu_times_system = psutil.cpu_times(percpu=False)._asdict()
    cpu_times_cores = [ct._asdict() for ct in psutil.cpu_times(percpu=True)]
    cpu_stats = psutil.cpu_stats()._asdict()
    cpu_freq_single = psutil.cpu_freq(percpu=False)
    cpu_freq_per_core = psutil.cpu_freq(percpu=True)
    cpu_freq_data = cpu_freq_single._asdict() if cpu_freq_single else {}
    cpu_freq_cores_data = (
        [freq._asdict() for freq in cpu_freq_per_core] if cpu_freq_per_core else []
    )

    # Load average (Unix-only)
    load_avg = {}
    try:
        load1, load5, load15 = os.getloadavg()
        load_avg = {"1min": load1, "5min": load5, "15min": load15}
    except (OSError, AttributeError):
        pass

    # Memory Metrics
    virtual_mem = psutil.virtual_memory()._asdict()
    swap_mem = psutil.swap_memory()._asdict()

    # Disk Metrics
    try:
        disk_usage_root = psutil.disk_usage("/")._asdict()
    except PermissionError:
        disk_usage_root = {"error": "Permission Denied"}
    disk_io_total = psutil.disk_io_counters(perdisk=False)
    disk_io_data = disk_io_total._asdict() if disk_io_total else {}

    # Network Metrics
    net_io_total = psutil.net_io_counters(pernic=False)
    net_io_data = net_io_total._asdict() if net_io_total else {}
    net_if_stats = {}
    for nic, stats in psutil.net_if_stats().items():
        net_if_stats[nic] = stats._asdict()

    # Sensor Metrics
    battery_data = {}
    try:
        batt = psutil.sensors_battery()
        if batt:
            battery_data = batt._asdict()
    except (NotImplementedError, AttributeError):
        pass
    temp_data = {}
    try:
        temps_info = psutil.sensors_temperatures()
        if temps_info:
            for name, entries in temps_info.items():
                temp_data[name] = [e._asdict() for e in entries]
    except (NotImplementedError, AttributeError):
        pass

    # Combine All Data
    combined_data = {
        "timestamp": timestamp,
        "cpu": {
            "percent_system": cpu_percent_system,
            "percent_cores": cpu_percent_cores,
            "times_system": cpu_times_system,
            "times_cores": cpu_times_cores,
            "stats": cpu_stats,
            "freq_single": cpu_freq_data,
            "freq_cores": cpu_freq_cores_data,
            "load_avg": load_avg,
        },
        "memory": {
            "virtual": virtual_mem,
            "swap": swap_mem,
        },
        "disk": {
            "usage_root": disk_usage_root,
            "io_total": disk_io_data,
        },
        "network": {"io_total": net_io_data, "if_stats": net_if_stats},
        "sensors": {"battery": battery_data, "temperature": temp_data},
    }

    flat_data = flatten_dict(combined_data)
    print("Number of flattened metrics:", len(flat_data))
    return flat_data


def main():
    sample_count = 0
    start_time = time.time()
    end_time = start_time + RUN_DURATION
    writer = None

    with open(CSV_FILENAME, "w", newline="") as csvfile:
        while time.time() < end_time:
            try:
                flat_data = collect_resource_once()
                if writer is None:
                    fieldnames = flat_data.keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                writer.writerow(flat_data)
                sample_count += 1

                if sample_count % BATCH_SIZE == 0:
                    csvfile.flush()
                    print(f"Flushed {sample_count} entries to {CSV_FILENAME}")
            except Exception as e:
                print(f"Error collecting local system metrics: {e}")
                break

    print(f"Metrics for PI written to {CSV_FILENAME}")


if __name__ == "__main__":
    main()
