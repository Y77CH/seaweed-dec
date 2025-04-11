#!/usr/bin/env python3
import sys
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

def process_log(filename):
    total_requests = 0
    count_get = 0
    count_put = 0
    count_delete = 0
    get_size_total = 0
    put_size_total = 0
    deleted_size_total = 0

    # Dictionary to store the most recent PUT size for each object id.
    put_history = {}

    # Dictionaries to store request counts for each operation keyed by timestamp.
    time_counts_get = defaultdict(int)
    time_counts_put = defaultdict(int)
    time_counts_delete = defaultdict(int)

    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue

            # The first field is the timestamp.
            try:
                timestamp = int(parts[0])
            except ValueError:
                continue

            total_requests += 1
            operation = parts[1]

            if operation == 'REST.PUT.OBJECT':
                count_put += 1
                time_counts_put[timestamp] += 1
                # The data size is in the fourth field.
                if len(parts) >= 4:
                    try:
                        size = int(parts[3])
                        put_size_total += size
                        object_id = parts[2]
                        # Update the record for this object id.
                        put_history[object_id] = size
                    except ValueError:
                        pass

            elif operation == 'REST.GET.OBJECT':
                count_get += 1
                time_counts_get[timestamp] += 1
                # The data size is in the fourth field.
                if len(parts) >= 4:
                    try:
                        size = int(parts[3])
                        get_size_total += size
                    except ValueError:
                        pass

            elif operation == 'REST.DELETE.OBJECT':
                count_delete += 1
                time_counts_delete[timestamp] += 1
                if len(parts) >= 3:
                    object_id = parts[2]
                    # Find the most recent PUT for this object.
                    if object_id in put_history:
                        deleted_size_total += put_history[object_id]
                        # Remove the record after deletion.
                        del put_history[object_id]

    # Print the computed statistics.
    print("Total requests:", total_requests)
    print("REST.PUT.OBJECT requests:", count_put)
    print("REST.GET.OBJECT requests:", count_get)
    print("REST.DELETE.OBJECT requests:", count_delete)
    print("Total PUT data size:", put_size_total)
    print("Total GET data size:", get_size_total)
    print("Total deleted object size:", deleted_size_total)

    # Create a union of timestamps from all three operation types.
    union_timestamps = sorted(set(time_counts_get.keys()) | set(time_counts_put.keys()) | set(time_counts_delete.keys()))
    
    # For each timestamp in the union, get counts or use 0 if no entry exists.
    get_counts = [time_counts_get.get(ts, 0) for ts in union_timestamps]
    put_counts = [time_counts_put.get(ts, 0) for ts in union_timestamps]
    delete_counts = [time_counts_delete.get(ts, 0) for ts in union_timestamps]

    # Build a 2D array for the heat map where rows are operations and columns are time indexes.
    # The row order is: GET, PUT, DELETE.
    data = np.array([get_counts, put_counts, delete_counts])

    # Plot the heat map.
    plt.figure()
    # Set origin to 'lower' so that the first row is at the bottom.
    plt.imshow(data, aspect='auto', interpolation='nearest', cmap='viridis', origin='lower')
    plt.colorbar(label='Number of requests')
    # Set y ticks to show operation names.
    plt.yticks([0, 1, 2], ['REST.GET.OBJECT', 'REST.PUT.OBJECT', 'REST.DELETE.OBJECT'])
    # Show a subset of x ticks if there are many timestamps.
    tick_step = max(1, len(union_timestamps) // 10)
    xtick_positions = list(range(0, len(union_timestamps), tick_step))
    xtick_labels = [str(union_timestamps[i]) for i in xtick_positions]
    plt.xticks(xtick_positions, xtick_labels, rotation=45)
    plt.xlabel("Timestamps (index)")
    plt.ylabel("Operation")
    plt.title("Heat Map of Requests over Time")
    plt.savefig("heatmap.png")
    print("Plots saved to "+"heatmap.png")
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: {} log_file".format(sys.argv[0]))
        sys.exit(1)
    process_log(sys.argv[1])
