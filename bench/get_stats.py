#!/usr/bin/env python3
import sys

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

    with open(filename, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue

            total_requests += 1
            operation = parts[1]

            if operation == 'REST.PUT.OBJECT':
                count_put += 1
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
                # The data size is in the fourth field.
                if len(parts) >= 4:
                    try:
                        size = int(parts[3])
                        get_size_total += size
                    except ValueError:
                        pass

            elif operation == 'REST.DELETE.OBJECT':
                count_delete += 1
                if len(parts) >= 3:
                    object_id = parts[2]
                    # Find the most recent PUT for this object.
                    if object_id in put_history:
                        deleted_size_total += put_history[object_id]
                        # Remove the record after deletion.
                        del put_history[object_id]

    print("Total requests:", total_requests)
    print("REST.PUT.OBJECT requests:", count_put)
    print("REST.GET.OBJECT requests:", count_get)
    print("REST.DELETE.OBJECT requests:", count_delete)
    print("Total PUT data size:", put_size_total)
    print("Total GET data size:", get_size_total)
    print("Total deleted object size:", deleted_size_total)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: {} log_file".format(sys.argv[0]))
        sys.exit(1)
    process_log(sys.argv[1])