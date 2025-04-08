#!/usr/bin/env python3

import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: {} trace_file".format(sys.argv[0]))
        sys.exit(1)
    
    trace_file = sys.argv[1]
    total_size = 0
    
    try:
        with open(trace_file, 'r') as file:
            for line in file:
                parts = line.strip().split()
                if len(parts) < 4:
                    continue
                # Check if the record is a PUT record
                if parts[1] == "REST.PUT.OBJECT":
                    try:
                        size = int(parts[3])
                        total_size += size
                    except ValueError:
                        continue
    except Exception as e:
        print("Error reading file:", e)
        sys.exit(1)
    
    # Convert from bytes to GB using binary conversion (1 GB = 1024^3 bytes)
    total_size_gb = total_size / (1024 ** 3)
    print("Total PUT size in GB:", total_size_gb)

if __name__ == '__main__':
    main()
