#!/usr/bin/env python3
import ast

def get_largest_garbage_ratio(log_file_path):
    max_ratio = -float('inf')
    max_timestamp = ""
    max_volume = None

    with open(log_file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            # The timestamp is formed by the first two tokens (date and time)
            tokens = line.split(" ", 2)
            if len(tokens) < 3:
                continue
            timestamp = tokens[0] + " " + tokens[1]
            # The dictionary is given after the text "Volumes: "
            try:
                dict_str = line.split("Volumes: ", 1)[1]
            except IndexError:
                continue

            try:
                # Convert the string representation of the dictionary into an actual dictionary
                volumes = ast.literal_eval(dict_str)
            except Exception as e:
                print("Error parsing volumes dictionary on line:", line, "\n", e)
                continue

            # Check each volume's garbage ratio
            for volume, ratio in volumes.items():
                if ratio > max_ratio:
                    max_ratio = ratio
                    max_timestamp = timestamp
                    max_volume = volume

    return max_timestamp, max_volume, max_ratio

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2:
        print("Usage: python script.py <log_file>")
    else:
        log_file = sys.argv[1]
        timestamp, volume, ratio = get_largest_garbage_ratio(log_file)
        if timestamp:
            print("Timestamp:", timestamp)
            print("Volume:", volume)
            print("Garbage Ratio:", ratio)
        else:
            print("No valid data found in the log file.")