#!/usr/bin/env python3
"""
Log Analyzer - A script to process performance, garbage collection, and trace logs,
then visualize throughput, garbage ratio, and DELETE events over time.
"""

import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
import numpy as np

def parse_performance_log(log_file):
    """
    Parse the performance log file and extract timestamp and throughput information.
    
    Args:
        log_file (str): Path to the performance log file
        
    Returns:
        pandas.DataFrame: DataFrame containing timestamp and throughput data
    """
    data = []
    # Pattern matches: timestamp, method, object id, size, time used, throughput
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) ([A-Z]+),([a-f0-9]+),(\d+),(\d+\.\d+),(\d+\.\d+)'
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                match = re.match(pattern, line.strip())
                if match:
                    timestamp = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S,%f')
                    method = match.group(2)
                    obj_id = match.group(3)
                    size = int(match.group(4))
                    time_used = float(match.group(5))
                    throughput = float(match.group(6))
                    
                    data.append({
                        'timestamp': timestamp,
                        'method': method,
                        'obj_id': obj_id,
                        'size': size,
                        'time_used': time_used,
                        'throughput': throughput
                    })
    except FileNotFoundError:
        print(f"Error: File {log_file} not found.")
    except Exception as e:
        print(f"Error processing performance log file: {e}")
    
    return pd.DataFrame(data)

def parse_garbage_log(log_file):
    """
    Parse the garbage log file and extract timestamp and garbage ratios per volume server.
    
    Args:
        log_file (str): Path to the garbage log file
        
    Returns:
        pandas.DataFrame: DataFrame containing timestamp and garbage ratios for each volume server
    """
    data = []
    # Pattern matches: timestamp and volumes dictionary
    pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) Volumes: \{(.+)\}'
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                match = re.match(pattern, line.strip())
                if match:
                    timestamp = datetime.strptime(match.group(1), '%Y-%m-%d %H:%M:%S,%f')
                    volumes_str = match.group(2)
                    
                    # Parse the volumes dictionary
                    volumes = {}
                    for item in volumes_str.split(', '):
                        key, value = item.split(': ')
                        volumes[f'vol_{key}'] = float(value)
                    
                    # Build the row with the timestamp and the garbage ratios per volume server
                    row = {'timestamp': timestamp}
                    row.update(volumes)
                    data.append(row)
    except FileNotFoundError:
        print(f"Error: File {log_file} not found.")
    except Exception as e:
        print(f"Error processing garbage log file: {e}")
    
    return pd.DataFrame(data)

def parse_trace_log(log_file):
    """
    Parse the trace log file and extract numeric timestamp, method, and object id.
    
    Args:
        log_file (str): Path to the trace log file
        
    Returns:
        pandas.DataFrame: DataFrame containing numeric timestamp, method, and object id
    """
    data = []
    # Pattern matches: numeric timestamp, method, object id with optional remaining fields
    pattern = r'(\d+)\s+(REST\.[A-Z]+\.[A-Z]+)\s+([a-f0-9]+).*'
    
    try:
        with open(log_file, 'r') as f:
            for line in f:
                line = line.strip()
                match = re.match(pattern, line)
                if match:
                    numeric_timestamp = int(match.group(1))
                    method = match.group(2)
                    obj_id = match.group(3)
                    
                    data.append({
                        'numeric_timestamp': numeric_timestamp,
                        'method': method,
                        'obj_id': obj_id
                    })
    except FileNotFoundError:
        print(f"Error: File {log_file} not found.")
    except Exception as e:
        print(f"Error processing trace log file: {e}")
    
    # Print summary to verify we are capturing DELETE operations
    df = pd.DataFrame(data)
    if not df.empty:
        method_counts = df['method'].value_counts()
        print(f"Method counts in trace log: {method_counts}")
        delete_count = df[df['method'].str.contains('DELETE')].shape[0]
        print(f"Total DELETE operations found: {delete_count}")
    
    return df

def synchronize_timestamps(performance_df, trace_df):
    """
    Synchronize timestamps between performance and trace logs.
    
    Args:
        performance_df (pandas.DataFrame): DataFrame with performance data
        trace_df (pandas.DataFrame): DataFrame with trace data
        
    Returns:
        pandas.DataFrame: Updated trace DataFrame with synchronized datetime timestamps
    """
    if performance_df.empty or trace_df.empty:
        print("Cannot synchronize timestamps: One or both DataFrames are empty.")
        return trace_df
    
    # Get the first timestamp from each log
    perf_first_timestamp = performance_df['timestamp'].min()
    trace_first_numeric = trace_df['numeric_timestamp'].min()
    
    # Create a copy of the trace DataFrame
    synced_trace_df = trace_df.copy()
    
    # Function to convert numeric timestamps to datetime
    def convert_to_datetime(numeric_ts):
        ms_diff = numeric_ts - trace_first_numeric
        ms_diff_int = int(ms_diff)
        return perf_first_timestamp + timedelta(milliseconds=ms_diff_int)
    
    # Apply the conversion function to create a new datetime timestamp column
    synced_trace_df['timestamp'] = synced_trace_df['numeric_timestamp'].apply(convert_to_datetime)
    
    # Print the first few DELETE events after synchronization to verify
    delete_events = synced_trace_df[synced_trace_df['method'].str.contains('DELETE')]
    if not delete_events.empty:
        print("\nTimestamp synchronized DELETE events (first 5):")
        print(delete_events.head(5)[['timestamp', 'method', 'obj_id']])
    
    return synced_trace_df

def normalize_timestamps(performance_df, garbage_df, trace_df):
    """
    Normalize timestamps to make the first event start at time zero.
    
    Args:
        performance_df (pandas.DataFrame): DataFrame with performance data
        garbage_df (pandas.DataFrame): DataFrame with garbage ratio data
        trace_df (pandas.DataFrame): DataFrame with trace data
        
    Returns:
        tuple: (performance_df, garbage_df, trace_df) with normalized timestamps
    """
    min_timestamps = []
    
    if not performance_df.empty and 'timestamp' in performance_df.columns:
        min_timestamps.append(performance_df['timestamp'].min())
    
    if not garbage_df.empty and 'timestamp' in garbage_df.columns:
        min_timestamps.append(garbage_df['timestamp'].min())
    
    if not trace_df.empty and 'timestamp' in trace_df.columns:
        min_timestamps.append(trace_df['timestamp'].min())
    
    if not min_timestamps:
        print("Warning: No valid timestamps found to normalize.")
        return performance_df, garbage_df, trace_df
    
    global_min_timestamp = min(min_timestamps)
    print(f"Global minimum timestamp: {global_min_timestamp}")
    
    def timestamp_to_seconds(ts):
        if isinstance(ts, datetime):
            delta = ts - global_min_timestamp
            return delta.total_seconds()
        return None
    
    if not performance_df.empty and 'timestamp' in performance_df.columns:
        performance_df['seconds_elapsed'] = performance_df['timestamp'].apply(timestamp_to_seconds)
        print(f"Performance log time range: 0 to {performance_df['seconds_elapsed'].max():.2f} seconds")
    
    if not garbage_df.empty and 'timestamp' in garbage_df.columns:
        garbage_df['seconds_elapsed'] = garbage_df['timestamp'].apply(timestamp_to_seconds)
        print(f"Garbage log time range: 0 to {garbage_df['seconds_elapsed'].max():.2f} seconds")
    
    if not trace_df.empty and 'timestamp' in trace_df.columns:
        trace_df['seconds_elapsed'] = trace_df['timestamp'].apply(timestamp_to_seconds)
        print(f"Trace log time range: 0 to {trace_df['seconds_elapsed'].max():.2f} seconds")
    
    return performance_df, garbage_df, trace_df

def plot_data_normalized(performance_df, garbage_df, trace_df, output_file):
    """
    Create and save plots for throughput and garbage ratio over time, 
    with time normalized to start at zero.
    
    Args:
        performance_df (pandas.DataFrame): DataFrame with performance data
        garbage_df (pandas.DataFrame): DataFrame with garbage ratio data
        trace_df (pandas.DataFrame): DataFrame with trace data (including DELETE events)
        output_file (str): Path to save the output plot file
    """
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    if not performance_df.empty and 'seconds_elapsed' in performance_df.columns:
        get_data = performance_df[performance_df['method'] == 'GET']
        put_data = performance_df[performance_df['method'] == 'PUT']
        
        if not get_data.empty:
            ax1.plot(get_data['seconds_elapsed'], get_data['throughput']/1000000, marker='o', linestyle='-', 
                     color='blue', label='GET')
        if not put_data.empty:
            ax1.plot(put_data['seconds_elapsed'], put_data['throughput']/1000000, marker='s', linestyle='-', 
                     color='green', label='PUT')
        
        ax1.set_xlabel('Time (seconds)')
        ax1.set_ylabel('Throughput (MB/s)')
        ax1.set_ylim(0, 100)
        ax1.set_yticks(np.arange(0, 101, 10))
        ax1.set_title('Throughput over Time by Operation Type')
        ax1.grid(True)
        ax1.legend()
    else:
        ax1.text(0.5, 0.5, 'No valid performance data found', 
                 horizontalalignment='center', verticalalignment='center',
                 transform=ax1.transAxes)
    
    if not garbage_df.empty and 'seconds_elapsed' in garbage_df.columns:
        volume_columns = [col for col in garbage_df.columns if col not in ('timestamp', 'seconds_elapsed')]
        for col in volume_columns:
            ax2.plot(garbage_df['seconds_elapsed'], garbage_df[col], marker='o', linestyle='-', label=col)
        
        # Add vertical lines for DELETE events
        if not trace_df.empty and 'seconds_elapsed' in trace_df.columns:
            delete_events = trace_df[trace_df['method'].str.contains('DELETE', case=False)]
            if not delete_events.empty:
                for i, ts in enumerate(delete_events['seconds_elapsed']):
                    if i == 0:
                        ax2.axvline(x=ts, color='purple', linestyle='--', alpha=0.7, label='DELETE Event')
                    else:
                        ax2.axvline(x=ts, color='purple', linestyle='--', alpha=0.7)
                ax2.legend()
        
        ax2.set_xlabel('Time (seconds)')
        ax2.set_ylabel('Garbage Ratio')
        ax2.set_ylim(0, 0.01)
        ax2.set_yticks(np.arange(0, 0.011, 0.001))
        ax2.set_title('Garbage Ratio over Time per Volume Server with DELETE Events')
        ax2.grid(True)
    else:
        ax2.text(0.5, 0.5, 'No valid garbage data found',
                 horizontalalignment='center', verticalalignment='center',
                 transform=ax2.transAxes)
    
    # Set the x-axis for both plots to the throughput range if available
    if not performance_df.empty and 'seconds_elapsed' in performance_df.columns:
        x_max = performance_df['seconds_elapsed'].max()
        ax1.set_xlim(0, x_max)
        ax2.set_xlim(0, x_max)
    else:
        ax1.set_xlim(left=0)
        ax2.set_xlim(left=0)
    
    plt.tight_layout()
    plt.savefig(output_file)
    print(f"Plots saved to {output_file}")
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='Process and plot log files')
    parser.add_argument('--perf_log', required=True, help='Path to performance log file')
    parser.add_argument('--garbage_log', required=True, help='Path to garbage log file')
    parser.add_argument('--trace_log', required=True, help='Path to trace log file')
    parser.add_argument('--output', default='log_analysis_plots.png', help='Output plot file name')
    
    args = parser.parse_args()
    
    performance_df = parse_performance_log(args.perf_log)
    garbage_df = parse_garbage_log(args.garbage_log)
    trace_df = parse_trace_log(args.trace_log)
    
    if performance_df.empty and garbage_df.empty and trace_df.empty:
        print("Error: No valid data found in log files.")
        return
    
    if not performance_df.empty and not trace_df.empty:
        trace_df = synchronize_timestamps(performance_df, trace_df)
    
    performance_df, garbage_df, trace_df = normalize_timestamps(performance_df, garbage_df, trace_df)
    
    plot_data_normalized(performance_df, garbage_df, trace_df, args.output)

if __name__ == "__main__":
    main()
