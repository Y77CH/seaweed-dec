#!/usr/bin/env python3
import os
import time
import requests
import argparse
from pathlib import Path

# Create temp directory if it doesn't exist
Path("./temp").mkdir(exist_ok=True)

# Global dictionary to store object_id -> (fid, public_url) mappings
object_mappings = {}

def create_random_file(file_path, size_bytes):
    """Create a file with random data of the specified size."""
    with open(file_path, 'wb') as f:
        # Write in chunks to handle large files efficiently
        chunk_size = min(1024 * 1024, size_bytes)  # 1MB chunks or smaller
        remaining = size_bytes
        
        while remaining > 0:
            current_chunk = min(chunk_size, remaining)
            f.write(os.urandom(current_chunk))
            remaining -= current_chunk
    
    print(f"Created temporary file {file_path} of size {size_bytes} bytes")

def put_object(master_addr, object_id, size_bytes):
    """Execute PUT operation."""
    print(f"Executing PUT for object {object_id} with size {size_bytes}")
    
    # Create temp file
    file_path = f"./temp/{object_id}"
    create_random_file(file_path, size_bytes)
    
    try:
        # Get assignment from master server
        assign_url = f"{master_addr}/dir/assign"
        print(f"Requesting directory assignment from {assign_url}")
        assign_response = requests.get(assign_url)
        assign_data = assign_response.json()
        
        public_url = assign_data.get("publicUrl")
        fid = assign_data.get("fid")
        
        if not public_url or not fid:
            print(f"Error: Missing publicUrl or fid in response: {assign_data}")
            return
        
        print(f"Received assignment: publicUrl={public_url}, fid={fid}")
        
        # Upload file to the assigned location
        upload_url = f"http://{public_url}/{fid}"
        print(f"Uploading file to {upload_url}")
        start_time = time.time()
        with open(file_path, 'rb') as file:
            upload_response = requests.post(
                upload_url,
                files={'file': file}
            )
        end_time = time.time()
        elapsed = end_time - start_time
        throughput = size_bytes / elapsed if elapsed > 0 else 0
        print(f"PUT throughput: {throughput:.2f} bytes/sec")
        
        print(f"PUT response: {upload_response.json()}")
        
        # Store the mapping for later GET and DELETE operations
        object_mappings[object_id] = (fid, public_url)
        print(f"Saved mapping for object {object_id}: fid={fid}, publicUrl={public_url}")
    
    except Exception as e:
        print(f"Error during PUT operation: {e}")
    
    finally:
        # Clean up temp file
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Cleaned up temporary file {file_path}")

def get_object(master_addr, object_id, range_start=None, range_end=None):
    """Execute GET operation."""
    print(f"Executing GET for object {object_id}")
    
    if object_id not in object_mappings:
        print(f"Error: No mapping found for object {object_id}. Cannot execute GET operation.")
        return
    
    fid, public_url = object_mappings[object_id]
    url = f"http://{public_url}/{fid}"
    
    try:
        headers = {}
        if range_start is not None and range_end is not None:
            print(f"With range: {range_start}-{range_end}")
            headers['Range'] = f'bytes={range_start}-{range_end}'
        
        print(f"Sending GET request to {url}")
        start_time = time.time()
        response = requests.get(url, headers=headers)
        end_time = time.time()
        elapsed = end_time - start_time
        
        if response.status_code == 200 or response.status_code == 206:
            content_length = len(response.content)
            print(f"GET operation for {object_id} completed successfully")
            print(f"Received {content_length} bytes of data")
            throughput = content_length / elapsed if elapsed > 0 else 0
            print(f"GET throughput: {throughput:.2f} bytes/sec")
            
            # Save the response to a file if needed for inspection or verification
            if range_start is not None and range_end is not None:
                output_path = f"./temp/{object_id}_range_{range_start}_{range_end}"
            else:
                output_path = f"./temp/{object_id}_full"
                
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Saved response to {output_path}")
        else:
            print(f"GET operation failed with status code {response.status_code}")
            print(f"Response: {response.text}")
    
    except Exception as e:
        print(f"Error during GET operation: {e}")

def delete_object(master_addr, object_id):
    """Execute DELETE operation."""
    print(f"Executing DELETE for object {object_id}")
    
    if object_id not in object_mappings:
        print(f"Error: No mapping found for object {object_id}. Cannot execute DELETE operation.")
        return
    
    fid, public_url = object_mappings[object_id]
    url = f"http://{public_url}/{fid}"
    
    try:
        print(f"Sending DELETE request to {url}")
        response = requests.delete(url)
        
        if response.status_code == 200 or response.status_code == 204:
            print(f"DELETE operation for {object_id} completed successfully")
            # Remove mapping after successful deletion
            del object_mappings[object_id]
            print(f"Removed mapping for object {object_id}")
        else:
            print(f"DELETE operation failed with status code {response.status_code}")
            print(f"Response: {response.text}")
    
    except Exception as e:
        print(f"Error during DELETE operation: {e}")

def execute_trace(trace_file, master_addr):
    """Execute operations from trace file with timing."""
    start_time = None
    
    print(f"Reading trace file: {trace_file}")
    with open(trace_file, 'r') as f:
        lines = f.readlines()
        
    print(f"Found {len(lines)} operations in trace file")
    
    for line in lines:
        parts = line.strip().split()
        if len(parts) < 3:
            print(f"Invalid trace line: {line}")
            continue
        
        timestamp_ms = int(parts[0])
        operation = parts[1]
        object_id = parts[2]
        
        # Set start time on first operation
        if start_time is None:
            start_time = time.time() * 1000 - timestamp_ms
            print(f"Setting start time reference point at {timestamp_ms}ms")
        
        # Calculate and wait for the target time
        target_time = start_time + timestamp_ms
        current_time = time.time() * 1000
        wait_time = max(0, (target_time - current_time) / 1000)  # Convert to seconds
        
        if wait_time > 0:
            print(f"Waiting {wait_time:.3f} seconds until timestamp {timestamp_ms}ms")
            time.sleep(wait_time)
        
        # Execute operation based on type
        if operation == "REST.PUT.OBJECT":
            if len(parts) >= 4:
                size_bytes = int(parts[3])
                put_object(master_addr, object_id, size_bytes)
            else:
                print(f"Missing size for PUT operation: {line}")
        
        elif operation == "REST.GET.OBJECT":
            if len(parts) >= 6:
                range_start = int(parts[4])
                range_end = int(parts[5])
                get_object(master_addr, object_id, range_start, range_end)
            else:
                get_object(master_addr, object_id)
        
        elif operation == "REST.DELETE.OBJECT":
            delete_object(master_addr, object_id)
        
        else:
            print(f"Unknown operation: {operation}")

def main():
    parser = argparse.ArgumentParser(description='Execute operations from trace file.')
    parser.add_argument('trace_file', help='Path to trace file')
    parser.add_argument('--master', required=True, help='Master server address (e.g., http://localhost:9333)')
    
    args = parser.parse_args()
    
    print(f"Starting trace execution from {args.trace_file} with master {args.master}")
    execute_trace(args.trace_file, args.master)
    print("Trace execution completed")

if __name__ == "__main__":
    main()
