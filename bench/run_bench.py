#!/usr/bin/env python3
import os
import time
import requests
import argparse
import io
import logging
import threading

logging.basicConfig(
    level=logging.DEBUG,
    format='%(levelname)s %(asctime)s %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("benchmark.log", mode="a")
    ]
)

# Create temp directory if it does not exist (for GET responses)
os.makedirs("./temp", exist_ok=True)

# Global dictionary to store object_id -> (fid, public_url) mappings
object_mappings = {}
# Global dictionary to store object_id -> size mapping for PUT operations
object_sizes = {}
# Global variable for the largest in-memory buffer for PUT operations
largest_file_data = None
largest_put_size = 0

def prepare_memory_buffer(trace_file):
    """Scan the trace file for PUT operations and pre-create a largest in-memory buffer."""
    global largest_file_data, largest_put_size, object_sizes
    logging.debug(f"Scanning trace file for PUT operations: {trace_file}")
    largest_put_size = 0
    with open(trace_file, 'r') as f:
        lines = f.readlines()
    for line in lines:
        parts = line.strip().split()
        if len(parts) < 3:
            continue
        if parts[1] == "REST.PUT.OBJECT":
            object_id = parts[2]
            if len(parts) >= 4:
                size_bytes = int(parts[3])
                object_sizes[object_id] = size_bytes
                if size_bytes > largest_put_size:
                    largest_put_size = size_bytes
            else:
                logging.error(f"Missing size for PUT operation in line: {line}")
    if largest_put_size > 0:
        largest_file_data = os.urandom(largest_put_size)
        logging.debug(f"Created in-memory buffer of size {largest_put_size} bytes")

def put_object(master_addr, object_id, size_bytes):
    """Execute PUT operation using a slice of the pre-created in-memory buffer."""
    logging.debug(f"Executing PUT for object {object_id} with size {size_bytes} bytes")
    
    if object_id not in object_sizes:
        logging.error(f"No size mapping found for object {object_id}. Skipping PUT operation.")
        return
    
    try:
        # Get assignment from master server
        assign_url = f"{master_addr}/dir/assign"
        logging.debug(f"Requesting directory assignment from {assign_url}")
        assign_response = requests.get(assign_url)
        assign_data = assign_response.json()
        
        public_url = assign_data.get("publicUrl")
        fid = assign_data.get("fid")
        
        if not public_url or not fid:
            logging.error(f"Error: Missing publicUrl or fid in response: {assign_data}")
            return
        
        logging.debug(f"Received assignment: publicUrl={public_url}, fid={fid}")
        
        # Slice out the needed portion of the in-memory buffer and wrap it in a BytesIO stream
        upload_url = f"http://{public_url}/{fid}"
        data_slice = largest_file_data[:size_bytes]
        file_obj = io.BytesIO(data_slice)
        logging.debug(f"Uploading data slice of size {len(data_slice)} bytes to {upload_url}")
        start_time = time.time()
        upload_response = requests.post(
            upload_url,
            files={'file': ('file', file_obj)}
        )
        end_time = time.time()
        elapsed = end_time - start_time
        throughput = size_bytes / elapsed if elapsed > 0 else 0
        logging.info(f"PUT,{object_id},{size_bytes},{elapsed},{throughput:.2f}")
        logging.debug(f"PUT response: {upload_response.json()}")
        
        # Store the mapping for later GET and DELETE operations
        object_mappings[object_id] = (fid, public_url)
        logging.debug(f"Saved mapping for object {object_id}: fid={fid}, publicUrl={public_url}")
    
    except Exception as e:
        logging.error(f"Error during PUT operation for object {object_id}: {e}")

def get_object(master_addr, object_id, range_start=None, range_end=None):
    """Execute GET operation."""
    logging.debug(f"Executing GET for object {object_id}")
    
    if object_id not in object_mappings:
        logging.error(f"No mapping found for object {object_id}. Cannot execute GET operation.")
        return
    
    fid, public_url = object_mappings[object_id]
    url = f"http://{public_url}/{fid}"
    
    try:
        headers = {}
        if range_start is not None and range_end is not None:
            logging.debug(f"With range: {range_start}-{range_end}")
            headers['Range'] = f'bytes={range_start}-{range_end}'
        
        logging.debug(f"Sending GET request to {url}")
        start_time = time.time()
        response = requests.get(url, headers=headers)
        end_time = time.time()
        elapsed = end_time - start_time
        
        if response.status_code in (200, 206):
            content_length = len(response.content)
            logging.debug(f"GET operation for {object_id} completed successfully")
            logging.debug(f"Received {content_length} bytes of data")
            throughput = content_length / elapsed if elapsed > 0 else 0
            logging.info(f"GET,{object_id},{content_length},{elapsed},{throughput:.2f}")
            
            if range_start is not None and range_end is not None:
                output_path = f"./temp/{object_id}_range_{range_start}_{range_end}"
            else:
                output_path = f"./temp/{object_id}_full"
                
            with open(output_path, 'wb') as f:
                f.write(response.content)
            logging.debug(f"Saved response to {output_path}")
        else:
            logging.error(f"GET operation for {object_id} failed with status code {response.status_code}. Response: {response.text}")
    
    except Exception as e:
        logging.error(f"Error during GET operation: {e}")

def delete_object(master_addr, object_id):
    """Execute DELETE operation."""
    logging.debug(f"Executing DELETE for object {object_id}")
    
    if object_id not in object_mappings:
        logging.error(f"No mapping found for object {object_id}. Cannot execute DELETE operation.")
        return
    
    fid, public_url = object_mappings[object_id]
    url = f"http://{public_url}/{fid}"
    
    try:
        logging.debug(f"Sending DELETE request to {url}")
        response = requests.delete(url)
        
        if response.status_code in (200, 204):
            logging.debug(f"DELETE operation for {object_id} completed successfully")
            del object_mappings[object_id]
            logging.debug(f"Removed mapping for object {object_id}")
        elif response.status_code == 202:
            logging.debug(f"DELETE operation for {object_id} returns 202")
        else:
            logging.error(f"DELETE operation for {object_id} failed with status code {response.status_code}. Response: {response.text}")
    
    except Exception as e:
        logging.error(f"Error during DELETE operation: {e}")

def execute_trace(trace_file, master_addr):
    """Execute operations from trace file with timing in separate threads."""
    threads = []
    start_time = None
    
    logging.debug(f"Reading trace file: {trace_file}")
    with open(trace_file, 'r') as f:
        lines = f.readlines()
        
    logging.debug(f"Found {len(lines)} operations in trace file")
    
    for line in lines:
        parts = line.strip().split()
        if len(parts) < 3:
            logging.debug(f"Invalid trace line: {line}")
            continue
        
        timestamp_ms = int(parts[0])
        operation = parts[1]
        object_id = parts[2]
        
        if start_time is None:
            start_time = time.time() * 1000 - timestamp_ms
            logging.debug(f"Setting start time reference point at {timestamp_ms}ms")
        
        target_time = start_time + timestamp_ms
        current_time = time.time() * 1000
        wait_time = max(0, (target_time - current_time) / 1000)
        
        if wait_time > 0:
            logging.debug(f"Waiting {wait_time:.3f} seconds until timestamp {timestamp_ms}ms")
            time.sleep(wait_time)
        
        if operation == "REST.PUT.OBJECT":
            if len(parts) >= 4:
                size_bytes = int(parts[3])
                thread = threading.Thread(target=put_object, args=(master_addr, object_id, size_bytes))
                thread.start()
                threads.append(thread)
            else:
                logging.error(f"Missing size for PUT operation: {line}")
        
        elif operation == "REST.GET.OBJECT":
            if len(parts) >= 6:
                range_start = int(parts[4])
                range_end = int(parts[5])
                thread = threading.Thread(target=get_object, args=(master_addr, object_id, range_start, range_end))
                thread.start()
                threads.append(thread)
            else:
                thread = threading.Thread(target=get_object, args=(master_addr, object_id))
                thread.start()
                threads.append(thread)
        
        elif operation == "REST.DELETE.OBJECT":
            thread = threading.Thread(target=delete_object, args=(master_addr, object_id))
            thread.start()
            threads.append(thread)
        
        else:
            logging.error(f"Unknown operation: {operation}")
    
    for thread in threads:
        thread.join()

def main():
    parser = argparse.ArgumentParser(description='Execute operations from trace file.')
    parser.add_argument('trace_file', help='Path to trace file')
    parser.add_argument('--master', required=True, help='Master server address (e.g., http://localhost:9333)')
    
    args = parser.parse_args()
    
    print(f"Starting trace execution from {args.trace_file} with master {args.master}")
    prepare_memory_buffer(args.trace_file)
    print("Data preparation completed")
    execute_trace(args.trace_file, args.master)
    print("Trace execution completed")

if __name__ == "__main__":
    main()
