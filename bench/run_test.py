#!/usr/bin/env python3
import os
import time
import hashlib
import requests
import argparse
from pathlib import Path

# Create temp directory if it doesn't exist
Path("./temp").mkdir(exist_ok=True)

# Global dictionary to store object_id -> (fid, public_url, content_hash) mappings
object_mappings = {}

def generate_patterned_content(size_bytes):
    """Generate patterned content of the specified size."""
    # Create a pattern that includes position information for easier debugging
    # Each chunk will be 16 bytes: 8 bytes for position + 8 bytes fixed pattern
    base_pattern = b"TESTtest"
    
    # Calculate how many full chunks we need
    chunk_size = 16  # 8 bytes for position + 8 bytes for pattern
    num_chunks = size_bytes // chunk_size
    remaining_bytes = size_bytes % chunk_size
    
    # Generate content in chunks
    content = bytearray()
    for i in range(num_chunks):
        # Add position (8 bytes) + base pattern (8 bytes)
        position_bytes = str(i).zfill(8).encode('ascii')
        content.extend(position_bytes + base_pattern)
    
    # Add any remaining bytes needed
    if remaining_bytes > 0:
        remainder = base_pattern[:remaining_bytes]
        content.extend(remainder)
    
    return bytes(content)

def calculate_content_hash(content):
    """Calculate SHA-256 hash of content."""
    return hashlib.sha256(content).hexdigest()

def create_patterned_file(file_path, size_bytes):
    """Create a file with patterned data of the specified size."""
    content = generate_patterned_content(size_bytes)
    
    with open(file_path, 'wb') as f:
        f.write(content)
    
    content_hash = calculate_content_hash(content)
    print(f"Created temporary file {file_path} of size {size_bytes} bytes")
    print(f"Content hash: {content_hash}")
    
    return content, content_hash

def verify_content(original_content, received_content, object_id, is_range=False):
    """Verify that received content matches the original content."""
    if original_content == received_content:
        print(f"Content verification SUCCESSFUL for {object_id}")
        return True
    else:
        print(f"Content verification FAILED for {object_id}")
        
        # Provide more detailed diagnostics
        original_hash = calculate_content_hash(original_content)
        received_hash = calculate_content_hash(received_content)
        
        print(f"Original content hash: {original_hash}")
        print(f"Received content hash: {received_hash}")
        
        # If content sizes are different, report that
        if len(original_content) != len(received_content):
            print(f"Content size mismatch: Original={len(original_content)} bytes, Received={len(received_content)} bytes")
        
        # If not a range request and sizes are the same, find first difference
        if not is_range and len(original_content) == len(received_content):
            for i, (orig_byte, recv_byte) in enumerate(zip(original_content, received_content)):
                if orig_byte != recv_byte:
                    print(f"First difference at position {i}: Original={orig_byte}, Received={recv_byte}")
                    # Show some context around the difference
                    start = max(0, i - 8)
                    end = min(len(original_content), i + 8)
                    print(f"Original context: {original_content[start:end]}")
                    print(f"Received context: {received_content[start:end]}")
                    break
        
        return False

def put_object(master_addr, object_id, size_bytes):
    """Execute PUT operation."""
    print(f"Executing PUT for object {object_id} with size {size_bytes}")
    
    # Create temp file with patterned content
    file_path = f"./temp/{object_id}"
    content, content_hash = create_patterned_file(file_path, size_bytes)
    
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
        
        with open(file_path, 'rb') as file:
            upload_response = requests.post(
                upload_url, 
                files={'file': file}
            )
        
        print(f"PUT response: {upload_response.json()}")
        
        # Store the mapping for later GET and DELETE operations
        # Also store the content or content hash for verification
        object_mappings[object_id] = {
            'fid': fid,
            'public_url': public_url,
            'content': content,  # Store actual content for verification
            'content_hash': content_hash,
            'size': size_bytes
        }
        print(f"Saved mapping for object {object_id}: fid={fid}, publicUrl={public_url}")
    
    except Exception as e:
        print(f"Error during PUT operation: {e}")
    
    finally:
        # We'll keep the temp file for verification purposes
        # but you can uncomment the following to delete it
        # if os.path.exists(file_path):
        #     os.remove(file_path)
        #     print(f"Cleaned up temporary file {file_path}")
        pass

def get_object(master_addr, object_id, range_start=None, range_end=None):
    """Execute GET operation with content verification."""
    print(f"Executing GET for object {object_id}")
    
    if object_id not in object_mappings:
        print(f"Error: No mapping found for object {object_id}. Cannot execute GET operation.")
        return
    
    mapping = object_mappings[object_id]
    fid = mapping['fid']
    public_url = mapping['public_url']
    original_content = mapping['content']
    original_hash = mapping['content_hash']
    
    url = f"http://{public_url}/{fid}"
    
    try:
        headers = {}
        is_range_request = False
        
        if range_start is not None and range_end is not None:
            print(f"With range: {range_start}-{range_end}")
            headers['Range'] = f'bytes={range_start}-{range_end}'
            is_range_request = True
        
        print(f"Sending GET request to {url}")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200 or response.status_code == 206:
            content_length = len(response.content)
            print(f"GET operation for {object_id} completed successfully")
            print(f"Received {content_length} bytes of data")
            
            # Save the response to a file for inspection or verification
            if range_start is not None and range_end is not None:
                output_path = f"./temp/{object_id}_range_{range_start}_{range_end}"
            else:
                output_path = f"./temp/{object_id}_full"
                
            with open(output_path, 'wb') as f:
                f.write(response.content)
            print(f"Saved response to {output_path}")
            
            # Verify content
            if is_range_request:
                # For range requests, compare with the corresponding portion of original content
                expected_content = original_content[range_start:range_end+1]
                print(f"Verifying range {range_start}-{range_end} (length: {len(expected_content)})")
                verify_content(expected_content, response.content, object_id, is_range=True)
            else:
                # For full requests, compare with the entire original content
                verify_content(original_content, response.content, object_id)
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
    
    mapping = object_mappings[object_id]
    fid = mapping['fid']
    public_url = mapping['public_url']
    
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
    parser = argparse.ArgumentParser(description='Execute operations from trace file with content verification.')
    parser.add_argument('trace_file', help='Path to trace file')
    parser.add_argument('--master', required=True, help='Master server address (e.g., http://localhost:9333)')
    parser.add_argument('--cleanup', action='store_true', help='Clean up temporary files after execution')
    
    args = parser.parse_args()
    
    print(f"Starting trace execution from {args.trace_file} with master {args.master}")
    print(f"Content verification is ENABLED")
    execute_trace(args.trace_file, args.master)
    
    # Clean up temp files if requested
    if args.cleanup:
        print("Cleaning up temporary files...")
        for file_path in Path("./temp").glob("*"):
            try:
                file_path.unlink()
                print(f"Removed {file_path}")
            except Exception as e:
                print(f"Failed to remove {file_path}: {e}")
    
    print("Trace execution completed")

if __name__ == "__main__":
    main()