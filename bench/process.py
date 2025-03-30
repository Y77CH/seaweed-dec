def process_traces(file_path):
    # Dictionary to store the most recent PUT for each object_id
    most_recent_puts = {}
    
    # List to store the result (matched PUT-DELETE pairs)
    result = []
    
    # First pass: process all traces and identify the most recent PUTs
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split()
            if len(parts) >= 3:
                timestamp = int(parts[0])
                operation = parts[1]
                object_id = parts[2]
                
                if "PUT" in operation:
                    # Store this as the most recent PUT for this object_id
                    most_recent_puts[object_id] = {
                        'timestamp': timestamp,
                        'operation': operation,
                        'object_id': object_id,
                        'line': line
                    }
    
    # Second pass: find DELETEs and match with their most recent PUTs
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split()
            if len(parts) >= 3:
                timestamp = int(parts[0])
                operation = parts[1]
                object_id = parts[2]
                
                if "DELETE" in operation:
                    # If we have a matching PUT, add the pair to the result
                    if object_id in most_recent_puts:
                        result.append(most_recent_puts[object_id])
                        result.append({
                            'timestamp': timestamp,
                            'operation': operation,
                            'object_id': object_id,
                            'line': line
                        })
    
    # Sort the result by timestamp
    result.sort(key=lambda x: x['timestamp'])
    
    # Return the formatted result as a list of lines
    return [trace['line'] for trace in result]

def main():
    input_file = 'IBMObjectStoreTrace002Part0'
    output_file = input_file + '_PUT_DEL'
    
    try:
        matched_traces = process_traces(input_file)
        
        # Write the result to the output file
        with open(output_file, 'w') as file:
            for line in matched_traces:
                file.write(line + '\n')
        
        print(f"Processing complete. Matched traces written to {output_file}")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()