import sys

def filter_invalid_operations(input_file, output_file=None):
    # If no output file specified, create a name based on the input file
    if output_file is None:
        import os
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_filtered{ext}"
    
    # Map to track existing objects
    existing_objects = {}
    
    # Statistics counters
    stats = {
        "total": 0,
        "filtered_out": 0,
        "put": 0,
        "get_valid": 0,
        "get_invalid": 0,
        "delete_valid": 0,
        "delete_invalid": 0
    }
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                stats["total"] += 1
                fields = line.strip().split()
                
                if len(fields) < 3:
                    continue  # Skip malformed lines
                
                operation = fields[1]
                object_id = fields[2]
                
                # Handle PUT operations - always add to map and output
                if "PUT" in operation:
                    existing_objects[object_id] = True
                    outfile.write(line)
                    stats["put"] += 1
                
                # Handle DELETE operations - only output if object exists
                elif "DELETE" in operation:
                    if object_id in existing_objects:
                        del existing_objects[object_id]
                        outfile.write(line)
                        stats["delete_valid"] += 1
                    else:
                        stats["delete_invalid"] += 1
                        stats["filtered_out"] += 1
                
                # Handle GET operations - only output if object exists
                elif "GET" in operation:
                    if object_id in existing_objects:
                        outfile.write(line)
                        stats["get_valid"] += 1
                    else:
                        stats["get_invalid"] += 1
                        stats["filtered_out"] += 1
                
                # For any other operation types, just write them through
                else:
                    outfile.write(line)
        
        # Print statistics
        print(f"Processing complete!")
        print(f"Total lines processed: {stats['total']}")
        print(f"Lines filtered out: {stats['filtered_out']}")
        print(f"PUT operations: {stats['put']}")
        print(f"Valid GET operations: {stats['get_valid']}")
        print(f"Invalid GET operations (filtered out): {stats['get_invalid']}")
        print(f"Valid DELETE operations: {stats['delete_valid']}")
        print(f"Invalid DELETE operations (filtered out): {stats['delete_invalid']}")
        print(f"Output written to: {output_file}")
        
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
    else:
        input_file = input("Enter the path to your trace file: ")
        output_file = input("Enter the path for output file (leave empty for automatic naming): ").strip() or None
    
    filter_invalid_operations(input_file, output_file)