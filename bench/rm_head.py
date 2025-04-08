import sys
import os

def remove_head_methods(input_file, output_file=None):
    # If no output file specified, create a name based on the input file
    if output_file is None:
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_no_head{ext}"
    
    try:
        # Count lines for statistics
        total_lines = 0
        removed_lines = 0
        
        # Open input file for reading and output file for writing
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                total_lines += 1
                fields = line.strip().split()
                
                # Check if this is a line with a method containing "HEAD"
                if len(fields) >= 2 and "HEAD" in fields[1]:
                    removed_lines += 1
                    continue  # Skip this line
                
                # Write lines without HEAD to the output file
                outfile.write(line)
        
        print(f"Processing complete!")
        print(f"Total lines processed: {total_lines}")
        print(f"Lines removed: {removed_lines}")
        print(f"Remaining lines: {total_lines - removed_lines}")
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
    
    remove_head_methods(input_file, output_file)