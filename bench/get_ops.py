import sys

def find_unique_methods(file_path):
    methods = set()
    
    try:
        with open(file_path, 'r') as file:
            for line in file:
                fields = line.strip().split()
                if len(fields) >= 2:
                    methods.add(fields[1])  # Add the method (second field) to the set
        
        print("Unique methods found:")
        for method in sorted(methods):
            print(f"- {method}")
            
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        file_path = input("Enter the path to your trace file: ")
    
    find_unique_methods(file_path)