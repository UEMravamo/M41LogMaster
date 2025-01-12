import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/../')
from real_time import real_time
def main():
    input_file = './input-file-10000.txt'
    hostname = 'Prueba'
    real_time(input_file, hostname)
    
if __name__ == "__main__":
    main()