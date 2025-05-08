import time
import socket
import argparse
import urllib.request
import gzip
from io import BytesIO

def stream_log_data(log_url='ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz', 
                    target_host='localhost', target_port=9999, delay=0.1):
    """
    Downloads and decompresses the NASA-HTTP web server log dataset, then streams each log entry 
    to a network socket for Flume to process.
    
    Args:
        log_url: URL of the NASA-HTTP log file (gzip compressed)
        target_host: Hostname where Flume agent is listening
        target_port: Port where Flume agent is listening
        delay: Delay between sending each log entry (in seconds)
    """
    try:
        # Download the log file using urllib for FTP
        response = urllib.request.urlopen(log_url)
        compressed_data = BytesIO(response.read())
        decompressed_data = gzip.GzipFile(fileobj=compressed_data).read().decode('utf-8', errors='replace')
        
        # Split the data into log entries
        logs = decompressed_data.splitlines()
        if not logs:
            print("Error: No log data received from URL")
            return
              
        # Open socket connection to Flume
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((target_host, target_port))
        
        print(f"Connected to {target_host}:{target_port}")
        print(f"Streaming log data from: {log_url}")
        
        for log_entry in logs:
            if log_entry.strip():  # Skip empty lines
                try:
                    # Ensure the log entry can be encoded as UTF-8 before sending
                    log_entry.encode('utf-8')
                    sock.send((log_entry.strip() + '\n').encode('utf-8'))
                    print(f"Sent: {log_entry.strip()}")
                except UnicodeEncodeError:
                    print(f"Skipping invalid log entry: {log_entry[:50]}...")
                # Add delay to simulate real-time streaming
                time.sleep(delay)
                  
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'sock' in locals():
            sock.close()
            print("Connection closed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stream NASA-HTTP web server logs to Flume')
    parser.add_argument('--log_url', default='ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz', 
                        help='URL of the NASA-HTTP log file')
    parser.add_argument('--host', default='localhost', help='Host where Flume is listening')
    parser.add_argument('--port', type=int, default=9999, help='Port where Flume is listening')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between log entries (seconds)')
    
    args = parser.parse_args()
    stream_log_data(args.log_url, args.host, args.port, args.delay)
