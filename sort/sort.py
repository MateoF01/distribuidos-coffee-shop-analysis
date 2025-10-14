import os
import signal
import sys
import threading
import struct
import configparser
import csv
import heapq
import time
from middleware.coffeeMiddleware import CoffeeMessageMiddlewareQueue
from shared.worker import FileProcessingWorker
from shared import protocol

class SorterConfig:
    def __init__(self, config_parser, data_type):
        """Initialize configuration from ConfigParser object"""
        # Sort configuration
        self.chunk_size = int(config_parser['sort']['chunk_size'])
        self.temp_file_suffix = config_parser['sort']['temp_file_suffix']
        self.temp_file_prefix = config_parser['sort']['temp_file_prefix']
        
        # Message configuration
        self.header_size = int(config_parser['messages']['header_size'])
        self.sort_signal_type = int(config_parser['messages']['sort_signal_type'])
        self.sort_signal_data_type = int(config_parser['messages']['sort_signal_data_type'])
        
        # File configuration
        self.encoding = config_parser['files']['encoding']
        self.newline_mode = config_parser['files']['newline_mode'] or None
        
        # Sorting configuration
        self.invalid_number_fallback = float(config_parser['sorting']['invalid_number_fallback'])
        
        # Data type specific configuration
        if data_type not in config_parser:
            raise ValueError(f"Unknown data type: {data_type}")
        self.sort_column_index = int(config_parser[data_type]['sort_column_index'])
        
        # Multi-column sorting support
        if 'sort_columns' in config_parser[data_type]:
            sort_columns_str = config_parser[data_type]['sort_columns']
            self.sort_columns = [int(col.strip()) for col in sort_columns_str.split(',')]
        else:
            self.sort_columns = [self.sort_column_index]
        
        # Validate configuration
        self._validate()
    
    def _validate(self):
        """Validate configuration parameters"""
        if self.chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {self.chunk_size}")
        
        if self.header_size <= 0:
            raise ValueError(f"header_size must be positive, got {self.header_size}")
        
        if not self.temp_file_suffix:
            raise ValueError("temp_file_suffix cannot be empty")
        
        if not self.temp_file_prefix:
            raise ValueError("temp_file_prefix cannot be empty")
        
        if not self.encoding:
            raise ValueError("encoding cannot be empty")
        
        if self.sort_column_index < 0:
            raise ValueError(f"sort_column_index must be non-negative, got {self.sort_column_index}")
    
    def __str__(self):
        """String representation for debugging"""
        return (f"SorterConfig(chunk_size={self.chunk_size}, "
                f"temp_file_suffix='{self.temp_file_suffix}', "
                f"temp_file_prefix='{self.temp_file_prefix}', "
                f"header_size={self.header_size}, "
                f"sort_signal_type={self.sort_signal_type}, "
                f"encoding='{self.encoding}', "
                f"invalid_number_fallback={self.invalid_number_fallback}, "
                f"sort_column_index={self.sort_column_index})")

class Sorter(FileProcessingWorker):
    def __init__(self, queue_in, input_file, output_file, rabbitmq_host, config: SorterConfig, completion_queue=None):
        # Initialize with completion_queue as output if provided
        queue_out = completion_queue if completion_queue else None
        super().__init__(queue_in, queue_out, rabbitmq_host, input_file=input_file, output_file=output_file)
        self.sort_column_index = config.sort_column_index
        self.config = config
        self.completion_queue_name = completion_queue
        self._temp_files = []

    def _write_sorted_chunk(self, chunk_data):
        """Write a sorted chunk to a temporary file"""
        try:
            # Sort the chunk by the specified columns (string/lexicographic sorting)
            if len(self.config.sort_columns) > 1:
                # Multi-column sorting
                chunk_data.sort(key=lambda row: tuple(row[col] for col in self.config.sort_columns))
            else:
                # Single column sorting (backward compatibility)
                chunk_data.sort(key=lambda row: row[self.sort_column_index])
            
            # Create temporary file manually
            timestamp = str(int(time.time() * 1000000))  # microsecond precision
            chunk_id = len(self._temp_files)
            temp_path = f"{self.config.temp_file_prefix}{timestamp}_{chunk_id}{self.config.temp_file_suffix}"
            
            self._temp_files.append(temp_path)
            
            with open(temp_path, 'w', newline=self.config.newline_mode, encoding=self.config.encoding) as temp_file:
                writer = csv.writer(temp_file)
                writer.writerows(chunk_data)
            
            print(f"Wrote sorted chunk with {len(chunk_data)} rows to {temp_path}")
            
        except Exception as e:
            print(f"Error writing sorted chunk: {e}")

    def _merge_sorted_files(self, header):
        """Merge all sorted temporary files into the final output file"""
        if not self._temp_files:
            return
            
        try:
            # Open all temporary files
            file_readers = []
            
            for temp_file in self._temp_files:
                f = open(temp_file, 'r', newline=self.config.newline_mode, encoding=self.config.encoding)
                reader = csv.reader(f)
                file_readers.append((reader, f))
            
            # Create heap with first row from each file
            heap = []
            
            for i, (reader, f) in enumerate(file_readers):
                try:
                    row = next(reader)
                    if len(self.config.sort_columns) > 1:
                        # Multi-column sorting key
                        sort_value = tuple(row[col] for col in self.config.sort_columns)
                    else:
                        # Single column sorting (backward compatibility)
                        sort_value = row[self.sort_column_index]
                    heapq.heappush(heap, (sort_value, i, row))
                except StopIteration:
                    f.close()
            
            # Write merged data to final file
            with open(self.output_file, 'w', newline=self.config.newline_mode, encoding=self.config.encoding) as output_file:
                writer = csv.writer(output_file)
                # Write header
                writer.writerow(header)
                
                # Merge sorted files
                while heap:
                    sort_value, file_idx, row = heapq.heappop(heap)
                    writer.writerow(row)
                    
                    # Get next row from the same file
                    reader, f = file_readers[file_idx]
                    try:
                        next_row = next(reader)
                        if len(self.config.sort_columns) > 1:
                            # Multi-column sorting key
                            next_sort_value = tuple(next_row[col] for col in self.config.sort_columns)
                        else:
                            # Single column sorting (backward compatibility)
                            next_sort_value = next_row[self.sort_column_index]
                        heapq.heappush(heap, (next_sort_value, file_idx, next_row))
                    except StopIteration:
                        f.close()
            
            # Close remaining files and cleanup
            for reader, f in file_readers:
                if not f.closed:
                    f.close()
                    
            # Remove temporary files
            for temp_file in self._temp_files:
                try:
                    os.unlink(temp_file)
                except OSError:
                    pass
            
            print(f"Successfully merged {len(self._temp_files)} sorted chunks into {self.output_file}")
            
        except Exception as e:
            print(f"Error merging sorted files: {e}")

    def _process_file(self):
        """Sort the input file using external merge sort approach"""
        try:
            if not os.path.exists(self.input_file):
                print(f"Input file {self.input_file} does not exist")
                return
            
            # Read and process file in chunks
            current_chunk = []
            header = None
            
            with open(self.input_file, 'r', newline=self.config.newline_mode, encoding=self.config.encoding) as csvfile:
                reader = csv.reader(csvfile)
                header = next(reader)  # Read header
                
                for row in reader:
                    current_chunk.append(row)
                    
                    # If chunk is full, write it as a sorted temporary file
                    if len(current_chunk) >= self.config.chunk_size:
                        self._write_sorted_chunk(current_chunk)
                        current_chunk = []
                
                # Process any remaining rows
                if current_chunk:
                    self._write_sorted_chunk(current_chunk)
            
            # Merge all sorted chunks
            self._merge_sorted_files(header)
            
            if len(self.config.sort_columns) > 1:
                print(f"Successfully sorted {self.input_file} by columns {self.config.sort_columns} and saved to {self.output_file}")
            else:
                print(f"Successfully sorted {self.input_file} by column {self.sort_column_index} and saved to {self.output_file}")
            
            # Send completion signal
            self._send_completion_signal()
            
        except Exception as e:
            print(f"Error sorting file: {e}")

    def _send_completion_signal(self):
        """Send a completion signal to notify that sorting is done"""
        try:
            if self.out_queues:
                # Create completion signal message
                message = protocol.create_notification_message(0, b'', self.current_request_id)
                self.out_queues[0].send(message)
                print(f"Sent completion signal for {self.output_file} with request_id={self.current_request_id}")
            else:
                print("No completion queue configured")
        except Exception as e:
            print(f"Error sending completion signal: {e}")

    def _process_message(self, message, msg_type, data_type, request_id, timestamp, payload, queue_name=None):
        """Process sort signal messages"""
        if msg_type == self.config.sort_signal_type:
            # Store request_id from the sort signal for use in completion message
            self.current_request_id = request_id
            print(f'Sort signal received for file: {self.input_file} with request_id={request_id}')
            self._process_file()
            print(f'Sort complete. Output saved to: {self.output_file}')
        else:
            print(f"Received unexpected message type: {msg_type}/{data_type}")
    
    def _validate_message(self, message):
        """Override to use custom header size"""
        if not isinstance(message, bytes) or len(message) < self.config.header_size:
            print(f"Invalid message format or too short: {message}")
            return False
        return True
        if self.completion_queue:
            self.completion_queue.close()

if __name__ == '__main__':
    def create_sorter():
        # Configure via environment variables
        queue_in = os.environ.get('QUEUE_IN')
        input_file = os.environ.get('INPUT_FILE')
        output_file = os.environ.get('OUTPUT_FILE')
        data_type = os.environ.get('DATA_TYPE')
        completion_queue = os.environ.get('COMPLETION_QUEUE')
        rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')

        if not all([queue_in, input_file, output_file, data_type]):
            raise ValueError("Missing required environment variables: QUEUE_IN, INPUT_FILE, OUTPUT_FILE, DATA_TYPE")

        # Load configuration
        config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
        config_parser = configparser.ConfigParser()
        config_parser.read(config_path)
        
        # Create configuration object
        config = SorterConfig(config_parser, data_type)
        print(f"Loaded configuration: {config}")
        
        return Sorter(queue_in, input_file, output_file, rabbitmq_host, config, completion_queue)
    
    # Use the Worker base class main entry point
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
    Sorter.run_worker_main(create_sorter, config_path)
