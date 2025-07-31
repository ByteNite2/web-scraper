# === BYTENITE PARTITIONER - MAIN SCRIPT ===

import json
import os
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
source_dir = os.getenv("SOURCE_DIR")
if source_dir is None:
    raise ValueError("Environment variable 'SOURCE_DIR' is not set.")
if not os.path.isdir(source_dir):
    raise ValueError(f"Source directory '{source_dir}' does not exist or is not accessible.")

chunks_dir = os.getenv("CHUNKS_DIR")
if not chunks_dir:
    raise ValueError("Environment variable 'CHUNKS_DIR' is not set.")
if not os.path.isdir(chunks_dir):
    raise ValueError(f"Chunks directory '{chunks_dir}' does not exist or is not a directory.")
if not os.access(chunks_dir, os.W_OK):
    raise ValueError(f"Chunks directory '{chunks_dir}' is not writable.")

# Chunk file naming convention
chunk_file_naming = "data_{chunk_index}.bin"

# Partitioner parameters
try:
    partitioner_params = os.getenv("PARTITIONER_PARAMS")
    if not partitioner_params:
        raise ValueError("Environment variable 'PARTITIONER_PARAMS' is not set.")
    params = json.loads(partitioner_params)
except json.JSONDecodeError:
    raise ValueError("Environment variable 'PARTITIONER_PARAMS' contains invalid JSON.")

def read_source_files():
    """Read all files from source directory."""
    source_files = []
    try:
        for filename in os.listdir(source_dir):
            file_path = os.path.join(source_dir, filename)
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    source_files.append(file.read())
        return source_files
    except OSError as e:
        raise RuntimeError(f"Error accessing source directory '{source_dir}': {e}")

def save_chunk(data):
    """Save chunk data with automatic index."""
    chunk_pattern = re.compile(re.escape(chunk_file_naming).replace(r"\{chunk_index\}", r"(\d+)"))
    
    existing_files = (
        f for f in os.listdir(chunks_dir)
        if os.path.isfile(os.path.join(chunks_dir, f)) and chunk_pattern.match(f)
    )
    chunk_indices = []
    for f in existing_files:
        match = chunk_pattern.match(f)
        if match:
            chunk_indices.append(int(match.group(1)))
    
    next_chunk_index = sorted(chunk_indices)[-1] + 1 if chunk_indices else 0
    output_path = os.path.join(chunks_dir, chunk_file_naming.format(chunk_index=next_chunk_index))
    
    try:
        with open(output_path, "wb") as outfile:
            outfile.write(data)
        logger.info(f"Chunk {next_chunk_index} written to {output_path}")
    except (IOError, OSError) as e:
        raise RuntimeError(f"Failed to write chunk {next_chunk_index} to {output_path}: {e}")

if __name__ == "__main__":
    logger.info("Partitioner task started")

    # Get URLs from parameters or source files
    all_urls = []
    
    if "urls" in params:
        all_urls = params["urls"]
        logger.info(f"Using URLs from parameters: {len(all_urls)} URLs")
    else:
        source_files = read_source_files()
        logger.info(f"Found {len(source_files)} source files")
        
        for file_content in source_files:
            try:
                file_data = json.loads(file_content)
                if isinstance(file_data, list):
                    all_urls.extend(file_data)
                elif "urls" in file_data:
                    all_urls.extend(file_data["urls"])
            except json.JSONDecodeError:
                lines = file_content.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and line.startswith('http'):
                        all_urls.append(line)

    logger.info(f"Total URLs to process: {len(all_urls)}")

    # Validate Amazon URLs only
    valid_urls = []
    for url in all_urls:
        url = url.strip()
        if url and ('amazon.com' in url or 'amzn.to' in url):
            valid_urls.append(url)
    
    logger.info(f"Valid Amazon URLs: {len(valid_urls)}")

    # Create chunks
    chunk_size = params.get("chunk_size", 1)
    
    for i in range(0, len(valid_urls), chunk_size):
        chunk_urls = valid_urls[i:i + chunk_size]
        chunk_data = json.dumps(chunk_urls).encode('utf-8')
        save_chunk(chunk_data)
    
    total_chunks = (len(valid_urls) + chunk_size - 1) // chunk_size
    logger.info(f"Created {total_chunks} chunks")
