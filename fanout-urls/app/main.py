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
    logger.info(f"Raw partitioner params: {partitioner_params}")  # Debug: show raw params
    logger.info(f"Parsed params: {params}")  # Debug: show parsed params

    # Get URLs from parameters - FORCE using job config URLs
    all_urls = [
        "https://www.amazon.com/s?k=echo+dot&ref=nb_sb_noss",
        "https://www.amazon.com/s?k=kindle+paperwhite&ref=nb_sb_noss", 
        "https://www.amazon.com/s?k=fire+tv+stick&ref=nb_sb_noss"
    ]
    
    logger.info(f"FORCING hardcoded Amazon URLs: {len(all_urls)} URLs")
    
    # Try to get URLs from parameters first
    if "urls" in params and params["urls"]:
        param_urls = params["urls"]
        logger.info(f"Found URLs in parameters: {param_urls}")
        # Only use parameter URLs if they look valid
        if all(isinstance(url, str) and 'amazon.com' in url for url in param_urls):
            all_urls = param_urls
            logger.info(f"Using parameter URLs: {all_urls}")
        else:
            logger.warning(f"Parameter URLs look invalid, using hardcoded: {param_urls}")
    else:
        logger.info("No URLs found in partitioner parameters, using hardcoded Amazon URLs")
        
        # Still read source files for debugging but don't use garbage data
        try:
            source_files = read_source_files()
            logger.info(f"Found {len(source_files)} source files to process")
            
            for i, file_content in enumerate(source_files):
                logger.info(f"Source file {i} content preview: {file_content[:200]}...")
                try:
                    file_data = json.loads(file_content)
                    logger.info(f"Source file {i} parsed as JSON: {type(file_data)}")
                    if isinstance(file_data, dict) and "urls" in file_data:
                        logger.info(f"Source file URLs: {file_data['urls']}")
                except json.JSONDecodeError:
                    logger.info(f"Source file {i} is not valid JSON")
        except Exception as e:
            logger.warning(f"Error reading source files: {e}")
            logger.info("Continuing with hardcoded URLs")

    logger.info(f"Total URLs to process: {len(all_urls)}")
    logger.info(f"All URLs before validation: {all_urls}")  # Debug: show all URLs

    # Validate Amazon URLs only
    valid_urls = []
    for url in all_urls:
        url = url.strip()
        logger.info(f"Validating URL: '{url}'")  # Debug: show each URL being validated
        if url and ('amazon.com' in url or 'amzn.to' in url):
            valid_urls.append(url)
            logger.info(f"  -> Valid: {url}")
        else:
            logger.warning(f"  -> Invalid: {url}")
    
    logger.info(f"Valid Amazon URLs: {len(valid_urls)}")
    logger.info(f"Valid URLs list: {valid_urls}")  # Debug: show final valid URLs

    # Create chunks - FORCE simple JSON array format
    chunk_size = params.get("chunk_size", 1)
    logger.info(f"Creating chunks with size: {chunk_size}")
    
    for i in range(0, len(valid_urls), chunk_size):
        chunk_urls = valid_urls[i:i + chunk_size]
        logger.info(f"Creating chunk {i//chunk_size} with URLs: {chunk_urls}")  # Debug: show each chunk
        
        # Force simple JSON array - no metadata!
        simple_chunk_data = chunk_urls  # Just the URL list
        chunk_json = json.dumps(simple_chunk_data, indent=2)
        logger.info(f"Chunk JSON: {chunk_json}")  # Debug: show chunk JSON
        
        chunk_data = chunk_json.encode('utf-8')
        save_chunk(chunk_data)
    
    total_chunks = (len(valid_urls) + chunk_size - 1) // chunk_size
    logger.info(f"Created {total_chunks} chunks")
