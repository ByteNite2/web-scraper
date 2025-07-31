# === BYTENITE ASSEMBLER - MAIN SCRIPT ===

import json
import os
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables
task_results_dir = os.getenv("TASK_RESULTS_DIR")
if task_results_dir is None:
    raise ValueError("Environment variable 'TASK_RESULTS_DIR' is not set.")
if not os.path.isdir(task_results_dir):
    raise ValueError(f"Task result directory '{task_results_dir}' does not exist or is not accessible.")

output_dir = os.getenv("OUTPUT_DIR")
if not output_dir:
    raise ValueError("Environment variable 'OUTPUT_DIR' is not set.")
if not os.path.isdir(output_dir):
    raise ValueError(f"Output directory '{output_dir}' does not exist or is not a directory.")
if not os.access(output_dir, os.W_OK):
    raise ValueError(f"Output directory '{output_dir}' is not writable.")

# Assembler parameters
try:
    assembler_params = os.getenv("ASSEMBLER_PARAMS")
    if not assembler_params:
        raise ValueError("Environment variable 'ASSEMBLER_PARAMS' is not set.")
    params = json.loads(assembler_params)
except json.JSONDecodeError:
    raise ValueError("Environment variable 'ASSEMBLER_PARAMS' contains invalid JSON.")

def read_result_files():
    """Read all result files from task results directory."""
    result_files = []
    try:
        for filename in os.listdir(task_results_dir):
            file_path = os.path.join(task_results_dir, filename)
            if os.path.isfile(file_path):
                with open(file_path, "r") as file:
                    result_files.append(file.read())
        return result_files
    except OSError as e:
        raise RuntimeError(f"Error accessing source directory '{task_results_dir}': {e}")

def find_cheapest_item(all_items):
    """Find the cheapest item based on price."""
    cheapest = None
    min_price = float('inf')
    
    for item in all_items:
        if item.get("error"):
            continue
            
        # Get price value - try multiple field names
        price_numeric = (item.get("price_numeric") or 
                        item.get("price_value") or 
                        999999)  # High default
        
        # Also try parsing price string if numeric not available
        if price_numeric == 999999 and item.get("price"):
            price_str = item.get("price", "")
            price_match = re.search(r'\$(\d+(?:\.\d{2})?)', price_str)
            if price_match:
                try:
                    price_numeric = float(price_match.group(1))
                except:
                    price_numeric = 999999
        
        if isinstance(price_numeric, (int, float)) and price_numeric < min_price:
            min_price = price_numeric
            cheapest = item
    
    return cheapest, min_price

if __name__ == "__main__":
    logger.info("Assembler task started")

    # Read all result files
    result_file_contents = read_result_files()
    logger.info(f"Found {len(result_file_contents)} result files to process")
    
    all_items = []
    
    # Parse all scraped items from chunks
    for file_content in result_file_contents:
        try:
            chunk_result = json.loads(file_content)
            scraped_items = chunk_result.get("scraped_items", [])
            all_items.extend(scraped_items)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing result file: {e}")
            continue

    logger.info(f"Total items collected: {len(all_items)}")

    # Find the cheapest item
    cheapest, min_price = find_cheapest_item(all_items)
    
    # Create final result
    final_result = {
        "total_items_analyzed": len(all_items),
        "cheapest_item": cheapest,
        "min_price": min_price,
        "summary": {
            "title": cheapest.get("title", "Unknown") if cheapest else "No items found",
            "price": cheapest.get("price", "Unknown") if cheapest else "N/A",
            "price_value": min_price if cheapest else 0,
            "url": cheapest.get("url", "") if cheapest else ""
        }
    }
    
    # Save the result
    output_path = os.path.join(output_dir, "cheapest_item.json")
    with open(output_path, "w") as outfile:
        json.dump(final_result, outfile, indent=2)
    
    logger.info(f"Cheapest item analysis completed")
    if cheapest:
        logger.info(f"Winner: '{cheapest.get('title', 'Unknown')}' at ${min_price}")
    else:
        logger.info("No items found")
