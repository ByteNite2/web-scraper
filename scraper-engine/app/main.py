#!/usr/bin/env python3
"""
ByteNite Web Scraper App - Main Script
Scrapes Amazon search results for product data using Playwright
"""

import json
import os
import asyncio
import re
import logging
from playwright.async_api import async_playwright

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables (set by ByteNite platform)
chunks_dir = os.getenv("CHUNKS_DIR")
if not chunks_dir:
    logger.error("Environment variable 'CHUNKS_DIR' is not set. This app must be run within ByteNite framework.")
    logger.info("Available environment variables:")
    for key, value in os.environ.items():
        if 'BYTE' in key.upper() or 'CHUNK' in key.upper() or 'TASK' in key.upper():
            logger.info(f"  {key}={value}")
    raise ValueError("Environment variable 'CHUNKS_DIR' is not set.")

if not os.path.isdir(chunks_dir):
    logger.error(f"Chunks directory '{chunks_dir}' does not exist or is not accessible.")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Directory contents: {os.listdir('.')}")
    raise ValueError(f"Chunks directory '{chunks_dir}' does not exist or is not accessible.")

task_results_dir = os.getenv("TASK_RESULTS_DIR")
if not task_results_dir:
    logger.error("Environment variable 'TASK_RESULTS_DIR' is not set.")
    raise ValueError("Environment variable 'TASK_RESULTS_DIR' is not set.")

if not os.path.isdir(task_results_dir):
    logger.error(f"Task results directory '{task_results_dir}' does not exist or is not accessible.")
    raise ValueError(f"Task results directory '{task_results_dir}' does not exist or is not accessible.")

# App parameters
try:
    app_params = os.getenv("APP_PARAMS")
    if not app_params:
        logger.error("Environment variable 'APP_PARAMS' is not set.")
        raise ValueError("Environment variable 'APP_PARAMS' is not set.")
    params = json.loads(app_params)
    logger.info(f"App parameters loaded: {params}")
except json.JSONDecodeError as e:
    logger.error(f"Environment variable 'APP_PARAMS' contains invalid JSON: {e}")
    raise ValueError("Environment variable 'APP_PARAMS' contains invalid JSON.")

async def scrape_amazon_search(url, params):
    """Scrape Amazon search results page for products."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=params.get("headless", True))
        page = await browser.new_page()
        
        try:
            await page.goto(url, timeout=params.get("timeout", 30000))
            await page.wait_for_timeout(3000)
            
            # Find all product containers on search results
            products = []
            product_elements = page.locator('[data-component-type="s-search-result"]')
            
            if await product_elements.count() == 0:
                product_elements = page.locator('.s-result-item')
                
            if await product_elements.count() == 0:
                logger.warning(f"No products found on page: {url}")
                await browser.close()
                return []
            
            logger.info(f"Found {await product_elements.count()} products on search page")
            
            # Extract data from each product (max 10 for performance)
            for i in range(min(10, await product_elements.count())):
                product = product_elements.nth(i)
                
                # Extract title using improved method
                title = "Unknown"
                try:
                    all_text = await product.text_content()
                    if all_text:
                        # Look for product title patterns
                        patterns = [
                            r'(Echo [^,]+(?:, [^,]+)*)',
                            r'(Kindle [^,]+(?:, [^,]+)*)', 
                            r'(Fire [^,]+(?:, [^,]+)*)',
                            r'(Amazon [^,]+(?:, [^,]+)*)'
                        ]
                        for pattern in patterns:
                            match = re.search(pattern, all_text)
                            if match:
                                title = match.group(1).strip()
                                # Clean up title (remove ratings, etc.)
                                title = re.sub(r'\s+\d+\.\d+\s+out\s+of\s+\d+.*', '', title)
                                break
                except:
                    pass
                
                # Extract price
                price = "Unknown"
                price_numeric = 999999
                try:
                    price_elem = product.locator('.a-price .a-offscreen').first
                    if await price_elem.count() > 0:
                        price_text = await price_elem.text_content()
                        if price_text:
                            price = price_text.strip()
                            # Extract numeric price
                            price_match = re.search(r'[\$](\d+(?:\.\d{2})?)', price)
                            if price_match:
                                price_numeric = float(price_match.group(1))
                except:
                    pass
                
                # Extract purchase/sales data
                purchases = 0
                try:
                    all_text = await product.text_content()
                    if all_text:
                        # Look for "10K+ bought in past month" pattern
                        k_bought_match = re.search(r'(\d+)K\+?\s+bought', all_text, re.IGNORECASE)
                        if k_bought_match:
                            purchases = int(k_bought_match.group(1)) * 1000
                        else:
                            # Look for other purchase patterns
                            purchase_patterns = [
                                r'(\d+(?:,\d+)*)\s+bought',
                                r'(\d+(?:,\d+)*)\s+purchased',
                                r'(\d+(?:,\d+)*)\s+ratings?',
                                r'(\d+(?:,\d+)*)\s+reviews?'
                            ]
                            
                            for pattern in purchase_patterns:
                                matches = re.findall(pattern, all_text, re.IGNORECASE)
                                if matches:
                                    num_str = matches[0].replace(',', '')
                                    try:
                                        purchases = int(num_str)
                                        if purchases > 0:
                                            break
                                    except:
                                        continue
                except:
                    pass
                
                if title != "Unknown" and price != "Unknown":
                    products.append({
                        "url": url,
                        "title": title,
                        "price": price,
                        "price_numeric": price_numeric,
                        "buyers": purchases,
                        "error": None
                    })
            
            await browser.close()
            return products
            
        except Exception as e:
            await browser.close()
            logger.error(f"Error scraping {url}: {e}")
            return [{
                "url": url,
                "title": "Error",
                "price": "Error",
                "buyers": 0,
                "error": str(e)
            }]

async def process_chunks():
    """Process all chunk files and scrape URLs."""
    chunk_files = [f for f in os.listdir(chunks_dir) if f.startswith('data_') and f.endswith('.bin')]
    
    if not chunk_files:
        logger.error("No chunk files found!")
        return
    
    logger.info(f"Processing {len(chunk_files)} chunk files")
    
    for chunk_file in chunk_files:
        chunk_path = os.path.join(chunks_dir, chunk_file)
        
        # Read chunk data
        with open(chunk_path, 'rb') as f:
            urls = json.loads(f.read().decode('utf-8'))
        
        logger.info(f"Processing chunk {chunk_file} with {len(urls)} URLs")
        
        scraped_items = []
        for url in urls:
            logger.info(f"Scraping: {url}")
            
            items = await scrape_amazon_search(url, params)
            scraped_items.extend(items)
            
            # Respect delay between requests
            delay = params.get("delay_between_requests", 2)
            if delay > 0:
                await asyncio.sleep(delay)
        
        # Save results
        chunk_number = chunk_file.replace('data_', '').replace('.bin', '')
        result = {
            "chunk_number": chunk_number,
            "urls_processed": len(urls),
            "scraped_items": scraped_items,
            "success_count": len([item for item in scraped_items if not item.get("error")]),
            "error_count": len([item for item in scraped_items if item.get("error")])
        }
        
        result_file = os.path.join(task_results_dir, f"chunk_{chunk_number}_results.json")
        with open(result_file, 'w') as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"Chunk {chunk_number}: {result['success_count']} success, {result['error_count']} errors")

if __name__ == "__main__":
    logger.info("Web scraper app started")
    asyncio.run(process_chunks())
    logger.info("Web scraper app completed")
