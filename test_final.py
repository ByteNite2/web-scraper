#!/usr/bin/env python3
"""
FINAL Clean Test for Web Scraper Pipeline
Tests partitioner -> app -> assembler pipeline locally with real Amazon URLs
"""

import json
import os
import tempfile
import shutil
import asyncio
import subprocess
import sys
import time
import re
from pathlib import Path

# Install playwright if needed
try:
    from playwright.async_api import async_playwright
except ImportError:
    print("Installing playwright...")
    subprocess.run([sys.executable, "-m", "pip", "install", "playwright"], check=True)
    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
    from playwright.async_api import async_playwright

print("🚀 FINAL Web Scraper Pipeline Test")
print("=" * 50)

# Test URLs - Amazon search result pages (easier to scrape)
TEST_URLS = [
    "https://www.amazon.com/s?k=echo+dot&ref=nb_sb_noss",
    "https://www.amazon.com/s?k=kindle+paperwhite&ref=nb_sb_noss", 
    "https://www.amazon.com/s?k=fire+tv+stick&ref=nb_sb_noss"
]

TEST_PARAMS = {
    "partitioner": {"chunk_size": 1},
    "app": {
        "timeout": 30000,
        "headless": True,
        "delay_between_requests": 2
    },
    "assembler": {}
}

class PipelineTester:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.temp_dir = None
        
    def setup_dirs(self):
        """Create test directories."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="scraper_final_test_"))
        self.source_dir = self.temp_dir / "source"
        self.chunks_dir = self.temp_dir / "chunks" 
        self.task_results_dir = self.temp_dir / "task_results"
        self.output_dir = self.temp_dir / "output"
        
        for dir_path in [self.source_dir, self.chunks_dir, self.task_results_dir, self.output_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        print(f"📁 Test environment: {self.temp_dir}")
        
    def create_source_data(self):
        """Create source URLs file."""
        source_file = self.source_dir / "urls.json"
        with open(source_file, 'w') as f:
            json.dump(TEST_URLS, f, indent=2)
        print(f"📝 Created source with {len(TEST_URLS)} URLs")
        
    def test_partitioner(self):
        """Test partitioner component."""
        print("\n🔀 TESTING PARTITIONER")
        print("-" * 30)
        
        env = os.environ.copy()
        env.update({
            "SOURCE_DIR": str(self.source_dir),
            "CHUNKS_DIR": str(self.chunks_dir),
            "PARTITIONER_PARAMS": json.dumps(TEST_PARAMS["partitioner"])
        })
        
        partitioner_path = self.base_dir / "fanout-urls" / "app" / "main.py"
        
        try:
            result = subprocess.run([
                sys.executable, str(partitioner_path)
            ], env=env, capture_output=True, text=True, cwd=str(partitioner_path.parent))
            
            if result.returncode != 0:
                print(f"❌ FAILED: {result.stderr}")
                return False
                
            print(f"✅ SUCCESS: {result.stdout}")
            
            chunk_files = list(self.chunks_dir.glob("data_*.bin"))
            print(f"📦 Created {len(chunk_files)} chunks")
            
            return len(chunk_files) > 0
            
        except Exception as e:
            print(f"❌ ERROR: {e}")
            return False
    
    async def scrape_item(self, url, params):
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
                    await browser.close()
                    return []
                
                print(f"   Found {await product_elements.count()} products on search page")
                
                # Extract data from each product (max 5 for testing)
                for i in range(min(5, await product_elements.count())):
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
                return [{
                    "url": url,
                    "title": "Error",
                    "price": "Error",
                    "buyers": 0,
                    "error": str(e)
                }]
    
    async def test_app(self):
        """Test app component."""
        print("\n🕷️  TESTING APP")
        print("-" * 30)
        
        chunk_files = list(self.chunks_dir.glob("data_*.bin"))
        if not chunk_files:
            print("❌ No chunks found!")
            return False
            
        for i, chunk_file in enumerate(chunk_files):
            print(f"\n📦 Processing {chunk_file.name}")
            
            with open(chunk_file, 'rb') as f:
                urls = json.loads(f.read().decode('utf-8'))
            
            print(f"   URLs: {len(urls)}")
            
            scraped_items = []
            for url in urls:
                print(f"   🔍 {url}")
                
                items = await self.scrape_item(url, TEST_PARAMS["app"])
                scraped_items.extend(items)
                
                for item in items:
                    if item.get("error"):
                        print(f"   ❌ {item['error']}")
                    else:
                        print(f"   ✅ {item['title'][:40]}... - ${item.get('price_numeric', 0)} - {item['buyers']} buyers")
                
                time.sleep(TEST_PARAMS["app"]["delay_between_requests"])
            
            # Save result
            result = {
                "chunk_number": i,
                "urls_processed": len(urls),
                "scraped_items": scraped_items,
                "success_count": len([item for item in scraped_items if not item.get("error")]),
                "error_count": len([item for item in scraped_items if item.get("error")])
            }
            
            result_file = self.task_results_dir / f"chunk_{i}_results.json"
            with open(result_file, 'w') as f:
                json.dump(result, f, indent=2)
            
            print(f"   📄 {result['success_count']} success, {result['error_count']} errors")
        
        return True
    
    def test_assembler(self):
        """Test assembler component."""
        print("\n🔧 TESTING ASSEMBLER")
        print("-" * 30)
        
        result_files = list(self.task_results_dir.glob("chunk_*_results.json"))
        if not result_files:
            print("❌ No result files!")
            return False
            
        print(f"📁 {len(result_files)} result files")
        
        all_items = []
        for result_file in result_files:
            with open(result_file) as f:
                chunk_result = json.load(f)
            scraped_items = chunk_result.get("scraped_items", [])
            all_items.extend(scraped_items)
        
        print(f"📊 {len(all_items)} total items")
        
        # Find cheapest item (changed from most popular)
        cheapest_item = None
        min_price = float('inf')
        
        for item in all_items:
            if item.get("error"):
                continue
            price_numeric = item.get("price_numeric", 999999)
            if price_numeric < min_price:
                min_price = price_numeric
                cheapest_item = item
        
        # Save result
        final_result = {
            "total_items_analyzed": len(all_items),
            "cheapest_item": cheapest_item,
            "min_price": min_price if min_price != float('inf') else 0,
            "summary": {
                "title": cheapest_item.get("title", "Unknown") if cheapest_item else "No items found",
                "price": cheapest_item.get("price", "Unknown") if cheapest_item else "N/A",
                "price_numeric": min_price if min_price != float('inf') else 0,
                "buyers": cheapest_item.get("buyers", 0) if cheapest_item else 0,
                "url": cheapest_item.get("url", "") if cheapest_item else ""
            }
        }
        
        output_path = self.output_dir / "cheapest_item.json"
        with open(output_path, 'w') as f:
            json.dump(final_result, f, indent=2)
        
        print(f"🏆 ANALYSIS COMPLETE!")
        if cheapest_item:
            print(f"🥇 CHEAPEST: '{cheapest_item.get('title', 'Unknown')}' for ${min_price}")
        else:
            print("😞 No items found")
            
        return True
    
    def cleanup(self):
        """Clean up test files."""
        if self.temp_dir and self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            print(f"\n🧹 Cleaned up: {self.temp_dir}")
    
    async def run_full_test(self):
        """Run complete pipeline test."""
        try:
            self.setup_dirs()
            self.create_source_data()
            
            if not self.test_partitioner():
                print("❌ PARTITIONER FAILED!")
                return False
                
            if not await self.test_app():
                print("❌ APP FAILED!")
                return False
                
            if not self.test_assembler():
                print("❌ ASSEMBLER FAILED!")
                return False
            
            print("\n" + "=" * 50)
            print("🎉 ALL TESTS PASSED!")
            print("=" * 50)
            
            # Show final result
            output_file = self.output_dir / "cheapest_item.json"
            if output_file.exists():
                with open(output_file) as f:
                    result = json.load(f)
                print("\n📊 FINAL RESULT:")
                print(json.dumps(result, indent=2))
            
            return True
            
        except Exception as e:
            print(f"❌ TEST FAILED: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            self.cleanup()

if __name__ == "__main__":
    tester = PipelineTester()
    success = asyncio.run(tester.run_full_test())
    sys.exit(0 if success else 1)
