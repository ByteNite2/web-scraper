# Web Scraper ByteNite App

A simple, efficient web scraper pipeline for finding the most popular Amazon products using Playwright and ByteNite's distributed computing platform.

## 🎯 What This Does

This web scraper pipeline:
- **Partitions** URLs into chunks for parallel processing
- **Scrapes** Amazon product data (title, price, buyer count) using Playwright
- **Assembles** results to find the most popular item by buyer count

## 🏗️ Architecture

```
URLs → Partitioner → Scraper Apps (parallel) → Assembler → Most Popular Item
```

### Components:
1. **fanout-urls** (Partitioner): Splits URL lists into processable chunks
2. **scraper-engine** (App): Scrapes Amazon products with Playwright browser automation  
3. **data-assembler** (Assembler): Analyzes results to find most popular items

## 🚀 Quick Start

### Prerequisites
```bash
pip install playwright
```

### Local Testing
```bash
# Run the complete pipeline test
python3 test_final.py
```

### Deploy to ByteNite Staging
```bash
# Authenticate with staging
bytenite-stage auth

# Push all components
cd fanout-urls && bytenite-stage app push .
cd scraper-engine && bytenite-stage app push .  
cd data-assembler && bytenite-stage app push .

# Push template
bytenite-stage template push ./templates/web-scraper-simple-template.json

# Activate apps (choose option A for each)
bytenite-stage app activate fanout-urls
bytenite-stage app activate scraper-engine
bytenite-stage app activate data-assembler

# Check status
bytenite-stage app status fanout-urls
bytenite-stage app status scraper-engine  
bytenite-stage app status data-assembler

# List all apps to verify deployment
bytenite-stage app list
```

## 📋 Configuration

### Job Parameters (`simple_job.json`)

```json
{
  "template": "web-scraper-simple-template",
  "partitioner": {
    "parameters": {
      "urls": ["https://amazon.com/dp/..."],
      "chunk_size": 1
    }
  },
  "app": {
    "parameters": {
      "timeout": 30000,
      "headless": true,
      "delay_between_requests": 2
    }
  }
}
```

## 📊 What Gets Scraped

For each Amazon product URL, the scraper extracts:
- **Title**: Product name
- **Price**: Current price 
- **Buyers**: Number of ratings/reviews (popularity metric)

## 🔧 Technical Details

### Scraping Technology
- **Playwright**: Modern browser automation (handles JavaScript, anti-bot measures)
- **Chromium**: Headless browser for reliable scraping
- **Async/Await**: Efficient concurrent processing

### ByteNite Integration
- **Automatic Scaling**: Processes URLs in parallel across multiple containers
- **Error Handling**: Graceful failure handling for blocked/unavailable URLs
- **Results Aggregation**: Combines chunk results into final analysis

## 📁 Project Structure

```
web-scraper-app/
├── fanout-urls/app/main.py          # URL partitioner
├── scraper-engine/app/main.py       # Playwright scraper  
├── data-assembler/app/main.py       # Results analyzer
├── simple_job.json                 # Job configuration
├── templates/web-scraper-simple-template.json
└── test_final.py                   # Complete local test
```

## 🎯 Output

The pipeline produces a JSON result identifying the most popular item:

```json
{
  "most_popular_item": {
    "title": "Echo Dot (3rd Gen, 2018 release) - Smart speaker with Alexa",
    "price": "$29.99",
    "buyers": 1044554,
    "url": "https://amazon.com/dp/B07FZ8S74R"
  },
  "total_items_analyzed": 3,
  "max_buyer_count": 1044554
}
```

## ⚡ Performance

- **Parallel Processing**: Each URL processed in separate container
- **Smart Delays**: Configurable delays between requests to avoid rate limiting
- **Error Recovery**: Failed URLs don't stop the entire pipeline
- **Scalable**: Handles 1 to 1000+ URLs efficiently

## 🛠️ Troubleshooting

### Common Issues
- **Timeout errors**: Increase `timeout` parameter in job config
- **Rate limiting**: Increase `delay_between_requests` parameter  
- **Empty results**: Some Amazon URLs may block automated access

### Local Testing
Run `python3 test_final.py` to verify all components work locally before deploying to ByteNite.

### Verify Deployment (Optional but Recommended)
```bash
# List all apps to confirm deployment status
bytenite-stage app list

# Confirm all apps are successfully deployed and active
bytenite-stage app status fanout-urls
bytenite-stage app status scraper-engine  
bytenite-stage app status data-assembler
```

Look for your apps in the list with "active" status:
- `data-assembler` - active
- `scraper-engine` - active  
- `fanout-urls` - active

---

Built with ByteNite distributed computing platform 🚀
