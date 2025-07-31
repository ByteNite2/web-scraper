FROM python:3.11-slim

# Install system dependencies needed for Playwright (minimal set)
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    libnss3-dev \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxss1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers with dependencies
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code
COPY app/ ./

# Ensure main.py is executable
RUN chmod +x main.py

# Run the scraper
CMD ["python", "main.py"]
