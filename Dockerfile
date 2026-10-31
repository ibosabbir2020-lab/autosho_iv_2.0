FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for curl_cffi (native C library)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    libcurl4-openssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy your application code
COPY . .

# Expose port (Render/Fly/Railway usually injects PORT env var, but 8080 is default fallback)
EXPOSE 8080

# START COMMAND
# Format: ["uvicorn", "module_name:app_variable", ...]
# Since your file is shopify.py and the app variable is 'app', we use "shopify:app"
CMD ["uvicorn", "shopify:app", "--host", "0.0.0.0", "--port", "8080"]
