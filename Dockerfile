FROM python:3.10-slim

WORKDIR /app

# Install librdkafka dependency first
RUN apt-get update && apt-get install -y \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Then install Python dependencies
RUN pip install "numpy<2.0.0" \
    sentence-transformers==3.4.1 \
    # For running in normal CPU
    torch==2.2.2+cpu -f https://download.pytorch.org/whl/torch_stable.html \ 
    elasticsearch==8.17.1 \
    confluent-kafka==2.3.0 \
    httpx==0.27.0 \
    aiohttp==3.9.5 \
    python-json-logger==3.2.1 \
    transformers==4.49.0 \
    python-dotenv==1.0.1 \
    nltk==3.8.1 \
    langdetect==1.0.9

# Copy your application
# COPY . .

CMD ["python", "multilingualMpnetProcessor.py"]