FROM python:3.11-slim

# Set working directory
WORKDIR /api
ENV PYTHONPATH=/api

# Install OS-level build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    curl \
    gcc \
    g++ \
    git \
    libffi-dev \
    libssl-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# Copy and install Python dependencies
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . .

CMD ["python", "recommendation/hybrid_recommendation_service.py"]