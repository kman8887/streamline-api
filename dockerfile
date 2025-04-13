FROM python:3.11-slim

# Set working directory
WORKDIR /api

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

# Give executable permission to your shell script
RUN chmod +x ./run_batch_recommender.sh

# Set the shell script as the entrypoint
CMD ["sh", "./run_batch_recommender.sh"]