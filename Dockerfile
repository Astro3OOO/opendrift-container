# Use official Python image
FROM python:3.12-slim

# Set working directory inside container
WORKDIR /opendrift-container

# Copy requirements first for caching
COPY requirements.txt /opendrift-container/

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gfortran \
        && pip install --no-cache-dir -r requirements.txt \
        && apt-get remove -y build-essential gfortran \
        && apt-get autoremove -y \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Copy all project files into container
COPY *.py /opendrift-container/
COPY DATA /opendrift-container/DATA/
COPY INPUT /opendrift-container/INPUT/
COPY tests /opendrift-container/tests

# Create output folder (optional, ensures folder exists)
RUN mkdir -p /opendrift-container/OUTPUT

# Default command when container runs
CMD ["python", "main.py"]
