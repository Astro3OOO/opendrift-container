# # Use official Python image
# FROM python:3.12-slim

# # Set working directory inside container
# WORKDIR /opendrift-container

# # Copy requirements first for caching
# COPY requirements.txt /opendrift-container/

# # Install dependencies
# RUN pip install --no-cache-dir -r requirements.txt

# # Copy all project files into container
# COPY *.py /opendrift-container/
# COPY DATA /opendrift-container/DATA/
# COPY INPUT /opendrift-container/INPUT/
# COPY tests /opendrift-container/tests

# # Create output folder (optional, ensures folder exists)
# RUN mkdir -p /opendrift-container/OUTPUT

# # Default command when container runs
# CMD ["python", "main.py"]


# Stage 1: Builder (includes tests)
FROM python:3.12-slim AS builder

WORKDIR /opendrift-container

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py /opendrift-container/
COPY DATA /opendrift-container/DATA/
COPY INPUT /opendrift-container/INPUT/
COPY tests /opendrift-container/tests

RUN python -m pytest tests  
# run tests at build time

# Stage 2: Production image
FROM python:3.12-slim

WORKDIR /opendrift-container

# copy installed Python packages
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# copy project files
COPY --from=builder /opendrift-container/*.py .
COPY --from=builder /opendrift-container/DATA ./DATA
COPY --from=builder /opendrift-container/INPUT ./INPUT

RUN mkdir -p OUTPUT
CMD ["python", "main.py"]