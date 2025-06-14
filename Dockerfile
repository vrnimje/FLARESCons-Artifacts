# Use official Python image
FROM python:3.10

# Set working directory inside the container
WORKDIR /app

# Copy the mining script and dataset
COPY . /app/

RUN mkdir -p /app/logs

# Install dependencies (modify if needed)
RUN pip install --default-timeout=1000 --no-cache-dir pandas numpy psutil zmq scikit-learn torch

# Set the ENTRYPOINT to allow passing arguments
# ENTRYPOINT ["python", "/app/Final/PoFL_server.py"] <-- comment/uncomment as needed
# ENTRYPOINT ["python", "/app/Final/pow.py"] <-- comment/uncomment as needed
ENTRYPOINT ["python", "/app/Final/FLARES.py"] # <-- comment/uncomment as needed
