#!/bin/bash

# Number of nodes in the network
NUM_NODES=10

# Base port number
BASE_PORT=7000

# Number of super nodes in subset (for FLARESCons)
NUM_SUPER=3

# Number of random nodes in subset (for FLARESCons)
NUM_RANDOM=3

# Number of blocks to mine
BLOCK_ITERS=10

# Difficulty (for PoW)
DIFFICULTY=4

IMAGE_NAME="mining_image"
NETWORK_NAME="mining_network"

rm -rf logs

# # Create a Docker network if it doesn't exist
echo "Creating Docker network..."
docker network create $NETWORK_NAME

# Build the Docker image first
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Create local logs directory if it doesn't exist
mkdir -p ./logs

# Start multiple mining nodes
for ((i=0; i<NUM_NODES; i++)); do
    PORT=$((BASE_PORT + i))
    CONTAINER_NAME="mining_node_$i"
    echo "Starting node $i on port $PORT..."

    # flarescons
    docker run -d --name $CONTAINER_NAME \
               --network=$NETWORK_NAME \
               --network-alias=$CONTAINER_NAME \
               -p $PORT:$PORT \
               -v "$(pwd)/logs:/app/logs" \
               $IMAGE_NAME $PORT $CONTAINER_NAME $NUM_NODES $NUM_SUPER $NUM_RANDOM $BLOCK_ITERS

    # PoFL
    # docker run -d --name $CONTAINER_NAME \
    #            --network=$NETWORK_NAME \
    #            --network-alias=$CONTAINER_NAME \
    #            -p $PORT:$PORT \
    #            -v "$(pwd)/logs:/app/logs" \
    #            $IMAGE_NAME $PORT $CONTAINER_NAME $NUM_NODES $BLOCK_ITERS

    # PoW
    # docker run -d --name $CONTAINER_NAME \
    #                --network=$NETWORK_NAME \
    #                --network-alias=$CONTAINER_NAME \
    #                -p $PORT:$PORT \
    #                -v "$(pwd)/logs:/app/pow_logs" \
    #                $IMAGE_NAME $PORT $CONTAINER_NAME $NUM_NODES $DIFFICULTY $BLOCK_ITERS
done

# Wait for all containers to get IP addresses
sleep 2

# Now retrieve all container IPs and update each node
for ((i=0; i<NUM_NODES; i++)); do
    CONTAINER_NAME="mining_node_$i"
    echo "Container $CONTAINER_NAME IP addresses:"
    docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $CONTAINER_NAME
done

echo "All nodes started!"
