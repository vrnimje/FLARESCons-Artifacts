# FLARESCons-Artifacts
FLARESCons: Federated Learning consensus with Assured Rewards, Evaluation and Subset Selection

## Abstract
Blockchain is a distributed ledger technology that has found applications across a wide range of domains beyond cryptocurrencies. A critical component enabling decentralization in blockchain networks is the consensus algorithm. Traditional algorithms such as Proof-of-Work (PoW) and Proof-of-Stake (PoS), despite widespread adoption, suffer from significant limitations—PoW is highly energy-intensive, while PoS often lacks fairness. Several ML-based Proof of Useful Work (PoUW) schemes, such as Proof of Learning (PoL) and Proof of Deep Learning (PoDL), have been proposed to utilize computational resources for meaningful model training tasks. However, they discard slightly inferior models, resulting in resource wastage. Proof of Federated Learning (PoFL) introduced a energy recycling federated learning-based consensus mechanism which mitigates this wastage. Despite these advances, PoFL suffers from high communication overhead due to the federated learning process, and still is reliant on presence of high quality training data to maintain its efficiency.

To address these shortcomings, we propose **FLARESCons**, a novel energy recycling and efficient consensus mechanism. FLARESCons employs a fair subset selection mechanism to determine participating nodes in the federated learning phase, where a global model predicts the next miner. Additionally, multi-phase validation is incorporated to verify both local models and mined blocks, mitigating a wide range of security threats. Experimental evaluation demonstrates that FLARESCons achieves high transaction throughput (TPS) and significantly improves energy efficiency compared to existing consensus algorithms, while slightly compromising on fairness in reward distribution.

## Instructions

### File structure
- **AICons** contains the shared source files between PoFL and FLARESCons.
- **data** contains the dataset used for training the models, and also a generated transaction list
- **Final** contains the final source files for FLARESCons, PoFL and PoW.

### Execution steps
To run a consensus algorithm, make sure that [Docker](https://www.docker.com/) is installed on the system. After that, follow these steps:

1. Change the [Dockerfile](./Dockerfile) to use the corresponding Dockerfile. For example, to run FLARESCons

```Dockerfile
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
ENTRYPOINT ["python", "/app/Final/FLARES.py"]
```

2. Open [docker-launch.sh](./docker-launch.sh) and set the parameters for the experiments

```sh
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
```

Also, comment and uncomment lines according to the algorithm to be executed

```sh
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
```

3. Finally, execute the following command to start the mining nodes:

```sh
$ ./docker-launch.sh
```
