import sys
import os

# Get the parent directory (FYP_Artifacts)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import zmq
import psutil
from typing import List, Dict
import AICons.main
import time
import pickle
import pandas as pd
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler
import torch
import numpy as np
import threading
import random
import logging
import traceback
import hashlib
import json
from datetime import datetime
import shutil
import zlib

# NODE_PORTS = [7000, 7001, 7002, 7003, 7004, 7005, 7006, 7007, 7008, 7009]
# NODE_ADDRESS = ["192.168.1.100", "192.168.1.110", "192.168.1.142", "192.168.1.152"]
# BLOCKCHAIN_FILE = "./data/blockchain.json"  # Path to store the blockchain
BASE_PORT = 9000

NODE_PORTS = []

MAX_TRANSACTIONS = 2000

LOCAL = True

class Block:
    def __init__(self, index, timestamp, transactions, previous_hash, winner_embedding, nonce=0):
        self.index = index
        self.timestamp = timestamp
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.nonce = nonce
        self.winner_embedding = winner_embedding
        self.hash = self.calculate_hash()


    def calculate_hash(self):
        block_string = json.dumps({
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "winner_embedding": self.winner_embedding.tolist()
        }, sort_keys=True).encode()

        return hashlib.sha256(block_string).hexdigest()

    def mine_block(self, difficulty):
        # target = '0' * difficulty
        # while self.hash[:difficulty] != target:
        #     self.nonce += 1
        self.hash = self.calculate_hash()

        return self.hash

    def to_dict(self):
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": self.transactions,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "winner_embedding": self.winner_embedding.tolist(),
            "hash": self.hash
        }

class Blockchain:
    def __init__(self, genesis_winner ,difficulty=0, BLOCKCHAIN_FILE = "./data/blockchain.json"):
        self.chain = []
        self.difficulty = difficulty
        self.pending_transactions = []
        self.blockchain_file = BLOCKCHAIN_FILE
        self.genesis_winnner = genesis_winner
        # Create genesis block if not exists
        if not os.path.exists(BLOCKCHAIN_FILE):
            self.create_genesis_block(genesis_winner)
        else:
            self.load_blockchain()

        self.transactions_df = pd.read_csv(TRANSACTIONS_FILE)

        # Add 'processed' column if missing
        if 'processed' not in self.transactions_df.columns:
            self.transactions_df['processed'] = False

    def create_genesis_block(self, genesis_winner):
        genesis_block = Block(0, 0, [], "0", genesis_winner)
        genesis_block.mine_block(self.difficulty)
        self.chain.append(genesis_block.to_dict())
        self.save_blockchain()

    def get_latest_block(self):
        return self.chain[-1]

    def add_block(self, new_block):
        new_block.previous_hash = self.get_latest_block()["hash"]
        new_block.hash = new_block.mine_block(self.difficulty)
        self.chain.append(new_block.to_dict())
        # self.save_blockchain()

    def is_chain_valid(self):
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i-1]

            # Check if hash is correct
            block_hash = Block(
                current_block["index"],
                current_block["timestamp"],
                current_block["transactions"],
                current_block["previous_hash"],
                current_block["nonce"]
            ).calculate_hash()

            if block_hash != current_block["hash"]:
                return False

            # Check if previous hash is correct
            if current_block["previous_hash"] != previous_block["hash"]:
                return False

        return True

    def save_blockchain(self):
        with open(self.blockchain_file, 'w') as f:
            json.dump(self.chain, f, indent=4)

    def load_blockchain(self):
        try:
            with open(self.blockchain_file, 'r') as f:
                self.chain = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self.create_genesis_block(self.genesis_winnner)

    def load_transactions(self) -> pd.DataFrame:
        # try:
        #     transactions_df = pd.read_csv(TRANSACTIONS_FILE)

        #     # Add 'processed' column if missing
        #     if 'processed' not in transactions_df.columns:
        #         transactions_df['processed'] = False

        #     return transactions_df
        # except Exception as e:
        #     print(f"Error loading transactions: {e}")
        #     return pd.DataFrame()
        #
        return self.transactions_df

    def mark_transactions_as_processed(self, transactions):
        # transactions_df = self.load_transactions()

        # Mark processed transactions
        for txn in transactions:
            mask = self.transactions_df['tx_id'] == txn['tx_id']
            self.transactions_df.loc[mask, 'processed'] = True


        # Save updated CSV
        # transactions_df.to_csv(TRANSACTIONS_FILE, index=False)

    def get_pending_transactions(self, count=MAX_TRANSACTIONS):
        """Get top transactions from CSV file"""
        try:
            # Sort by fee (highest first) and take top 'count' transactions
            top_transactions = self.transactions_df[self.transactions_df['processed'] == False].sort_values(by='fee', ascending=False).head(count)

            # Convert to list of dictionaries
            # transactions = top_transactions

            return top_transactions.to_dict('records')
        except Exception as e:
            logger.debug(f"Error loading transactions: {e}")
            return []

def init_node(port: int) -> AICons.main.Node:
    context = zmq.Context()

    # Create REP socket to receive requests with optimized settings
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{port}")  # Bind to all interfaces

    # Create REQ sockets to send requests to other nodes
    req_sockets = {}
    for node_port in NODE_PORTS:
        if node_port != port:
            req_socket = context.socket(zmq.REQ)

            # Use container name pattern for each node
            container_id = node_port - BASE_PORT  # Calculate container ID from port
            container_name = f"mining_node_{container_id}"
            if not LOCAL:
                req_socket.connect(f"tcp://{container_name}:{node_port}")
            else:
                req_socket.connect(f"tcp://{NODE_IP}:{node_port}")

            req_sockets[node_port] = req_socket

    return AICons.main.Node(
        id=port,
        rep_socket=rep_socket,
        req_sockets=req_sockets,
        context=context
    )

def listener_thread(node, shared_data):
    """Background thread to handle incoming messages."""
    while True:
        try:
            message = node.rep_socket.recv(zmq.NOBLOCK)
            message_type, sender_id, data = pickle.loads(zlib.decompress(message))
            already = False
            # Store the received data
            if message_type not in shared_data:
                shared_data[message_type] = {}
            else:
                already = True
            shared_data[message_type][sender_id] = data

            if already and message_type == "NEW_BLOCK":
                logger.debug(f"Block Already there")

            # Send acknowledgment
            node.rep_socket.send(pickle.dumps("ACK"))
        except zmq.Again:
            time.sleep(0.1)  # Don't busy-wait
        except Exception as e:
            logger.debug(f"Error in listener thread: {e}")
            time.sleep(0.1)

def broadcast_to_nodes(node, message, message_type, timeout=10.0):
    """Send message to all nodes with timeout."""
    responses = {}
    for target_port, req_socket in node.req_sockets.items():
        try:
            # Set socket timeout
            req_socket.setsockopt(zmq.RCVTIMEO, int(timeout * 1000))

            # Send message
            full_message = pickle.dumps((message_type, node.id, message))
            compressed_message = zlib.compress(full_message)
            req_socket.send(compressed_message)

            # Wait for response
            response = pickle.loads(req_socket.recv())
            if response == "ACK":
                responses[target_port] = True

        except zmq.Again:
            logger.debug(f"Timeout waiting for response from node {target_port}")
            continue
        except Exception as e:
            logger.debug(f"Error communicating with node {target_port}: {e}")
            continue

    return responses

def wait_for_responses(shared_data, message_type, expected_count):
    """Wait for expected number of responses or timeout."""
    start_time = time.time()
    while (message_type not in shared_data) or (len(shared_data[message_type]) < expected_count):
        time.sleep(0.5)
    return shared_data.get(message_type, {})

def compute_node_score(node, weights):
    """
    Compute the weighted score for a node using its performance metrics.
    """
    cpu_score = node[1] * weights["cpu_usage"]
    memory_score = node[2] * weights["memory_usage"]
    disk_score = node[3] * weights["disk_throughput"]
    network_score = node[4] * weights["network_throughput"]
    selection_penalty = node[5] * weights["selection_count"]
    # duration_penalty = node[6] * weights["mining_duration"]

    return cpu_score + memory_score + disk_score + network_score - selection_penalty # - duration_penalty

def mine_transactions(node_id, blockchain: Blockchain, cur_winner_embedding):
    """Mine pending transactions and create a new block"""
    # Get pending transactions
    transactions = blockchain.get_pending_transactions(count=MAX_TRANSACTIONS)

    if not transactions:
        logger.debug(f"Node {node_id}: No transactions to mine")
        return None

    # Create new block
    last_block = blockchain.get_latest_block()

    new_block = Block(
        index=last_block["index"] + 1,
        timestamp=datetime.now().isoformat(),
        transactions=transactions,
        previous_hash=last_block["hash"],
        winner_embedding = cur_winner_embedding
    )

    # Mine the block (find nonce that satisfies difficulty)
    logger.debug(f"Node {node_id}: Mining block {new_block.index}...")
    start_time = time.time()
    new_block.mine_block(blockchain.difficulty)
    mining_time = time.time() - start_time
    blockchain.mark_transactions_as_processed(transactions)
    logger.debug(f"Node {node_id}: Block mined in {mining_time} seconds with hash {new_block.hash}")

    return new_block

def test_model(model, test_data):
    model.eval()
    features = test_data["features"]
    labels = test_data["labels"]

    with torch.no_grad():
        logits = model(features)
        predictions = torch.argmax(logits, dim=1)

    accuracy = (predictions == labels).float().mean().item()
    return accuracy

def run(node: AICons.main.Node, dataset_subsets, scaler, logger, genesis_winnner):
    # Shared data structure for communication between threads
    shared_data = {}

    # Start listener thread
    listener = threading.Thread(target=listener_thread, args=(node, shared_data))
    listener.daemon = True
    listener.start()

    # Initialize blockchain
    blockchain_file_path = f"./logs/blockchain_{node.id}.json"
    blockchain = Blockchain( genesis_winner, difficulty=0, BLOCKCHAIN_FILE = blockchain_file_path)

    winner_node_id = None

    block_iter = 0
    random.seed(42)

    node_readings = []
    models, losses = [], []
    winner_embedding = None
    model_accs, final_models = {}, {}

    n = NUM_NODES

    while True:
        try:
            logger.info(F"BLOCK_ITER: {block_iter}")

            idle_start_time = time.time()
            idle_initial_net = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
            idle_initial_disk = psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes

            if block_iter % 10 == 0:
                # Train the model locally
                start_time = time.time()
                initial_net = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
                initial_disk = psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes

                loss = node.train_model(dataset_subsets[node.id], logger)

                final_net = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
                final_disk = psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes

                # Calculate metrics
                cpu_usage = psutil.cpu_percent()
                mem_usage = psutil.virtual_memory().percent
                duration = time.time() - start_time
                network_throughput = (final_net - initial_net) / (1024) / duration
                disk_throughput = (final_disk - initial_disk) / (1024) / duration

                logger.debug(f"Node {node.id}: Training Loss = {loss}")

                # Broadcast model state
                broadcast_to_nodes(node, (node.model_state, loss), "MODEL")

                # Wait for other models
                expected_responses = n - 1
                received_models = wait_for_responses(shared_data, "MODEL", expected_responses)

                # Process received models
                models = {node.id: node.model_state}
                losses = {node.id: loss}
                for sender_id, (model_state, sender_loss) in received_models.items():
                    models[sender_id] = model_state
                    losses[sender_id] = sender_loss

                # Test models
                subset_tensor = dataset_subsets[node.id]
                features = subset_tensor["features"]
                labels = subset_tensor["labels"]
                indices = torch.randperm(features.size(0))[:10]
                test_data = {
                    "features": features[indices],
                    "labels": labels[indices]
                }

                model_valids = {}
                model_accs = {}

                for m in models.keys():
                    temp = AICons.main.NeuralNetwork(input_dim=4, embedding_dim=node.embedding_dim, num_classes=node.num_classes)
                    temp.load_state_dict(models[m])
                    acc = test_model(temp, test_data)
                    model_valids[m] = (acc > 0.5)
                    model_accs[m] = acc

                logger.debug(f"Node {node.id}: Calculated accs: {model_accs}")
                logger.debug(f"Node {node.id}: Calculated model_valids: {model_valids}")

                # Broadcast validation results
                broadcast_to_nodes(node, model_valids, "VALIDATION")
                received_validations = wait_for_responses(shared_data, "VALIDATION", expected_responses)

                model_votes = {node.id: model_valids}
                model_votes.update(received_validations)

                logger.debug(f"Node {node.id}: Collected model votes: {model_votes}")

                # Calculate final models
                final_models = {k: 0 for k in NODE_PORTS}
                for i in model_votes.keys():
                    for j in model_votes[i].keys():
                        final_models[j] += int(model_votes[i].get(i, 0))

                logger.debug(f"Node {node.id}: Collected votes: {final_models}")

                # Merge models
                node.merge_models(
                    [models[i] for i in models.keys() if final_models[i] > n / 2],
                    [losses[i] for i in models.keys() if final_models[i] > n / 2]
                )

                # Handle metrics
                real_time = pd.DataFrame([[cpu_usage, mem_usage, network_throughput, disk_throughput]], columns=["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"])
                scaled_real_time = scaler.transform(real_time)

                logger.debug(f"Real time: {real_time.values}")
                logger.debug(f"Scaled Real time: {scaled_real_time}")

                broadcast_to_nodes(node, scaled_real_time, "METRICS")
                received_metrics = wait_for_responses(shared_data, "METRICS", expected_responses)

                nodes = {node.id: scaled_real_time}
                nodes.update(received_metrics)

                logger.debug(f"Node {node.id}: Collected real-time nodes: {nodes}")

                # Calculate rankings
                # positive_indices = torch.where(labels == 1)[0]
                # random_index = positive_indices[torch.randint(len(positive_indices), (1,))].item()
                winner = blockchain.get_latest_block()["winner_embedding"]
                rank = node.rankings(nodes, np.array(winner))
                logger.debug(f"Node {node.id}: Rankings: {rank}")

                # Mining and blockchain update
                winner_node_id = rank[0][0]
                winner_embedding = rank[2][0]
                logger.debug(f"Winner node for mining: {winner_node_id}")

            block_valid = False

            if node.id == winner_node_id:
                logger.debug(f"Node {node.id}: I am the winner! Mining transactions...")

                # Mine transactions and create a new block
                new_block = mine_transactions(node.id, blockchain, winner_embedding)

                if new_block:
                    # Add block to blockchain
                    blockchain.add_block(new_block)

                    # Broadcast new block to other nodes
                    block_broad = broadcast_to_nodes(node, new_block.to_dict(), "NEW_BLOCK")
                    logger.debug(f"Node {node.id}: Block broadcast complete")
                    block_valid = True
                else:
                    broadcast_to_nodes(node, {}, "NEW_BLOCK")
                    logger.debug(f"Node {node.id}: Block broadcast complete")

            else:
                # Wait for potential new block from winner
                logger.debug(f"Node {node.id}: Waiting for block from winner node {winner_node_id}")
                new_blocks = wait_for_responses(shared_data, "NEW_BLOCK", 1)

                if new_blocks:
                    # Update local blockchain with new block
                    for sender_id, block_data in new_blocks.items():
                        logger.debug(f"Node {node.id}: Received new block from node {sender_id}")
                        logger.debug(f"Block Data: {block_data['previous_hash']}, Previous_Hash = {blockchain.get_latest_block()['hash']}")
                        if block_data:
                            block_valid = True
                            # Verify and add block to blockchain
                            last_block = blockchain.get_latest_block()
                            if block_data["previous_hash"] == last_block["hash"] and block_data["index"] == last_block["index"] + 1:
                                blockchain.chain.append(block_data)
                                blockchain.mark_transactions_as_processed(block_data["transactions"])
                                logger.debug(f"Node {node.id}: Added new block to blockchain")
                            else:
                                logger.debug(f"Node {node.id}: Invalid block received")

                if block_valid:
                    # Reward distribution
                    latest_block = blockchain.get_latest_block()
                    transactions_df = blockchain.load_transactions()

                    processed_txns_fees = 0.0
                    for txn in latest_block["transactions"]:
                        processed_txns_fees += float(txn['fee'])

                    total_fee = processed_txns_fees
                    logger.debug(f"Total fee: {total_fee}")

                    # Reward to only those nodes which have atleast half of the votes
                    node_metrics = {x: {'loss': losses[x], 'accuracy': model_accs[x]} for x in NODE_PORTS if final_models[x] > n / 2}

                    total_loss = sum(1 / metrics['loss'] if metrics['loss'] > 0 else 1 for metrics in node_metrics.values())  # Avoid div by zero

                    weights = {}
                    for x, metrics in node_metrics.items():
                        loss_score = (1 / metrics['loss']) / total_loss if metrics['loss'] > 0 else 1 / total_loss  # Lower loss = higher reward

                        # Weight is an equal balance of both factors
                        weights[x] = loss_score

                    # Normalize weights to ensure full distribution
                    total_weight = sum(weights.values())
                    rewards = {x: (weights[x] / total_weight) * total_fee for x in node_metrics.keys()}
                    logger.debug(f"Rewards: {rewards}")

            idle_final_net = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
            idle_final_disk = psutil.disk_io_counters().read_bytes + psutil.disk_io_counters().write_bytes

            # Calculate readings
            idle_cpu_usage = psutil.cpu_percent()
            idle_mem_usage = psutil.virtual_memory().percent
            idle_duration = time.time() - idle_start_time
            idle_network_throughput = (idle_final_net - idle_initial_net) / (1024) / idle_duration
            idle_disk_throughput = (idle_final_disk - idle_initial_disk) / (1024) / idle_duration

            broadcast_to_nodes(node, [node.id, idle_cpu_usage, idle_mem_usage, idle_network_throughput, idle_disk_throughput, node.selection_time], "IDLE_READINGS")
            recv_node_readings = wait_for_responses(shared_data, "IDLE_READINGS", len(NODE_PORTS) - 1)

            node_readings = [[node.id, idle_cpu_usage, idle_mem_usage, idle_network_throughput, idle_disk_throughput, node.selection_time]]

            for k in recv_node_readings.keys():
                node_readings.append(recv_node_readings[k])

            logger.debug(f"Node readings: {node_readings}")

            # Clear shared data for new round
            shared_data.clear()

            block_iter += 1

            logger.debug(f"Sleep for sync")
            time.sleep(2.5)

            if (block_iter == MAX_ITERS):
                blockchain.transactions_df.to_csv(TRANSACTIONS_FILE, index=False)
                blockchain.save_blockchain()
                break

        except Exception as e:
            logger.info(f"Error in main loop")
            logger.info(traceback.format_exc())
            break

if __name__ == "__main__":
    port = int(sys.argv[1])
    TRANSACTIONS_FILE = f"./logs/transactions_{port}.csv"

    os.makedirs("./logs/", exist_ok=True)
    shutil.copy("./data/transactions_60000_with_ids.csv", TRANSACTIONS_FILE)

    node_ip = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    NODE_IP = node_ip

    NUM_NODES = int(sys.argv[3]) if len(sys.argv) > 3 else 10
    NUM_SUPER = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    NUM_RANDOM = int(sys.argv[5]) if len(sys.argv) > 5 else 3
    MAX_ITERS = int(sys.argv[6]) if len(sys.argv) > 6 else 40

    print(f"Iters: {MAX_ITERS}")

    for i in range(NUM_NODES):
        NODE_PORTS.append(BASE_PORT + i)

    logger = logging.getLogger("FYP Logs")
    logging.basicConfig(filename=f'./logs/final_{port}.log', level=logging.DEBUG, filemode='w')

    file_path = "./data/05_labeled.csv"
    data = pd.read_csv(file_path)

    # data = data.sample(frac=1).reset_index(drop=True)

    features = ["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]
    labels = data["Winner"]

    scaler = StandardScaler()
    normalized_data = pd.DataFrame(scaler.fit_transform(data[features]), columns=features)
    normalized_data["Winner"] = labels

    winner = normalized_data[normalized_data["Winner"] == 1][features].mean().values

    features = data[["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]].values
    labels = data["Winner"].values

    # Stratified split into `n` subsets
    skf = StratifiedKFold(n_splits=len(NODE_PORTS), shuffle=True)
    subsets = []

    for train_idx, _ in skf.split(features, labels):
        subset = data.iloc[train_idx].reset_index(drop=True)
        subsets.append(subset)

    dataset_subsets = {}

    for i in range(len(NODE_PORTS)):
        subset_tensor = {
            "features": torch.tensor(subsets[i][["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]].values, dtype=torch.float64),
            "labels": torch.tensor(subsets[i]["Winner"].values, dtype=torch.long),
        }
        dataset_subsets[NODE_PORTS[i]] = (subset_tensor)
    node = init_node(port)
    torch.manual_seed(42)
    positive_indices = torch.where(torch.tensor(data["Winner"].values) == 1)[0]
    random_index = positive_indices[torch.randint(len(positive_indices), (1,))].item()
    genesis_winner = features[random_index]

    run(node, dataset_subsets, scaler, logger, genesis_winner)
