import sys
import os
import zmq
import psutil
from typing import List, Dict
import time
import pickle
import pandas as pd
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

# Get the parent directory (adjust if needed, but might not be necessary without AICons)
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# import AICons.main # Not needed for PoW

BASE_PORT = 9000
NODE_PORTS = []
MAX_TRANSACTIONS = 2000 # Max transactions per block
LOCAL = True
NODE_IP = "127.0.0.1" # Default if not provided

# --- Global variable for shutdown signal ---
shutdown_flag = threading.Event()

class Block:
    def __init__(self, index, timestamp, transactions, previous_hash, nonce=0):
        self.index = index
        self.timestamp = timestamp
        # Ensure transactions are serializable (list of dicts)
        self.transactions = transactions
        self.previous_hash = previous_hash
        self.nonce = nonce
        # Calculate hash during initialization, will be recalculated during mining
        self.hash = self.calculate_hash()

    def calculate_hash(self):
        """Calculates the SHA256 hash of the block."""
        block_dict = self.to_dict(include_hash=False) # Exclude hash from hashing input
        block_string = json.dumps(block_dict, sort_keys=True).encode()
        return hashlib.sha256(block_string).hexdigest()

    def mine_block(self, difficulty, stop_event):
        """
        Finds a nonce such that the block hash starts with a certain number of zeros.
        Returns the hash if successful, None if stopped.
        """
        target = '0' * difficulty
        logger.debug(f"Mining block {self.index} with difficulty {difficulty} (target: {target}...)")
        start_time = time.time()

        check_interval = 10

        while self.hash[:difficulty] != target:
             # Check if we received a block from another node or shutdown signal
            if stop_event.is_set():
                logger.debug(f"Mining stopped for block {self.index} due to external event.")
                return None

            self.nonce += 1
            self.hash = self.calculate_hash()

            # Check the stop event periodically within the loop
            if self.nonce % check_interval == 0:
                    if stop_event.is_set():
                        logger.debug(f"Mining stopped externally (periodic check) for block {self.index}.")
                        return None

            # Optional: Log progress periodically to avoid flooding logs
            # if self.nonce % 100000 == 0:
            #    logger.debug(f"Mining block {self.index}, nonce: {self.nonce}, hash: {self.hash}")

        if stop_event.is_set():
            logger.debug(f"Mining stopped externally (post-discovery check) for block {self.index}.")
            return None

        duration = time.time() - start_time
        logger.debug(f"Block {self.index} mined successfully! Nonce: {self.nonce}, Hash: {self.hash}, Time: {duration:.2f}s")
        return self.hash

    def to_dict(self, include_hash=True):
        """Returns a dictionary representation of the block."""
        data = {
            "index": self.index,
            "timestamp": self.timestamp,
            "transactions": self.transactions, # Assuming transactions are already dicts
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
        }
        if include_hash:
            data["hash"] = self.hash
        return data

    # Create a block from a dictionary (useful when receiving blocks)
    @staticmethod
    def from_dict(block_data):
        block = Block(
            index=block_data["index"],
            timestamp=block_data["timestamp"],
            transactions=block_data["transactions"],
            previous_hash=block_data["previous_hash"],
            nonce=block_data["nonce"]
        )
        # Important: Set the hash from the received data, don't recalculate yet
        # Verification happens separately
        block.hash = block_data.get("hash", block.calculate_hash())
        return block


class Blockchain:
    def __init__(self, difficulty, blockchain_file):
        self.chain = []
        self.difficulty = difficulty
        # self.pending_transactions = [] # Transactions are read from CSV directly
        self.blockchain_file = blockchain_file
        self.transactions_file = TRANSACTIONS_FILE # Derive from blockchain file name

        # Create genesis block if blockchain file doesn't exist
        if not os.path.exists(self.blockchain_file):
            logger.info(f"No blockchain file found at {self.blockchain_file}. Creating genesis block.")
            self.create_genesis_block()
        else:
            logger.info(f"Loading blockchain from {self.blockchain_file}")
            self.load_blockchain()

        # Load transactions DataFrame
        try:
            logger.info(f"Loading transactions from {self.transactions_file}")
            self.transactions_df = pd.read_csv(self.transactions_file)
            # Add 'processed' column if missing
            if 'processed' not in self.transactions_df.columns:
                logger.info("Adding 'processed' column to transactions DataFrame.")
                self.transactions_df['processed'] = False
                # Save immediately so all nodes start consistently
                self.transactions_df.to_csv(self.transactions_file, index=False)
        except FileNotFoundError:
            logger.error(f"Transaction file {self.transactions_file} not found! Cannot proceed.")
            sys.exit(1) # Critical error
        except Exception as e:
            logger.error(f"Error loading transactions: {e}")
            sys.exit(1)

    def create_genesis_block(self):
        """Creates the first block in the chain."""
        genesis_block = Block(0, datetime.now().isoformat(), [], "0", nonce=0)
        # The genesis block doesn't need "mining" in the traditional sense,
        # but we calculate its hash. If difficulty > 0, you *could* mine it.
        genesis_block.hash = genesis_block.calculate_hash()
        logger.info(f"Created Genesis Block: Index={genesis_block.index}, Hash={genesis_block.hash}")
        self.chain.append(genesis_block.to_dict())
        self.save_blockchain()

    def get_latest_block(self):
        """Returns the most recent block in the chain."""
        return self.chain[-1] if self.chain else None

    def add_block(self, new_block: Block):
        """
        Adds a new block to the chain after verification.
        Assumes the block's hash and nonce are already set (from mining or network).
        Verification should happen *before* calling this.
        """
        if self.validate_block(new_block):
            logger.info(f"Adding valid block {new_block.index} to the chain.")
            self.chain.append(new_block.to_dict())
            self.mark_transactions_as_processed(new_block.transactions)
            # Save periodically or on shutdown, not necessarily every block
            # self.save_blockchain()
            return True
        else:
            logger.warning(f"Attempted to add invalid block {new_block.index}. Discarding.")
            return False

    def validate_block(self, block_to_validate: Block) -> bool:
        """Validates a single block."""
        latest_block_dict = self.get_latest_block()
        if not latest_block_dict:
             # Only the genesis block should be added when the chain is empty
            return block_to_validate.index == 0 and block_to_validate.previous_hash == "0"

        # 1. Check index
        if block_to_validate.index != latest_block_dict["index"] + 1:
            logger.warning(f"Validation failed: Invalid index. Expected {latest_block_dict['index'] + 1}, got {block_to_validate.index}")
            return False

        # 2. Check previous hash
        if block_to_validate.previous_hash != latest_block_dict["hash"]:
            logger.warning(f"Validation failed: Invalid previous hash. Expected {latest_block_dict['hash']}, got {block_to_validate.previous_hash}")
            return False

        # 3. Check PoW (hash validity and difficulty)
        calculated_hash = block_to_validate.calculate_hash() # Recalculate based on content + nonce
        if calculated_hash != block_to_validate.hash:
             logger.warning(f"Validation failed: Hash mismatch. Received {block_to_validate.hash}, calculated {calculated_hash}")
             return False
        if not block_to_validate.hash.startswith('0' * self.difficulty):
            logger.warning(f"Validation failed: Hash does not meet difficulty {self.difficulty}. Hash: {block_to_validate.hash}")
            return False

        # Optional: 4. Validate transactions within the block (e.g., format, signatures - not implemented here)
        logger.debug(f"Block {block_to_validate.index} passed validation.")
        return True

    def is_chain_valid(self):
        """Checks the integrity of the entire blockchain."""
        for i in range(1, len(self.chain)):
            current_block_dict = self.chain[i]
            previous_block_dict = self.chain[i-1]
            current_block_obj = Block.from_dict(current_block_dict)

            # Check hash integrity
            if current_block_obj.calculate_hash() != current_block_dict["hash"]:
                logger.error(f"Chain invalid: Hash mismatch at index {i}")
                return False

            # Check previous hash link
            if current_block_dict["previous_hash"] != previous_block_dict["hash"]:
                logger.error(f"Chain invalid: Previous hash link broken at index {i}")
                return False

             # Check difficulty (optional, but good practice)
            if not current_block_dict["hash"].startswith('0' * self.difficulty):
                 logger.error(f"Chain invalid: Block {i} hash does not meet difficulty.")
                 return False

        logger.info("Blockchain integrity check passed.")
        return True

    def save_blockchain(self):
        """Saves the current blockchain state to the JSON file."""
        try:
            with open(self.blockchain_file, 'w') as f:
                json.dump(self.chain, f, indent=4)
            logger.debug(f"Blockchain saved to {self.blockchain_file}")
        except Exception as e:
            logger.error(f"Failed to save blockchain: {e}")

    def load_blockchain(self):
        """Loads the blockchain from the JSON file."""
        try:
            with open(self.blockchain_file, 'r') as f:
                self.chain = json.load(f)
            # Basic validation after loading
            if not self.chain or self.chain[0].get('index') != 0:
                 logger.warning(f"Loaded blockchain from {self.blockchain_file} seems invalid (no genesis block?). Reinitializing.")
                 self.chain = []
                 self.create_genesis_block()
            else:
                 logger.info(f"Successfully loaded {len(self.chain)} blocks from {self.blockchain_file}.")
                 # Full validation is optional here, can be slow for long chains
                 # self.is_chain_valid()
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Error loading blockchain file {self.blockchain_file}: {e}. Creating genesis block.")
            self.chain = [] # Ensure chain is empty before creating genesis
            self.create_genesis_block()
        except Exception as e:
             logger.error(f"Unexpected error loading blockchain: {e}")
             sys.exit(1)


    def load_transactions(self) -> pd.DataFrame:
        """Returns the current state of the transactions DataFrame."""
        # In this PoW setup, we manipulate self.transactions_df directly
        return self.transactions_df

    def mark_transactions_as_processed(self, transactions_in_block):
        """Marks transactions in the DataFrame as processed."""
        if not transactions_in_block:
            return

        txn_ids_in_block = {txn['tx_id'] for txn in transactions_in_block}
        logger.debug(f"Marking {len(txn_ids_in_block)} transactions as processed.")

        mask = self.transactions_df['tx_id'].isin(txn_ids_in_block)
        self.transactions_df.loc[mask, 'processed'] = True

        # Persist changes to the CSV file
        try:
            self.transactions_df.to_csv(self.transactions_file, index=False)
            logger.debug(f"Updated transaction status saved to {self.transactions_file}")
        except Exception as e:
             logger.error(f"Error saving updated transaction file: {e}")


    def get_pending_transactions(self, count=MAX_TRANSACTIONS):
        """Gets top unprocessed transactions from the DataFrame based on fee."""
        try:
            # Filter for unprocessed transactions
            unprocessed_txns = self.transactions_df[self.transactions_df['processed'] == False]

            if unprocessed_txns.empty:
                logger.debug("No pending transactions found.")
                return []

            # Sort by fee (highest first) and take top 'count'
            top_transactions = unprocessed_txns.sort_values(by='fee', ascending=False).head(count)

            # Convert to list of dictionaries for the block
            transactions_list = top_transactions.to_dict('records')
            logger.debug(f"Retrieved {len(transactions_list)} pending transactions.")
            return transactions_list

        except Exception as e:
            logger.error(f"Error retrieving pending transactions: {e}")
            return []

# --- ZMQ Node and Communication ---

class PoWNode:
    """Represents a node in the PoW network."""
    def __init__(self, node_id: int, rep_socket: zmq.Socket, req_sockets: Dict[int, zmq.Socket], context: zmq.Context):
        self.id = node_id
        self.rep_socket = rep_socket
        self.req_sockets = req_sockets
        self.context = context

def init_node(port: int) -> PoWNode:
    """Initializes ZMQ sockets for a node."""
    context = zmq.Context()

    # REP socket for incoming messages
    rep_socket = context.socket(zmq.REP)
    rep_socket.bind(f"tcp://*:{port}")
    logger.info(f"Node {port} listening on tcp://*:{port}")

    # REQ sockets for outgoing messages to peers
    req_sockets = {}
    for node_port in NODE_PORTS:
        if node_port != port:
            req_socket = context.socket(zmq.REQ)
            # Set linger to 0 to prevent hanging on close
            req_socket.setsockopt(zmq.LINGER, 0)

            # Determine target address based on LOCAL flag
            if not LOCAL:
                # Assumes docker container names follow a pattern
                container_id = node_port - BASE_PORT
                container_name = f"mining_node_{container_id}" # Adjust if needed
                target_address = f"tcp://{container_name}:{node_port}"
            else:
                target_address = f"tcp://{NODE_IP}:{node_port}"

            logger.debug(f"Node {port} connecting to Node {node_port} at {target_address}")
            req_socket.connect(target_address)
            req_sockets[node_port] = req_socket

    return PoWNode(
        node_id=port,
        rep_socket=rep_socket,
        req_sockets=req_sockets,
        context=context
    )

def listener_thread(node: PoWNode, shared_data: Dict, stop_event: threading.Event):
    """
    Background thread to listen for incoming messages (e.g., new blocks).
    Uses a shared dictionary to pass received data to the main thread.
    """
    poller = zmq.Poller()
    poller.register(node.rep_socket, zmq.POLLIN)
    logger.info(f"Listener thread started for Node {node.id}")

    while not stop_event.is_set():
        try:
            # Poll with a timeout to allow checking the stop_event
            socks = dict(poller.poll(timeout=1000)) # 1 second timeout

            if node.rep_socket in socks and socks[node.rep_socket] == zmq.POLLIN:
                message = node.rep_socket.recv() # Should not block due to poll
                decompressed_message = zlib.decompress(message)
                message_type, sender_id, data = pickle.loads(decompressed_message)

                logger.debug(f"Node {node.id} received message: Type={message_type}, Sender={sender_id}")

                # Store received data (e.g., new blocks)
                # Use a list to handle multiple blocks arriving close together (unlikely but possible)
                if message_type == "NEW_BLOCK":
                    if "NEW_BLOCK" not in shared_data:
                        shared_data["NEW_BLOCK"] = []
                    shared_data["NEW_BLOCK"].append({"sender": sender_id, "block_data": data})
                    logger.debug(f"Node {node.id} queued received block {data.get('index', 'N/A')} from {sender_id}")

                # TODO: Handle other message types if needed (e.g., transaction propagation)

                # Send acknowledgment
                node.rep_socket.send(pickle.dumps("ACK"))

        except zmq.ZMQError as e:
            if e.errno == zmq.ETERM:
                 logger.info("Listener thread interrupted (context terminated).")
                 break # Exit loop if context is terminated
            elif e.errno == zmq.EAGAIN:
                 continue # Timeout occurred, loop again
            else:
                 logger.error(f"Listener thread ZMQ error: {e}")
                 time.sleep(0.1) # Avoid busy-looping on other errors
        except Exception as e:
            logger.error(f"Error in listener thread: {e}\n{traceback.format_exc()}")
            time.sleep(0.1) # Prevent rapid error loops

    logger.info(f"Listener thread stopped for Node {node.id}")


def broadcast_to_nodes(node: PoWNode, message_data, message_type: str, timeout=5.0):
    """Sends a message to all connected peer nodes."""
    responses = {}
    full_message = pickle.dumps((message_type, node.id, message_data))
    compressed_message = zlib.compress(full_message)
    # logger.debug(f"Broadcasting '{message_type}'. Size: {len(compressed_message)} bytes")

    for target_port, req_socket in node.req_sockets.items():
        try:
            # Use Poller for non-blocking send/recv with timeout
            poller = zmq.Poller()
            poller.register(req_socket, zmq.POLLIN)

            logger.debug(f"Node {node.id} sending '{message_type}' to Node {target_port}")
            req_socket.send(compressed_message)

            # Wait for ACK with timeout
            socks = dict(poller.poll(timeout=int(timeout * 1000)))
            if req_socket in socks and socks[req_socket] == zmq.POLLIN:
                response = req_socket.recv() # Should not block
                response_data = pickle.loads(response)
                if response_data == "ACK":
                    logger.debug(f"Node {node.id} received ACK for '{message_type}' from Node {target_port}")
                    responses[target_port] = True
                else:
                    logger.warning(f"Node {node.id} received unexpected response '{response_data}' from Node {target_port}")
            else:
                logger.warning(f"Node {node.id} timed out waiting for ACK from Node {target_port} for '{message_type}'")
                # Need to reset the socket state after a timeout on REQ
                # Close and reconnect might be necessary in robust implementations
                # For simplicity here, we might just log and continue

        except zmq.ZMQError as e:
            logger.error(f"Node {node.id} ZMQ error communicating with Node {target_port}: {e}")
            # Consider socket reset or attempting reconnect
        except Exception as e:
            logger.error(f"Node {node.id} error broadcasting to Node {target_port}: {e}\n{traceback.format_exc()}")

    return responses


# --- Main Execution Logic ---

def run_pow(node: PoWNode, blockchain: Blockchain, max_iters: int):
    """Main loop for the Proof-of-Work node."""
    shared_data = {} # For communication between listener and main thread
    mining_stop_event = threading.Event() # Signal to stop current mining attempt

    # Start listener thread
    listener = threading.Thread(target=listener_thread, args=(node, shared_data, shutdown_flag))
    listener.daemon = True # Allow main thread to exit even if listener is running
    listener.start()

    block_iter = 0
    required_blocks = max_iters # Target number of blocks to mine/process

    # --- Performance Metric Logging (Optional) ---
    log_interval = 30 # Log metrics every 30 seconds
    last_log_time = time.time()
    initial_net = psutil.net_io_counters()
    initial_disk = psutil.disk_io_counters()
    # ---------------------------------------------

    while not shutdown_flag.is_set():
        current_block_count = len(blockchain.chain)
        if current_block_count > required_blocks: # +1 because genesis is index 0
            logger.info(f"Reached target block count ({required_blocks}). Shutting down.")
            break

        logger.info(f"--- Iteration {block_iter} | Current Block Height: {current_block_count -1 } ---")

        # 1. Check for and process newly received blocks
        mining_stop_event.clear() # Reset stop event for this iteration
        processed_new_block = False
        if "NEW_BLOCK" in shared_data:
            received_blocks = shared_data.pop("NEW_BLOCK") # Get and clear queue
            for item in received_blocks:
                sender_id = item["sender"]
                block_data = item["block_data"]
                logger.info(f"Node {node.id} processing received block {block_data.get('index')} from {sender_id}")
                new_block_obj = Block.from_dict(block_data)

                # Validate against the *current* latest block
                if blockchain.validate_block(new_block_obj):
                     # Prevent adding if we already have a block at this index
                    if new_block_obj.index > blockchain.get_latest_block()["index"]:
                        blockchain.add_block(new_block_obj) # Adds to chain and marks transactions
                        processed_new_block = True
                        mining_stop_event.set() # Signal ongoing mining (if any) to stop
                        logger.info(f"Node {node.id} added valid block {new_block_obj.index} from {sender_id} to chain.")
                        # If multiple valid blocks for the same height arrive, first one wins in this simple model
                        break # Stop processing more blocks for now if one was added
                    else:
                        logger.debug(f"Node {node.id} already has block {new_block_obj.index}. Ignoring.")
                else:
                     logger.warning(f"Node {node.id} received invalid block {new_block_obj.index} from {sender_id}. Discarding.")


        # 2. Attempt to mine a new block if no new block was processed
        if not processed_new_block and not shutdown_flag.is_set():
            pending_txns = blockchain.get_pending_transactions(MAX_TRANSACTIONS)

            if not pending_txns:
                logger.debug(f"Node {node.id}: No pending transactions to mine. Waiting.")
                time.sleep(5) # Wait before checking again
            else:
                logger.info(f"Node {node.id}: Found {len(pending_txns)} transactions. Attempting to mine block {len(blockchain.chain)}...")
                latest_block = blockchain.get_latest_block()
                new_block = Block(
                    index=latest_block["index"] + 1,
                    timestamp=datetime.now().isoformat(),
                    transactions=pending_txns,
                    previous_hash=latest_block["hash"]
                )

                # Start mining attempt
                mined_hash = new_block.mine_block(blockchain.difficulty, mining_stop_event)

                # Check if mining was successful (not stopped externally)
                if mined_hash and not mining_stop_event.is_set():
                    # Crucial check: Re-verify the chain hasn't changed while mining
                    if new_block.previous_hash == blockchain.get_latest_block()["hash"]:
                        logger.info(f"Node {node.id} successfully mined block {new_block.index}!")
                        # Add the successfully mined block to own chain first
                        blockchain.add_block(new_block) # Also marks transactions
                        # Broadcast the new block to peers
                        broadcast_to_nodes(node, new_block.to_dict(), "NEW_BLOCK")
                    else:
                         logger.warning(f"Node {node.id}: Mined block {new_block.index}, but chain changed underneath! Discarding.")
                elif mining_stop_event.is_set():
                    logger.info(f"Node {node.id}: Mining for block {new_block.index} was interrupted by a received block or shutdown.")
                else:
                    # This case shouldn't happen if mine_block returns None only when stopped
                    logger.error(f"Node {node.id}: Mining failed unexpectedly for block {new_block.index}.")

        # --- Log Performance Metrics Periodically (Optional) ---
        current_time = time.time()
        if current_time - last_log_time >= log_interval:
            try:
                duration = current_time - last_log_time
                final_net = psutil.net_io_counters()
                final_disk = psutil.disk_io_counters()

                cpu_usage = psutil.cpu_percent()
                mem_usage = psutil.virtual_memory().percent
                net_sent = (final_net.bytes_sent - initial_net.bytes_sent) / duration
                net_recv = (final_net.bytes_recv - initial_net.bytes_recv) / duration
                disk_read = (final_disk.read_bytes - initial_disk.read_bytes) / duration
                disk_write = (final_disk.write_bytes - initial_disk.write_bytes) / duration

                logger.info(f"Metrics Update: CPU: {cpu_usage:.1f}%, Mem: {mem_usage:.1f}%, Net Sent: {net_sent/1024:.2f} KB/s, Net Recv: {net_recv/1024:.2f} KB/s, Disk Read: {disk_read/1024:.2f} KB/s, Disk Write: {disk_write/1024:.2f} KB/s")

                # Reset for next interval
                last_log_time = current_time
                initial_net = final_net
                initial_disk = final_disk
            except Exception as e:
                logger.warning(f"Could not retrieve performance metrics: {e}")
        # ------------------------------------------------------

        block_iter += 1
        # Add a small delay to prevent busy-looping if no work is done
        time.sleep(0.5) # Adjust as needed

    # --- Shutdown ---
    logger.info(f"Node {node.id} main loop finished. Final block height: {len(blockchain.chain)-1}")
    # Ensure final state is saved
    blockchain.save_blockchain()
    blockchain.transactions_df.to_csv(blockchain.transactions_file, index=False) # Save final txn state
    logger.info("Final blockchain and transaction states saved.")

# --- Main Entry Point ---
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python pow_node.py <port> <num_nodes> [difficulty] [max_iters] [local_ip]")
        print("Example: python pow_node.py 7000 5 3 50 192.168.1.100")
        sys.exit(1)

    port = int(sys.argv[1])
    num_nodes = int(sys.argv[2])
    difficulty = int(sys.argv[3]) if len(sys.argv) > 3 else 3 # Default difficulty
    max_iters = int(sys.argv[4]) if len(sys.argv) > 4 else 50 # Default max blocks
    node_ip = sys.argv[5] if len(sys.argv) > 5 else "127.0.0.1" # Use provided IP or default

    TRANSACTIONS_FILE = f"./pow_logs/transactions_{port}.csv"

    # Setup Logging
    log_dir = "./pow_logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'pow_node_{port}.log')
    logger = logging.getLogger(f"PoWNode_{port}")
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w'), # Overwrite log file each run
        ]
    )

    # Update global NODE_IP if running locally
    if node_ip != "127.0.0.1":
        LOCAL = True # Assume local if specific IP is given
        NODE_IP = node_ip
    elif 'DOCKER_ENV' in os.environ: # Simple check if running in Docker (adjust if needed)
        LOCAL = False
        logger.info("Detected Docker environment, using container names for connection.")
    else:
        LOCAL = True
        NODE_IP = "127.0.0.1"

    # Define node ports
    NODE_PORTS = [BASE_PORT + i for i in range(num_nodes)]

    logger.info(f"Starting PoW Node {port}...")
    logger.info(f"Configuration: Num Nodes={num_nodes}, Difficulty={difficulty}, Target Blocks={max_iters}, Local={LOCAL}, Node IP={NODE_IP}")
    logger.info(f"All Node Ports: {NODE_PORTS}")

    # Setup Transaction File (copy base if needed)
    base_transactions_file = "./data/transactions_60000_with_ids.csv" # Path to your full transaction list
    node_transactions_file = os.path.join(log_dir, f'transactions_{port}.csv')
    blockchain_file = os.path.join(log_dir, f'blockchain_{port}.json')

    if not os.path.exists(node_transactions_file):
        try:
            logger.info(f"Copying base transactions from {base_transactions_file} to {node_transactions_file}")
            shutil.copy(base_transactions_file, node_transactions_file)
        except FileNotFoundError:
            logger.error(f"Base transaction file {base_transactions_file} not found! Please create it.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Error copying transaction file: {e}")
            sys.exit(1)
    else:
        logger.info(f"Using existing transaction file: {node_transactions_file}")

    # Initialize Node and Blockchain
    pow_node = init_node(port)
    blockchain = Blockchain(difficulty=difficulty, blockchain_file=blockchain_file)

    # Start the main process
    try:
        run_pow(pow_node, blockchain, max_iters)
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}")
        logger.debug(f"{traceback.format_exc()}")
