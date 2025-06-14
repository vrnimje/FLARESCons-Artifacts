import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
import pandas as pd
import sys
# import matplotlib.pyplot as plt
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import StandardScaler
import socket

# Define the neural network
class NeuralNetwork(nn.Module):
    def __init__(self, input_dim, embedding_dim, num_classes):
        super(NeuralNetwork, self).__init__()
        self.fc1 = nn.Linear(input_dim, 64, dtype=torch.float64)
        self.fc2 = nn.Linear(64, 128, dtype=torch.float64)
        self.fc3 = nn.Linear(128, embedding_dim, dtype=torch.float64)  # Embedding layer
        self.classifier = nn.Linear(embedding_dim, num_classes, dtype=torch.float64)  # Classification head

    def forward(self, x, return_embeddings=False):
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        embeddings = self.fc3(x)  # Embeddings
        normalized_embeddings = F.normalize(embeddings, p=2, dim=-1)

        if return_embeddings:
            return normalized_embeddings  # For similarity-based inference

        logits = self.classifier(normalized_embeddings)
        return logits


# AICons node
class Node:
    def __init__(self, id, req_sockets, rep_socket, context, embedding_dim=32, num_classes=2):
        self.id = id
        self.embedding_dim = embedding_dim
        self.num_classes = num_classes
        self.model_state = None
        self.global_model_state = None
        self.cosine_similarity = nn.CosineSimilarity(dim=-1)
        self.cpu_usage = 0.0
        self.memory_usage = 0.0
        self.disk_throughput = 0.0
        self.network_throughput = 0.0
        self.mining_duration = 0.0
        self.selection_time = 0
        self.req_sockets = req_sockets
        self.rep_socket = rep_socket
        self.context = context

    # Training loop using classification
    def train_model(self, dataset, logger):
        # Hyperparameters
        input_dim = dataset["features"].shape[1]
        batch_size = 64
        num_epochs = 25
        learning_rate = 2.5e-2

        # Initialize neural network and loss function
        model = NeuralNetwork(input_dim, self.embedding_dim, self.num_classes)
        loss_fn = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=learning_rate)

        dataset_tensor = dataset["features"]
        labels = dataset["labels"]

        losses = []
        for epoch in range(num_epochs):
            # Shuffle data
            idx = torch.randperm(len(dataset_tensor))
            epoch_loss = 0
            batch_count = 0

            for i in range(0, len(dataset_tensor), batch_size):
                batch_idx = idx[i:i + batch_size]
                batch_data = dataset_tensor[batch_idx]
                batch_labels = labels[batch_idx]

                # Forward pass
                logits = model(batch_data)
                loss = loss_fn(logits, batch_labels)
                epoch_loss += loss.item()
                batch_count += 1

                # Backpropagation
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

            final_loss = epoch_loss / batch_count
            losses.append(final_loss)
            logger.info(f"Epoch [{epoch + 1}/{num_epochs}], Loss: {final_loss}")

        self.model_state = model.state_dict()

        # Plot loss curve
        # plt.plot(range(num_epochs), losses)
        # plt.xlabel("Epochs")
        # plt.ylabel("Loss")
        # plt.title(f"Node {self.id} Training Loss Curve")
        # plt.show()

        return final_loss

    def merge_models(self, models, losses):
        epsilon = 1e-8  # Small value to avoid division by zero
        adjusted_losses = [(loss + epsilon) for loss in losses]

        # Compute weights as inverse of adjusted loss, then normalize
        weights = [1 / loss for loss in adjusted_losses]
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        # Weighted average of model parameters
        global_state = {k: torch.zeros_like(v) for k, v in models[0].items()}
        for model_state, weight in zip(models, weights):
            for key in global_state:
                global_state[key] += model_state[key] * weight

        self.global_model_state = global_state  # Save global state

    # Prediction using embedding similarity
    def rankings(self, miners, winner):
        global_model = NeuralNetwork(input_dim=4, embedding_dim=self.embedding_dim, num_classes=self.num_classes)
        global_model.load_state_dict(self.global_model_state)

        ord = []
        winner_tensor = torch.tensor(winner)
        z_w = global_model(winner_tensor, return_embeddings=True)  # Winner embedding

        for miner in (miners.keys()):
            z_m = global_model(torch.tensor(miners[miner], dtype=torch.float64), return_embeddings=True)  # Miner embedding
            similarity = self.cosine_similarity(z_m.unsqueeze(0), z_w.unsqueeze(0)).item()
            ord.append((miner, similarity, miners[miner]))

        ord.sort(key=lambda t: t[1], reverse=True)

        return [i[0] for i in ord], [i[1] for i in ord], [i[2] for i in ord]

class AICons_Network:
    def __init__(self):
        self.nodes = []

    def add_nodes(self, nodes: list[Node]):
        self.nodes.extend(nodes)

    def fed_train(self, dataset: pd.DataFrame):
        n, d = len(self.nodes), len(dataset)

        # Extract features and labels
        features = dataset[["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]].values
        labels = dataset["Winner"].values

        # Stratified split into `n` subsets
        skf = StratifiedKFold(n_splits=n, shuffle=True, random_state=42)
        subsets = []

        for train_idx, _ in skf.split(features, labels):
            subset = dataset.iloc[train_idx].reset_index(drop=True)
            subsets.append(subset)

        losses = []
        for i, node in enumerate(self.nodes):
            print(f"Training Node {i + 1}/{n}")
            subset_tensor = {
                "features": torch.tensor(subsets[i][["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]].values, dtype=torch.float64),
                "labels": torch.tensor(subsets[i]["Winner"].values, dtype=torch.long),
            }
            loss = node.train_model(subset_tensor)
            losses.append(loss)

        for node in self.nodes:
            node.merge_models([node.model_state for node in self.nodes], losses)

    def pred(self, miners_list, winner):
        for node in self.nodes:
            print(node.rankings(miners_list, torch.tensor(winner, dtype=torch.float64)))


# Load and preprocess dataset
def main():
    file_path = sys.argv[1]
    data = pd.read_csv(file_path)

    data = data.sample(frac=1).reset_index(drop=True)

    features = ["CPU usage [%]", "Memory usage [%]", "Network throughput", "Disk throughput"]
    labels = data["Winner"]

    scaler = StandardScaler()
    normalized_data = pd.DataFrame(scaler.fit_transform(data[features]), columns=features)
    normalized_data["Winner"] = labels

    winner = normalized_data[normalized_data["Winner"] == 1][features].mean().values

    # Initialize network and train
    net = AICons_Network()
    net.add_nodes([Node(id=0), Node(id=1), Node(id=2)])
    net.fed_train(normalized_data)

    # Test data
    test_data = normalized_data.sample(n=10).reset_index(drop=True)
    print(test_data)
    net.pred(test_data[features].values, winner)

if __name__ == 'main':
    main()
