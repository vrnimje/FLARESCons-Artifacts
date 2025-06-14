import pandas as pd
import sys
import os

# Load the dataset (update this with the actual file path if necessary)
file_path = sys.argv[1]
data = pd.read_csv(file_path, delimiter=";")

# Convert percentage columns to numeric (replace commas and convert to float)
data["CPU usage [%]"] = data["CPU usage [%]"].str.replace(",", ".").astype(float)
data["Memory usage [%]"] = data["Memory usage [%]"].str.replace(",", ".").astype(float)

# Add new calculated metrics for throughput and overall usage
data["Disk throughput"] = (data["Disk read throughput [KB/s]"] + data["Disk write throughput [KB/s]"])
data["Network throughput"] = data["Network received throughput [KB/s]"] + data["Network transmitted throughput [KB/s]"]
data["Overall usage"] = (
    data["CPU usage [%]"] +
    data["Memory usage [%]"] +
    data["Disk throughput"] +
    data["Network throughput"]
)

# Define threshold for labeling winners
threshold = data["Overall usage"].quantile(0.75)  # Top 25% of overall usage

# Label rows where overall usage exceeds the threshold
data["Winner"] = data["Overall usage"] > threshold

data['Winner'] = data['Winner'].astype(int)

# Save the labeled dataset to a new file
fn = file_path.split('/')[-1][0:-4]
output_path = os.path.join(os.path.dirname(file_path), f"{fn}_labeled.csv")
data.to_csv(output_path, index=False)

print(f"Labeled dataset saved to {output_path}")