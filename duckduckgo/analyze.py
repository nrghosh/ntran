import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
from datetime import datetime
import glob

def get_latest_csv(directory):
    """Find the most recently modified CSV file in the specified directory."""
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {directory}")
    return max(csv_files, key=os.path.getmtime)

def create_timestamp():
    """Create a formatted timestamp string."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# Ensure directories exist
os.makedirs("results", exist_ok=True)
os.makedirs("figures", exist_ok=True)

# Get the latest CSV file
latest_csv = get_latest_csv("results")
print(f"Using data from: {latest_csv}")

# Load experiment data
data = pd.read_csv(latest_csv)

# Create the plot
plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x="TransactionCount", y="ElapsedTime", 
                hue="ExecutionMode", style="TransactionType")
plt.title("Elapsed Time by Transaction Count")
plt.xlabel("Transaction Count")
plt.ylabel("Elapsed Time (s)")
plt.legend(title="Mode and Type")

# Save the figure with timestamp (use DB count and timestamp from csv title)
figure_path = os.path.join("figures", f"figure_{latest_csv.split("/")[1].replace(".csv", "")}.png")
plt.savefig(figure_path, dpi=300, bbox_inches='tight')
print(f"Figure saved to: {figure_path}")
plt.close()