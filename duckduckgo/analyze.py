import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load experiment data
data = pd.read_csv("experiment_results.csv")

# Basic scatter plot of transaction count vs. elapsed time for each mode and type
plt.figure(figsize=(10, 6))
sns.scatterplot(data=data, x="TransactionCount", y="ElapsedTime", hue="ExecutionMode", style="TransactionType")
plt.title("Elapsed Time by Transaction Count")
plt.xlabel("Transaction Count")
plt.ylabel("Elapsed Time (s)")
plt.legend(title="Mode and Type")
plt.show()
