import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import gridspec

# Load the CSV file
file_path = 'AccountInfo.csv'
account_info_df = pd.read_csv(file_path)
account_info_df.columns = ['timestamp','position', 'open_profit', 'open_Notional', 'realized_pnl']
# Ensure the timestamp is in datetime format
account_info_df['timestamp'] = pd.to_datetime(account_info_df['timestamp'])
account_info_df = account_info_df[account_info_df['timestamp']>='2024-06-21 17:53:08']
# Function to calculate the Sharpe ratio
def calculate_sharpe_ratio(pnl_series, risk_free_rate=0):
    # Calculate the returns
    returns = pnl_series.pct_change().dropna()
    # Calculate the excess returns
    excess_returns = returns
    sharpe_ratio = np.sqrt(365) * (excess_returns.mean() / excess_returns.std())
    return sharpe_ratio

fig = plt.figure(figsize=(12, 18))
gs = gridspec.GridSpec(3, 1, height_ratios=[2, 1, 1])

# Real-time realized PnL (occupying 1/2 area)
ax0 = plt.subplot(gs[0])
ax0.plot(account_info_df['timestamp'], account_info_df['realized_pnl'], label='Realized PnL', color='blue')
ax0.set_ylabel('Realized PnL')
ax0.set_title(f'Sharp: {calculate_sharpe_ratio(account_info_df.realized_pnl)}')
ax0.legend()

# Real-time position (smaller)
ax1 = plt.subplot(gs[1])
ax1.plot(account_info_df['timestamp'], account_info_df['position'], label='Position', color='orange')
ax1.set_ylabel('Position')
ax1.set_title('Real-time Position')
ax1.legend()

# Real-time open profit (smaller)
ax2 = plt.subplot(gs[2])
ax2.plot(account_info_df['timestamp'], account_info_df['open_profit'], label='Open Profit', color='green')
ax2.set_xlabel('Timestamp')
ax2.set_ylabel('Open Profit')
ax2.set_title('Real-time Open Profit')
ax2.legend()

# Adjust layout to prevent overlap
plt.tight_layout(rect=[0.0, 0.0, 1.0, 0.95])
plt.subplots_adjust(hspace=0.3)  # Adjust the height space between plots
plt.show()


# Calculate the Sharpe ratio for the realized PnL
realized_pnl_series = account_info_df['realized_pnl']
sharpe_ratio = calculate_sharpe_ratio(realized_pnl_series)
print(f'Sharpe Ratio: {sharpe_ratio}')
