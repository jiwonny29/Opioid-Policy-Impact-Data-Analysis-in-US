import os
import pandas as pd
import matplotlib.pyplot as plt

# Load the data
data = pd.read_csv("../20_intermediate_files/final_merged_table.csv")


######################### Pre-Post Model Comparison on Morphine mg Per Capita #########################
# Calculate Morphine mg per capita for each county
data["morphine_mg_per_capita"] = data["total_morphine_mg"] / data["total_population"]

# Group by state and year to calculate the mean Morphine mg per capita
state_yearly_avg = (
    data.groupby(["state", "year"])["morphine_mg_per_capita"].mean().reset_index()
)

# Set the policy change years
policy_change_years = {
    "FL": 2010,  # Policy change year for Florida (FL)
    "WA": 2012,  # Policy change year for Washington (WA)
}

# Normalize years relative to the policy change year
for state, policy_change_year in policy_change_years.items():
    state_yearly_avg[f"years_from_policy_{state.lower()}"] = (
        state_yearly_avg["year"] - policy_change_year
    )

# Extract data for each state
fl_data = state_yearly_avg[state_yearly_avg["state"] == "FL"]
wa_data = state_yearly_avg[state_yearly_avg["state"] == "WA"]

# Create the Florida (FL) graph
fig, ax_fl = plt.subplots(figsize=(6, 4.5))  # Adjusted figsize

# Set the universal y-axis limits
y_axis_limits = (0, 750)

# Colors for the lines
color_before = "#8D99AE"  # Dark blue-gray
color_after = "#D90429"  # Bright red

# Florida (FL) graph
fl_pre_policy_data = fl_data[fl_data["years_from_policy_fl"] < 0]
fl_post_policy_data = fl_data[fl_data["years_from_policy_fl"] >= 0]
ax_fl.plot(
    fl_pre_policy_data["years_from_policy_fl"],
    fl_pre_policy_data["morphine_mg_per_capita"],
    color=color_before,
    label="FL Before",
)
ax_fl.plot(
    fl_post_policy_data["years_from_policy_fl"],
    fl_post_policy_data["morphine_mg_per_capita"],
    color=color_after,
    label="FL After",
)
ax_fl.axvline(x=0, color="black", linestyle="--", label="Policy Change (2010)")
ax_fl.set_title("The Effect of Policy on Morphine Per Capita in Florida")
ax_fl.set_xlabel("Years from Policy Change")
ax_fl.set_ylabel("Morphine Per Capita (mg)")
ax_fl.set_ylim(y_axis_limits)
ax_fl.legend()

# Define the directory to save the graphs
result_directory = "../30_results"  # Replace with your actual path
os.makedirs(result_directory, exist_ok=True)

# Save the Florida (FL) graph as a PNG file in the result directory
fl_graph_filename = os.path.join(result_directory, "FL_MPP_Change.png")
plt.tight_layout()
plt.savefig(fl_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display

# Create the Washington (WA) graph
fig, ax_wa = plt.subplots(figsize=(6, 4.5))  # Adjusted figsize

# Washington (WA) graph
wa_pre_policy_data = wa_data[wa_data["years_from_policy_wa"] < 0]
wa_post_policy_data = wa_data[wa_data["years_from_policy_wa"] >= 0]
ax_wa.plot(
    wa_pre_policy_data["years_from_policy_wa"],
    wa_pre_policy_data["morphine_mg_per_capita"],
    color=color_before,
    label="WA Before",
)
ax_wa.plot(
    wa_post_policy_data["years_from_policy_wa"],
    wa_post_policy_data["morphine_mg_per_capita"],
    color=color_after,
    label="WA After",
)
ax_wa.axvline(x=0, color="black", linestyle="--", label="Policy Change (2012)")
ax_wa.set_title("The Effect of Policy on Morphine Per Capita in Washington")
ax_wa.set_xlabel("Years from Policy Change")
ax_wa.set_ylabel("Morphine Per Capita (mg)")
ax_wa.set_ylim(y_axis_limits)
ax_wa.legend()

# Save the Washington (WA) graph as a PNG file in the result directory
wa_graph_filename = os.path.join(result_directory, "WA_MPP_Change.png")
plt.tight_layout()
plt.savefig(wa_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


###################################### Pre-Post Model Comparison on Mortality Rate ######################################
import os
import pandas as pd
import matplotlib.pyplot as plt

# Load the data
data = pd.read_csv("../20_intermediate_files/final_merged_table.csv")

state_yearly_avg = (
    data.groupby(["state", "year"])["mean_mortality_rate_per_state_year"]
    .mean()
    .reset_index()
)

# Set the policy change years for Texas, Florida, and Washington
policy_change_years = {
    "FL": 2010,  # Policy change year for Florida (FL)
    "WA": 2012,  # Policy change year for Washington (WA)
    "TX": 2007,  # Policy change year for Texas (TX)
}

# Normalize years relative to the policy change year
for state, policy_change_year in policy_change_years.items():
    state_yearly_avg[f"years_from_policy_{state.lower()}"] = (
        state_yearly_avg["year"] - policy_change_year
    )

# Extract data for each state
fl_data = state_yearly_avg[state_yearly_avg["state"] == "FL"]
wa_data = state_yearly_avg[state_yearly_avg["state"] == "WA"]
tx_data = state_yearly_avg[state_yearly_avg["state"] == "TX"]

# Create the Florida (FL) graph
fig, ax_fl = plt.subplots(figsize=(6, 4))  # Adjusted figsize

# Set the universal y-axis limits
y_axis_limits = (9, 16)

# Colors for the lines
color_before = "#8D99AE"  # Dark blue-gray
color_after = "#D90429"  # Bright red

# Florida (FL) graph
fl_pre_policy_data = fl_data[fl_data["years_from_policy_fl"] < 0]
fl_post_policy_data = fl_data[fl_data["years_from_policy_fl"] >= 0]
ax_fl.plot(
    fl_pre_policy_data["years_from_policy_fl"],
    fl_pre_policy_data["mean_mortality_rate_per_state_year"],
    color=color_before,
    label="FL Before",
)
ax_fl.plot(
    fl_post_policy_data["years_from_policy_fl"],
    fl_post_policy_data["mean_mortality_rate_per_state_year"],
    color=color_after,
    label="FL After",
)
ax_fl.axvline(x=0, color="black", linestyle="--", label="Policy Change (2010)")
ax_fl.set_title("The Effect of Policy on Drug-Related Mortality in Florida")
ax_fl.set_xlabel("Years from Policy Change")
ax_fl.set_ylabel("Avg Mortality Rate per 100K (Per State)")
ax_fl.set_ylim(y_axis_limits)
ax_fl.legend()

# Define the directory to save the graphs
result_directory = "../30_results"  # Replace with your actual path
os.makedirs(result_directory, exist_ok=True)

# Save the Florida (FL) graph as a PNG file in the result directory
fl_graph_filename = os.path.join(result_directory, "FL_Mortality_Change.png")
plt.tight_layout()
plt.savefig(fl_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


# Create the Texas (TX) graph
fig, ax_tx = plt.subplots(figsize=(6, 4))  # Adjusted figsize

# Texas (TX) graph
tx_pre_policy_data = tx_data[tx_data["years_from_policy_tx"] < 0]
tx_post_policy_data = tx_data[tx_data["years_from_policy_tx"] >= 0]
ax_tx.plot(
    tx_pre_policy_data["years_from_policy_tx"],
    tx_pre_policy_data["mean_mortality_rate_per_state_year"],
    color=color_before,
    label="TX Before",
)
ax_tx.plot(
    tx_post_policy_data["years_from_policy_tx"],
    tx_post_policy_data["mean_mortality_rate_per_state_year"],
    color=color_after,
    label="TX After",
)
ax_tx.axvline(x=0, color="black", linestyle="--", label="Policy Change (2007)")
ax_tx.set_title("The Effect of Policy on Drug-Related Mortality in Texas")
ax_tx.set_xlabel("Years from Policy Change")
ax_tx.set_ylabel("Avg Mortality Rate per 100K (Per State)")
ax_tx.set_ylim(y_axis_limits)
ax_tx.legend()

# Save the Texas (TX) graph as a PNG file in the result directory
tx_graph_filename = os.path.join(result_directory, "TX_Mortality_Change.png")
plt.tight_layout()
plt.savefig(tx_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


# Create the Washington (WA) graph
fig, ax_wa = plt.subplots(figsize=(6, 4))  # Adjusted figsize

# Washington (WA) graph
wa_pre_policy_data = wa_data[wa_data["years_from_policy_wa"] < 0]
wa_post_policy_data = wa_data[wa_data["years_from_policy_wa"] >= 0]
ax_wa.plot(
    wa_pre_policy_data["years_from_policy_wa"],
    wa_pre_policy_data["mean_mortality_rate_per_state_year"],
    color=color_before,
    label="WA Before",
)
ax_wa.plot(
    wa_post_policy_data["years_from_policy_wa"],
    wa_post_policy_data["mean_mortality_rate_per_state_year"],
    color=color_after,
    label="WA After",
)
ax_wa.axvline(x=0, color="black", linestyle="--", label="Policy Change (2012)")
ax_wa.set_title("The Effect of Policy on Drug-Related Mortality in Washington")
ax_wa.set_xlabel("Years from Policy Change")
ax_wa.set_ylabel("Avg Mortality Rate per 100K (Per State)")
ax_wa.set_ylim(y_axis_limits)
ax_wa.legend()

# Save the Washington (WA) graph as a PNG file in the result directory
wa_graph_filename = os.path.join(result_directory, "WA_Mortality_Change.png")
plt.tight_layout()
plt.savefig(wa_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display
