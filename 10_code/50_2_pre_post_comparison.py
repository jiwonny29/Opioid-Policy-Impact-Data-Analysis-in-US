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

# Set the policy change years for Texas, Florida, and Washington
policy_change_years = {
    "FL": 2010,  # Policy change year for Florida (FL)
    "WA": 2012,  # Policy change year for Washington (WA)
    "TX": 2007,  # Policy change year for Texas (TX)
}

# Filtering data for the states Texas (TX), Florida (FL), and Washington (WA) between the years 2006 and 2015
state_yearly_avg = data[
    (data["state"].isin(["TX", "FL", "WA"])) & (data["year"].between(2006, 2015))
][["state", "year", "mean_mortality_rate_per_state_year"]]

# Normalize years relative to the policy change year for each state
state_yearly_avg["years_from_policy"] = state_yearly_avg.apply(
    lambda row: row["year"] - policy_change_years[row["state"]], axis=1
)

# Extract data for each state
fl_data = state_yearly_avg[state_yearly_avg["state"] == "FL"]
wa_data = state_yearly_avg[state_yearly_avg["state"] == "WA"]
tx_data = state_yearly_avg[state_yearly_avg["state"] == "TX"]

# Define the directory to save the graphs
result_directory = "../30_results"  # Replace with your actual path
os.makedirs(result_directory, exist_ok=True)


# Define a function to create and save the pre-post policy graphs with a single line before and after the policy change
def create_and_save_single_line_pre_post_policy_graph(
    data, state_abbr, policy_change_year, filename
):
    fig, ax = plt.subplots(figsize=(6, 4))  # Narrower vertical height

    pre_policy_data = data[data["years_from_policy"] < 0]
    post_policy_data = data[data["years_from_policy"] >= 0]

    # Calculate the average before and after the policy change
    pre_policy_mean = pre_policy_data["mean_mortality_rate_per_state_year"].mean()
    post_policy_mean = post_policy_data["mean_mortality_rate_per_state_year"].mean()

    # Get the range of years before and after the policy change
    pre_policy_years = pre_policy_data["years_from_policy"].unique()
    post_policy_years = post_policy_data["years_from_policy"].unique()

    # Plot a single line for before and after policy change with specified colors
    ax.plot(
        [min(pre_policy_years), 0],
        [pre_policy_mean, pre_policy_mean],
        label=f"{state_abbr} Before",
        marker="o",
        color="#8d99ae",  # Set line color for "before"
    )
    ax.plot(
        [0, max(post_policy_years)],
        [post_policy_mean, post_policy_mean],
        label=f"{state_abbr} After",
        marker="o",
        color="#d90429",  # Set line color for "after"
    )
    ax.axvline(
        x=0,
        color="black",
        linestyle="--",
        label=f"Policy Change ({policy_change_year})",
    )
    ax.set_title(f"The Effect of Policy on Drug-Related Mortality in {state_abbr}")
    ax.set_xlabel("Years from Policy Change")
    ax.set_ylabel("Avg Mortality Rate per 100K (Per State)")
    ax.set_ylim(10, 15)  # Set y-axis limits
    ax.legend()

    # Adjust layout
    plt.tight_layout()

    # Save the figure in the result directory
    graph_filename = os.path.join(result_directory, filename)
    plt.savefig(graph_filename, dpi=300)

    # Close the figure to avoid display in the notebook
    plt.close()


# Generate and save graphs for each state separately
create_and_save_single_line_pre_post_policy_graph(
    fl_data, "Florida", policy_change_years["FL"], "FL_Mortality_Change.png"
)
create_and_save_single_line_pre_post_policy_graph(
    wa_data, "Washington", policy_change_years["WA"], "WA_Mortality_Change.png"
)
create_and_save_single_line_pre_post_policy_graph(
    tx_data, "Texas", policy_change_years["TX"], "TX_Mortality_Change.png"
)
