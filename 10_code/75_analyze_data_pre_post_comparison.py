import pandas as pd
import matplotlib.pyplot as plt
import os

######################### Pre-Post Model Comparison on MME (FL, WA) #########################
# Switch to the main branch to access the data file
os.system(
    "git checkout main"
)  # Replace 'main' with the actual main branch name if necessary

# Load the data from the main branch us_population_vitalstatistics_opioids_yearly.csv file
rawdata = pd.read_csv(
    "../20_intermediate_files/us_population_vitalstatistics_opioids_yearly.csv"
)

# Switch back to your working branch (replace 'your_working_branch' with the actual branch name)
os.system("git checkout pre_post_comparison")

# Filter rows where 'merge_population_vitalstatistics' and 'merge_population_vitalstatistics_opioids' are both True
data = rawdata[
    (rawdata["merge_population_vitalstatistics"] == "both")
    & (rawdata["merge_population_vitalstatistics_opioids"] == "both")
]

# Select the relevant columns
data = data[["year", "state_name", "mme_per_capita"]]

# Group by year and state to calculate the mean and standard error of Morphine mg per capita across all counties
state_year_avg = (
    data.groupby(["year", "state_name"])["mme_per_capita"]
    .agg(["mean", "sem"])
    .reset_index()
)

# Set the policy change year for Florida (FL)
policy_change_year_fl = 2010

# Normalize years relative to the policy change year for FL
state_year_avg_fl = state_year_avg[state_year_avg["state_name"] == "FL"].copy()
state_year_avg_fl["years_from_policy"] = (
    state_year_avg_fl["year"] - policy_change_year_fl
)

# Sort the dataframe by 'years_from_policy' to ensure the line connects points in the correct order
state_year_avg_fl = state_year_avg_fl.sort_values("years_from_policy")

# Create the Florida (FL) graph
fig, ax_fl = plt.subplots(figsize=(8, 6))

# Define colors for error bars and lines
error_bar_color = "#2B2F42"
line_color_before = "#8D99AE"
line_color_after = "#D80032"

# Separate the data into before and after the policy change
before_policy_change = state_year_avg_fl[state_year_avg_fl["years_from_policy"] < 0]
after_policy_change = state_year_avg_fl[state_year_avg_fl["years_from_policy"] >= 0]

# Plot the mean with error bars for each period
ax_fl.errorbar(
    before_policy_change["years_from_policy"],
    before_policy_change["mean"],
    yerr=before_policy_change["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_before,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2006-2010",
)
ax_fl.errorbar(
    after_policy_change["years_from_policy"],
    after_policy_change["mean"],
    yerr=after_policy_change["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_after,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2011-2015",
)

# Set y-axis to start from 0
ax_fl.set_ylim(bottom=0)

# Additional plot formatting
ax_fl.axvline(x=0, color="black", linestyle="--", label="Policy Change (2010)")
ax_fl.set_title(
    "The Effect of Policy on Morphine Milligram Equivalent Per Capita in FL"
)
ax_fl.set_xlabel("Years from Policy Change")
ax_fl.set_ylabel("Morphine Milligram Equivalent Per Capita")
ax_fl.legend()

# Define the directory to save the graphs
result_directory = "../30_results"
os.makedirs(result_directory, exist_ok=True)

# Save the Florida (FL) graph as a PNG file in the result directory
fl_graph_filename = os.path.join(result_directory, "FL_MEE_Change_Avg.png")
plt.tight_layout()
plt.savefig(fl_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


# Now, let's create a similar plot for Washington (WA)
policy_change_year_wa = 2012  # Set the policy change year for Washington (WA)

# Normalize years relative to the policy change year for WA
state_year_avg_wa = state_year_avg[state_year_avg["state_name"] == "WA"].copy()
state_year_avg_wa["years_from_policy"] = (
    state_year_avg_wa["year"] - policy_change_year_wa
)

# Sort the dataframe by 'years_from_policy' to ensure the line connects points in the correct order
state_year_avg_wa = state_year_avg_wa.sort_values("years_from_policy")

# Create the Washington (WA) graph
fig, ax_wa = plt.subplots(figsize=(8, 6))

# Separate the data into before and after the policy change
before_policy_change_wa = state_year_avg_wa[state_year_avg_wa["years_from_policy"] < 0]
after_policy_change_wa = state_year_avg_wa[state_year_avg_wa["years_from_policy"] >= 0]

# Plot the mean with error bars for each period
ax_wa.errorbar(
    before_policy_change_wa["years_from_policy"],
    before_policy_change_wa["mean"],
    yerr=before_policy_change_wa["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_before,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2006-2012",
)
ax_wa.errorbar(
    after_policy_change_wa["years_from_policy"],
    after_policy_change_wa["mean"],
    yerr=after_policy_change_wa["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_after,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2013-2015",
)

# Set y-axis to start from 0
ax_wa.set_ylim(bottom=0)

# Additional plot formatting
ax_wa.axvline(x=0, color="black", linestyle="--", label="Policy Change (2012)")
ax_wa.set_title(
    "The Effect of Policy on Morphine Milligram Equivalent Per Capita in WA"
)
ax_wa.set_xlabel("Years from Policy Change")
ax_wa.set_ylabel("Morphine Milligram Equivalent Per Capita")
ax_wa.legend()

# Save the Washington (WA) graph as a PNG file in the result directory
wa_graph_filename = os.path.join(result_directory, "WA_MEE_Change_Avg.png")
plt.tight_layout()
plt.savefig(wa_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


###################################### Pre-Post Model Comparison on Mortality Rate (FL, WA, TX) ######################################

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Switch to the main branch to access the data file
os.system(
    "git checkout main"
)  # Replace 'main' with the actual main branch name if necessary

# Load the data from the main branch us_population_vitalstatistics_opioids_yearly.csv file
rawdata = pd.read_csv(
    "../20_intermediate_files/us_population_vitalstatistics_opioids_yearly.csv"
)

# Switch back to your working branch (replace 'your_working_branch' with the actual branch name)
os.system("git checkout pre_post_comparison")

# Filter rows where 'merge_population_vitalstatistics' and 'merge_population_vitalstatistics_opioids' are both True
data = rawdata[
    (rawdata["merge_population_vitalstatistics"] == "both")
    & (rawdata["merge_population_vitalstatistics_opioids"] == "both")
]

# Select the relevant columns
data = data[
    ["year", "state_name", "filled_mortality_rate_unintentional_drug_poisoning"]
]

# Group by year and state to calculate the mean and standard error of Morphine mg per capita across all counties
state_year_avg = (
    data.groupby(["year", "state_name"])[
        "filled_mortality_rate_unintentional_drug_poisoning"
    ]
    .agg(["mean", "sem"])
    .reset_index()
)

# Set the policy change year for Florida (FL)
policy_change_year_fl = 2010

# Normalize years relative to the policy change year for FL
state_year_avg_fl = state_year_avg[state_year_avg["state_name"] == "FL"].copy()
state_year_avg_fl["years_from_policy"] = (
    state_year_avg_fl["year"] - policy_change_year_fl
)

# Sort the dataframe by 'years_from_policy' to ensure the line connects points in the correct order
state_year_avg_fl = state_year_avg_fl.sort_values("years_from_policy")

# Create the Florida (FL) graph
fig, ax_fl = plt.subplots(figsize=(8, 6))

# Define colors for error bars and lines
error_bar_color = "#2B2F42"
line_color_before = "#8D99AE"
line_color_after = "#D80032"

# Separate the data into before and after the policy change
before_policy_change = state_year_avg_fl[state_year_avg_fl["years_from_policy"] < 0]
after_policy_change = state_year_avg_fl[state_year_avg_fl["years_from_policy"] >= 0]

# Plot the mean with error bars for each period
ax_fl.errorbar(
    before_policy_change["years_from_policy"],
    before_policy_change["mean"],
    yerr=before_policy_change["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_before,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2006-2010",
)
ax_fl.errorbar(
    after_policy_change["years_from_policy"],
    after_policy_change["mean"],
    yerr=after_policy_change["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_after,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2011-2015",
)

# Set y-axis to start from 0
ax_fl.set_ylim(bottom=0)

# Additional plot formatting
ax_fl.axvline(x=0, color="black", linestyle="--", label="Policy Change (2010)")
ax_fl.set_title(
    "The Effect of Policy on Unintentional Drug Poisoning Mortality Rate in FL"
)
ax_fl.set_xlabel("Years from Policy Change")
ax_fl.set_ylabel("Unintentional Drug Poisoning Mortality Rate")
ax_fl.legend()

# Define the directory to save the graphs
result_directory = "../30_results"  # Replace with your actual path
os.makedirs(result_directory, exist_ok=True)

# Save the Florida (FL) graph as a PNG file in the result directory
fl_graph_filename = os.path.join(result_directory, "FL_MortalityRate_Change_Avg.png")
plt.tight_layout()
plt.savefig(fl_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display

# Now, let's create a similar plot for Washington (WA)
policy_change_year_wa = 2012  # Set the policy change year for Washington (WA)

# Normalize years relative to the policy change year for WA
state_year_avg_wa = state_year_avg[state_year_avg["state_name"] == "WA"].copy()
state_year_avg_wa["years_from_policy"] = (
    state_year_avg_wa["year"] - policy_change_year_wa
)

# Sort the dataframe by 'years_from_policy' to ensure the line connects points in the correct order
state_year_avg_wa = state_year_avg_wa.sort_values("years_from_policy")

# Create the Washington (WA) graph
fig, ax_wa = plt.subplots(figsize=(8, 6))

# Separate the data into before and after the policy change
before_policy_change_wa = state_year_avg_wa[state_year_avg_wa["years_from_policy"] < 0]
after_policy_change_wa = state_year_avg_wa[state_year_avg_wa["years_from_policy"] >= 0]

# Plot the mean with error bars for each period
ax_wa.errorbar(
    before_policy_change_wa["years_from_policy"],
    before_policy_change_wa["mean"],
    yerr=before_policy_change_wa["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_before,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2006-2012",
)
ax_wa.errorbar(
    after_policy_change_wa["years_from_policy"],
    after_policy_change_wa["mean"],
    yerr=after_policy_change_wa["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_after,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2013-2015",
)

# Set y-axis to start from 0
ax_wa.set_ylim(bottom=0)

# Additional plot formatting
ax_wa.axvline(x=0, color="black", linestyle="--", label="Policy Change (2012)")
ax_wa.set_title(
    "The Effect of Policy on Unintentional Drug Poisoning Mortality Rate in WA"
)
ax_wa.set_xlabel("Years from Policy Change")
ax_wa.set_ylabel("Unintentional Drug Poisoning Mortality Rate")
ax_wa.legend()

# Save the Washington (WA) graph as a PNG file in the result directory
wa_graph_filename = os.path.join(result_directory, "WA_MortalityRate_Change_Avg.png")
plt.tight_layout()
plt.savefig(wa_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display

# Now, let's create a similar plot for Texas (TX)
policy_change_year_tx = 2007  # Set the policy change year for Texas (TX)

# Normalize years relative to the policy change year for TX
state_year_avg_tx = state_year_avg[state_year_avg["state_name"] == "TX"].copy()
state_year_avg_tx["years_from_policy"] = (
    state_year_avg_tx["year"] - policy_change_year_tx
)

# Sort the dataframe by 'years_from_policy' to ensure the line connects points in the correct order
state_year_avg_tx = state_year_avg_tx.sort_values("years_from_policy")

# Create the Texas (TX) graph
fig, ax_tx = plt.subplots(figsize=(8, 6))

# Separate the data into before and after the policy change
before_policy_change_tx = state_year_avg_tx[state_year_avg_tx["years_from_policy"] < 0]
after_policy_change_tx = state_year_avg_tx[state_year_avg_tx["years_from_policy"] >= 0]

# Plot the mean with error bars for each period
ax_tx.errorbar(
    before_policy_change_tx["years_from_policy"],
    before_policy_change_tx["mean"],
    yerr=before_policy_change_tx["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_before,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2006-2007",
)
ax_tx.errorbar(
    after_policy_change_tx["years_from_policy"],
    after_policy_change_tx["mean"],
    yerr=after_policy_change_tx["sem"],
    fmt="o-",  # 'o' for circular markers, '-' for a solid line
    color=line_color_after,
    ecolor=error_bar_color,
    capsize=3,  # Set the size of the caps on the error bars
    label="2013-2015",
)

# Set y-axis to start from 0
ax_tx.set_ylim(bottom=0)

# Additional plot formatting
ax_tx.axvline(x=0, color="black", linestyle="--", label="Policy Change (2012)")
ax_tx.set_title(
    "The Effect of Policy on Unintentional Drug Poisoning Mortality Rate in TX"
)
ax_tx.set_xlabel("Years from Policy Change")
ax_tx.set_ylabel("Unintentional Drug Poisoning Mortality Rate")
ax_tx.legend()

# Save the Texas (TX) graph as a PNG file in the result directory
tx_graph_filename = os.path.join(result_directory, "TX_MortalityRate_Change_Avg.png")
plt.tight_layout()
plt.savefig(tx_graph_filename, dpi=300)
plt.close()  # Close the figure to avoid display


######################### Pre-Post Model Comparison on MME - Monthly Basis (TX) #########################
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Switch to the main branch to access the data file
os.system(
    "git checkout main"
)  # Replace 'main' with the actual main branch name if necessary

# Load the data from the main branch us_population_vitalstatistics_opioids_yearly.csv file
rawdata = pd.read_csv("../20_intermediate_files/texas_population_opioids_monthly.csv")

# Switch back to your working branch (replace 'your_working_branch' with the actual branch name)
os.system("git checkout pre_post_comparison")


# Filter rows where 'merge_population_opioids' is both True
data = rawdata[rawdata["merge_population_opioids"] == "both"]

# Select the relevant columns
data = data[["year_month", "state_name", "mme_per_capita"]]

# Convert year_month to datetime
data["year_month"] = pd.to_datetime(data["year_month"])

# Group by year_month and state to calculate the mean and standard error of Morphine mg per capita across all counties
grouped_data = (
    data.groupby(["year_month", "state_name"])["mme_per_capita"]
    .agg(["mean", "sem"])
    .reset_index()
)

# Set the policy change year and month for Texas (TX)
policy_change_date_tx = pd.Timestamp(year=2007, month=1, day=1)

# Normalize months relative to the policy change year and month for TX
grouped_data_tx = grouped_data[grouped_data["state_name"] == "TX"].copy()
grouped_data_tx["months_from_policy"] = (
    grouped_data_tx["year_month"].dt.year - policy_change_date_tx.year
) * 12 + (grouped_data_tx["year_month"].dt.month - policy_change_date_tx.month)

# Sort the dataframe by 'months_from_policy' to ensure the line connects points in the correct order
grouped_data_tx.sort_values("months_from_policy", inplace=True)

# Create the Texas (TX) graph
fig, ax_tx = plt.subplots(figsize=(10, 6))

# Define colors for before and after policy change
color_before = "#8D99AE"  # Color for before policy change
color_after = "#D80032"  # Color for after policy change

# Plot before policy change
before_policy_change = grouped_data_tx[grouped_data_tx["months_from_policy"] < 0]
ax_tx.errorbar(
    before_policy_change["months_from_policy"],
    before_policy_change["mean"],
    yerr=before_policy_change["sem"],
    fmt="o-",
    color=color_before,
    ecolor="black",
    capsize=3,
    label="Before Policy Change",
)

# Plot after policy change
after_policy_change = grouped_data_tx[grouped_data_tx["months_from_policy"] >= 0]
ax_tx.errorbar(
    after_policy_change["months_from_policy"],
    after_policy_change["mean"],
    yerr=after_policy_change["sem"],
    fmt="o-",
    color=color_after,
    ecolor="black",
    capsize=3,
    label="After Policy Change",
)

# Set x-axis labels to the year_month format and adjust for readability
ax_tx.set_xticks(grouped_data_tx["months_from_policy"][::6])
ax_tx.set_xticklabels(
    grouped_data_tx["year_month"].dt.strftime("%Y-%m")[::6], rotation=90
)

# Set y-axis to start from 0
ax_tx.set_ylim(bottom=0)

# Additional plot formatting
ax_tx.axvline(x=0, color="black", linestyle="--", label="Policy Change (2007)")
ax_tx.set_title(
    "The Effect of Policy on Morphine Milligram Equivalent Per Capita in TX"
)
ax_tx.set_xlabel("Months from Policy Change")
ax_tx.set_ylabel("Morphine Milligram Equivalent Per Capita")
ax_tx.legend()

# Define the directory to save the graphs
result_directory = "../30_results"
os.makedirs(result_directory, exist_ok=True)

# Save the Texas (TX) graph as a PNG file in the result directory
tx_graph_filename = os.path.join(result_directory, "TX_MEE_Change_Avg.png")
plt.tight_layout()
plt.savefig(tx_graph_filename, dpi=300)
plt.show()  # Display the figure
