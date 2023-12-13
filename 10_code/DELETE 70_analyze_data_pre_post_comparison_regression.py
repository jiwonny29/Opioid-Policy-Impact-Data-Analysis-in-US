import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


def plot_regression(
    data,
    state_filter,
    break_year,
    response_variable,
    save_suffix,
):
    # Filter data by state
    data_filtered = data[data["state_name"] == state_filter]

    # Filter data before and after the breakpoint year
    df_before_break = data_filtered[data_filtered["year"] < break_year]
    df_after_break = data_filtered[data_filtered["year"] >= break_year]

    # Create a figure
    plt.figure(figsize=(10, 6))

    # Linear regression for before the breakpoint year
    sns.regplot(
        x="year",
        y=response_variable,
        data=df_before_break,
        ci=95,
        scatter=False,
        label=f"{state_filter} Before",
        line_kws={"color": "#8D99AE"},
    )

    # Linear regression for after the breakpoint year
    sns.regplot(
        x="year",
        y=response_variable,
        data=df_after_break,
        ci=95,
        scatter=False,
        label=f"{state_filter} After",
        line_kws={"color": "#D90429"},
    )

    # Add dashed lines to indicate the breakpoint year
    plt.axvline(
        x=break_year,
        color="gray",
        linestyle="--",
        label=f"Policy Change ({break_year})",
    )

    # Add labels and legend
    plt.xlabel("Year")

    # Customize the Y-axis label based on the variable
    if response_variable == "mme_per_capita":
        plt.ylabel("Morphine Milligram Equivalent Per Capita")
        plt.title(
            f"The Effect of Policy on Morphine Milligram Equivalent Per Capita in {state_filter}"
        )
    elif response_variable == "filled_mortality_rate_unintentional_drug_poisoning":
        plt.ylabel("Unintentional Drug Poisoning Mortality Rate")
        plt.title(
            f"The Effect of Policy on Unintentional Drug Poisoning Mortality Rate in {state_filter}"
        )

    plt.legend()

    # Save the figure to the specified path
    save_path = f"../30_results/{save_suffix}.png"
    plt.savefig(save_path)


# Load the data
annual_data = pd.read_csv(
    "../20_intermediate_files/us_population_vitalstatistics_opioids_yearly.csv"
)


# combinations of state and response variables with specific break years and save_suffix
combinations = [
    (
        "TX",
        "filled_mortality_rate_unintentional_drug_poisoning",
        2007,
        "TX_MortalityRate_Regression",
    ),
    (
        "FL",
        "filled_mortality_rate_unintentional_drug_poisoning",
        2010,
        "FL_MortalityRate_Regression",
    ),
    ("FL", "mme_per_capita", 2010, "FL_MEE_Regression"),
    (
        "WA",
        "filled_mortality_rate_unintentional_drug_poisoning",
        2012,
        "WA_MortalityRate_Regression",
    ),
    ("WA", "mme_per_capita", 2012, "WA_MEE_Regression"),
]


for state, response_var, break_year, save_suffix in combinations:
    plot_regression(
        annual_data,
        state_filter=state,
        break_year=break_year,
        response_variable=response_var,
        save_suffix=save_suffix,
    )
