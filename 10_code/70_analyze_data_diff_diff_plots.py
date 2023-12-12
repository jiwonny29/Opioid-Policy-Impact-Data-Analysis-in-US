import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def plot_regression(
    data,
    state_filter,
    counterfactual_states,
    break_year,
    response_variable,
    save_suffix,
):
    # Filter data by the original state
    data_filtered = data[data["state_name"] == state_filter]

    # Filter data before and after the breakpoint year for the original state
    df_before_break = data_filtered[data_filtered["year"] < break_year]
    df_after_break = data_filtered[data_filtered["year"] >= break_year]

    # Create a figure
    plt.figure(figsize=(10, 6))

    # Define color variables
    color_original_before = "#9D9D9D"
    color_original_after = "#ef233c"
    color_counterfactual_before = "#001d3d"
    color_counterfactual_after = "#800f2f"
    color_breakline = "gray"

    linestyle_counterfactual = "--"

    # Linear regression for the original state before and after the breakpoint year
    for label, df, color in zip(
        [f"{state_filter} Before", f"{state_filter} After"],
        [df_before_break, df_after_break],
        [color_original_before, color_original_after],
    ):
        sns.regplot(
            x="year",
            y=response_variable,
            data=df,
            ci=95,
            scatter=False,
            label=label,
            line_kws={"color": color},
        )

    # Filter data for the counterfactual states and combine their data
    df_counterfactual = data[data["state_name"].isin(counterfactual_states)]
    df_before_break_counterfactual = df_counterfactual[
        (df_counterfactual["year"] < break_year)
    ]
    df_after_break_counterfactual = df_counterfactual[
        (df_counterfactual["year"] >= break_year)
    ]

    # Linear regression for the counterfactual states before and after the breakpoint year
    for label, df, state, color in zip(
        ["Counterfactual Before", "Counterfactual After"],
        [df_before_break_counterfactual, df_after_break_counterfactual],
        counterfactual_states,
        [
            color_counterfactual_before,
            color_counterfactual_after,
        ],
    ):
        sns.regplot(
            x="year",
            y=response_variable,
            data=df,
            ci=95,
            scatter=False,
            label=f"{state_filter} {label}",
            line_kws={"color": color, "linestyle": linestyle_counterfactual},
        )

    # Add breakpoint year
    plt.axvline(
        x=break_year,
        color=color_breakline,
        linestyle="--",
        label=f"Policy Change ({break_year})",
    )

    # Add labels and legend
    plt.xlabel("Year")

    if response_variable == "mme_per_capita":
        plt.ylabel("Morphine Milligram Equivalent Per Capita")
        plt.title(
            f"The Effect of Policy on Morphine Milligram Equivalent Per Capita in {state_filter} and Counterfactual States"
        )
    elif response_variable == "filled_mortality_rate_unintentional_drug_poisoning":
        plt.ylabel("Unintentional Drug Poisoning Mortality Rate")
        plt.title(
            f"The Effect of Policy on Unintentional Drug Poisoning Mortality Rate in {state_filter} and Counterfactual States"
        )

    plt.legend()

    # Save the figure to the specified path
    save_path = f"../30_results/{save_suffix}.png"
    plt.savefig(save_path)


# Load the data
annual_data = pd.read_csv(
    "../20_intermediate_files/us_population_vitalstatistics_opioids_yearly.csv"
)

# Texas vs NY, VA, ID - filled_mortality_rate_unintentional_drug_poisoning
plot_regression(
    annual_data,
    state_filter="TX",
    counterfactual_states=["NY", "VA", "ID"],
    break_year=2007,
    response_variable="filled_mortality_rate_unintentional_drug_poisoning",
    save_suffix="TX_vs_NY-VA-ID_Mortality",
)

# Florida vs DE, NV, TN - filled_mortality_rate_unintentional_drug_poisoning
plot_regression(
    annual_data,
    state_filter="FL",
    counterfactual_states=["DE", "NV", "TN"],
    break_year=2010,
    response_variable="filled_mortality_rate_unintentional_drug_poisoning",
    save_suffix="FL_vs_DE-NV-TN_Mortality",
)

# Florida vs DE, NV, TN - mme_per_capita
plot_regression(
    annual_data,
    state_filter="FL",
    counterfactual_states=["DE", "NV", "TN"],
    break_year=2010,
    response_variable="mme_per_capita",
    save_suffix="FL_vs_DE-NV-TN_MME",
)

# Washington vs MA, VT, MT - filled_mortality_rate_unintentional_drug_poisoning
plot_regression(
    annual_data,
    state_filter="WA",
    counterfactual_states=["MA", "VT", "MT"],
    break_year=2012,
    response_variable="filled_mortality_rate_unintentional_drug_poisoning",
    save_suffix="WA_vs_MA-VT-MT_Mortality",
)

# Washington vs MA, VT, MT - mme_per_capita
plot_regression(
    annual_data,
    state_filter="WA",
    counterfactual_states=["MA", "VT", "MT"],
    break_year=2012,
    response_variable="mme_per_capita",
    save_suffix="WA_vs_MA-VT-MT_MME",
)
