import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib import dates as mdates


def plot_regression(
    data,
    state_filter,
    counterfactual_states,
    break_date,
    response_variable,
    save_suffix,
):
    # Filter data by the original state
    data_filtered = data[data["state_name"] == state_filter]

    # Filter data before and after the breakpoint date for the original state
    df_before_break = data_filtered[data_filtered["year_month"] < break_date]
    df_after_break = data_filtered[data_filtered["year_month"] >= break_date]

    # Create a figure
    plt.figure(figsize=(10, 6))

    # Define color variables
    color_original_before = "#9D9D9D"
    color_original_after = "#ef233c"
    color_counterfactual_before = "#001d3d"
    color_counterfactual_after = "#800f2f"
    color_breakline = "gray"

    linestyle_counterfactual = "--"

    # Linear regression for the original state before and after the breakpoint date
    for label, df, color in zip(
        [f"{state_filter} Before Policy Change", f"{state_filter} After Policy Change"],
        [df_before_break, df_after_break],
        [color_original_before, color_original_after],
    ):
        x_values = mdates.date2num(df["year_month"])
        sns.regplot(
            x=x_values,
            y=response_variable,
            data=df,
            ci=90,
            scatter=False,
            label=label,
            line_kws={"color": color},
        )

    # Filter data for the counterfactual states and combine their data
    df_counterfactual = data[data["state_name"].isin(counterfactual_states)]
    df_before_break_counterfactual = df_counterfactual[
        (df_counterfactual["year_month"] < break_date)
    ]
    df_after_break_counterfactual = df_counterfactual[
        (df_counterfactual["year_month"] >= break_date)
    ]

    # Linear regression for the counterfactual states before and after the breakpoint date
    for label, df, state, color in zip(
        ["CF Before Policy Change", "CF After Policy Change"],
        [df_before_break_counterfactual, df_after_break_counterfactual],
        counterfactual_states,
        [
            color_counterfactual_before,
            color_counterfactual_after,
        ],
    ):
        x_values = mdates.date2num(df["year_month"])
        sns.regplot(
            x=x_values,
            y=response_variable,
            data=df,
            ci=90,
            scatter=False,
            label=f"{state_filter} {label}",
            line_kws={"color": color, "linestyle": linestyle_counterfactual},
        )

    # Add breakpoint date
    plt.axvline(
        x=mdates.date2num(break_date),
        color=color_breakline,
        linestyle="--",
        label=f"Policy Change (2007)",
    )

    # Add labels and legend
    plt.xlabel("Year and Month")
    plt.ylabel("Morphine Milligram Equivalent Per Capita")
    plt.title(
        f"The Effect of Policy on Morphine Milligram Equivalent Per Capita in {state_filter} and Counterfactual States"
    )

    plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=90)
    plt.legend()

    plt.tight_layout()

    # Save the figure to the specified path
    save_path = f"../30_results/{save_suffix}.png"
    plt.savefig(save_path)


# Load the data
monthly_data = pd.read_csv(
    "../20_intermediate_files/texas_population_opioids_monthly.csv"
)
monthly_data = monthly_data[monthly_data["merge_population_opioids"] == "both"]
monthly_data["year_month"] = pd.to_datetime(monthly_data["year_month"])

# Set the breakpoint
breakpoint_date = pd.to_datetime("2007-01")

plot_regression(
    monthly_data,
    state_filter="TX",
    counterfactual_states=["NY", "VA", "ID"],
    break_date=breakpoint_date,
    response_variable="mme_per_capita",
    save_suffix="TX_vs_NY-VA-ID_MME",
)
