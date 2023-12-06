import pandas as pd

print("Rationale for Cause of Death Selection:\n")

print(
    "In this analysis, our objective is to evaluate the viability of selecting the most prevalent cause of\n"
    "drug-related deaths, as opposed to aggregating all existing causes. This choice arises from disparities\n"
    "in completeness among each cause. Particularly, for 'Unintentional Drug Poisonings Deaths', we observe\n"
    "higher completeness. Consequently, we aim to estimate the advantages and disadvantages associated with\n"
    "adopting this alternative.\n"
)


### Importing Counterfactuals
# "For this analysis, we will only use the 3 states under investigation
filtered_states = ["TX", "FL", "WA"]


### Importing Death data. We have info between 2003 and 2015
vitalstatistics = "../20_intermediate_files/Transformed_US_VitalStatistics.csv"
vitalstatistics = pd.read_csv(vitalstatistics)

vitalstatistics = vitalstatistics[vitalstatistics["State"].isin(filtered_states)]

new_names = {
    "Deaths caused by All other drug-induced causes": "Other Drug Deaths",
    "Deaths caused by Drug poisonings (overdose) Suicide (X60-X64)": "Suicide Drug Poisonings Deaths",
    "Deaths caused by Drug poisonings (overdose) Undetermined (Y10-Y14)": "Undetermined Drug Poisonings Deaths",
    "Deaths caused by Drug poisonings (overdose) Unintentional (X40-X44)": "Unintentional Drug Poisonings Deaths",
}

vitalstatistics = vitalstatistics.rename(columns=new_names)


# We filter out 5 cases where the value of deaths takes on the 'Missing' value.
vitalstatistics = vitalstatistics[vitalstatistics["Other Drug Deaths"] != "Missing"]

print(
    "First, as a test, we will begin by analyzing the 3 states under investigation. After filtering the 3\n"
    f"states (TX, WA, and FL), we have {vitalstatistics.shape[0]:,} rows, representing a combination of county/year.\n"
)

remaining_rows_1_cause = vitalstatistics[
    vitalstatistics["Unintentional Drug Poisonings Deaths"].notna()
].shape[0]
print(
    f"In the scenario where we only remove missing values for the variable 'Unintentional Drug Poisonings\n"
    f"Deaths' we would be left with {remaining_rows_1_cause:,} rows of data.\n"
)


vitalstatistics["all_drug_death_causes_present"] = (
    ~vitalstatistics["Other Drug Deaths"].isna()
    & ~vitalstatistics["Suicide Drug Poisonings Deaths"].isna()
    & ~vitalstatistics["Undetermined Drug Poisonings Deaths"].isna()
    & ~vitalstatistics["Unintentional Drug Poisonings Deaths"].isna()
).astype(int)


# For the next analysis, let's only consider the states that have complete data for
# all 4 causes of death related to drug consumption."


vitalstatistics = vitalstatistics[vitalstatistics["all_drug_death_causes_present"] == 1]

remaining_rows_4_cause = vitalstatistics.shape[0]

print(
    f"However, in the scenario of removing the rows that have at least 1 missing value in the drug-related\n"
    f"death causes, we would have {remaining_rows_4_cause:,} rows representing a combination of county/year.\n"
)


# Calculate the total number of drug-related deaths by summing individual causes
vitalstatistics["Other Drug Deaths"] = vitalstatistics["Other Drug Deaths"].astype(
    float
)
vitalstatistics["Suicide Drug Poisonings Deaths"] = vitalstatistics[
    "Suicide Drug Poisonings Deaths"
].astype(float)
vitalstatistics["Undetermined Drug Poisonings Deaths"] = vitalstatistics[
    "Undetermined Drug Poisonings Deaths"
].astype(float)
vitalstatistics["Unintentional Drug Poisonings Deaths"] = vitalstatistics[
    "Unintentional Drug Poisonings Deaths"
].astype(float)


# Group the 'vitalstatistics' DataFrame by state and sum the selected columns
grouped_data = vitalstatistics.groupby("State")[
    [
        "Other Drug Deaths",
        "Suicide Drug Poisonings Deaths",
        "Undetermined Drug Poisonings Deaths",
        "Unintentional Drug Poisonings Deaths",
    ]
].sum()

# Calculate the percentages of each column with respect to the total sum for each state
percentage_data = grouped_data.div(grouped_data.sum(axis=1), axis=0) * 100


print("Percentage Distribution of Drug-Related Deaths by Cause and State:\n")

print(percentage_data.to_string())

print(
    f"\nWhen we sum the number of deaths due to drug-related causes for these {remaining_rows_1_cause:,} combinations of county/year\n"
    f"that do have complete data, we can see that more than 70% are attributed to the 'Unintentional Drug\n"
    f"Poisonings Deaths' cause. Therefore, we consider the decision to stick with these {remaining_rows_1_cause:,} \n"
    f"records instead of {remaining_rows_4_cause:,}, using the cause of death with the highest prevalence.\n"
)
