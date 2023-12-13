import pandas as pd
import numpy as np

###################################################################################################################################################
### Importing state_fips

state_fips = pd.read_csv(
    "https://gist.github.com/soodoku/f9e18efe98f7d74931d8b4a79a49e6f5/raw/891f0ca1c84fb64c3aa8950f7e21c79b117402bb/state_abbrev_fips.txt",
    sep=" ",
    names=["state_fips", "state"],
)

dictionary_fips = pd.read_csv("../00_source_data/usa_fips.csv")


dictionary_fips["county_name"] = dictionary_fips["county_name"].str.upper()

dictionary_fips = dictionary_fips.merge(
    state_fips, how="left", on="state_fips", indicator=True
)

dictionary_fips = dictionary_fips[["state", "state_fips", "county_name", "county_fips"]]

###################################################################################################################################################
### Importing Counterfactuals

# I am adding the full file path in comments for clarity.
## https://raw.githubusercontent.com/MIDS-at-Duke/IDS720_PracticalDataScience_JBR/main/20_intermediate_files/counterfactuals_by_euclidean_distance.csv?token=GHSAT0AAAAAACG6UAG4AGFFWD4C6QE3HWXIZLBABZA

counterfactuals_path = (
    "../20_intermediate_files/counterfactuals_by_euclidean_distance.csv"
)
counterfactuals = pd.read_csv(counterfactuals_path)
counterfactuals = counterfactuals[["State"]]
counterfactuals_states = list(counterfactuals["State"])
filtered_states = counterfactuals_states + ["TX", "FL", "WA"]

###################################################################################################################################################
### Importing Death data. We have info between 2003 and 2015

vitalstatistics = "../20_intermediate_files/Transformed_US_VitalStatistics.csv"
vitalstatistics = pd.read_csv(vitalstatistics)

# Dropping source column
vitalstatistics = vitalstatistics.drop(columns=["source"])

# We convert to int, as it was in float
vitalstatistics["County Code"] = vitalstatistics["County Code"].astype(int)

# We now convert to string to be able to separate Statefips from Countyfips
vitalstatistics["County Code"] = vitalstatistics["County Code"].astype(str)
vitalstatistics["state_fips"] = vitalstatistics["County Code"].str[:2]
vitalstatistics["county_fips"] = vitalstatistics["County Code"].str[2:]

# We convert the strings to integers so that they are in the same format as population for the subsequent merge.
vitalstatistics["Year"] = vitalstatistics["Year"].astype(int)
vitalstatistics["state_fips"] = vitalstatistics["state_fips"].astype(int)
vitalstatistics["county_fips"] = vitalstatistics["county_fips"].astype(int)
vitalstatistics = vitalstatistics.drop(columns=["County Code"])

vitalstatistics = vitalstatistics.rename(columns={"Year": "year", "State": "state"})

# Now we filter by states
vitalstatistics = vitalstatistics[vitalstatistics["state"].isin(filtered_states)]


### Importing population data.  We have info between 2000 and 2019

population = pd.read_csv(
    "../00_source_data/Additional_datasets/population_by_age_and_sex.csv"
)
population = population[
    ["Statefips", "Countyfips", "Description", "Year", "Total Population"]
]


# adding state
population = population.merge(
    state_fips, how="left", left_on="Statefips", right_on="state_fips", indicator=True
)

# "We remove Countyfips = 0 since it represents the total population of the state."
population = population[population["Countyfips"] != 0]

population = population.drop(["Statefips", "_merge", "Description"], axis=1)
population["state_fips"] = population["state_fips"].astype(int)


population = population.rename(
    columns={
        "Total Population": "total_population",
        "Countyfips": "county_fips",
        "Year": "year",
    }
)


# Now we filter by states
population = population[population["state"].isin(filtered_states)]


###################################################################################################################################################
# Now we validate that we have one row per county per year for each dataset before merge

assert not vitalstatistics.duplicated(["state_fips", "county_fips", "year"]).any()
assert not population.duplicated(["state_fips", "county_fips", "year"]).any()


population_vitalstatistics = population.merge(
    vitalstatistics,
    on=["state_fips", "county_fips", "year"],
    how="inner",
    indicator=True,
)


population_vitalstatistics = population_vitalstatistics[
    (population_vitalstatistics["year"] >= 2003)
    & (population_vitalstatistics["year"] <= 2015)
]

"""
CHECK LATER
population_vitalstatistics.value_counts("_merge")

_merge
both          10224
left_only       157
right_only        6
dtype: int64
"""

#####################################################


assert not population_vitalstatistics.duplicated(
    ["state_fips", "county_fips", "year"]
).any()

# Droping some columns
population_vitalstatistics = population_vitalstatistics.drop(
    [
        "state_x",
        "County",
        "state_y",
        "_merge",
        "Deaths caused by All other drug-induced causes",
        "Deaths caused by Drug poisonings (overdose) Suicide (X60-X64)",
        "Deaths caused by Drug poisonings (overdose) Undetermined (Y10-Y14)",
        "Deaths caused by Drug poisonings (overdose) Homicide (X85)",
    ],
    axis=1,
)

population_vitalstatistics = population_vitalstatistics.rename(
    columns={
        "Deaths caused by Drug poisonings (overdose) Unintentional (X40-X44)": "death_drug_poisonings_unintentional"
    }
)

population_vitalstatistics[
    "death_drug_poisonings_unintentional"
] = population_vitalstatistics["death_drug_poisonings_unintentional"].astype(float)

# "We calculate the mortality rate per 100,000 people."
population_vitalstatistics["mortality_rate"] = (
    population_vitalstatistics["death_drug_poisonings_unintentional"]
    / population_vitalstatistics["total_population"]
    * 100000
)

# Calculate the mean mortality rate per state and year using groupby and transform
population_vitalstatistics[
    "mean_mortality_rate_per_state_year"
] = population_vitalstatistics.groupby(["year", "state_fips"])[
    "mortality_rate"
].transform(
    np.mean
)

# Create a new variable filled_mortality_rate that defaults to mortality_rate but fills NaN values with the mean_mortality_rate_per_state_year
population_vitalstatistics["filled_mortality_rate"] = population_vitalstatistics[
    "mortality_rate"
].fillna(population_vitalstatistics["mean_mortality_rate_per_state_year"])

output_path = "../20_intermediate_files/final_population_vitalstatistics.csv"
population_vitalstatistics.to_csv(output_path, index=False)
