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
dictionary_fips = dictionary_fips[["state", "state_fips", "county_fips", "county_name"]]

assert not dictionary_fips.duplicated(["state", "state_fips", "county_fips"]).any()


###################################################################################################################################################
### Importing Counterfactuals

# I am adding the full file path in comments for clarity.
## https://raw.githubusercontent.com/MIDS-at-Duke/IDS720_PracticalDataScience_JBR/main/20_intermediate_files/counterfactuals_by_euclidean_distance.csv?token=GHSAT0AAAAAACG6UAG4AGFFWD4C6QE3HWXIZLBABZA

counterfactuals_path = (
    "../20_intermediate_files/counterfactuals_by_euclidean_distance.csv"
)
counterfactuals = pd.read_csv(counterfactuals_path)
counterfactuals_states = counterfactuals[["State"]]
counterfactuals_states = list(counterfactuals_states["State"])
filtered_states = counterfactuals_states + ["TX", "FL", "WA"]

filtered_states_texas = list(
    counterfactuals[counterfactuals["pairing_state"] == "TX"]["State"]
) + ["TX"]


###################################################################################################################################################
### Importing Vitalstatistics data. We have info between 2003 and 2015

vitalstatistics = "../20_intermediate_files/Transformed_US_VitalStatistics.csv"
vitalstatistics = pd.read_csv(vitalstatistics)

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

vitalstatistics = vitalstatistics.rename(
    columns={"Year": "year", "State": "state_name", "County": "county"}
)


# Dropping columns
columns_to_drop = [
    "Deaths caused by Drug poisonings (overdose) Undetermined (Y10-Y14)",
    "Deaths caused by Drug poisonings (overdose) Suicide (X60-X64)",
    "Deaths caused by Drug poisonings (overdose) Homicide (X85)",
    "Deaths caused by All other drug-induced causes",
    "County Code",
    "source",
    "county",
]
vitalstatistics = vitalstatistics.drop(columns=columns_to_drop)

vitalstatistics = vitalstatistics.rename(
    columns={
        "Deaths caused by Drug poisonings (overdose) Unintentional (X40-X44)": "death_drug_poisonings_unintentional"
    }
)


# Dropping Bedford city	2015 and Clifton Forge city	 2015 (2 rows, missing)
vitalstatistics = vitalstatistics[
    vitalstatistics["death_drug_poisonings_unintentional"] != "Missing"
]

# Now we filter by states
vitalstatistics = vitalstatistics[vitalstatistics["state_name"].isin(filtered_states)]

###################################################################################################################################################
### Importing Opiods data. We have info between 2006 and 2019

opioids_path = "../20_intermediate_files/arcos_all_washpost_collapsed.parquet"
opioids = pd.read_parquet(opioids_path)
opioids["year"] = [int(i[:4]) for i in opioids["year_month"]]

opioids = opioids[opioids["BUYER_COUNTY"].notna()]

opioids = opioids.merge(
    dictionary_fips,
    left_on=["BUYER_STATE", "BUYER_COUNTY"],
    right_on=["state", "county_name"],
    how="left",
    indicator=True,
)

# Now we filter by states
opioids = opioids[opioids["BUYER_STATE"].isin(filtered_states)]

assert (opioids["_merge"] == "both").all()

opioids = opioids.rename(columns={"Year": "year", "state": "state_name"})

opioids = (
    opioids.groupby(
        ["year", "year_month", "state_name", "state_fips", "county_fips", "county_name"]
    )
    .agg(mme=("total_morphine_mg", "sum"))
    .reset_index()
)

assert not opioids.duplicated(
    ["year_month", "state_fips", "county_fips", "year"], keep=False
).any(), "Se encontraron duplicados en 'opioids'"


###################################################################################################################################################
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

# "We remove Countyfips = 0 (US) and Countyfips = 72 (Puerto Rico)
population = population[
    (population["Countyfips"] != 0) & (population["Countyfips"] != 72)
]

population = population.drop(["Statefips", "_merge", "Description"], axis=1)
population["state_fips"] = population["state_fips"].astype(int)


population = population.rename(
    columns={
        "Total Population": "total_population",
        "Countyfips": "county_fips",
        "Year": "year",
        "state": "state_name",
    }
)

# Now we filter by states
population = population[population["state_name"].isin(filtered_states)]


###################################################################################################################################################
# Now we validate that we have one row per county per year for each dataset before merge

assert not vitalstatistics.duplicated(["state_fips", "county_fips", "year"]).any()
assert not opioids.duplicated(["year_month", "state_fips", "county_fips", "year"]).any()
assert not population.duplicated(["state_fips", "county_fips", "year"]).any()


###################################################################################################################################################
# Texas monthly opioids dataset
# For ease of calculation, we will use the assumption that every month has the same population (since we have population data at an annual level).
# A more detailed approach could be to perform interpolation, but we will not do that in this case

population_opioids_texas = population.merge(
    opioids,
    on=["state_fips", "county_fips", "year", "state_name"],
    how="outer",
    indicator="merge_population_opioids",
)

population_opioids_texas = population_opioids_texas[
    population_opioids_texas["state_name"].isin(filtered_states_texas)
]
population_opioids_texas = population_opioids_texas[
    (population_opioids_texas["year"] >= 2006)
]

# We add morphine_mg_per_capita
population_opioids_texas["mme_per_capita"] = (
    population_opioids_texas["mme"] / population_opioids_texas["total_population"]
)


# If we validate the join, we obtain 550 records that do not match. However, it has been validated in the database that these are
# approximately 30 counties in Texas, 10 in Virginia , 4 en Idaho, that are not present in the original Opioids database.
# population_opioids_texas["merge_population_opioids"].value_counts()
# both          75533
# left_only       550
# right_only        0
# Name: _merge, dtype: int64


# Now we save the monthly Texas database with the information on opioids

population_opioids_texas = population_opioids_texas[
    [
        "year",
        "year_month",
        "state_name",
        "state_fips",
        "county_name",
        "county_fips",
        "total_population",
        "mme",
        "mme_per_capita",
        "merge_population_opioids",
    ]
]

texas_path = "../20_intermediate_files/texas_population_opioids_monthly.csv"
population_opioids_texas.to_csv(texas_path, index=False)


###################################################################################################################################################
# US Yearly opioids - vitalstatistics dataset

# Grouping by year the opiods dataset
opioids = (
    opioids.groupby(["year", "state_name", "state_fips", "county_name", "county_fips"])[
        "mme"
    ]
    .sum()
    .reset_index()
)

#
vitalstatistics = vitalstatistics[vitalstatistics["year"] >= 2003]


# MERGE
population_vitalstatistics = pd.merge(
    population,
    vitalstatistics,
    on=["state_fips", "county_fips", "year", "state_name"],
    how="outer",
    indicator="merge_population_vitalstatistics",
)


population_vitalstatistics_opioids = pd.merge(
    population_vitalstatistics,
    opioids,
    on=["state_fips", "county_fips", "year", "state_name"],
    how="outer",
    indicator="merge_population_vitalstatistics_opioids",
)

# Check JOIN
# population_vitalstatistics_opioids["merge_population_vitalstatistics"].value_counts()
# both          10224
# left_only      5746
# right_only        4
# Name: merge_population_vitalstatistics, dtype: int64


# right_only = 4
# BEDFORD CITY has no population data for 2010, 2011, 2012, 2013, 2014, 2015.
# CLIFTON FORGE CITY has no population data for 2015.
# Validate with population_vitalstatistics[population_vitalstatistics["_merge"]=="right_only"]
# I checked, and in the original population data, they are not included. Since there are only two counties, I would remove them.
# Countyfips = 560 , State = VA
# Countyfips = 515, State = VA


# population_vitalstatistics_opioids["merge_population_vitalstatistics_opioids"].value_counts()
# both          10457
# left_only      5517
# right_only        0
# Name: merge_population_vitalstatistics_opioids, dtype: int64


population_vitalstatistics_opioids[
    "death_drug_poisonings_unintentional"
] = population_vitalstatistics_opioids["death_drug_poisonings_unintentional"].astype(
    float
)

# "We calculate the mortality rate per 100,000 people."
population_vitalstatistics_opioids["mortality_rate_unintentional_drug_poisoning"] = (
    population_vitalstatistics_opioids["death_drug_poisonings_unintentional"]
    / population_vitalstatistics_opioids["total_population"]
    * 100000
)


# Calculate the mean mortality rate per state and year using groupby and transform
population_vitalstatistics_opioids[
    "mean_mortality_rate_unintentional_drug_poisoning_per_state_year"
] = population_vitalstatistics_opioids.groupby(["year", "state_fips"])[
    "mortality_rate_unintentional_drug_poisoning"
].transform(
    np.mean
)

# Create a new variable filled_mortality_rate that defaults to mortality_rate but fills NaN values with the mean_mortality_rate_per_state_year
population_vitalstatistics_opioids[
    "filled_mortality_rate_unintentional_drug_poisoning"
] = population_vitalstatistics_opioids[
    "mortality_rate_unintentional_drug_poisoning"
].fillna(
    population_vitalstatistics_opioids[
        "mean_mortality_rate_unintentional_drug_poisoning_per_state_year"
    ]
)

# We add morphine_mg_per_capita
population_vitalstatistics_opioids["mme_per_capita"] = (
    population_vitalstatistics_opioids["mme"]
    / population_vitalstatistics_opioids["total_population"]
)


population_vitalstatistics_opioids = population_vitalstatistics_opioids[
    [
        "year",
        "state_name",
        "state_fips",
        "county_name",
        "county_fips",
        "total_population",
        "death_drug_poisonings_unintentional",
        "mortality_rate_unintentional_drug_poisoning",
        "mean_mortality_rate_unintentional_drug_poisoning_per_state_year",
        "filled_mortality_rate_unintentional_drug_poisoning",
        "mme",
        "mme_per_capita",
        "merge_population_vitalstatistics",
        "merge_population_vitalstatistics_opioids",
    ]
]

output_path = (
    "../20_intermediate_files/us_population_vitalstatistics_opioids_yearly.csv"
)
population_vitalstatistics_opioids.to_csv(output_path, index=False)
