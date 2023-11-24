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


###################################################################################################################################################
### Importing Opiods data. We have info between 2006 and 2019

opioids_path = "../20_intermediate_files/arcos_all_washpost_collapsed.parquet"
opioids = pd.read_parquet(opioids_path)
opioids["Year"] = [int(i[:4]) for i in opioids["year_month"]]

opioids = opioids[opioids["BUYER_COUNTY"].notna()]

# Dropping drugname and counties column
opioids = opioids.drop(columns=["DRUG_NAME", "year_month"])

# Now we filter by states
opioids = opioids[opioids["BUYER_STATE"].isin(filtered_states)]

# Grouping by year and state
opioids = (
    opioids.groupby(["BUYER_STATE", "BUYER_COUNTY", "Year"])["total_morphine_mg"]
    .sum()
    .reset_index()
)


opioids = opioids.merge(
    dictionary_fips,
    left_on=["BUYER_STATE", "BUYER_COUNTY"],
    right_on=["state", "county_name"],
    how="left",
    indicator=True,
)

assert (opioids["_merge"] == "both").all()

opioids = opioids.drop(columns=["BUYER_STATE", "BUYER_COUNTY", "_merge"])
opioids = opioids.rename(columns={"Year": "year"})


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
assert not opioids.duplicated(["state_fips", "county_fips", "year"]).any()
assert not population.duplicated(["state_fips", "county_fips", "year"]).any()


# BEDFORD CITY has no population data for 2010, 2011, 2012, 2013, 2014, 2015.
# CLIFTON FORGE CITY has no population data for 2015.
# Validate with population_vitalstatistics[population_vitalstatistics["_merge"]=="right_only"]
# I checked, and in the original population data, they are not included. Since there are only two counties, I would remove them.
# Countyfips = 560 , State = VA
# Countyfips = 515, State = VA


# We filter by the years that we will consider in the analysis.
population = population[(population["year"] >= 2006) & (population["year"] <= 2015)]
vitalstatistics = vitalstatistics[
    (vitalstatistics["year"] >= 2006) & (vitalstatistics["year"] <= 2015)
]
opioids = opioids[(opioids["year"] >= 2006) & (opioids["year"] <= 2015)]


population_vitalstatistics = population.merge(
    vitalstatistics,
    on=["state_fips", "county_fips", "year"],
    how="outer",
    indicator=True,
)


population_opioids = population.merge(
    opioids, on=["state_fips", "county_fips", "year"], how="outer", indicator=True
)


# Join check. Pending investigation of the causes. More info below.
population_vitalstatistics.value_counts("_merge")
population_opioids.value_counts("_merge")


"""
"For now, we will continue progressing with an inner join, ignoring cases where the merge is not happening, 
so that another team member can proceed with building the graphics for the analysis.

The points where we lost data and need to further investigate the causes are:
- In the population database, 6 records are missing when joining with the vital statistics database.
- In the vital statistics database, 117 records are missing when joining with the population.
- In the opioids database, 514 records are missing when joining with the population.
- In the final join, we are left with 7470 records out of a total of 7984 that we had in the population. It is pending to investigate the causes."
"""


## final inner
population_vitalstatistics = population_vitalstatistics.drop(columns=["_merge"])
merged_table = population_vitalstatistics.merge(
    opioids, on=["state_fips", "county_fips", "year"], how="inner", indicator=True
)

# This check validates duplicates, but we still need to evaluate the counties
# that were lost in the join of population_vitalstatistics and merge_table.
assert not merged_table.duplicated(["state_fips", "county_fips", "year"]).any()

# Droping some columns
merged_table = merged_table.drop(
    [
        "state_x",
        "County",
        "state_y",
        "_merge",
        "Deaths caused by All other drug-induced causes",
        "Deaths caused by Drug poisonings (overdose) Suicide (X60-X64)",
        "Deaths caused by Drug poisonings (overdose) Undetermined (Y10-Y14)",
    ],
    axis=1,
)

merged_table = merged_table.rename(
    columns={
        "Deaths caused by Drug poisonings (overdose) Unintentional (X40-X44)": "death_drug_poisonings_unintentional"
    }
)

merged_table["death_drug_poisonings_unintentional"] = merged_table[
    "death_drug_poisonings_unintentional"
].astype(float)

# "We calculate the mortality rate per 100,000 people."
merged_table["mortality_rate"] = (
    merged_table["death_drug_poisonings_unintentional"]
    / merged_table["total_population"]
    * 100000
)

# Calculate the mean mortality rate per state and year using groupby and transform
merged_table["mean_mortality_rate_per_state_year"] = merged_table.groupby(
    ["year", "state_fips"]
)["mortality_rate"].transform(np.mean)

# Create a new variable filled_mortality_rate that defaults to mortality_rate but fills NaN values with the mean_mortality_rate_per_state_year
merged_table["filled_mortality_rate"] = merged_table["mortality_rate"].fillna(
    merged_table["mean_mortality_rate_per_state_year"]
)

output_path = "../20_intermediate_files/final_merged_table.csv"
merged_table.to_csv(output_path, index=False)
