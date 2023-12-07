import pandas as pd
import numpy as np

# Importing opiods data
opioids = pd.read_parquet(
    "../20_intermediate_files/arcos_all_washpost_collapsed.parquet"
)

# We need to group it by year and state

# First we need to create a year column
opioids["year"] = [int(i[:4]) for i in opioids["year_month"]]

# Dropping drugname and counties column
opioids = opioids.drop(columns=["DRUG_NAME", "BUYER_COUNTY", "year_month"])

# Grouping by year and state
opioids = opioids.groupby(["BUYER_STATE", "year"]).sum().reset_index()

# I found that in the opioids we have 57 unique states, which is weird because there are only 50 states in the US

# I'm going to check which are the unique states
states = pd.read_csv(
    "https://github.com/jasonong/List-of-US-States/raw/master/states.csv"
)

states_opioids = opioids[["BUYER_STATE"]].copy()
# Keep uniques
states_opioids.drop_duplicates(inplace=True)
# reset index
states_opioids.reset_index(drop=True, inplace=True)
# merge opioids and fips
states_opioids = states_opioids.merge(
    states, how="left", left_on="BUYER_STATE", right_on="Abbreviation", indicator=True
)

# If we look at merge "onlyleft" we can find: ARMED FORCES, Gunea, Northern Mariana Islands, Puerto Rico, Palau and Virgin Islands
# we can drop them and drop Alaska and Hawaii

uncommon_states = list(
    states_opioids[states_opioids["_merge"] == "left_only"]["BUYER_STATE"]
)

# Hawaii and Alaska
uncommon_states.extend(["AK", "HI"])

# Dropping them
opioids.drop(opioids[opioids["BUYER_STATE"].isin(uncommon_states)].index, inplace=True)

# importing population data
population = pd.read_csv(
    "../00_source_data/Additional_datasets/population_by_age_and_sex.csv"
)
# renaming columns of interest
population = population.rename(
    columns={
        "Total Population": "tot_pop",
        "Statefips": "state_fips",
        "Year": "year",
        "Population 45-64": "p_45_65",
        "Population 65+": "p_greater65",
    }
)

population["p_greater45"] = population["p_45_65"] + population["p_greater65"]

# Keep just the variables of interest
population = population["state_fips year tot_pop p_greater45".split()]
# Grouping by year and state
population = population.groupby(["state_fips", "year"], as_index=False).sum()
# Importing fips for states
state_fips = pd.read_csv(
    "https://gist.github.com/soodoku/f9e18efe98f7d74931d8b4a79a49e6f5/raw/891f0ca1c84fb64c3aa8950f7e21c79b117402bb/state_abbrev_fips.txt",
    sep=" ",
    names=["fips", "Abr"],
)
# Merging population and fips
population = population.merge(
    state_fips, how="left", left_on="state_fips", right_on="fips", indicator=True
)
# Only 2 fips weren't merged 0 and 72, values with no interest for us thus we can drop them
population = population[population["_merge"] == "both"]
# Keeping just one fips column
population = population.drop(columns=["_merge", "state_fips"])
# Merging population and opioids
opioids = opioids.merge(
    population,
    how="left",
    left_on=["BUYER_STATE", "year"],
    right_on=["Abr", "year"],
    indicator=True,
)
# All data was matched
# Dropping columns
opioids = opioids.drop(columns=["Abr", "_merge"])
# Importing unemployment
unemployment = pd.read_excel(
    "../00_source_data/Additional_datasets/unemployment00-22.xlsx"
)
# Taking only unmeployment
unemp_columns = [name for name in unemployment.columns if "Unemployment_rate" in name]
vars = list(["State"])
vars.extend(unemp_columns)
# Filtering data
unemployment = unemployment[vars]
# Melting
unemployment = pd.melt(unemployment, id_vars=["State"], value_vars=unemp_columns)
# Collapsing
unemployment = unemployment.groupby(["State", "variable"], as_index=False).mean()
# Getting year
unemployment["year"] = [int(string[-4:]) for string in unemployment["variable"]]
# drop unnecessary variables
unemployment.drop(columns=["variable"], inplace=True)
# renaming
unemployment = unemployment.rename(columns={"value": "unemp_avg_rate"})
print(f"unemployment shape: {unemployment.shape}")
# merging with opioids
opioids = opioids.merge(
    unemployment,
    left_on=["BUYER_STATE", "year"],
    right_on=["State", "year"],
    indicator=True,
)
print(f"opioids shape: {opioids.shape}")
# All values were merged, thus we can drop _merge
opioids.drop(columns=["_merge", "BUYER_STATE"], inplace=True)
# Exporting
opioids.to_csv("../20_intermediate_files/opioids_unemp_educ.csv", index=False)
