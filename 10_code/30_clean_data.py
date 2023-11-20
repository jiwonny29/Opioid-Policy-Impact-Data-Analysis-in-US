import pandas as pd

# US_VitalStatistics/Underlying Cause of Death
# This part of the code takes the files from the US_VitalStatistics files
# in the 00_source_data directory, transforms it into a format of 1 line per county per year,
# and leaves the final table in the 20_intermediate_files directory.

# "We are working with one file per year, and we have created an empty list to store the data from each file."
list_US_VitalStatistics = []

# "We iterate over all the files and store them in our list."
for year in range(2003, 2016):
    path = f"../00_source_data/US_VitalStatistics/Underlying Cause of Death, {year}.txt"
    file = pd.read_csv(path, delimiter="\t")

    # "We add a 'source' column to maintain traceability of where the data comes from, in case we encounter any future inconsistencies."
    file["source"] = f"Underlying Cause of Death, {year}.txt"
    list_US_VitalStatistics.append(file)

# "We concatenate the DataFrames stored in our list."
concat_US_VitalStatistics = pd.concat(list_US_VitalStatistics, ignore_index=True)

# In the files, there are some lines that do not belong to the table we need and refer to the description of
# the database. To filter them out, we are using only the rows that contain information about their Counties
#  (previously, we checked if this rule is valid and if it works to eliminate the unwanted rows).

filtered_concat_US_VitalStatistics = concat_US_VitalStatistics.copy()[
    concat_US_VitalStatistics["County"].notna()
]


# "At this point, our dataset should have a shape of (57241, 9), so we are validating this."
assert filtered_concat_US_VitalStatistics.shape == (57241, 9)


# Now we will validate that we only have one record per County, year, and Drug/Alcohol Induced Cause.
assert not filtered_concat_US_VitalStatistics.duplicated(
    ["County Code", "Year", "Drug/Alcohol Induced Cause Code"]
).any()

# We add the word 'death' to the cause of death so that when we pivot, we can identify what the column is about.
filtered_concat_US_VitalStatistics["Drug/Alcohol Induced Cause"] = (
    "Deaths caused by "
    + filtered_concat_US_VitalStatistics["Drug/Alcohol Induced Cause"]
)

# We removed 'Year Code' and 'Notes' as it does not provide additional information.
filtered_concat_US_VitalStatistics = filtered_concat_US_VitalStatistics.drop(
    ["Year Code", "Notes", "Drug/Alcohol Induced Cause Code"], axis=1
)

# We pivot our table to have it at the county, year level.
Transformed_US_VitalStatistics = filtered_concat_US_VitalStatistics.pivot(
    index=["County", "County Code", "Year", "source"],
    columns="Drug/Alcohol Induced Cause",
    values="Deaths",
).reset_index()

# Now we validate that our transformed dataset has a shape of (40337, 12).
assert Transformed_US_VitalStatistics.shape == (40337, 12)


# Finally, we save the result in our directory '20_intermediate_files'.
output_path = "../20_intermediate_files/Transformed_US_VitalStatistics.csv"
Transformed_US_VitalStatistics.to_csv(output_path, index=False)
