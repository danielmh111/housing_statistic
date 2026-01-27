from project_paths import paths
import polars as pl
import polars.selectors as cs

OVERCROWDING_FILE = paths.overcrowding
CONNECTIVITY_FILE = paths.connectivity
GP_RATIO_FILE = ...
HOUSE_AFFORDABILITY_FILE = ...
HOMELESSNESS_FILE = ...
BROADBAND_FILE = paths.broadband
LSOA_LOOKUP = paths.lsoa_lookup
POSTCODE_LOOKUP = paths.postcode_lookup



def main():
    lsoa_lookup_df = pl.read_csv(LSOA_LOOKUP)
    postcode_lookup_df = pl.read_csv(POSTCODE_LOOKUP)

    connectivity_df = pl.read_csv(CONNECTIVITY_FILE)
    overcrowding_df = pl.read_csv(OVERCROWDING_FILE)
    broadband_df = pl.read_csv(BROADBAND_FILE, null_values="#N/A")

    connectivity_df = lsoa_lookup_df.join(
        other=connectivity_df,
        how="inner",
        left_on="lsoa_code",
        right_on="LSOA21CD",
    ).select(
        pl.exclude(["lsoa_name", "msoa_code", "ward_code", "local_authority_code",]),
        pl.exclude(["lsoa_code", "lsoa_name", "msoa_code", "ward_code", "local_authority_code",]).rank(descending=True).name.suffix("_rank"),
    )


    overcrowding_joined_df = lsoa_lookup_df.join(
        other=overcrowding_df,
        how="inner",
        left_on="lsoa_code",
        right_on="Lower layer Super Output Areas Code",
    )

    broadband_df = (
        postcode_lookup_df.join(
            other=broadband_df, how="inner", left_on="oa11cd", right_on="oa11cd"
        )
        .join(
            other=lsoa_lookup_df, how="inner", left_on="lsoa11nm", right_on="lsoa_name"
        )
        .group_by("lsoa_code").agg(cs.starts_with("bba").mean().name.suffix("_mean"))
        .select(
            cs.all(), cs.starts_with("bba").rank().name.suffix("_rank")
        )
    )

    print("connectivity df")
    print(connectivity_df.head())
    print("overcrowding df")
    print(overcrowding_joined_df.head())
    print("broadband df")
    print(broadband_df.head())


if __name__ == "__main__":
    main()


# psudocode below

# def main():


#     connectivity_df = load_connectivity(CONNECTIVITY_FILE)
#     overcrowding_df = load_overcrowding(OVERCROWDING_FILE)

#     connectivity_lsoa_col = ... # geography code
#     connectivity_score_col = ...
#     overcrowding_lsoa_col = ... # geography code

#     # filter the input data to just bristol
# pl.col(    conn_bristol = ).mean().over(pl.col("lsoa_code")).alias()filter_to_bristol(connectivity_df"," 
# connectivity_lsoa_col)
# pl.col(    overcrowd_bristol = ).mean().over(pl.col("lsoa_code")).alias()filter_to_bristol(overcrowding_df"," 
# overcrowding_lsoa_col)

#     # For connectivity
#     # higher score means more connected, so lower deprivation index

#     # turn the ranks to normalized scores


#     # Calculate overcrowding rate (% households with rating -1 or less)

#     # For overcrowding
#     # higher rate means more overcrowded, so higher deprivation index


#     # combine into mini barriers score

#     # merge on LSOA code

#     # merged = ...

#     # combine 50/50 (simplified - real IMD uses sub-domain weights)
#     # merged['barriers_score'] = (merged['connectivity'] + merged['overcrowding']) / 2

#     # rank to lsoas (1 = most deprived)
#     # merged['barriers_rank'] = merged['barriers_score'].rank(ascending=False, method='min').astype(int)

#     # create groups / categories like 'highly deprived', 'least deprived' etc
#     # merged['barriers_decile'] = pd.qcut(merged['barriers_rank'], 10, labels=range(1, 11))
#     # then group by decile and rename the values

#     # output_cols = [
#     #     ...
#     # ]

#     # merged[output_cols].to_csv(OUTPUT_FILE, index=False)
