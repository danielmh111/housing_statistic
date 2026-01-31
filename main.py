import polars as pl
import polars.selectors as cs
from project_paths import paths
from rich.pretty import pprint

OVERCROWDING_FILE = paths.overcrowding
CONNECTIVITY_FILE = paths.connectivity
GP_RATIO_FILE = ...
HOUSE_AFFORDABILITY_FILE = ...
HOMELESSNESS_FILE = ...
BROADBAND_FILE = paths.broadband
LSOA_LOOKUP = paths.lsoa_lookup
POSTCODE_LOOKUP = paths.postcode_lookup
BENCHMARK = paths.benchmark


def main():
    lsoa_lookup_df = pl.scan_csv(LSOA_LOOKUP)
    postcode_lookup_df = pl.scan_csv(POSTCODE_LOOKUP)

    connectivity_df = pl.scan_csv(CONNECTIVITY_FILE)
    overcrowding_df = pl.scan_csv(OVERCROWDING_FILE)
    broadband_df = pl.scan_csv(BROADBAND_FILE, null_values="#N/A")

    connectivity_df = lsoa_lookup_df.join(
        other=connectivity_df,
        how="inner",
        left_on="lsoa_code",
        right_on="LSOA21CD",
    ).select(
        pl.col("lsoa_code"),
        pl.exclude(
            [
                "lsoa_code",
                "lsoa_name",
                "msoa_code",
                "ward_code",
                "local_authority_code",
            ]
        )
        .rank(descending=False)
        .name.suffix("_rank"),
    )

    overcrowding_df = (
        lsoa_lookup_df.join(
            other=overcrowding_df,
            how="inner",
            left_on="lsoa_code",
            right_on="Lower layer Super Output Areas Code",
        )
        .select(
            pl.exclude(
                [
                    "Lower layer Super Output Areas",
                    "lsoa_name",
                    "msoa_code",
                    "ward_code",
                    "local_authority_code",
                ]
            ),
            pl.col("Observation").sum().over("lsoa_code").alias("total"),
        )
        .select(
            pl.all(),
            (pl.col("Observation") / pl.col("total")).alias("rate"),
        )
        .pivot(
            on="Occupancy rating for bedrooms (6 categories)",
            index="lsoa_code",
            values="rate",
            on_columns=(
                [
                    "Occupancy rating of bedrooms: -1",
                    "Occupancy rating of bedrooms: -2 or less",
                ]
            ),
        )
        .select(
            pl.col("lsoa_code"),
            cs.starts_with("O")
            .name.replace(
                "Occupancy rating of bedrooms: ",
                "",
                literal=True,
            )
            .name.replace(" ", "_", literal=True)
            .name.suffix("_rate"),
        )
        .select(
            pl.col("lsoa_code"),
            (cs.starts_with("-1") + cs.starts_with("-2")).alias("overcrowding_rate"),
        )
        .select(
            cs.all(),
            pl.col("overcrowding_rate").rank(descending=True).name.suffix("_rank"),
        )
    )

    broadband_df = (
        postcode_lookup_df.join(
            other=broadband_df,
            how="inner",
            left_on="oa11cd",
            right_on="oa11cd",
        )
        .join(
            other=lsoa_lookup_df,
            how="inner",
            left_on="lsoa11nm",
            right_on="lsoa_name",
        )
        .group_by("lsoa_code")
        .agg(cs.starts_with("bba").mean().name.suffix("_mean"))
        .select(
            cs.all(),
            cs.starts_with("bba").rank(descending=False).name.suffix("_rank"),
        )
    )

    combined_df = (
        connectivity_df.join(other=overcrowding_df, how="inner", on="lsoa_code", validate="1:1")
        .join(other=broadband_df, how="inner", on="lsoa_code", validate="1:1")
        .select(
            pl.col("lsoa_code"),
            pl.col("Overall_rank").alias("overall_connectivity_rank"),
            pl.col("overcrowding_rate_rank"),
            pl.col("bba225_dow_mean_rank").alias("mean_download_speed_rank"),  # just picking one
        )
        .select(
            pl.col("lsoa_code"),
            (
                (
                    pl.col("overall_connectivity_rank")
                    + pl.col("overcrowding_rate_rank")
                    + pl.col("mean_download_speed_rank")
                )
                / 3.0
            )
            .rank(descending=False)
            .alias("combined_rank"),
        )
    )

    df_comparison = (
        pl.scan_csv(BENCHMARK)
        .select(
            lsoa_code="LSOA code (2021)",
            score="Barriers to Housing and Services Score",
            rank="Barriers to Housing and Services Rank (where 1 is most deprived)",
            decile_rank="Barriers to Housing and Services Decile (where 1 is most deprived 10% of LSOAs)",
        )
        .join(
            other=combined_df,
            on="lsoa_code",
            how="inner",
            validate="1:1",
        )
        .select(pl.corr(a="rank", b="combined_rank", method="spearman"))
    )

    pprint(f"spearman correlation: {df_comparison.collect().row(0)[0]}")


if __name__ == "__main__":
    main()
