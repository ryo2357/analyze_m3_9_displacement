use m3_9_displacement::v1::FileName;

use polars::prelude::*;

use anyhow::Result;

/*
Polarsで行を検索するコードの検討
*/
fn main() -> Result<()>{
    let original_lf = FileName::Row(1).read_file()?;

    let shift_lf = original_lf
        .with_column(col("time").shift(lit(1)).alias("previous_time"));

    let filtered_lf = shift_lf
        .filter(col("interval").gt_eq(lit(800000)));

    let result_df = filtered_lf.collect()?;

    println!("result_df: {}", result_df);

    Ok(())
}
/*
result_df: shape: (1, 6)
┌────────────┬────────────┬──────────┬───────────┬───────────┬───────────────┐
│ row_number ┆ date       ┆ time     ┆ interval  ┆ deviation ┆ previous_time │
│ ---        ┆ ---        ┆ ---      ┆ ---       ┆ ---       ┆ ---           │
│ u32        ┆ date       ┆ time     ┆ u64       ┆ i32       ┆ time          │
╞════════════╪════════════╪══════════╪═══════════╪═══════════╪═══════════════╡
│ 41631      ┆ 2024-08-26 ┆ 20:15:30 ┆ 112032723 ┆ -367      ┆ 19:02:02      │
└────────────┴────────────┴──────────┴───────────┴───────────┴───────────────┘

*/