use m3_9_displacement::v2::FileName;
use m3_9_displacement::set_env;

use polars::prelude::*;
use anyhow::Result;

/*
中間ファイルの作成
PolarsのIOの確認
112515:08/27 12:05 - 14:28 稼働時間: 143分、直前の停止時間:  21分39秒
116766:08/27 14:28 - 14:31 稼働時間:   2分、直前の停止時間: 100分47秒
上記のデータを抜き出す
*/

// const SAVE_CSV_PATH:&str = "D:/data/processed/240903_m3_9_displacement/07_make_processed.csv";
const SAVE_PARQUET_PATH:&str = "D:/data/processed/240903_m3_9_displacement/07_make_processed.parquet";

const START_ROW:u32 = 112515;
const NEXT_START_ROW:u32 = 116766;


fn main() -> Result<()>{
    set_env();
    let lf = FileName::ConcatenatedRow.read_file()?;

    let lf = lf
        .filter(
            col("row_number").gt_eq(lit(START_ROW))
            .and(col("row_number").lt(NEXT_START_ROW))
        )
        .with_column(
            when(col("interval").shift(lit(1)).is_null())
            .then(lit(0).cast(DataType::UInt64))
            .otherwise(col("interval"))
            .alias("interval")
        )
        // cum_agg feature
        .with_column(
            col("interval").cum_sum(false).alias("interval_sum")
        )
        .select([col("datetime"), col("interval_sum"), col("deviation")]);


    let first_datetime:i64 = lf.clone().select([col("datetime").first().cast(DataType::Int64)]).collect()?.column("datetime")?.get(0)?.try_extract()?;

    let lf = lf
        .with_column(
        (col("interval_sum")/lit(1000) + lit(first_datetime)).alias("datetime_add_interval")
        ).with_column(
            col("datetime_add_interval").cast(DataType::Datetime(TimeUnit::Milliseconds,None))
        )
        // .select([col("datetime_add_interval"),  col("deviation"),col("datetime")])
        // .rename(vec!["datetime"], vec!["datetime_before"])
        // .rename(vec!["datetime_add_interval"], vec!["datetime"])
        .select([col("datetime_add_interval"),  col("deviation")])
        .rename(vec!["datetime_add_interval"], vec!["datetime"]);

    let mut result_df = lf.collect()?;
    println!("{}",result_df);

    // csv featureが必要
    // let mut file = std::fs::File::create(SAVE_CSV_PATH).unwrap();
    // CsvWriter::new(&mut file).finish(&mut result_df).unwrap();

    // parquet featureが必要
    let mut file = std::fs::File::create(SAVE_PARQUET_PATH).unwrap();
    ParquetWriter::new(&mut file).finish(&mut result_df).unwrap();

    Ok(())
}