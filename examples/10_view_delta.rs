use plotters::prelude::*;
use polars::prelude::*;
use anyhow::Result;
use chrono::{DateTime, Local,  TimeZone,Duration};
use m3_9_displacement::v2::OFFSET_HOURS;
/*
変位の状態確認
統計値とグラフ作成
*/

const LOAD_PARQUET_PATH:&str = "D:/data/processed/240903_m3_9_displacement/07_make_processed.parquet";
const SAVE_PLOT_PATH:&str = "D:/data/processed/240903_m3_9_displacement/10_view_delta.png";


fn main() -> Result<()>{
    let mut file = std::fs::File::open(LOAD_PARQUET_PATH)?;
    let lf = ParquetReader::new(&mut file).finish()?.lazy();

    let lf = lf
        .with_column((
            (col("deviation") - col("deviation").mean())/ lit(1000.0)
        ).alias("deviation_mm"))
        .select([col("datetime"), col("deviation_mm")]);

    let lf = lf
        .with_column(
            when(col("deviation_mm").shift(lit(1)).is_null())
                .then(lit(NULL))
                .otherwise(col("deviation_mm") - col("deviation_mm").shift(lit(1)))
                .alias("delta")
        )
        .drop_nulls(None);

    let df = lf.collect()?;

    // max()メソッドがResult<Option<T>, PolarsError>を返すのでややこしい
    let max_delta:f64= df.column("delta")?.max()?.unwrap_or(0.0);
    let min_delta:f64 = df.column("delta")?.min()?.unwrap_or(0.0);
    let max_abs_delta = max_delta.abs().max(min_delta.abs());

    // _ddof:Delta Degrees of Freedom
    // 0は母集団標準偏差、1は標本標準偏差
    let std_delta: f64 = df.column("delta")?.std(1).unwrap_or(0.0);

    println!("{}",df );
    println!("最大変動：{:.3}、3σ：{:.3}", max_abs_delta,std_delta * 3.0);
    let caption:String = format!("前回シールとの差  最大絶対値:{:.3}、3σ：{:.3}", max_abs_delta,std_delta * 3.0);

    let offset= Duration::hours(OFFSET_HOURS);
    let datetimes: Vec<DateTime<Local>> = df
        .column("datetime")?
        .datetime()?
        .into_iter()
        .map(|s| {
            Local.timestamp_millis_opt(s.unwrap()).unwrap() - offset
        }) 
        .collect();

    let deviations: Vec<f64> = df
        .column("delta")?
        .f64()?
        .into_iter()
        .map(|v| v.unwrap())
        .collect();

    let root = BitMapBackend::new(SAVE_PLOT_PATH, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(caption, ("Meiryo", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(
            datetimes[0]..datetimes[datetimes.len() - 1],
            -0.5..0.5,
        )?;

    chart.configure_mesh()
        .x_labels(5)
        .x_label_formatter(&|x|  x.format("%H:%M").to_string())
        .x_desc("時間 (hh:mm)")
        .y_desc("変位 (mm)")
        .x_label_style(("Meiryo", 18).into_font())
        .y_label_style(("Meiryo", 18).into_font())
        .draw()?;
    
    chart.draw_series(LineSeries::new(
        datetimes.iter().zip(deviations.iter()).map(|(x, y)| (*x, *y)),
        &RED,
    ))?;

    Ok(())
}

/*
shape: (4_250, 3)
┌─────────────────────────┬──────────────┬────────┐
│ datetime                ┆ deviation_mm ┆ delta  │
│ ---                     ┆ ---          ┆ ---    │
│ datetime[ms]            ┆ f64          ┆ f64    │
╞═════════════════════════╪══════════════╪════════╡
│ 2024-08-27 12:05:24.600 ┆ 0.241793     ┆ 0.093  │
│ 2024-08-27 12:05:25.200 ┆ 0.193793     ┆ -0.048 │
│ 2024-08-27 12:05:25.800 ┆ 0.241793     ┆ 0.048  │
│ 2024-08-27 12:05:26.400 ┆ 0.169793     ┆ -0.072 │
│ 2024-08-27 12:05:27     ┆ 0.201793     ┆ 0.032  │
│ …                       ┆ …            ┆ …      │
│ 2024-08-27 12:47:56.100 ┆ -0.164207    ┆ 0.074  │
│ 2024-08-27 12:47:56.700 ┆ -0.221207    ┆ -0.057 │
│ 2024-08-27 12:47:57.300 ┆ -0.146207    ┆ 0.075  │
│ 2024-08-27 12:47:57.900 ┆ -0.197207    ┆ -0.051 │
│ 2024-08-27 12:47:58.500 ┆ -0.206207    ┆ -0.009 │
└─────────────────────────┴──────────────┴────────┘
最大変動：0.140、3σ：0.136
*/