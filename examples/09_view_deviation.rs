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
const SAVE_PLOT_PATH:&str = "D:/data/processed/240903_m3_9_displacement/09_view_deviation.png";


fn main() -> Result<()>{
    let mut file = std::fs::File::open(LOAD_PARQUET_PATH)?;
    let lf = ParquetReader::new(&mut file).finish()?.lazy();

    let lf = lf
        .with_column((
            (col("deviation") - col("deviation").mean())/ lit(1000.0)
        ).alias("deviation_mm"))
        .select([col("datetime"), col("deviation_mm")]);

    let df = lf.collect()?;

    // max()メソッドがResult<Option<T>, PolarsError>を返すのでややこしい
    let max_deviation:f64= df.column("deviation_mm")?.max()?.unwrap_or(0.0);
    let min_deviation:f64 = df.column("deviation_mm")?.min()?.unwrap_or(0.0);
    let max_abs_deviation = max_deviation.abs().max(min_deviation.abs());

    // _ddof:Delta Degrees of Freedom
    // 0は母集団標準偏差、1は標本標準偏差
    let std_deviation: f64 = df.column("deviation_mm")?.std(1).unwrap_or(0.0);

    println!("{}",df );
    println!("最大変位：{:.3}、3σ：{:.3}", max_abs_deviation,std_deviation * 3.0);


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
        .column("deviation_mm")?
        .f64()?
        .into_iter()
        .map(|v| v.unwrap())
        .collect();

    let root = BitMapBackend::new(SAVE_PLOT_PATH, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption("変位のプロット", ("Meiryo", 24).into_font())
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
shape: (4_251, 2)
┌─────────────────────────┬──────────────┐
│ datetime                ┆ deviation_mm │
│ ---                     ┆ ---          │
│ datetime[ms]            ┆ f64          │
╞═════════════════════════╪══════════════╡
│ 2024-08-27 12:05:24     ┆ 0.148793     │
│ 2024-08-27 12:05:24.600 ┆ 0.241793     │
│ 2024-08-27 12:05:25.200 ┆ 0.193793     │
│ 2024-08-27 12:05:25.800 ┆ 0.241793     │
│ 2024-08-27 12:05:26.400 ┆ 0.169793     │
│ …                       ┆ …            │
│ 2024-08-27 12:47:56.100 ┆ -0.164207    │
│ 2024-08-27 12:47:56.700 ┆ -0.221207    │
│ 2024-08-27 12:47:57.300 ┆ -0.146207    │
│ 2024-08-27 12:47:57.900 ┆ -0.197207    │
│ 2024-08-27 12:47:58.500 ┆ -0.206207    │
└─────────────────────────┴──────────────┘
最大変位：0.353、3σ：0.294
*/