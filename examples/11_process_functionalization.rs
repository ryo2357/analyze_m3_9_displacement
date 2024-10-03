use m3_9_displacement::v2::FileName;
use m3_9_displacement::set_env;
use m3_9_displacement::v2::OFFSET_HOURS;

use plotters::prelude::*;

use polars::prelude::*;
use anyhow::Result;
use chrono::{DateTime, Local,  TimeZone,Duration};


const SAVE_PARQUET_DIR:&str = "D:/data/processed/240903_m3_9_displacement/11_process_functionalization/";

const THRESHOLD_TIME_MINUTE:u32 = 120;


fn main() -> Result<()>{
    set_env();
    let lf = FileName::ConcatenatedRow.read_file()?;
    let rows = get_extract_row_number(&lf)?;

    let mut plot_data:Vec<(DataFrame,DataFrame,String)>= Vec::new();
    let mut y_axis_max:f64 = 0.0;

    for (start,end) in rows{
        // println!("{},{}",start,end);
        let extract_lf = extract_from_row_number(&lf,start,end)?;
        let extract_lf = convert_datetime_from_interval(extract_lf)?;
        let (extract_lf,max_abs_deviation)= add_column_devitation_mm(extract_lf)?;
        let new_y_axis_max = max_abs_deviation.ceil();
        if new_y_axis_max > y_axis_max{
            y_axis_max = new_y_axis_max;
        }

        let deviation_df = make_devitation_df(&extract_lf)?;
        let delta_df = make_delta_df(&extract_lf)?;
        let datetime_string = get_format_first_datetime(extract_lf)?;
        
        plot_data.push((deviation_df,delta_df,datetime_string))
    }
    
    
    for (deviation_df,delta_df,datetime_string) in plot_data{
        let delta_plot_path:String= format!("{}{}_delta.png",SAVE_PARQUET_DIR,datetime_string);
        make_delta_plot_png(&delta_df, &delta_plot_path, y_axis_max)?;

        // println!("create delta");
        let deviation_plot_path:String= format!("{}{}_deviation.png",SAVE_PARQUET_DIR,datetime_string);
        make_deviation_plot_png(&deviation_df, &deviation_plot_path, y_axis_max)?;
        // println!("create deviation");
    }


    Ok(())
}

fn get_format_first_datetime(lf:LazyFrame)->Result<String>{
    let datetime:i64 = lf.select([col("datetime").first().cast(DataType::Int64)])
        .collect()?.column("datetime")?
        .get(0)?.try_extract()?;
    let offset= Duration::hours(OFFSET_HOURS);
    let datetime=  Local.timestamp_millis_opt(datetime).unwrap()- offset;
    let datetime_string= datetime.format("%m%d%H%M").to_string();
    Ok(datetime_string)
}

fn extract_from_row_number(lf:&LazyFrame,start:u32,end:u32)->Result<LazyFrame>{
    let lf = lf.clone()
        .filter(
            col("row_number").gt_eq(lit(start))
            .and(col("row_number").lt(end))
        );
    
    Ok(lf)

}

fn convert_datetime_from_interval(lf:LazyFrame)->Result<LazyFrame>{
    let lf = lf
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
        .select([col("datetime_add_interval"),  col("deviation")])
        .rename(vec!["datetime_add_interval"], vec!["datetime"]);
    Ok(lf)

}

fn add_column_devitation_mm(lf:LazyFrame)->Result<(LazyFrame,f64)>{
    let lf = lf
        .with_column((
            (col("deviation") - col("deviation").mean())/ lit(1000.0)
        ).alias("deviation_mm"));

    let deviation_mm_df = lf.clone().select([col("deviation_mm")]).collect()?;
    let max_deviation:f64= deviation_mm_df.column("deviation_mm")?.max()?.unwrap_or(0.0);
    let min_deviation:f64 = deviation_mm_df.column("deviation_mm")?.min()?.unwrap_or(0.0);
    let max_abs_deviation = max_deviation.abs().max(min_deviation.abs());
 
    Ok((lf,max_abs_deviation))

}


fn make_devitation_df(lf:&LazyFrame)->Result<DataFrame>{
    let lf = lf.clone()
        .with_column(
            when(col("deviation_mm").shift(lit(1)).is_null())
                .then(lit(NULL))
                .otherwise(col("deviation_mm") - col("deviation_mm").shift(lit(1)))
                .alias("delta")
        )
        .drop_nulls(None);

    let df = lf.collect()?;
    Ok(df)
}

fn make_delta_df(lf:&LazyFrame)->Result<DataFrame>{
    let lf = lf.clone()
        .with_column(
            when(col("deviation_mm").shift(lit(1)).is_null())
                .then(lit(NULL))
                .otherwise(col("deviation_mm") - col("deviation_mm").shift(lit(1)))
                .alias("delta")
        )
        .drop_nulls(None);

    let df = lf.collect()?;
    Ok(df)
}

fn get_extract_row_number(lf:&LazyFrame)->Result<Vec<(u32,u32)>>{
    let result_df = lf.clone()
        .with_column(
            (col("datetime")-col("datetime").shift(lit(1))).alias("stop_time")
        )
        .filter(col("interval").gt_eq(lit(800000)))
        .with_column(
            (col("datetime").shift(lit(-1))-col("datetime")).alias("working_time")
        )
        .with_column(
            col("row_number").shift(lit(-1)).alias("row_number_next")
        )
        .filter(col("working_time").gt_eq(lit(THRESHOLD_TIME_MINUTE*60*1000)))
        .drop_nulls(None)
        .collect()?;

    let rows:Vec<(u32,u32)> = result_df
        .column("row_number")?.u32()?.into_iter()
        .zip(result_df.column("row_number_next")?.u32()?.into_iter())
        .map(|(start, end)|{
            let start = start.unwrap();
            let end = end.unwrap();
            (start,end)
        })
        .collect();

    Ok(rows)
}

fn make_delta_plot_png(df:&DataFrame,save_file_path:&str ,y_axis_max:f64)->Result<()>{

    let max_delta:f64= df.column("delta")?.max()?.unwrap_or(0.0);
    let min_delta:f64 = df.column("delta")?.min()?.unwrap_or(0.0);
    let max_abs_delta = max_delta.abs().max(min_delta.abs());
    let std_delta: f64 = df.column("delta")?.std(1).unwrap_or(0.0);
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


    let root = BitMapBackend::new(save_file_path, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;
    let min = -y_axis_max;

    let mut chart = ChartBuilder::on(&root)
        .caption(caption, ("Meiryo", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(
            datetimes[0]..datetimes[datetimes.len() - 1],
            min..y_axis_max,
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

fn make_deviation_plot_png(df:&DataFrame,save_file_path:&str ,y_axis_max:f64)->Result<()>{

    let max_deviation:f64= df.column("deviation_mm")?.max()?.unwrap_or(0.0);
    let min_deviation:f64 = df.column("deviation_mm")?.min()?.unwrap_or(0.0);
    let max_abs_deviation = max_deviation.abs().max(min_deviation.abs());

    let std_deviation: f64 = df.column("deviation_mm")?.std(1).unwrap_or(0.0);
    let caption:String = format!("シール毎のエッジの位置  最大絶対値:{:.3}、3σ：{:.3}", max_abs_deviation,std_deviation * 3.0);


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


    let root = BitMapBackend::new(save_file_path, (640, 480)).into_drawing_area();
    root.fill(&WHITE)?;

    let min = -y_axis_max;
    let mut chart = ChartBuilder::on(&root)
        .caption(caption, ("Meiryo", 24).into_font())
        .margin(10)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_cartesian_2d(
            datetimes[0]..datetimes[datetimes.len() - 1],
            min..y_axis_max,
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

