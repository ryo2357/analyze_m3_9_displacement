use m3_9_displacement::v2::FileName;
use m3_9_displacement::v2::OFFSET_HOURS;
use m3_9_displacement::set_env;

use polars::prelude::*;
use anyhow::Result;
use chrono::{DateTime, Local,  TimeZone,Duration};

/*
データ処理には強いが集計後の処理の記述はやりずらい
集計後のデータを構造体にシリアライズしてプログラム内でハンドリングしたい
*/
fn main() -> Result<()>{
    set_env();
    let lf = FileName::Row(0).read_file()?;

    
    let result_df = lf

        .with_column(
            (col("datetime")-col("datetime").shift(lit(1))).alias("stop_time")
        )
        .filter(col("interval").gt_eq(lit(800000)))
        .with_column(
            (col("datetime").shift(lit(-1))-col("datetime")).alias("working_time")
        )
        .collect()?;


    println!("{}", result_df);

    let offset= Duration::hours(OFFSET_HOURS);

    let rows:Vec<WorkingTime> = result_df
        .column("interval")?.u64()?.into_iter()
        .zip(result_df.column("row_number")?.u32()?.into_iter())
        .zip(result_df.column("datetime")?.datetime()?.into_iter())
        .zip(result_df.column("stop_time")?.duration()?.into_iter())
        .zip(result_df.column("working_time")?.duration()?.into_iter())
        // .zip(result_df.column("row_number")?.u32()?.into_iter())
        .map(|((((interval, row_number),
            datetime),
            stop_time),
            working_time)
            | {
            let datetime=  Local.timestamp_millis_opt(datetime.unwrap()).unwrap()- offset;
            let stop_time = Duration::milliseconds(stop_time.unwrap());
            let working_time = working_time.map(Duration::milliseconds);
            // let working_time = match working_time {
            //     Some(t) => Some(Duration::milliseconds(t)),
            //     None=> None
            // };

            WorkingTime {
                row_number: row_number.unwrap(),
                interval: interval.unwrap(),
                datetime,
                stop_time,
                working_time
            }

        })
        .collect();
    
    // println!("rows:{:?}",rows);
    for row in rows {
        println!("row:{:?}",row);
        
    }


    Ok(())
}
#[derive(Debug)]
struct WorkingTime {
    row_number: u32,
    interval: u64,
    datetime: DateTime<Local>,
    stop_time:Duration,
    working_time:Option<Duration>
}

/*
shape: (7, 6)
┌────────────┬─────────────────────┬────────────┬───────────┬──────────────┬──────────────┐
│ row_number ┆ datetime            ┆ interval   ┆ deviation ┆ stop_time    ┆ working_time │
│ ---        ┆ ---                 ┆ ---        ┆ ---       ┆ ---          ┆ ---          │
│ u32        ┆ datetime[ms]        ┆ u64        ┆ i32       ┆ duration[ms] ┆ duration[ms] │
╞════════════╪═════════════════════╪════════════╪═══════════╪══════════════╪══════════════╡
│ 398        ┆ 2024-08-26 09:06:21 ┆ 54999986   ┆ -2020     ┆ 55s          ┆ 14m 23s      │
│ 1057       ┆ 2024-08-26 09:20:44 ┆ 467100020  ┆ -529      ┆ 7m 47s       ┆ 10m 31s      │
│ 1283       ┆ 2024-08-26 09:31:15 ┆ 495800007  ┆ -530      ┆ 8m 16s       ┆ 34m 50s      │
│ 2771       ┆ 2024-08-26 10:06:05 ┆ 1196600024 ┆ -256      ┆ 19m 56s      ┆ 3m 39s       │
│ 3017       ┆ 2024-08-26 10:09:44 ┆ 71500007   ┆ -721      ┆ 1m 11s       ┆ 5h 2m 20s    │
│ 21134      ┆ 2024-08-26 15:12:04 ┆ 2955732668 ┆ -446      ┆ 2h 51s       ┆ 33m 51s      │
│ 22053      ┆ 2024-08-26 15:45:55 ┆ 1479600035 ┆ -401      ┆ 24m 39s      ┆ null         │
└────────────┴─────────────────────┴────────────┴───────────┴──────────────┴──────────────┘
row:WorkingTime { row_number: 398, interval: 54999986, datetime: 2024-08-26T09:06:21+09:00, stop_time: TimeDelta { secs: 55, nanos: 0 }, working_time: Some(TimeDelta { secs: 863, nanos: 0 }) }    
row:WorkingTime { row_number: 1057, interval: 467100020, datetime: 2024-08-26T09:20:44+09:00, stop_time: TimeDelta { secs: 467, nanos: 0 }, working_time: Some(TimeDelta { secs: 631, nanos: 0 }) } 
row:WorkingTime { row_number: 1283, interval: 495800007, datetime: 2024-08-26T09:31:15+09:00, stop_time: TimeDelta { secs: 496, nanos: 0 }, working_time: Some(TimeDelta { secs: 2090, nanos: 0 }) }row:WorkingTime { row_number: 2771, interval: 1196600024, datetime: 2024-08-26T10:06:05+09:00, stop_time: TimeDelta { secs: 1196, nanos: 0 }, working_time: Some(TimeDelta { secs: 219, nanos: 0 }) 
}
row:WorkingTime { row_number: 3017, interval: 71500007, datetime: 2024-08-26T10:09:44+09:00, stop_time: TimeDelta { secs: 71, nanos: 0 }, working_time: Some(TimeDelta { secs: 18140, nanos: 0 }) } 
row:WorkingTime { row_number: 21134, interval: 2955732668, datetime: 2024-08-26T15:12:04+09:00, stop_time: TimeDelta { secs: 7251, nanos: 0 }, working_time: Some(TimeDelta { secs: 2031, nanos: 0 }) }
row:WorkingTime { row_number: 22053, interval: 1479600035, datetime: 2024-08-26T15:45:55+09:00, stop_time: TimeDelta { secs: 1479, nanos: 0 }, working_time: None }

*/

#[allow(dead_code)]
fn another_code() -> Result<()>{
    let lf = FileName::Row(0).read_file()?;

    let result_df = lf

        .with_column(
            (col("datetime")-col("datetime").shift(lit(1))).alias("stop_time")
        )
        .filter(col("interval").gt_eq(lit(800000)))
        .with_column(
            (col("datetime").shift(lit(-1))-col("datetime")).alias("working_time")
        )
        .collect()?;

    let offset= Duration::hours(OFFSET_HOURS);
    // 各列のデータを個別に収集
    let intervals: Vec<Option<u64>> = result_df.column("interval")?.u64()?.into_iter().collect();
    let row_numbers: Vec<Option<u32>> = result_df.column("row_number")?.u32()?.into_iter().collect();
    let datetimes: Vec<Option<i64>> = result_df.column("datetime")?.datetime()?.into_iter().collect();
    let stop_times: Vec<Option<i64>> = result_df.column("stop_time")?.duration()?.into_iter().collect();
    let working_times: Vec<Option<i64>> = result_df.column("working_time")?.duration()?.into_iter().collect();

    // 収集したデータをインデックスで参照し、構造体にマッピング
    let rows: Vec<WorkingTime> = intervals.into_iter().enumerate().map(|(i, interval)| {
        let row_number = row_numbers[i].unwrap();
        let datetime = Local.timestamp_millis_opt(datetimes[i].unwrap()).unwrap() - offset;
        let stop_time = Duration::milliseconds(stop_times[i].unwrap());
        let working_time = working_times[i].map(Duration::milliseconds);

        WorkingTime {
            row_number,
            interval: interval.unwrap(),
            datetime,
            stop_time,
            working_time,
        }
    }).collect();
    println!("rows:{:?}",rows);
    Ok(())
}