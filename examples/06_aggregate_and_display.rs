use m3_9_displacement::v2::FileName;
use m3_9_displacement::set_env;
use m3_9_displacement::v2::OFFSET_HOURS;

use polars::prelude::*;
use anyhow::Result;
use chrono::{DateTime, Local,  TimeZone,Duration};

/*
データ処理⇒表示の流れを検証
*/
fn main() -> Result<()>{
    set_env();
    let lf = FileName::ConcatenatedRow.read_file()?;

    
    let result_df = lf

        .with_column(
            (col("datetime")-col("datetime").shift(lit(1))).alias("stop_time")
        )
        .filter(col("interval").gt_eq(lit(800000)))
        .with_column(
            (col("datetime").shift(lit(-1))-col("datetime")).alias("working_time")
        )
        // nullが含まれる行を削除する
        // 引数のsubsetはよくわからん
        .drop_nulls(None)
        .collect()?;
    // println!("{}", result_df);

    let offset= Duration::hours(OFFSET_HOURS);

    let row_number_iter = result_df.column("row_number")?.u32()?.into_iter();
    let mut datetime_iter = result_df.column("datetime")?.datetime()?.into_iter();
    let mut stop_time_iter = result_df.column("stop_time")?.duration()?.into_iter();
    let mut working_time_iter = result_df.column("working_time")?.duration()?.into_iter();

    let rows:Vec<WorkingTime> = row_number_iter.map(
        |row_number|{
            let row_number= row_number.unwrap_or(0);
            let datetime = datetime_iter.next().unwrap().unwrap_or(0);
            let stop_time = stop_time_iter.next().unwrap().unwrap_or(0);
            let working_time = working_time_iter.next().unwrap().unwrap_or(0);

            let end_datetime = datetime + working_time;

            let start_datetime=  Local.timestamp_millis_opt(datetime).unwrap()- offset;
            let end_datetime=  Local.timestamp_millis_opt(end_datetime).unwrap()- offset;
            let stop_time = Duration::milliseconds(stop_time);
            let working_time = Duration::milliseconds(working_time);
        
            WorkingTime {
                row_number,
                start_datetime,
                end_datetime,
                stop_time,
                working_time
            }
        }
    ).collect();

    for row in &rows {
        row.show()
        
    }

    println!("稼働時刻,稼働時間,直前の停止時間");
    for row in rows {
        row.show2()   
    }


    Ok(())
}
#[derive(Debug)]
struct WorkingTime {
    row_number: u32,
    start_datetime: DateTime<Local>,
    end_datetime: DateTime<Local>,
    stop_time:Duration,
    working_time:Duration
}

impl WorkingTime{
    fn show(&self){
        let start_time_string= self.start_datetime.format("%m/%d %H:%M").to_string();
        let end_time_string= self.end_datetime.format("%H:%M").to_string();
        let working_minutes = self.working_time.num_minutes();
        let stop_minutes = self.stop_time.num_minutes();
        let stop_seconds = self.stop_time.num_seconds()% 60;

        println!("{:>10}:{} - {} 稼働時間:{:>4}分、直前の停止時間:{:>4}分{:02}秒",
            self.row_number,
            start_time_string,
            end_time_string,
            working_minutes,
            stop_minutes,
            stop_seconds
        );

    }
    fn show2(&self){
        let start_time_string= self.start_datetime.format("%m/%d %H:%M").to_string();
        let end_time_string= self.end_datetime.format("%H:%M").to_string();
        let working_minutes = self.working_time.num_minutes();
        let stop_minutes = self.stop_time.num_minutes();
        let stop_seconds = self.stop_time.num_seconds()% 60;

        println!("{} - {},{:>4}分,{:>4}分{:02}秒",
            start_time_string,
            end_time_string,
            working_minutes,
            stop_minutes,
            stop_seconds
        );

    }
}

/*
       398:08/26 09:06 - 09:20 稼働時間:  14分、直前の停止時間:   0分55秒
      1057:08/26 09:20 - 09:31 稼働時間:  10分、直前の停止時間:   7分47秒
      1283:08/26 09:31 - 10:06 稼働時間:  34分、直前の停止時間:   8分16秒
      2771:08/26 10:06 - 10:09 稼働時間:   3分、直前の停止時間:  19分56秒
      3017:08/26 10:09 - 15:12 稼働時間: 302分、直前の停止時間:   1分11秒
     21134:08/26 15:12 - 15:45 稼働時間:  33分、直前の停止時間: 120分51秒
     22053:08/26 15:45 - 20:15 稼働時間: 269分、直前の停止時間:  24分39秒
     41631:08/26 20:15 - 01:09 稼働時間: 294分、直前の停止時間:  73分28秒
     61144:08/27 01:09 - 01:12 稼働時間:   2分、直前の停止時間:  98分54秒
     61282:08/27 01:12 - 01:14 稼働時間:   1分、直前の停止時間:   0分56秒
     61409:08/27 01:14 - 04:31 稼働時間: 197分、直前の停止時間:   0分43秒
     78723:08/27 04:31 - 08:39 稼働時間: 247分、直前の停止時間:  24分16秒
     97286:08/27 08:39 - 11:37 稼働時間: 177分、直前の停止時間:  61分43秒
    112130:08/27 11:37 - 11:42 稼働時間:   5分、直前の停止時間:  28分50秒
    112415:08/27 11:42 - 12:05 稼働時間:  22分、直前の停止時間:   2分52秒
    112515:08/27 12:05 - 14:28 稼働時間: 143分、直前の停止時間:  21分39秒
    116766:08/27 14:28 - 14:31 稼働時間:   2分、直前の停止時間: 100分47秒
    117011:08/27 14:31 - 18:36 稼働時間: 244分、直前の停止時間:   0分20秒
    136466:08/27 18:36 - 22:12 稼働時間: 216分、直前の停止時間:  49分47秒
    156044:08/27 22:12 - 02:48 稼働時間: 276分、直前の停止時間:  20分01秒
    174124:08/28 02:48 - 07:05 稼働時間: 257分、直前の停止時間:  95分26秒
稼働時刻,稼働時間,直前の停止時間
08/26 09:06 - 09:20,  14分,   0分55秒
08/26 09:20 - 09:31,  10分,   7分47秒
08/26 09:31 - 10:06,  34分,   8分16秒
08/26 10:06 - 10:09,   3分,  19分56秒
08/26 10:09 - 15:12, 302分,   1分11秒
08/26 15:12 - 15:45,  33分, 120分51秒
08/26 15:45 - 20:15, 269分,  24分39秒
08/26 20:15 - 01:09, 294分,  73分28秒
08/27 01:09 - 01:12,   2分,  98分54秒
08/27 01:12 - 01:14,   1分,   0分56秒
08/27 01:14 - 04:31, 197分,   0分43秒
08/27 04:31 - 08:39, 247分,  24分16秒
08/27 08:39 - 11:37, 177分,  61分43秒
08/27 11:37 - 11:42,   5分,  28分50秒
08/27 11:42 - 12:05,  22分,   2分52秒
08/27 12:05 - 14:28, 143分,  21分39秒
08/27 14:28 - 14:31,   2分, 100分47秒
08/27 14:31 - 18:36, 244分,   0分20秒
08/27 18:36 - 22:12, 216分,  49分47秒
08/27 22:12 - 02:48, 276分,  20分01秒
08/28 02:48 - 07:05, 257分,  95分26秒
*/
