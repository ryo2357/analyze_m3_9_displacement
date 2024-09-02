
use polars::prelude::*;
use std::path::PathBuf;
use anyhow::Result;
use std::sync::Arc;

use crate::*;

pub const OFFSET_HOURS:i64 = 9;
pub enum FileName {
    Row(usize),
    ConcatenatedRow,
}

impl FileName {
    fn to_filepath(&self) -> Result<PathBuf> {
        let name:&str = match self {
            FileName::Row(num) => {
                if num > &ROW_FILE_PATHS.len(){
                    anyhow::bail!("Index {} is out of ROW_FILE_PATHS", num)
                }
                ROW_FILE_PATHS[*num]
            },
            FileName::ConcatenatedRow => anyhow::bail!("to_filepath is can not execute"),
        };
        let path = PathBuf::from(name);
        Ok(path)
    }
    pub fn read_file(&self)-> Result<LazyFrame> {
    
        let lf = match self {
            FileName::Row(_) => self.create_from_row_csv()?,
            FileName::ConcatenatedRow => create_concatenated()?,
        };
    
        Ok(lf)
    }

    fn create_from_row_csv(&self)-> Result<LazyFrame> {

        let mut sc = Schema::new();
        sc.with_column("row_number".to_string().into(), DataType::UInt32);
        sc.with_column("date".to_string().into(), DataType::Date);
        sc.with_column("time".to_string().into(), DataType::Time);
        sc.with_column("interval".to_string().into(), DataType::UInt64);
        sc.with_column("deviation".to_string().into(), DataType::Int32);

        let sc = Arc::new(sc);

        let lf = LazyCsvReader::new(self.to_filepath()?)
            .with_has_header(false)
            .with_skip_rows(1)
            .with_dtype_overwrite(Some(sc))
            .finish()?
            .lazy();

        // [StringNameSpace in polars_lazy::dsl::string - Rust](https://docs.pola.rs/api/rust/dev/polars_lazy/dsl/string/struct.StringNameSpace.html#method.to_datetime)
        
        let dt_options = StrptimeOptions {
            format: Some("%Y-%m-%d %H:%M:%S".into()),
            ..Default::default()
        };

        let lf = lf.with_column(
            (col("date").cast(DataType::String) + lit(" ") + col("time").cast(DataType::String))
                .str().to_datetime(Some(TimeUnit::Milliseconds),None,dt_options, lit(""))
                .alias("datetime")
            )
            .select(&[
                col("row_number"),
                col("datetime"),
                col("interval"),
                col("deviation"),
                ]);
    
        Ok(lf)
    }
}

fn create_concatenated()-> Result<LazyFrame> {
    let mut lf = FileName::Row(0).create_from_row_csv()?;
    for num in 1..ROW_FILE_PATHS.len(){
        // println!("num:{}",num);
        let file_lf = FileName::Row(num).create_from_row_csv()?;
        lf = concat([lf, file_lf], UnionArgs::default() )?

    }
    Ok(lf)
}