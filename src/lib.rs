pub mod v1;
pub mod v2;

pub fn set_env() {
    std::env::set_var("POLARS_FMT_MAX_COLS", "50"); // default: 8
    std::env::set_var("POLARS_FMT_MAX_ROWS", "60"); // default: 8
    std::env::set_var("RUST_BACKTRACE", "full");
}


pub const ROW_FILE_DIR:&str = "D:/data/row/240828_三芳三方9号機蛇行センサ";
pub const ROW_FILE_PATHS:[&str;8]=[
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log000_240826085833.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log001_240826170532.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log002_240826231930.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log003_240827062448.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log004_240827150125.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log005_240827205143.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log006_240828034739.csv",
    "D:/data/row/240828_三芳三方9号機蛇行センサ/log007_240828095326.csv",
];
