use m3_9_displacement::ROW_FILE_DIR;

use std::fs;
fn main() {
    match fs::read_dir(ROW_FILE_DIR) {
        Ok(entries) => {
            for entry in entries {
                match entry {
                    Ok(entry) => println!("\"{}/{}\",", ROW_FILE_DIR,entry.file_name().into_string().unwrap()),
                    Err(e) => println!("Error reading entry: {}", e),
                }
            }
        }
        Err(e) => println!("Error reading directory: {}", e),
    }
}

