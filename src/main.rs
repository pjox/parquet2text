use polars::prelude::*;
use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use walkdir::DirEntry;
use walkdir::WalkDir;

fn main() {
    let args: Vec<String> = env::args().collect();

    let folder = &args[1];
    let file_paths: Vec<DirEntry> = WalkDir::new(folder)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter(|e| e.file_name().to_str().unwrap().ends_with(".parquet"))
        .collect();

    let output = File::create("test.txt").unwrap();
    let mut writer = BufWriter::new(output);

    for file_path in file_paths {
        let mut file = std::fs::File::open(file_path.path()).unwrap();
        let df = ParquetReader::new(&mut file)
            .with_columns(Some(vec!["dataset_id".to_string()]))
            .finish()
            .unwrap();

        let ca = df.column("dataset_id").unwrap().utf8().unwrap();

        for doc in ca.into_no_null_iter() {
            writeln!(writer, "{}", doc).unwrap();
            writeln!(writer, "\n").unwrap();
        }
    }
}
