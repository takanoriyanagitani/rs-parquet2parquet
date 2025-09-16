use std::io;
use std::process::ExitCode;

use clap::Parser;

use rs_parquet2parquet::Args;

fn sub() -> Result<(), io::Error> {
    let args = Args::parse();
    args.convert_parquet()
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
