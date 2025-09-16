use std::io;

use io::Write;

use clap::Parser;
use clap::ValueEnum;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::record_batch::RecordBatchReader;

use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    basic::Compression,
    file::properties::{
        EnabledStatistics, WriterProperties, WriterPropertiesBuilder, WriterVersion,
    },
};

pub fn fsync_all(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_all()
}

pub fn fsync_dat(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_data()
}

pub fn fsync_nop(_: &mut std::fs::File) -> Result<(), io::Error> {
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FsyncType {
    /// No fsync.
    Nop,

    /// fsync_data.
    Dat,

    /// fsync.
    All,
}

impl FsyncType {
    pub fn to_fn(self) -> fn(&mut std::fs::File) -> Result<(), io::Error> {
        match self {
            FsyncType::Nop => fsync_nop,
            FsyncType::Dat => fsync_dat,
            FsyncType::All => fsync_all,
        }
    }
}

impl core::str::FromStr for FsyncType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "nop" => Ok(FsyncType::Nop),
            "dat" => Ok(FsyncType::Dat),
            "all" => Ok(FsyncType::All),
            _ => Err(format!("invalid fsync type: {}", s)),
        }
    }
}

#[derive(Parser, Debug)]
pub struct Args {
    /// Enables debug log.
    #[arg(short, long, default_value_t = false)]
    pub debug: bool,

    /// The name of the parquet file to be converted.
    #[arg(short, long)]
    pub input_parquet_filename: String,

    /// The name of the parquet file to be saved.
    #[arg(short, long)]
    pub output_parquet_filename: String,

    /// The type of fsync to use.
    #[arg(short, long, value_enum, default_value = "nop")]
    pub fsync_after_write: FsyncType,

    /// The version of the writer to be written to the parquet metadata.
    /// Possible values: PARQUET_2_0, PARQUET_1_0.
    #[arg(long, value_enum, default_value = "PARQUET_2_0")]
    pub writer_version: WriterVersion,

    /// Best‑effort maximum size of a data page in bytes.
    #[arg(short = 'd', long, default_value_t = 1024 * 1024)]
    pub data_page_size_limit: usize,

    /// Best‑effort maximum number of rows in a data page.
    #[arg(long, default_value_t = 20_000)]
    pub data_page_row_count_limit: usize,

    /// Write‑batch size (rows per internal write).
    #[arg(long, default_value_t = 1024)]
    pub write_batch_size: usize,

    /// Maximum number of rows in a row‑group.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub max_row_group_size: usize,

    /// “Created‑by” property.  Default is the current crate name and
    /// version (handled at runtime).
    #[arg(long, default_value = "parquet-rs")]
    pub created_by: String,

    /// Disable offset indexes.
    #[arg(long, default_value_t = false)]
    pub offset_index_disabled: bool,

    /// Coerce types to parquet native types.
    #[arg(long, default_value_t = false)]
    pub coerce_types: bool,

    // ───────  Column‑level defaults  ───────────────────────────────────
    /// Default compression codec for all columns.
    /// Possible values: UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4_RAW, ZSTD.
    #[arg(long, value_enum, default_value = "UNCOMPRESSED")]
    pub compression: Compression,

    /// Default dictionary‑encoding flag for all columns.
    #[arg(long, default_value_t = true)]
    pub dictionary_enabled: bool,

    /// Best‑effort maximum dictionary page size in bytes.
    #[arg(long, default_value_t = 1024 * 1024)]
    pub dictionary_page_size_limit: usize,

    /// Default `EnabledStatistics` level for all columns.
    /// Possible values: NONE, CHUNK, PAGE
    #[arg(long, value_enum, default_value = "PAGE")]
    pub statistics_enabled: EnabledStatistics,

    /// Write statistics in the page header for all columns.
    #[arg(long, default_value_t = false)]
    pub write_page_header_statistics: bool,

    /// Write Bloom filters for all columns.
    #[arg(long, default_value_t = false)]
    pub bloom_filter_enabled: bool,

    /// Default Bloom‑filter false‑positive probability (sets FPP and
    /// implicitly enables Bloom filters).
    #[arg(long, default_value_t = 0.0)]
    pub bloom_filter_fpp: f64,

    /// Default Bloom‑filter NDV (distinct values) for all columns.
    #[arg(long, default_value_t = 0)]
    pub bloom_filter_ndv: u64,
}

impl Args {
    pub fn convert_parquet(self) -> Result<(), io::Error> {
        let debug: bool = self.debug;
        let input_file = std::fs::File::open(&self.input_parquet_filename)?;
        let output_file = std::fs::File::create(&self.output_parquet_filename)?;
        let fsync_fn = self.fsync_after_write.to_fn();
        let props: WriterProperties = self.into();
        if debug {
            println!("{props:#?}");
        }
        let writer_props: Option<WriterProperties> = Some(props);
        parquet2parquet(input_file, output_file, writer_props, fsync_fn)?;
        Ok(())
    }
}

impl From<Args> for WriterPropertiesBuilder {
    fn from(a: Args) -> Self {
        let mut builder = WriterPropertiesBuilder::default();

        builder = builder
            .set_writer_version(a.writer_version)
            .set_data_page_size_limit(a.data_page_size_limit)
            .set_data_page_row_count_limit(a.data_page_row_count_limit)
            .set_write_batch_size(a.write_batch_size)
            .set_max_row_group_size(a.max_row_group_size)
            .set_created_by(a.created_by)
            .set_offset_index_disabled(a.offset_index_disabled)
            .set_coerce_types(a.coerce_types);

        builder = builder
            .set_compression(a.compression)
            .set_dictionary_enabled(a.dictionary_enabled)
            .set_dictionary_page_size_limit(a.dictionary_page_size_limit)
            .set_statistics_enabled(a.statistics_enabled)
            .set_write_page_header_statistics(a.write_page_header_statistics)
            .set_bloom_filter_enabled(a.bloom_filter_enabled);

        if a.bloom_filter_fpp > 0.0 {
            builder = builder.set_bloom_filter_fpp(a.bloom_filter_fpp);
        }
        if a.bloom_filter_ndv > 0 {
            builder = builder.set_bloom_filter_ndv(a.bloom_filter_ndv);
        }

        builder
    }
}

impl From<Args> for WriterProperties {
    fn from(a: Args) -> Self {
        WriterPropertiesBuilder::from(a).build()
    }
}

pub fn wtr2arrow_writer<W>(
    w: W,
    schema: SchemaRef,
    props: Option<WriterProperties>,
) -> Result<ArrowWriter<W>, io::Error>
where
    W: Write + Send,
{
    ArrowWriter::try_new(w, schema, props).map_err(io::Error::other)
}

pub fn file2reader(f: std::fs::File) -> Result<impl RecordBatchReader, io::Error> {
    ParquetRecordBatchReaderBuilder::try_new(f)
        .and_then(|bldr| bldr.build())
        .map_err(io::Error::other)
}

pub fn rdr2schema(rdr: &impl RecordBatchReader) -> SchemaRef {
    rdr.schema()
}

pub fn rdr2wtr<R, W>(rdr: R, mut wtr: ArrowWriter<W>) -> Result<W, io::Error>
where
    R: RecordBatchReader,
    W: Write + Send,
{
    for rbat in rdr {
        let bat: RecordBatch = rbat.map_err(io::Error::other)?;
        wtr.write(&bat)?;
    }
    wtr.into_inner().map_err(io::Error::other)
}

pub fn input2output<W>(
    input_parquet: std::fs::File,
    output_parquet: W,
    props: Option<WriterProperties>,
) -> Result<W, io::Error>
where
    W: Write + Send,
{
    let rdr = file2reader(input_parquet)?;
    let schema: SchemaRef = rdr2schema(&rdr);
    let wtr: ArrowWriter<W> = wtr2arrow_writer(output_parquet, schema, props)?;
    rdr2wtr(rdr, wtr)
}

pub fn parquet2parquet<F>(
    input_parquet: std::fs::File,
    mut output_parquet: std::fs::File,
    props: Option<WriterProperties>,
    fsync: F,
) -> Result<(), io::Error>
where
    F: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let out_file = input2output(input_parquet, &mut output_parquet, props)?;
    out_file.flush()?;
    fsync(out_file)?;
    Ok(())
}
