//! Typed, bounded output batches; output format follows the portfolio input.
use crate::{civil, Config, Result, OUT};
use arrow_array::{
    builder::{ArrayBuilder, Date32Builder, Float64Builder, StringBuilder, UInt32Builder},
    RecordBatch,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
    sync::Arc,
};

pub fn path(c: &Config, input: &Path) -> PathBuf {
    c.output.join(format!(
        "{}-{}.{}",
        c.scenario,
        input.file_stem().unwrap().to_string_lossy(),
        input.extension().unwrap().to_string_lossy()
    ))
}
pub fn partial(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.partial", path.display()))
}

pub enum Writer {
    Csv(csv::Writer<BufWriter<File>>),
    Parquet {
        writer: ArrowWriter<File>,
        schema: Arc<Schema>,
        builders: Vec<Box<dyn ArrayBuilder>>,
    },
}
impl Writer {
    pub fn new(path: &Path, parquet: bool) -> Result<Self> {
        Self::new_with_sector(path, parquet, false)
    }
    pub fn new_with_sector(path: &Path, parquet: bool, sector: bool) -> Result<Self> {
        if !parquet {
            let mut writer = csv::WriterBuilder::new()
                .buffer_capacity(1024 * 1024)
                .from_writer(BufWriter::with_capacity(1024 * 1024, File::create(path)?));
            let mut header = OUT.to_vec();
            if sector {
                header.extend(["Sector", "Security", "Count"]);
            }
            writer.write_record(header)?;
            return Ok(Self::Csv(writer));
        }
        let mut fields = Vec::new();
        let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();
        let mut header = OUT.to_vec();
        if sector {
            header.extend(["Sector", "Security", "Count"]);
        }
        for (i, name) in header.iter().enumerate() {
            let dtype = match i {
                2 | 3 => DataType::Date32,
                4 | 22 => DataType::UInt32,
                8..=19 => DataType::Float64,
                _ => DataType::Utf8,
            };
            let builder: Box<dyn ArrayBuilder> = match dtype {
                DataType::Date32 => Box::new(Date32Builder::new()),
                DataType::UInt32 => Box::new(UInt32Builder::new()),
                DataType::Float64 => Box::new(Float64Builder::new()),
                _ => Box::new(StringBuilder::new()),
            };
            fields.push(Field::new(*name, dtype, false));
            builders.push(builder);
        }
        let schema = Arc::new(Schema::new(fields));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(65536))
            .build();
        let writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props))?;
        Ok(Self::Parquet {
            writer,
            schema,
            builders,
        })
    }
    pub fn row(
        &mut self,
        fields: &[&str; 8],
        numbers: &[f64; 12],
        record: &mut csv::ByteRecord,
    ) -> Result<()> {
        self.row_with_group(fields, numbers, record, None)
    }
    pub fn row_with_group(
        &mut self,
        fields: &[&str; 8],
        numbers: &[f64; 12],
        record: &mut csv::ByteRecord,
        group: Option<(&str, &str, u32)>,
    ) -> Result<()> {
        match self {
            Self::Csv(writer) => {
                record.clear();
                for f in fields {
                    record.push_field(f.as_bytes());
                }
                let mut buffer = ryu::Buffer::new();
                for n in numbers {
                    record.push_field(buffer.format(*n).as_bytes());
                }
                if let Some((sector, security, count)) = group {
                    record.push_field(sector.as_bytes());
                    record.push_field(security.as_bytes());
                    record.push_field(count.to_string().as_bytes());
                }
                writer.write_byte_record(record)?;
            }
            Self::Parquet { builders, .. } => {
                for (i, field) in fields.iter().enumerate() {
                    match i {
                        2 | 3 => builders[i]
                            .as_any_mut()
                            .downcast_mut::<Date32Builder>()
                            .unwrap()
                            .append_value(civil(field)?.3.try_into()?),
                        4 => builders[i]
                            .as_any_mut()
                            .downcast_mut::<UInt32Builder>()
                            .unwrap()
                            .append_value(field.parse::<u32>()?),
                        _ => builders[i]
                            .as_any_mut()
                            .downcast_mut::<StringBuilder>()
                            .unwrap()
                            .append_value(field),
                    }
                }
                for (i, n) in numbers.iter().enumerate() {
                    builders[i + 8]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .unwrap()
                        .append_value(*n);
                }
                if let Some((sector, security, count)) = group {
                    builders[20]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(sector);
                    builders[21]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .unwrap()
                        .append_value(security);
                    builders[22]
                        .as_any_mut()
                        .downcast_mut::<UInt32Builder>()
                        .unwrap()
                        .append_value(count);
                }
                if builders[0].len() == 65536 {
                    self.flush_batch()?;
                }
            }
        }
        Ok(())
    }
    fn flush_batch(&mut self) -> Result<()> {
        if let Self::Parquet {
            writer,
            schema,
            builders,
        } = self
        {
            if builders[0].len() != 0 {
                let arrays = builders.iter_mut().map(|b| b.finish()).collect();
                writer.write(&RecordBatch::try_new(schema.clone(), arrays)?)?;
            }
        }
        Ok(())
    }
    pub fn finish(mut self) -> Result<()> {
        self.flush_batch()?;
        match self {
            Self::Csv(mut writer) => {
                writer.flush()?;
                writer.get_ref().get_ref().sync_all()?;
            }
            Self::Parquet { writer, .. } => {
                writer.into_inner()?.sync_all()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::UInt32Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn sector_columns_survive_output_batch_boundary() -> Result<()> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "sector-output-{}-{stamp}.parquet",
            std::process::id()
        ));
        let mut writer = Writer::new_with_sector(&path, true, true)?;
        let mut record = csv::ByteRecord::new();
        let fields = [
            "sector_attribution",
            "PF",
            "2023-01-01",
            "2023-01-04",
            "4",
            "security",
            "S1",
            "base",
        ];
        for i in 0..65537u32 {
            writer.row_with_group(
                &fields,
                &[0.0; 12],
                &mut record,
                Some(("Technology", "S1", i)),
            )?;
        }
        writer.finish()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path)?)?.build()?;
        let mut count = 0usize;
        for batch in reader {
            let batch = batch?;
            assert_eq!(batch.num_columns(), 23);
            let values = batch
                .column_by_name("Count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(values.value(i), (count + i) as u32);
            }
            count += batch.num_rows();
        }
        assert_eq!(count, 65537);
        std::fs::remove_file(path)?;
        Ok(())
    }
}
