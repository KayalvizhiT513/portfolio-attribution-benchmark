//! Format adapters only: typed Arrow batches feed the existing daily engine.
use crate::{reader, Result};
use arrow_array::{
    Array, Date32Array, Float64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::DataType;
use parquet::arrow::{
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
    ProjectionMask,
};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

pub fn resolve(dir: &Path, stem: &str) -> Result<PathBuf> {
    let matches: Vec<_> = ["csv", "parquet"]
        .iter()
        .map(|ext| dir.join(format!("{stem}.{ext}")))
        .filter(|p| p.is_file())
        .collect();
    match matches.as_slice() {
        [path] => Ok(path.clone()),
        [] => Err(format!("missing {stem}.csv or {stem}.parquet").into()),
        _ => Err(format!("ambiguous input: both CSV and Parquet exist for {stem}").into()),
    }
}

pub fn portfolios(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = BTreeMap::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if !path.is_file()
            || !matches!(
                path.extension().and_then(|s| s.to_str()),
                Some("csv" | "parquet")
            )
        {
            continue;
        }
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        if stem.starts_with("portfolio_") && paths.insert(stem.clone(), path).is_some() {
            return Err(format!("ambiguous portfolio input: {stem}").into());
        }
    }
    if paths.is_empty() {
        return Err("no portfolios".into());
    }
    Ok(paths.into_values().collect())
}

pub enum Input {
    Csv {
        reader: csv::Reader<File>,
        row: csv::StringRecord,
    },
    Parquet {
        reader: ParquetRecordBatchReader,
        batch: Option<RecordBatch>,
        row: usize,
        columns: Vec<usize>,
    },
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

impl Input {
    pub fn open(path: &Path, header: &[&str]) -> Result<Self> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => Ok(Self::Csv {
                reader: reader(path, header)?,
                row: csv::StringRecord::new(),
            }),
            Some("parquet") => {
                let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
                let schema = builder.schema();
                let mut indices = Vec::new();
                for &name in header {
                    let matches: Vec<_> = schema
                        .fields()
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| f.name() == name)
                        .collect();
                    if matches.len() != 1 {
                        return Err(format!("missing or duplicate Parquet column {name}").into());
                    }
                    let (index, field) = matches[0];
                    let valid = match name {
                        "Weight" | "BaseReturn" | "LocalReturn" => {
                            field.data_type() == &DataType::Float64
                        }
                        "EffectiveDate" => {
                            is_string(field.data_type()) || field.data_type() == &DataType::Date32
                        }
                        _ => is_string(field.data_type()),
                    };
                    if !valid {
                        return Err(format!(
                            "unsupported Parquet type for {name}: {}",
                            field.data_type()
                        )
                        .into());
                    }
                    indices.push(index);
                }
                let projection = ProjectionMask::roots(builder.parquet_schema(), indices);
                let reader = builder
                    .with_projection(projection)
                    .with_batch_size(65_536)
                    .build()?;
                use arrow_array::RecordBatchReader;
                let columns = header
                    .iter()
                    .map(|name| reader.schema().index_of(name))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(Self::Parquet {
                    reader,
                    batch: None,
                    row: 0,
                    columns,
                })
            }
            _ => Err(format!("unsupported input format: {}", path.display()).into()),
        }
    }

    pub fn advance(&mut self) -> Result<bool> {
        match self {
            Self::Csv { reader, row } => Ok(reader.read_record(row)?),
            Self::Parquet {
                reader, batch, row, ..
            } => {
                if let Some(b) = batch {
                    *row += 1;
                    if *row < b.num_rows() {
                        return Ok(true);
                    }
                }
                loop {
                    let Some(next) = reader.next() else {
                        *batch = None;
                        return Ok(false);
                    };
                    let next = next?;
                    if next.num_rows() == 0 {
                        continue;
                    }
                    if next.columns().iter().any(|a| a.null_count() != 0) {
                        return Err("null Parquet input field".into());
                    }
                    *batch = Some(next);
                    *row = 0;
                    return Ok(true);
                }
            }
        }
    }

    pub fn text(&self, col: usize) -> Result<Cow<'_, str>> {
        match self {
            Self::Csv { row, .. } => Ok(Cow::Borrowed(&row[col])),
            Self::Parquet {
                batch,
                row,
                columns,
                ..
            } => {
                let a = batch.as_ref().unwrap().column(columns[col]);
                if let Some(a) = a.as_any().downcast_ref::<StringArray>() {
                    return Ok(Cow::Borrowed(a.value(*row)));
                }
                if let Some(a) = a.as_any().downcast_ref::<LargeStringArray>() {
                    return Ok(Cow::Borrowed(a.value(*row)));
                }
                if let Some(a) = a.as_any().downcast_ref::<StringViewArray>() {
                    return Ok(Cow::Borrowed(a.value(*row)));
                }
                if let Some(a) = a.as_any().downcast_ref::<Date32Array>() {
                    let date = arrow_array::temporal_conversions::date32_to_datetime(a.value(*row))
                        .ok_or("date32 out of range")?;
                    return Ok(Cow::Owned(date.format("%Y-%m-%d").to_string()));
                }
                Err("expected string or date32".into())
            }
        }
    }

    pub fn number(&self, col: usize) -> Result<f64> {
        match self {
            Self::Csv { row, .. } => Ok(row[col].parse()?),
            Self::Parquet {
                batch,
                row,
                columns,
                ..
            } => Ok(batch
                .as_ref()
                .unwrap()
                .column(columns[col])
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("expected float64")?
                .value(*row)),
        }
    }

    // CSV parser offsets have no equivalent for compressed columnar storage.
    // Parquet reports zero here; input_file_bytes reports physical file sizes.
    pub fn parsed_bytes(&self) -> u64 {
        match self {
            Self::Csv { reader, .. } => reader.position().byte(),
            Self::Parquet { .. } => 0,
        }
    }
}
