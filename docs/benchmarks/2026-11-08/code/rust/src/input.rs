//! Format adapters only: typed Arrow batches feed the existing daily engine.
use crate::{reader, Result};
use arrow_array::{
    Array, Date32Array, Float64Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::DataType;
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Instant,
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
        reader: csv::Reader<std::io::Take<File>>,
        row: csv::StringRecord,
        expected_fields: usize,
    },
    Parquet {
        reader: Box<
            dyn Iterator<Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>> + Send,
        >,
        batch: Option<RecordBatch>,
        row: usize,
        columns: Vec<usize>,
        date_cache: Option<DateCache>,
        profile: ReadProfile,
    },
}

pub fn read_profile_enabled() -> bool {
    std::env::var("ATTRIBUTION_PROFILE_READ").is_ok_and(|v| v == "1")
}

pub struct ReadProfile {
    enabled: bool,
    batch_seconds: f64,
    batches: u64,
}
impl ReadProfile {
    fn new() -> Self {
        Self {
            enabled: read_profile_enabled(),
            batch_seconds: 0.0,
            batches: 0,
        }
    }
}

// Date32 values repeat throughout a date and across batch boundaries. Keep the
// formatted text until the integer day changes instead of allocating per row.
pub struct DateCache {
    column: usize,
    day: Option<i32>,
    text: String,
    #[cfg(test)]
    formats: usize,
}
impl DateCache {
    fn new(column: usize) -> Self {
        Self {
            column,
            day: None,
            text: String::new(),
            #[cfg(test)]
            formats: 0,
        }
    }
    fn refresh(&mut self, batch: &RecordBatch, row: usize) -> Result<()> {
        let day = batch
            .column(self.column)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row);
        if self.day != Some(day) {
            let date = arrow_array::temporal_conversions::date32_to_datetime(day)
                .ok_or("date32 out of range")?;
            self.text = date.format("%Y-%m-%d").to_string();
            self.day = Some(day);
            #[cfg(test)]
            {
                self.formats += 1;
            }
        }
        Ok(())
    }
}

fn is_string(t: &DataType) -> bool {
    matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

impl Input {
    // Already decoded complete dates reuse the existing validation/daily engine.
    pub(crate) fn from_batches(batches: Vec<RecordBatch>, header: &[&str]) -> Result<Self> {
        let schema = batches.first().ok_or("empty date")?.schema();
        let columns = header
            .iter()
            .map(|n| schema.index_of(n))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let date_cache = schema.index_of("EffectiveDate").ok().map(DateCache::new);
        Ok(Self::Parquet {
            reader: Box::new(batches.into_iter().map(Ok)),
            batch: None,
            row: 0,
            columns,
            date_cache,
            profile: ReadProfile::new(),
        })
    }
    pub fn open(path: &Path, header: &[&str]) -> Result<Self> {
        Self::open_groups(path, header, None)
    }

    pub fn open_groups(path: &Path, header: &[&str], groups: Option<Vec<usize>>) -> Result<Self> {
        match path.extension().and_then(|s| s.to_str()) {
            Some("csv") => Ok(Self::Csv {
                reader: reader(path, header)?,
                row: csv::StringRecord::new(),
                expected_fields: header.len(),
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
                let builder = if let Some(groups) = groups {
                    builder.with_row_groups(groups)
                } else {
                    builder
                };
                let reader = builder
                    .with_projection(projection)
                    .with_batch_size(65_536)
                    .build()?;
                use arrow_array::RecordBatchReader;
                let columns = header
                    .iter()
                    .map(|name| reader.schema().index_of(name))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let date_cache = reader
                    .schema()
                    .index_of("EffectiveDate")
                    .ok()
                    .filter(|&i| reader.schema().field(i).data_type() == &DataType::Date32)
                    .map(DateCache::new);
                Ok(Self::Parquet {
                    reader: Box::new(reader),
                    batch: None,
                    row: 0,
                    columns,
                    date_cache,
                    profile: ReadProfile::new(),
                })
            }
            _ => Err(format!("unsupported input format: {}", path.display()).into()),
        }
    }

    pub fn csv_range(path: &Path, start: u64, end: u64) -> Result<Self> {
        let mut file = File::open(path)?;
        file.seek(SeekFrom::Start(start))?;
        let reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .buffer_capacity(1024 * 1024)
            .from_reader(file.take(end - start));
        Ok(Self::Csv {
            reader,
            row: csv::StringRecord::new(),
            expected_fields: crate::HEADER.len(),
        })
    }

    pub fn advance(&mut self) -> Result<bool> {
        match self {
            Self::Csv {
                reader,
                row,
                expected_fields,
            } => {
                let advanced = reader.read_record(row)?;
                if advanced && row.len() != *expected_fields {
                    return Err("invalid CSV field count".into());
                }
                Ok(advanced)
            }
            Self::Parquet {
                reader,
                batch,
                row,
                date_cache,
                profile,
                ..
            } => {
                if let Some(b) = batch {
                    *row += 1;
                    if *row < b.num_rows() {
                        if let Some(cache) = date_cache {
                            cache.refresh(b, *row)?;
                        }
                        return Ok(true);
                    }
                }
                loop {
                    let started = profile.enabled.then(Instant::now);
                    let next = reader.next();
                    if let Some(started) = started {
                        profile.batch_seconds += started.elapsed().as_secs_f64();
                        if next.is_some() {
                            profile.batches += 1;
                        }
                    }
                    let Some(next) = next else {
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
                    if let Some(cache) = date_cache {
                        cache.refresh(&next, 0)?;
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
                date_cache,
                ..
            } => {
                if let Some(cache) = date_cache {
                    if columns[col] == cache.column {
                        return Ok(Cow::Borrowed(&cache.text));
                    }
                }
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

    pub fn read_profile(&self) -> Option<(f64, u64)> {
        match self {
            Self::Parquet { profile, .. } if profile.enabled => {
                Some((profile.batch_seconds, profile.batches))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn dates(values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "EffectiveDate",
                DataType::Date32,
                false,
            )])),
            vec![Arc::new(Date32Array::from(values))],
        )
        .unwrap()
    }
    #[test]
    fn date_text_is_formatted_only_when_the_day_changes() {
        let mut cache = DateCache::new(0);
        let a = dates(vec![19358, 19358]);
        cache.refresh(&a, 0).unwrap();
        assert_eq!(cache.text, "2023-01-01");
        let address = cache.text.as_ptr();
        cache.refresh(&a, 1).unwrap();
        let b = dates(vec![19358, 19359, 19359]);
        cache.refresh(&b, 0).unwrap();
        assert_eq!(cache.formats, 1);
        assert_eq!(cache.text.as_ptr(), address);
        cache.refresh(&b, 1).unwrap();
        cache.refresh(&b, 2).unwrap();
        assert_eq!(cache.text, "2023-01-02");
        assert_eq!(cache.formats, 2);
    }
    #[test]
    fn invalid_date32_still_fails() {
        assert!(DateCache::new(0)
            .refresh(&dates(vec![i32::MAX]), 0)
            .is_err());
    }
}
