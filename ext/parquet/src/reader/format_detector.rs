use crate::types::ParquetGemError;
use std::io::{Read, Seek, SeekFrom};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Parquet,
    Arrow,
}

/// Detect the file format by examining magic bytes
pub fn detect_file_format<R: Read + Seek>(source: &mut R) -> Result<FileFormat, ParquetGemError> {
    let mut magic = [0u8; 8];

    // Read the first 8 bytes
    let bytes_read = source.read(&mut magic).map_err(ParquetGemError::from)?;

    // Reset to beginning
    source
        .seek(SeekFrom::Start(0))
        .map_err(ParquetGemError::from)?;

    if bytes_read >= 6 {
        // Arrow IPC file format magic: "ARROW1\0\0"
        if &magic[0..6] == b"ARROW1" {
            return Ok(FileFormat::Arrow);
        }
    }

    if bytes_read >= 4 {
        // Parquet magic: "PAR1" at start
        if &magic[0..4] == b"PAR1" {
            return Ok(FileFormat::Parquet);
        }
    }

    // If we can't detect from the beginning, check the end for Parquet
    // Parquet files also have "PAR1" at the end
    if let Ok(pos) = source.seek(SeekFrom::End(-4)) {
        if pos >= 4 {
            let mut end_magic = [0u8; 4];
            if source.read_exact(&mut end_magic).is_ok() && &end_magic == b"PAR1" {
                // Important: Reset to beginning before returning
                source
                    .seek(SeekFrom::Start(0))
                    .map_err(ParquetGemError::from)?;
                return Ok(FileFormat::Parquet);
            }
        }
    }

    // Always reset to beginning, even for unknown format
    source
        .seek(SeekFrom::Start(0))
        .map_err(ParquetGemError::from)?;

    Err(ParquetGemError::UnknownFormat)
}

/// Detect format from file extension as a fallback
pub fn detect_format_from_extension(path: &str) -> Option<FileFormat> {
    let lower = path.to_lowercase();
    if lower.ends_with(".parquet") || lower.ends_with(".parq") {
        Some(FileFormat::Parquet)
    } else if lower.ends_with(".arrow") || lower.ends_with(".feather") || lower.ends_with(".ipc") {
        Some(FileFormat::Arrow)
    } else {
        None
    }
}
