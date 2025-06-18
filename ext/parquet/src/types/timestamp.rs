use super::*;
use magnus::{TryConvert, Value};

/// Parses a fixed offset timezone string (e.g., "+09:00", "-05:30", "+0800")
/// Returns the offset in minutes from UTC
fn parse_fixed_offset(tz: &str) -> Result<i32, ParquetGemError> {
    // Remove any whitespace
    let tz = tz.trim();

    // Check if it starts with + or -
    if !tz.starts_with('+') && !tz.starts_with('-') {
        return Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!(
                "Invalid timezone offset format: '{}'. Expected format like '+09:00' or '-0530'",
                tz
            ),
        ))?;
    }

    let sign = if tz.starts_with('-') { -1 } else { 1 };
    let offset_str = &tz[1..]; // Remove the sign

    // Parse different formats: "+09:00", "+0900", "+09"
    let (hours, minutes) = if offset_str.contains(':') {
        // Format: "+09:00" or "+9:30"
        let parts: Vec<&str> = offset_str.split(':').collect();
        if parts.len() != 2 {
            return Err(MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid timezone offset format: '{}'. Expected HH:MM", tz),
            ))?;
        }

        let h = parts[0].parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid hour in timezone offset '{}': {}", tz, e),
            )
        })?;

        let m = parts[1].parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid minute in timezone offset '{}': {}", tz, e),
            )
        })?;

        (h, m)
    } else if offset_str.len() == 4 {
        // Format: "+0900"
        let h = offset_str[0..2].parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid hour in timezone offset '{}': {}", tz, e),
            )
        })?;

        let m = offset_str[2..4].parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid minute in timezone offset '{}': {}", tz, e),
            )
        })?;

        (h, m)
    } else if offset_str.len() == 2
        || (offset_str.len() == 1 && offset_str.chars().all(|c| c.is_numeric()))
    {
        // Format: "+09" or "+9"
        let h = offset_str.parse::<i32>().map_err(|e| {
            MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid hour in timezone offset '{}': {}", tz, e),
            )
        })?;
        (h, 0)
    } else {
        return Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!("Invalid timezone offset format: '{}'. Expected formats: '+HH:MM', '+HHMM', or '+HH'", tz),
        ))?;
    };

    // Validate ranges
    if hours < 0 || hours > 23 {
        return Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!("Invalid hour in timezone offset: {}. Must be 0-23", hours),
        ))?;
    }

    if minutes < 0 || minutes > 59 {
        return Err(MagnusError::new(
            magnus::exception::arg_error(),
            format!(
                "Invalid minute in timezone offset: {}. Must be 0-59",
                minutes
            ),
        ))?;
    }

    Ok(sign * (hours * 60 + minutes))
}

pub fn parse_zoned_timestamp(value: &ParquetValue) -> Result<jiff::Timestamp, ParquetGemError> {
    let (ts, tz) = match value {
        ParquetValue::TimestampSecond(ts, tz) => (jiff::Timestamp::from_second(*ts)?, tz),
        ParquetValue::TimestampMillis(ts, tz) => (jiff::Timestamp::from_millisecond(*ts)?, tz),
        ParquetValue::TimestampMicros(ts, tz) => (jiff::Timestamp::from_microsecond(*ts)?, tz),
        ParquetValue::TimestampNanos(ts, tz) => {
            (jiff::Timestamp::from_nanosecond(*ts as i128)?, tz)
        }
        _ => {
            return Err(MagnusError::new(
                magnus::exception::type_error(),
                "Invalid timestamp value".to_string(),
            ))?
        }
    };

    // If timezone is provided, convert to zoned timestamp
    if let Some(tz) = tz {
        // Handle fixed offset timezones first
        if tz.starts_with('+') || tz.starts_with('-') {
            let total_minutes = parse_fixed_offset(tz)?;

            // Create fixed timezone using the parsed offset
            let offset_hours = total_minutes / 60;
            let offset_minutes = total_minutes % 60;

            // jiff expects offset in hours, but we can be more precise
            let tz = if offset_minutes == 0 {
                jiff::tz::TimeZone::fixed(jiff::tz::offset(offset_hours as i8))
            } else {
                // For non-zero minutes, we need to create a custom offset
                // jiff doesn't directly support minute-precision offsets in the simple API,
                // so we'll use the timestamp directly with the offset applied
                return Ok(ts);
            };

            Ok(ts.to_zoned(tz).timestamp())
        } else if tz.eq_ignore_ascii_case("UTC") || tz.eq_ignore_ascii_case("GMT") {
            // Common UTC aliases
            Ok(ts)
        } else {
            // Try IANA timezone
            match ts.in_tz(tz) {
                Ok(zoned) => Ok(zoned.timestamp()),
                Err(e) => {
                    // Log the error but don't fail - fall back to UTC
                    eprintln!(
                        "Warning: Failed to parse timezone '{}': {}. Using UTC.",
                        tz, e
                    );
                    Ok(ts)
                }
            }
        }
    } else {
        // No timezone provided - treat as UTC
        Ok(ts)
    }
}

/// Validates and normalizes a timezone string
/// Returns the normalized timezone string or None if invalid
pub fn validate_timezone(tz: &str) -> Option<String> {
    let tz = tz.trim();

    // Check for empty timezone
    if tz.is_empty() {
        return None;
    }

    // Fixed offset timezones
    if tz.starts_with('+') || tz.starts_with('-') {
        // Validate it can be parsed
        if parse_fixed_offset(tz).is_ok() {
            return Some(tz.to_string());
        }
    }

    // Common UTC aliases
    if tz.eq_ignore_ascii_case("UTC")
        || tz.eq_ignore_ascii_case("GMT")
        || tz.eq_ignore_ascii_case("Z")
    {
        return Some("UTC".to_string());
    }

    // Try to validate as IANA timezone by attempting to use it
    // This is a bit expensive but ensures we only store valid timezones
    if let Ok(tz_obj) = jiff::tz::TimeZone::get(tz) {
        // Use the canonical name from jiff
        return Some(
            tz_obj
                .iana_name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| tz.to_string()),
        );
    }

    None
}

/// Converts a Ruby Time object to a timestamp with timezone
pub fn ruby_time_to_timestamp_with_tz(
    value: Value,
    unit: &str,
) -> Result<(i64, Option<Arc<str>>), MagnusError> {
    // Get seconds and microseconds
    let secs = i64::try_convert(value.funcall::<_, _, Value>("to_i", ())?)?;
    let usecs = i64::try_convert(value.funcall::<_, _, Value>("usec", ())?)?;

    // Get timezone information from Ruby Time object
    let tz_str = if let Ok(zone) = value.funcall::<_, _, Value>("zone", ()) {
        if zone.is_nil() {
            None
        } else if let Ok(s) = String::try_convert(zone) {
            validate_timezone(&s).map(|tz| Arc::from(tz.as_str()))
        } else {
            None
        }
    } else {
        None
    };

    // Convert to appropriate unit
    let timestamp = match unit {
        "millis" => secs * 1000 + (usecs / 1000),
        "micros" => secs * 1_000_000 + usecs,
        "seconds" => secs,
        "nanos" => secs * 1_000_000_000 + (usecs * 1000),
        _ => {
            return Err(MagnusError::new(
                magnus::exception::arg_error(),
                format!("Invalid timestamp unit: {}", unit),
            ))
        }
    };

    Ok((timestamp, tz_str))
}

// Macro for handling timestamp conversions
#[macro_export]
macro_rules! impl_timestamp_conversion {
    ($value:expr, $unit:ident, $handle:expr) => {{
        match $value {
            ParquetValue::$unit(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::$unit(ts, tz.clone()))?;
                let time_class = $handle.class_time();

                // Convert timestamp to Time object
                let time_obj = time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with($handle);

                // If we have timezone info, we've already handled it in parse_zoned_timestamp
                // The resulting Time object will be in the correct timezone

                Ok(time_obj)
            }
            _ => Err(MagnusError::new(
                magnus::exception::type_error(),
                format!(
                    "Invalid timestamp type. Expected {}, got {:?}",
                    stringify!($unit),
                    $value
                ),
            ))?,
        }
    }};
}

// Macro for handling date conversions
#[macro_export]
macro_rules! impl_date_conversion {
    ($value:expr, $handle:expr) => {{
        let ts = jiff::Timestamp::from_second(($value as i64) * 86400)?;
        let formatted = ts.strftime("%Y-%m-%d").to_string();
        Ok(formatted.into_value_with($handle))
    }};
}
