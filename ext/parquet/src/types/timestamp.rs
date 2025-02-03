use super::*;

pub fn parse_zoned_timestamp(value: &ParquetValue) -> jiff::Timestamp {
    let (ts, tz) = match value {
        ParquetValue::TimestampSecond(ts, tz) => (jiff::Timestamp::from_second(*ts).unwrap(), tz),
        ParquetValue::TimestampMillis(ts, tz) => {
            (jiff::Timestamp::from_millisecond(*ts).unwrap(), tz)
        }
        ParquetValue::TimestampMicros(ts, tz) => {
            (jiff::Timestamp::from_microsecond(*ts).unwrap(), tz)
        }
        ParquetValue::TimestampNanos(ts, tz) => {
            (jiff::Timestamp::from_nanosecond(*ts as i128).unwrap(), tz)
        }
        _ => panic!("Invalid timestamp value"),
    };

    // If timezone is provided, convert to zoned timestamp
    if let Some(tz) = tz {
        // Handle fixed offset timezones like "+09:00" first
        if tz.starts_with('+') || tz.starts_with('-') {
            // Parse the offset string into hours and minutes
            let (hours, minutes) = if tz.len() >= 5 && tz.contains(':') {
                // Format: "+09:00" or "-09:00"
                let h = tz[1..3].parse::<i32>().unwrap_or(0);
                let m = tz[4..6].parse::<i32>().unwrap_or(0);
                (h, m)
            } else if tz.len() >= 3 {
                // Format: "+09" or "-09"
                let h = tz[1..3].parse::<i32>().unwrap_or(0);
                (h, 0)
            } else {
                (0, 0)
            };

            // Apply sign
            let total_minutes = if tz.starts_with('-') {
                -(hours * 60 + minutes)
            } else {
                hours * 60 + minutes
            };

            // Create fixed timezone
            let tz = jiff::tz::TimeZone::fixed(jiff::tz::offset((total_minutes / 60) as i8));
            ts.to_zoned(tz).timestamp()
        } else {
            // Try IANA timezone
            match ts.intz(&tz) {
                Ok(zoned) => zoned.timestamp(),
                Err(_) => ts, // Fall back to UTC if timezone is invalid
            }
        }
    } else {
        // No timezone provided - treat as UTC
        ts
    }
}

// Macro for handling timestamp conversions
#[macro_export]
macro_rules! impl_timestamp_conversion {
    ($value:expr, $unit:ident, $handle:expr) => {{
        match $value {
            ParquetValue::$unit(ts, tz) => {
                let ts = parse_zoned_timestamp(&ParquetValue::$unit(ts, tz));
                let time_class = $handle.class_time();
                Ok(time_class
                    .funcall::<_, _, Value>("parse", (ts.to_string(),))?
                    .into_value_with($handle))
            }
            _ => panic!("Invalid timestamp type"),
        }
    }};
}

// Macro for handling date conversions
#[macro_export]
macro_rules! impl_date_conversion {
    ($value:expr, $handle:expr) => {{
        let ts = jiff::Timestamp::from_second(($value as i64) * 86400).unwrap();
        let formatted = ts.strftime("%Y-%m-%d").to_string();
        Ok(formatted.into_value_with($handle))
    }};
}
