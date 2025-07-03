use crate::string_cache::StringCache;
use bytes::Bytes;
use indexmap::IndexMap;
use magnus::r_hash::ForEach;
use magnus::value::ReprValue;
use magnus::{
    Error as MagnusError, IntoValue, Module, RArray, RHash, RString, Ruby, Symbol, TryConvert,
    Value,
};
use ordered_float::OrderedFloat;
use parquet_core::{ParquetError, ParquetValue, Result};
use std::cell::RefCell;
use std::sync::Arc;

/// Ruby value converter
///
/// Note: This converter is not thread-safe due to Ruby's GIL requirements.
/// It should only be used within Ruby's thread context.
#[derive(Default)]
pub struct RubyValueConverter {
    string_cache: RefCell<Option<StringCache>>,
}

impl RubyValueConverter {
    pub fn new() -> Self {
        Self {
            string_cache: RefCell::new(None),
        }
    }

    pub fn with_string_cache(cache: StringCache) -> Self {
        Self {
            string_cache: RefCell::new(Some(cache)),
        }
    }

    pub fn string_cache_stats(&self) -> Option<crate::string_cache::CacheStats> {
        self.string_cache
            .borrow()
            .as_ref()
            .map(|cache| cache.stats())
    }

    /// Convert a Ruby value to ParquetValue with schema hint
    /// This handles both primitive and complex types
    pub fn to_parquet_with_schema_hint(
        &mut self,
        value: Value,
        schema_hint: Option<&parquet_core::SchemaNode>,
    ) -> Result<ParquetValue> {
        // Handle nil values
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // If we have a schema hint, use it to guide conversion
        if let Some(schema) = schema_hint {
            return self.convert_with_schema_hint(value, schema);
        }

        // Otherwise, infer type from Ruby value
        self.infer_and_convert(value)
    }

    /// Convert with explicit schema hint
    fn convert_with_schema_hint(
        &mut self,
        value: Value,
        schema: &parquet_core::SchemaNode,
    ) -> Result<ParquetValue> {
        use parquet_core::SchemaNode;

        match schema {
            SchemaNode::Primitive {
                primitive_type,
                format,
                ..
            } => self.convert_with_type_hint_and_format(value, primitive_type, format.as_deref()),
            SchemaNode::List { item, .. } => self.convert_to_list(value, item.as_ref()),
            SchemaNode::Map {
                key, value: val, ..
            } => self.convert_to_map(value, key.as_ref(), val.as_ref()),
            SchemaNode::Struct { fields, .. } => self.convert_to_struct(value, fields),
        }
    }

    /// Convert with explicit type hint and optional format
    fn convert_with_type_hint_and_format(
        &mut self,
        value: Value,
        type_hint: &parquet_core::PrimitiveType,
        format: Option<&str>,
    ) -> Result<ParquetValue> {
        use parquet_core::PrimitiveType::*;

        // Special handling for UUID format
        if let (FixedLenByteArray(16), Some("uuid")) = (type_hint, format) {
            return self.convert_to_uuid_binary(value);
        }

        // Handle date types with format
        match type_hint {
            Date32 => return self.convert_to_date32(value, format),
            Date64 => return self.convert_to_date64(value, format),
            _ => {}
        }

        // Default type hint conversion
        self.convert_with_type_hint(value, type_hint)
    }

    /// Convert with explicit type hint
    fn convert_with_type_hint(
        &mut self,
        value: Value,
        type_hint: &parquet_core::PrimitiveType,
    ) -> Result<ParquetValue> {
        use parquet_core::PrimitiveType::*;

        match type_hint {
            Boolean => self.convert_to_boolean(value),
            Int8 => self.convert_to_int8(value),
            Int16 => self.convert_to_int16(value),
            Int32 => self.convert_to_int32(value),
            Int64 => self.convert_to_int64(value),
            UInt8 => self.convert_to_uint8(value),
            UInt16 => self.convert_to_uint16(value),
            UInt32 => self.convert_to_uint32(value),
            UInt64 => self.convert_to_uint64(value),
            Float32 => self.convert_to_float32(value),
            Float64 => self.convert_to_float64(value),
            String => self.convert_to_string(value),
            Binary => self.convert_to_binary(value),
            Date32 => self.convert_to_date32(value, None),
            Date64 => self.convert_to_date64(value, None),
            TimeMillis => self.convert_to_time_millis(value),
            TimeMicros => self.convert_to_time_micros(value),
            TimeNanos => self.convert_to_time_nanos(value),
            TimestampSecond(schema_tz) => {
                self.convert_to_timestamp_second_with_tz(value, schema_tz.as_deref())
            }
            TimestampMillis(schema_tz) => {
                self.convert_to_timestamp_millis_with_tz(value, schema_tz.as_deref())
            }
            TimestampMicros(schema_tz) => {
                self.convert_to_timestamp_micros_with_tz(value, schema_tz.as_deref())
            }
            TimestampNanos(schema_tz) => {
                self.convert_to_timestamp_nanos_with_tz(value, schema_tz.as_deref())
            }
            Decimal128(precision, scale) => self.convert_to_decimal128(value, *precision, *scale),
            Decimal256(precision, scale) => self.convert_to_decimal256(value, *precision, *scale),
            FixedLenByteArray(len) => self.convert_to_fixed_len_byte_array(value, *len),
        }
    }

    /// Infer type from Ruby value and convert
    fn infer_and_convert(&mut self, value: Value) -> Result<ParquetValue> {
        let class_name = value.class().to_string();

        match class_name.as_str() {
            "Integer" => {
                let i: i64 = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                Ok(ParquetValue::Int64(i))
            }
            "Float" => {
                let f: f64 = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                Ok(ParquetValue::Float64(OrderedFloat(f)))
            }
            "String" => {
                let s: String = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                Ok(ParquetValue::String(s.into()))
            }
            "TrueClass" | "FalseClass" => {
                let b: bool = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                Ok(ParquetValue::Boolean(b))
            }
            "Array" => {
                let array: RArray = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                let mut list = Vec::with_capacity(array.len());

                for item in array.into_iter() {
                    list.push(self.infer_and_convert(item)?);
                }

                Ok(ParquetValue::List(list))
            }
            "Hash" => {
                let hash: RHash = TryConvert::try_convert(value)
                    .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
                let mut map = Vec::new();
                let mut conversion_error = None;

                hash.foreach(|key: Value, val: Value| {
                    match (self.infer_and_convert(key), self.infer_and_convert(val)) {
                        (Ok(k), Ok(v)) => {
                            map.push((k, v));
                            Ok(ForEach::Continue)
                        }
                        (Err(e), _) | (_, Err(e)) => {
                            conversion_error = Some(e);
                            Ok(ForEach::Stop)
                        }
                    }
                })
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;

                if let Some(err) = conversion_error {
                    return Err(err);
                }

                Ok(ParquetValue::Map(map))
            }
            "Time" => {
                // Convert Ruby Time to timestamp millis
                let millis = value
                    .funcall::<_, _, i64>("to_i", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))?
                    * 1000
                    + value
                        .funcall::<_, _, i32>("nsec", ())
                        .map_err(|e| ParquetError::Conversion(e.to_string()))?
                        as i64
                        / 1_000_000;
                let tz = self.extract_timezone(value)?;

                Ok(ParquetValue::TimestampMillis(millis, tz))
            }
            "BigDecimal" => {
                // Convert BigDecimal to Decimal128
                let str_val: String = value
                    .funcall("to_s", ("F",))
                    .map_err(|e| ParquetError::Conversion(e.to_string()))?;
                self.parse_decimal128(&str_val, 38, 10) // Default precision and scale
            }
            _ => {
                // Try to convert to string as fallback
                let s: String = value.to_string();
                Ok(ParquetValue::String(s.into()))
            }
        }
    }

    // Helper methods

    /// Normalize timestamp for Parquet storage according to Parquet specification:
    /// - WITH timezone in schema: Store as UTC (isAdjustedToUTC = true)
    /// - WITHOUT timezone in schema: Store as local/unzoned time (isAdjustedToUTC = false)
    ///
    /// IMPORTANT: Parquet can ONLY store:
    /// 1. UTC timestamps (when schema has ANY timezone)
    /// 2. Local/unzoned timestamps (when schema has NO timezone)
    ///
    /// Non-UTC timezones like "+09:00" or "America/New_York" are NOT preserved.
    fn normalize_timestamp_for_parquet(
        &self,
        time_value: Value,
        schema_has_timezone: bool,
    ) -> Result<Value> {
        if schema_has_timezone {
            // Schema has timezone -> MUST convert to UTC (Parquet limitation)
            // The original timezone offset is lost - only UTC is stored
            time_value
                .funcall("utc", ())
                .map_err(|e| ParquetError::Conversion(format!("Failed to convert to UTC: {}", e)))
        } else {
            // Schema has no timezone -> keep as local/unzoned time
            // This represents a "wall clock" time without timezone information
            Ok(time_value)
        }
    }

    /// Extract timezone information from a Ruby Time object
    fn extract_timezone(&self, time_value: Value) -> Result<Option<std::sync::Arc<str>>> {
        let _ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;

        // Check if the time is in UTC
        let is_utc: bool = time_value
            .funcall("utc?", ())
            .map_err(|e| ParquetError::Conversion(format!("Failed to check UTC: {}", e)))?;

        if is_utc {
            return Ok(Some("UTC".into()));
        }

        // Get the UTC offset in seconds
        let utc_offset: i32 = time_value
            .funcall("utc_offset", ())
            .map_err(|e| ParquetError::Conversion(format!("Failed to get UTC offset: {}", e)))?;

        // If offset is 0 and not explicitly UTC, it might be local time
        if utc_offset == 0 {
            // Check if this is actually UTC or just happens to have 0 offset
            // We already checked utc? above, so this is local time with 0 offset
            return Ok(None);
        }

        // Convert offset to hours and minutes
        let hours = utc_offset / 3600;
        let minutes = (utc_offset.abs() % 3600) / 60;

        // Format as +HH:MM or -HH:MM
        let tz_string = if minutes == 0 {
            format!("{:+03}:00", hours)
        } else {
            format!("{:+03}:{:02}", hours, minutes)
        };

        Ok(Some(tz_string.into()))
    }

    // Conversion methods for specific types

    fn convert_to_boolean(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let b: bool = TryConvert::try_convert(value)
            .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
        Ok(ParquetValue::Boolean(b))
    }

    fn convert_to_int8(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<i8>(value)?;
        Ok(ParquetValue::Int8(i))
    }

    fn convert_to_int16(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<i16>(value)?;
        Ok(ParquetValue::Int16(i))
    }

    fn convert_to_int32(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<i32>(value)?;
        Ok(ParquetValue::Int32(i))
    }

    fn convert_to_int64(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<i64>(value)?;
        Ok(ParquetValue::Int64(i))
    }

    fn convert_to_uint8(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<u8>(value)?;
        Ok(ParquetValue::UInt8(i))
    }

    fn convert_to_uint16(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<u16>(value)?;
        Ok(ParquetValue::UInt16(i))
    }

    fn convert_to_uint32(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<u32>(value)?;
        Ok(ParquetValue::UInt32(i))
    }

    fn convert_to_uint64(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let i = self.convert_numeric::<u64>(value)?;
        Ok(ParquetValue::UInt64(i))
    }

    fn convert_to_float32(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let f = self.convert_numeric::<f32>(value)?;
        Ok(ParquetValue::Float32(OrderedFloat(f)))
    }

    fn convert_to_float64(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let f = self.convert_numeric::<f64>(value)?;
        Ok(ParquetValue::Float64(OrderedFloat(f)))
    }

    fn convert_to_string(&mut self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Convert any value to string using to_s
        let s: String = value
            .funcall("to_s", ())
            .map_err(|e| ParquetError::Conversion(e.to_string()))?;

        // Use string cache if available for statistics tracking
        // Note: Currently doesn't provide memory savings due to ParquetValue storing String
        if let Some(ref mut cache) = self.string_cache.borrow_mut().as_mut() {
            let interned = cache.intern(s);
            Ok(ParquetValue::String(interned))
        } else {
            Ok(ParquetValue::String(s.into()))
        }
    }

    fn convert_to_binary(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_string()) {
            let s: RString = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            let bytes = unsafe { Bytes::copy_from_slice(s.as_slice()) };
            Ok(ParquetValue::Bytes(bytes))
        } else {
            // Try to convert to string first
            let s: String = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            Ok(ParquetValue::Bytes(s.into()))
        }
    }

    fn convert_to_uuid_binary(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Convert value to string
        let uuid_str: String = value
            .to_r_string()
            .map_err(|e: MagnusError| {
                ParquetError::Conversion(format!("Failed to convert to UUID string: {}", e))
            })?
            .to_string()
            .map_err(|e: MagnusError| {
                ParquetError::Conversion(format!("Failed to convert to UUID string: {}", e))
            })?;

        let parsed = uuid::Uuid::parse_str(&uuid_str)
            .map_err(|e| ParquetError::Conversion(format!("Failed to parse UUID: {}", e)))?;
        let bytes = Bytes::copy_from_slice(parsed.as_bytes());
        Ok(ParquetValue::Bytes(bytes))
    }

    fn convert_to_date32(&self, value: Value, date_format: Option<&str>) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Handle Time objects
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            let secs: i64 = value
                .funcall("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let days = (secs / 86400) as i32;
            return Ok(ParquetValue::Date32(days));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Date module
            let _ = ruby.require("date");
            let kernel = ruby.module_kernel();
            let date_module = kernel
                .const_get::<_, Value>("Date")
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            // Use strptime if format is provided, otherwise use parse
            let date = if let Some(format) = date_format {
                date_module
                    .funcall::<_, _, Value>("strptime", (value, format))
                    .map_err(|e| {
                        ParquetError::Conversion(format!(
                            "Failed to parse date with format '{}': {}",
                            format, e
                        ))
                    })?
            } else {
                date_module
                    .funcall::<_, _, Value>("parse", (value,))
                    .map_err(|e| ParquetError::Conversion(format!("Failed to parse date: {}", e)))?
            };

            // Convert to Time object then to days since epoch
            let time = date
                .funcall::<_, _, Value>("to_time", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let secs: i64 = time
                .funcall("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let days = (secs / 86400) as i32;
            return Ok(ParquetValue::Date32(days));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to date32",
            value.class()
        )))
    }

    fn convert_to_date64(&self, value: Value, date_format: Option<&str>) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Similar to date32 but returns milliseconds since epoch
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            let millis: i64 = value
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1000;
            return Ok(ParquetValue::Date64(millis));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Date module
            let _ = ruby.require("date");
            let kernel = ruby.module_kernel();
            let date_module = kernel
                .const_get::<_, Value>("Date")
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            // Use strptime if format is provided, otherwise use parse
            let date = if let Some(format) = date_format {
                date_module
                    .funcall::<_, _, Value>("strptime", (value, format))
                    .map_err(|e| {
                        ParquetError::Conversion(format!(
                            "Failed to parse date with format '{}': {}",
                            format, e
                        ))
                    })?
            } else {
                date_module
                    .funcall::<_, _, Value>("parse", (value,))
                    .map_err(|e| ParquetError::Conversion(format!("Failed to parse date: {}", e)))?
            };

            // Convert to Time object then to milliseconds since epoch
            let time = date
                .funcall::<_, _, Value>("to_time", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let secs: i64 = time
                .funcall("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let millis = secs * 1000;
            return Ok(ParquetValue::Date64(millis));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to date64",
            value.class()
        )))
    }

    fn convert_to_time_millis(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Convert to milliseconds since midnight
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            let hour: i32 = value
                .funcall("hour", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let min: i32 = value
                .funcall("min", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let sec: i32 = value
                .funcall("sec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let nsec: i32 = value
                .funcall("nsec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            let millis = (hour * 3600 + min * 60 + sec) * 1000 + nsec / 1_000_000;
            return Ok(ParquetValue::TimeMillis(millis));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to time_millis",
            value.class()
        )))
    }

    fn convert_to_time_micros(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Convert to microseconds since midnight
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            let hour: i64 = value
                .funcall("hour", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let min: i64 = value
                .funcall("min", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let sec: i64 = value
                .funcall("sec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let nsec: i64 = value
                .funcall("nsec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            let micros = (hour * 3600 + min * 60 + sec) * 1_000_000 + nsec / 1000;
            return Ok(ParquetValue::TimeMicros(micros));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to time_micros",
            value.class()
        )))
    }

    fn convert_to_time_nanos(&self, value: Value) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // Convert to microseconds since midnight
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            let hour: i64 = value
                .funcall("hour", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let min: i64 = value
                .funcall("min", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let sec: i64 = value
                .funcall("sec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let nsec: i64 = value
                .funcall("nsec", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            let nanos = (hour * 3600 + min * 60 + sec) * 1_000_000_000 + nsec;
            return Ok(ParquetValue::TimeNanos(nanos));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to time_micros",
            value.class()
        )))
    }

    // Timestamp conversion methods that respect schema timezone
    fn convert_to_timestamp_second_with_tz(
        &self,
        value: Value,
        schema_tz: Option<&str>,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(value, schema_tz.is_some())?;

            let secs: i64 = adjusted_time
                .funcall("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampSecond(secs, tz));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Time.parse to handle timestamp strings
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("parse", (value,))
                .map_err(|e| {
                    ParquetError::Conversion(format!("Failed to parse timestamp: {}", e))
                })?;

            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(time, schema_tz.is_some())?;

            let secs: i64 = adjusted_time
                .funcall("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampSecond(secs, tz));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to timestamp_second",
            value.class()
        )))
    }

    fn convert_to_timestamp_millis_with_tz(
        &self,
        value: Value,
        schema_tz: Option<&str>,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(value, schema_tz.is_some())?;

            let millis = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64
                    / 1_000_000;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampMillis(millis, tz));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Time.parse to handle timestamp strings
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("parse", (value,))
                .map_err(|e| {
                    ParquetError::Conversion(format!("Failed to parse timestamp: {}", e))
                })?;

            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(time, schema_tz.is_some())?;

            let millis = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64
                    / 1_000_000;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampMillis(millis, tz));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to timestamp_millis",
            value.class()
        )))
    }

    fn convert_to_timestamp_micros_with_tz(
        &self,
        value: Value,
        schema_tz: Option<&str>,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(value, schema_tz.is_some())?;

            let micros = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1_000_000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64
                    / 1000;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampMicros(micros, tz));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Time.parse to handle timestamp strings
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("parse", (value,))
                .map_err(|e| {
                    ParquetError::Conversion(format!("Failed to parse timestamp: {}", e))
                })?;

            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(time, schema_tz.is_some())?;

            let micros = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1_000_000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64
                    / 1000;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampMicros(micros, tz));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to timestamp_micros",
            value.class()
        )))
    }

    fn convert_to_timestamp_nanos_with_tz(
        &self,
        value: Value,
        schema_tz: Option<&str>,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_time()) {
            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(value, schema_tz.is_some())?;

            let nanos = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1_000_000_000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampNanos(nanos, tz));
        }

        // Handle strings
        if value.is_kind_of(ruby.class_string()) {
            // Use Ruby's Time.parse to handle timestamp strings
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("parse", (value,))
                .map_err(|e| {
                    ParquetError::Conversion(format!("Failed to parse timestamp: {}", e))
                })?;

            // Normalize timestamp according to Parquet spec
            let adjusted_time = self.normalize_timestamp_for_parquet(time, schema_tz.is_some())?;

            let nanos = adjusted_time
                .funcall::<_, _, i64>("to_i", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                * 1_000_000_000
                + adjusted_time
                    .funcall::<_, _, i32>("nsec", ())
                    .map_err(|e| ParquetError::Conversion(e.to_string()))? as i64;

            // PARQUET TIMESTAMP STORAGE:
            // - Schema WITH timezone -> Store as UTC (isAdjustedToUTC = true)
            // - Schema WITHOUT timezone -> Store as unzoned (isAdjustedToUTC = false)
            // NOTE: Original timezone like "+09:00" is converted to "UTC" for storage
            let tz = if schema_tz.is_some() {
                Some(Arc::from("UTC")) // Always UTC, never the original timezone
            } else {
                None // Unzoned/local timestamp
            };

            return Ok(ParquetValue::TimestampNanos(nanos, tz));
        }

        Err(ParquetError::Conversion(format!(
            "Cannot convert {} to timestamp_nanos",
            value.class()
        )))
    }

    fn convert_to_decimal128(
        &self,
        value: Value,
        precision: u8,
        scale: i8,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // For BigDecimal, use to_s("F") to get non-scientific notation
        let str_val: String = if value.class().to_string() == "BigDecimal" {
            value
                .funcall("to_s", ("F",))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
        } else {
            value
                .funcall("to_s", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
        };

        self.parse_decimal128(&str_val, precision, scale)
    }

    fn convert_to_decimal256(
        &self,
        value: Value,
        precision: u8,
        scale: i8,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        // For BigDecimal, use to_s("F") to get non-scientific notation
        let str_val: String = if value.class().to_string() == "BigDecimal" {
            value
                .funcall("to_s", ("F",))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
        } else {
            value
                .funcall("to_s", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
        };

        self.parse_decimal256(&str_val, precision, scale)
    }

    fn convert_to_fixed_len_byte_array(&self, value: Value, len: i32) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        let bytes = if value.is_kind_of(ruby.class_string()) {
            let s: RString = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            unsafe { s.as_slice() }.to_vec()
        } else {
            let s: String = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            s.into_bytes()
        };

        if bytes.len() != len as usize {
            return Err(ParquetError::Conversion(format!(
                "Expected {} bytes, got {}",
                len,
                bytes.len()
            )));
        }

        Ok(ParquetValue::Bytes(bytes.into()))
    }

    // Helper methods

    fn convert_numeric<T>(&self, value: Value) -> Result<T>
    where
        T: TryConvert + std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Display,
    {
        // Try direct conversion first
        if let Ok(val) = TryConvert::try_convert(value) {
            return Ok(val);
        }

        // If that fails, try converting to i64/f64 first, then to target type
        let ruby = Ruby::get()
            .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;
        if value.is_kind_of(ruby.class_integer()) {
            // Convert Integer to i64 first, then to target type
            let i: i64 = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            i.to_string().parse::<T>().map_err(|e| {
                ParquetError::Conversion(format!("Failed to convert {} to target type: {}", i, e))
            })
        } else if value.is_kind_of(ruby.class_float()) {
            // Convert Float to f64 first, then to target type
            let f: f64 = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            f.to_string().parse::<T>().map_err(|e| {
                ParquetError::Conversion(format!("Failed to convert {} to target type: {}", f, e))
            })
        } else if value.is_kind_of(ruby.class_string()) {
            let s: String = TryConvert::try_convert(value)
                .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;
            s.trim().parse::<T>().map_err(|e| {
                ParquetError::Conversion(format!("Failed to parse '{}' as numeric: {}", s, e))
            })
        } else {
            Err(ParquetError::Conversion(format!(
                "Cannot convert {} to numeric",
                value.class()
            )))
        }
    }

    fn parse_decimal128(&self, s: &str, _precision: u8, scale: i8) -> Result<ParquetValue> {
        // Parse decimal string to i128
        let clean = s.trim();

        // Handle scientific notation by converting to regular decimal format
        let normalized = if clean.to_lowercase().contains('e') {
            // Parse as f64 first to handle scientific notation
            let f: f64 = clean.parse().map_err(|e| {
                ParquetError::Conversion(format!("Failed to parse scientific notation: {}", e))
            })?;
            // Convert to string with enough precision
            format!("{:.15}", f)
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            clean.to_string()
        };

        let is_negative = normalized.starts_with('-');
        let clean_abs = normalized.trim_start_matches('-').trim_start_matches('+');

        let parts: Vec<&str> = clean_abs.split('.').collect();

        if parts.len() > 2 {
            return Err(ParquetError::Conversion(
                "Invalid decimal format".to_string(),
            ));
        }

        let integer_part = if parts.is_empty() || parts[0].is_empty() {
            "0"
        } else {
            parts[0]
        };
        let fractional_part = if parts.len() == 2 { parts[1] } else { "" };

        // Calculate the actual value considering the scale
        let current_scale = fractional_part.len() as i8;

        if scale < 0 {
            return Err(ParquetError::Conversion(
                "Negative scale not supported".to_string(),
            ));
        }

        // Parse integer and fractional parts
        let integer_value: i128 = integer_part.parse().map_err(|e| {
            ParquetError::Conversion(format!("Failed to parse integer part: {}", e))
        })?;

        let fractional_value: i128 = if fractional_part.is_empty() {
            0
        } else {
            fractional_part.parse().map_err(|e| {
                ParquetError::Conversion(format!("Failed to parse fractional part: {}", e))
            })?
        };

        // Calculate the final value based on scale
        let scale_factor = 10_i128.pow(scale as u32);
        let current_scale_factor = 10_i128.pow(current_scale as u32);

        let mut value = if current_scale <= scale {
            // Current scale is less than or equal to target scale - pad with zeros
            integer_value * scale_factor + fractional_value * (scale_factor / current_scale_factor)
        } else {
            // Current scale is greater than target scale - need to truncate/round
            let adjustment_factor = 10_i128.pow((current_scale - scale) as u32);
            let adjusted_fractional = fractional_value / adjustment_factor;
            integer_value * scale_factor + adjusted_fractional
        };

        if is_negative {
            value = -value;
        }

        Ok(ParquetValue::Decimal128(value, scale))
    }

    fn parse_decimal256(&self, s: &str, _precision: u8, scale: i8) -> Result<ParquetValue> {
        // Parse decimal string to BigInt
        use num::{BigInt, Zero};

        let clean = s.trim();

        // Handle scientific notation by converting to regular decimal format
        let normalized = if clean.to_lowercase().contains('e') {
            // Parse as f64 first to handle scientific notation
            let f: f64 = clean.parse().map_err(|e| {
                ParquetError::Conversion(format!("Failed to parse scientific notation: {}", e))
            })?;
            // Convert to string with enough precision
            format!("{:.15}", f)
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            clean.to_string()
        };

        let is_negative = normalized.starts_with('-');
        let clean_abs = normalized.trim_start_matches('-').trim_start_matches('+');

        let parts: Vec<&str> = clean_abs.split('.').collect();

        if parts.len() > 2 {
            return Err(ParquetError::Conversion(
                "Invalid decimal format".to_string(),
            ));
        }

        let integer_part = if parts.is_empty() || parts[0].is_empty() {
            "0"
        } else {
            parts[0]
        };
        let fractional_part = if parts.len() == 2 { parts[1] } else { "" };

        // Calculate the actual value considering the scale
        let current_scale = fractional_part.len() as i8;

        if scale < 0 {
            return Err(ParquetError::Conversion(
                "Negative scale not supported".to_string(),
            ));
        }

        // Parse integer and fractional parts
        let integer_value: BigInt = integer_part.parse().map_err(|e| {
            ParquetError::Conversion(format!("Failed to parse integer part: {}", e))
        })?;

        let fractional_value: BigInt = if fractional_part.is_empty() {
            BigInt::zero()
        } else {
            fractional_part.parse().map_err(|e| {
                ParquetError::Conversion(format!("Failed to parse fractional part: {}", e))
            })?
        };

        // Calculate the final value based on scale
        let scale_factor = BigInt::from(10).pow(scale as u32);
        let current_scale_factor = BigInt::from(10).pow(current_scale as u32);

        let mut value = if current_scale <= scale {
            // Current scale is less than or equal to target scale - pad with zeros
            integer_value * &scale_factor + fractional_value * (scale_factor / current_scale_factor)
        } else {
            // Current scale is greater than target scale - need to truncate/round
            let adjustment_factor = BigInt::from(10).pow((current_scale - scale) as u32);
            let adjusted_fractional = fractional_value / adjustment_factor;
            integer_value * &scale_factor + adjusted_fractional
        };

        if is_negative {
            value = -value;
        }

        Ok(ParquetValue::Decimal256(value, scale))
    }

    /// Convert a Ruby array to a ParquetValue::List
    fn convert_to_list(
        &mut self,
        value: Value,
        item_schema: &parquet_core::SchemaNode,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let array: RArray = TryConvert::try_convert(value).map_err(|e: MagnusError| {
            ParquetError::Conversion(format!("Expected Array for List type: {}", e))
        })?;

        let mut list = Vec::with_capacity(array.len());
        for item in array.into_iter() {
            list.push(self.convert_with_schema_hint(item, item_schema)?);
        }

        Ok(ParquetValue::List(list))
    }

    /// Convert a Ruby hash to a ParquetValue::Map
    fn convert_to_map(
        &mut self,
        value: Value,
        key_schema: &parquet_core::SchemaNode,
        value_schema: &parquet_core::SchemaNode,
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let hash: RHash = TryConvert::try_convert(value).map_err(|e: MagnusError| {
            ParquetError::Conversion(format!("Expected Hash for Map type: {}", e))
        })?;

        // Collect key-value pairs first
        let mut kv_pairs = Vec::new();
        hash.foreach(|k: Value, v: Value| {
            kv_pairs.push((k, v));
            Ok(ForEach::Continue)
        })
        .map_err(|e: MagnusError| ParquetError::Conversion(e.to_string()))?;

        // Now convert them with mutable self
        let mut map = Vec::new();
        for (k, v) in kv_pairs {
            let key = self.convert_with_schema_hint(k, key_schema)?;
            let val = self.convert_with_schema_hint(v, value_schema)?;
            map.push((key, val));
        }

        Ok(ParquetValue::Map(map))
    }

    /// Convert a Ruby hash to a ParquetValue::Record (struct)
    fn convert_to_struct(
        &mut self,
        value: Value,
        fields: &[parquet_core::SchemaNode],
    ) -> Result<ParquetValue> {
        if value.is_nil() {
            return Ok(ParquetValue::Null);
        }

        let hash: RHash = TryConvert::try_convert(value).map_err(|e: MagnusError| {
            ParquetError::Conversion(format!("Expected Hash for Struct type: {}", e))
        })?;

        let mut record = IndexMap::new();

        for field in fields {
            let field_name = field.name();
            let ruby_key = Symbol::new(field_name);

            // Try symbol key first, then string key
            let field_value = if let Some(val) = hash.get(ruby_key) {
                val
            } else if let Some(val) = hash.get(field_name) {
                val
            } else {
                // Field not found, use null
                Ruby::get()
                    .map_err(|_| {
                        ParquetError::Conversion("Failed to get Ruby runtime".to_string())
                    })?
                    .qnil()
                    .as_value()
            };

            let converted = self.convert_with_schema_hint(field_value, field)?;
            record.insert(field_name.into(), converted);
        }

        Ok(ParquetValue::Record(record))
    }
}

// Helper functions for one-off conversions where we don't need string caching

pub fn ruby_to_parquet(value: Value) -> Result<ParquetValue> {
    let mut converter = RubyValueConverter::new();
    converter.infer_and_convert(value)
}

pub fn parquet_to_ruby(value: ParquetValue) -> Result<Value> {
    let ruby = Ruby::get()
        .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;

    match value {
        ParquetValue::Null => Ok(ruby.qnil().as_value()),
        ParquetValue::Boolean(b) => Ok(b.into_value_with(&ruby)),
        ParquetValue::Int8(i) => Ok((i as i64).into_value_with(&ruby)),
        ParquetValue::Int16(i) => Ok((i as i64).into_value_with(&ruby)),
        ParquetValue::Int32(i) => Ok((i as i64).into_value_with(&ruby)),
        ParquetValue::Int64(i) => Ok(i.into_value_with(&ruby)),
        ParquetValue::UInt8(i) => Ok((i as u64).into_value_with(&ruby)),
        ParquetValue::UInt16(i) => Ok((i as u64).into_value_with(&ruby)),
        ParquetValue::UInt32(i) => Ok((i as u64).into_value_with(&ruby)),
        ParquetValue::UInt64(i) => Ok(i.into_value_with(&ruby)),
        ParquetValue::Float16(OrderedFloat(f)) => Ok((f as f64).into_value_with(&ruby)),
        ParquetValue::Float32(OrderedFloat(f)) => Ok((f as f64).into_value_with(&ruby)),
        ParquetValue::Float64(OrderedFloat(f)) => Ok(f.into_value_with(&ruby)),
        ParquetValue::String(s) => Ok(s.into_value_with(&ruby)),
        ParquetValue::Uuid(u) => Ok(u.to_string().into_value_with(&ruby)),
        ParquetValue::Bytes(b) => Ok(ruby.enc_str_new(&b, ruby.ascii8bit_encoding()).as_value()),
        ParquetValue::Date32(days) => {
            // Convert days since epoch to Date object
            let _ = ruby.require("date");
            let kernel = ruby.module_kernel();
            let date_class = kernel
                .const_get::<_, Value>("Date")
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let secs = days as i64 * 86400;
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("at", (secs,))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?
                .funcall::<_, _, Value>("utc", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let year: i32 = time
                .funcall("year", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let month: i32 = time
                .funcall("month", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let day: i32 = time
                .funcall("day", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            date_class
                .funcall("new", (year, month, day))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::Date64(millis) => {
            // Convert millis to Time object
            let time_class = ruby.class_time();
            let secs = millis / 1000;
            let nsec = (millis % 1000) * 1_000_000;
            time_class
                .funcall("at", (secs, nsec))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::TimeMillis(millis) => {
            // Convert to Time object for today with given time
            let time_class = ruby.class_time();
            let hours = millis / (3600 * 1000);
            let minutes = (millis % (3600 * 1000)) / (60 * 1000);
            let seconds = (millis % (60 * 1000)) / 1000;
            let ms = millis % 1000;

            let now: Value = time_class
                .funcall("now", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let year: i32 = now
                .funcall("year", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let month: i32 = now
                .funcall("month", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let day: i32 = now
                .funcall("day", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            time_class
                .funcall(
                    "utc",
                    (year, month, day, hours, minutes, seconds, ms * 1000),
                )
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::TimeMicros(micros) => {
            // Similar to TimeMillis but with microsecond precision
            let time_class = ruby.class_time();
            let hours = micros / (3600 * 1_000_000);
            let minutes = (micros % (3600 * 1_000_000)) / (60 * 1_000_000);
            let seconds = (micros % (60 * 1_000_000)) / 1_000_000;
            let us = micros % 1_000_000;

            let now: Value = time_class
                .funcall("now", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let year: i32 = now
                .funcall("year", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let month: i32 = now
                .funcall("month", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            let day: i32 = now
                .funcall("day", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;

            time_class
                .funcall("utc", (year, month, day, hours, minutes, seconds, us))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::TimestampSecond(secs, tz) => {
            let time_class = ruby.class_time();
            let time = time_class
                .funcall::<_, _, Value>("at", (secs,))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            apply_timezone(time, &tz)
        }
        ParquetValue::TimestampMillis(millis, tz) => {
            let time_class = ruby.class_time();
            let secs = millis / 1000;
            let usec = (millis % 1000) * 1000; // Convert millisecond remainder to microseconds
            let time = time_class
                .funcall::<_, _, Value>("at", (secs, usec))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            apply_timezone(time, &tz)
        }
        ParquetValue::TimestampMicros(micros, tz) => {
            let time_class = ruby.class_time();
            let secs = micros / 1_000_000;
            let usec = micros % 1_000_000; // Already in microseconds
            let time = time_class
                .funcall::<_, _, Value>("at", (secs, usec))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            apply_timezone(time, &tz)
        }
        ParquetValue::TimeNanos(nanos) => {
            let time_class = ruby.class_time();
            let secs = nanos / 1_000_000_000;
            let nsec = nanos % 1_000_000_000;
            time_class
                .funcall("at", (secs, nsec, Symbol::new("nanosecond")))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::TimestampNanos(nanos, tz) => {
            let time_class = ruby.class_time();
            let secs = nanos / 1_000_000_000;
            let nsec = nanos % 1_000_000_000;
            // Use the nanosecond form of Time.at
            let time = time_class
                .funcall::<_, _, Value>("at", (secs, nsec, Symbol::new("nanosecond")))
                .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            apply_timezone(time, &tz)
        }
        ParquetValue::Decimal128(val, scale) => {
            // Load BigDecimal if needed
            let _ = ruby.require("bigdecimal");

            // Format decimal with scale
            let str_val = format_decimal128(val, scale);
            let kernel = ruby.module_kernel();
            kernel
                .funcall("BigDecimal", (str_val,))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::Decimal256(val, scale) => {
            // Load BigDecimal if needed
            let _ = ruby.require("bigdecimal");

            // Format decimal with scale
            let str_val = format_decimal256(&val, scale);
            let kernel = ruby.module_kernel();
            kernel
                .funcall("BigDecimal", (str_val,))
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        ParquetValue::List(list) => {
            let array = ruby.ary_new_capa(list.len());
            for item in list {
                let ruby_val = parquet_to_ruby(item)?;
                array
                    .push(ruby_val)
                    .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            }
            Ok(array.as_value())
        }
        ParquetValue::Map(map) => {
            let hash = ruby.hash_new();
            for (k, v) in map {
                let ruby_key = parquet_to_ruby(k)?;
                let ruby_val = parquet_to_ruby(v)?;
                hash.aset(ruby_key, ruby_val)
                    .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            }
            Ok(hash.as_value())
        }
        ParquetValue::Record(record) => {
            // Convert Record to Ruby Hash
            let hash = ruby.hash_new();
            for (field_name, field_value) in record {
                let ruby_key = ruby.str_new(&field_name);
                let ruby_val = parquet_to_ruby(field_value)?;
                hash.aset(ruby_key, ruby_val)
                    .map_err(|e| ParquetError::Conversion(e.to_string()))?;
            }
            Ok(hash.as_value())
        }
    }
}

// Helper functions for decimal formatting

fn format_decimal128(value: i128, scale: i8) -> String {
    if scale == 0 {
        return value.to_string();
    }

    let abs_value = value.abs();
    let sign = if value < 0 { "-" } else { "" };

    if scale > 0 {
        let divisor = 10_i128.pow(scale as u32);
        let integer_part = abs_value / divisor;
        let fractional_part = abs_value % divisor;
        format!(
            "{}{}.{:0>width$}",
            sign,
            integer_part,
            fractional_part,
            width = scale as usize
        )
    } else {
        // Negative scale means multiply by 10^(-scale)
        let multiplier = 10_i128.pow((-scale) as u32);
        format!("{}{}", sign, abs_value * multiplier)
    }
}

fn format_decimal256(value: &num::BigInt, scale: i8) -> String {
    use num::{BigInt, Signed};

    if scale == 0 {
        return value.to_string();
    }

    let abs_value = value.abs();
    let sign = if value.is_negative() { "-" } else { "" };

    if scale > 0 {
        let ten = BigInt::from(10);
        let divisor = ten.pow(scale as u32);
        let integer_part = &abs_value / &divisor;
        let fractional_part = &abs_value % &divisor;

        // Format fractional part with leading zeros
        let frac_str = fractional_part.to_string();
        let padding = scale as usize - frac_str.len();
        let zeros = "0".repeat(padding);

        format!("{}{}.{}{}", sign, integer_part, zeros, frac_str)
    } else {
        // Negative scale means multiply by 10^(-scale)
        let ten = BigInt::from(10);
        let multiplier = ten.pow((-scale) as u32);
        format!("{}{}", sign, abs_value * multiplier)
    }
}

/// Apply timezone when reading timestamp from Parquet file
///
/// PARQUET SPEC COMPLIANCE:
/// - If schema has ANY timezone -> values are UTC (isAdjustedToUTC = true)
/// - If schema has NO timezone -> values are local/unzoned (isAdjustedToUTC = false)
///
/// NOTE: The actual timezone string in the schema is irrelevant for reading.
/// Whether it's "UTC", "+09:00", or "America/New_York", the stored values
/// are ALWAYS UTC-normalized. We return them as UTC Time objects.
fn apply_timezone(time: Value, tz: &Option<std::sync::Arc<str>>) -> Result<Value> {
    let _ruby = Ruby::get()
        .map_err(|_| ParquetError::Conversion("Failed to get Ruby runtime".to_string()))?;

    match tz {
        Some(_) => {
            // ANY timezone = UTC storage (Parquet spec requirement)
            // Original timezone like "+09:00" is NOT preserved
            time.funcall("utc", ())
                .map_err(|e| ParquetError::Conversion(e.to_string()))
        }
        None => {
            // No timezone = local/unzoned timestamp
            // This is a "wall clock" time without timezone context
            Ok(time)
        }
    }
}

// Note: These wrapper functions are needed because ValueConverter is not thread-safe
// due to Ruby's GIL requirements. They are called from Ruby FFI functions where we know
// we're in the correct thread context.
