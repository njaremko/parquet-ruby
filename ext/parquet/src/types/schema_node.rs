use std::sync::Arc;

use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema,
};
use magnus::{Error as MagnusError, RArray, RHash, Ruby, Symbol, TryConvert, Value};

use crate::logger::RubyLogger;
use crate::types::{PrimitiveType, SchemaNode};
use crate::utils::parse_string_or_symbol;

/// Builds an Arrow schema from a SchemaNode tree - placeholder declaration
/// The actual implementation appears later in the file
fn _build_arrow_schema_placeholder() {}

/// Helper to extract common fields from a schema node hash
fn extract_common_fields(
    ruby: &Ruby,
    node_hash: &RHash,
) -> Result<(String, bool, Option<String>), MagnusError> {
    // extract `name:` if present, else default
    let name_val = node_hash.get(Symbol::new("name"));
    let name: String = if let Some(v) = name_val {
        let name_option = parse_string_or_symbol(ruby, v)?;
        name_option.unwrap_or_else(|| "".to_string())
    } else {
        "".to_string() // top-level might omit name
    };

    // extract `nullable:`
    let nullable_val = node_hash.get(Symbol::new("nullable"));
    let nullable: bool = if let Some(v) = nullable_val {
        bool::try_convert(v).unwrap_or(true)
    } else {
        true // default to nullable
    };

    // optional `format:`
    let format_val = node_hash.get(Symbol::new("format"));
    let format: Option<String> = if let Some(v) = format_val {
        parse_string_or_symbol(ruby, v)?
    } else {
        None
    };

    Ok((name, nullable, format))
}

/// Parse a struct schema node
fn parse_struct_node(
    ruby: &Ruby,
    node_hash: &RHash,
    name: String,
    nullable: bool,
) -> Result<SchemaNode, MagnusError> {
    // parse subfields array from `fields`
    let fields_val = node_hash.get(Symbol::new("fields")).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "Struct must have :fields array defined",
        )
    })?;
    let fields_arr: RArray = RArray::try_convert(fields_val).map_err(|_| {
        MagnusError::new(
            ruby.exception_type_error(),
            "The :fields value must be an array",
        )
    })?;

    // Check for empty struct immediately
    if fields_arr.is_empty() {
        return Err(MagnusError::new(
            ruby.exception_arg_error(),
            format!("Cannot create a struct with zero fields. Struct name: '{}'. Parquet doesn't support empty structs", name)
        ));
    }

    let mut fields = Vec::with_capacity(fields_arr.len());
    for item in fields_arr.into_iter() {
        fields.push(parse_schema_node(ruby, item)?);
    }

    Ok(SchemaNode::Struct {
        name,
        nullable,
        fields,
    })
}

/// Parse a list schema node
fn parse_list_node(
    ruby: &Ruby,
    node_hash: &RHash,
    name: String,
    nullable: bool,
) -> Result<SchemaNode, MagnusError> {
    // parse `item`
    let item_val = node_hash.get(Symbol::new("item")).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "List type must have :item field defined",
        )
    })?;
    let item_node = parse_schema_node(ruby, item_val)?;

    Ok(SchemaNode::List {
        name,
        nullable,
        item: Box::new(item_node),
    })
}

/// Parse a map schema node
fn parse_map_node(
    ruby: &Ruby,
    node_hash: &RHash,
    name: String,
    nullable: bool,
) -> Result<SchemaNode, MagnusError> {
    // parse `key` and `value`
    let key_val = node_hash.get(Symbol::new("key")).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "Map type must have :key field defined",
        )
    })?;
    let value_val = node_hash.get(Symbol::new("value")).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "Map type must have :value field defined",
        )
    })?;

    let key_node = parse_schema_node(ruby, key_val)?;
    let value_node = parse_schema_node(ruby, value_val)?;

    Ok(SchemaNode::Map {
        name,
        nullable,
        key: Box::new(key_node),
        value: Box::new(value_node),
    })
}

/// Parse a Ruby schema hash into a SchemaNode tree
pub fn parse_schema_node(ruby: &Ruby, node_value: Value) -> Result<SchemaNode, MagnusError> {
    // The node_value should be a Ruby Hash with keys: :name, :type, :nullable, etc.
    let node_hash = RHash::from_value(node_value).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_type_error(),
            "Schema node must be a Hash with :type and other fields",
        )
    })?;

    // extract `type:` which is a symbol/string
    let type_val = node_hash.get(Symbol::new("type")).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "Missing required :type field in schema node",
        )
    })?;
    let type_str_option = parse_string_or_symbol(ruby, type_val)?;
    let type_str = type_str_option.ok_or_else(|| {
        MagnusError::new(
            ruby.exception_arg_error(),
            "Type cannot be nil - please specify a valid type string or symbol",
        )
    })?;

    // Extract common fields (name, nullable, format)
    let (name, nullable, format) = extract_common_fields(ruby, &node_hash)?;

    // Delegate to type-specific parsers with clear error handling
    match type_str.as_str() {
        "struct" => parse_struct_node(ruby, &node_hash, name, nullable),
        "list" => parse_list_node(ruby, &node_hash, name, nullable),
        "map" => parse_map_node(ruby, &node_hash, name, nullable),
        "decimal" => {
            // Check for precision and scale
            let precision_val = node_hash.get(Symbol::new("precision"));
            let scale_val = node_hash.get(Symbol::new("scale"));

            // Handle different precision/scale combinations:
            // 1. When no precision or scale - use max precision (38)
            // 2. When precision only - use scale 0
            // 3. When scale only - use max precision (38)
            let (precision, scale) = match (precision_val, scale_val) {
                (None, None) => (38, 0), // Maximum accuracy, scale 0
                (Some(p), None) => {
                    // Precision provided, scale defaults to 0
                    let prec = u8::try_convert(p).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid precision value for decimal type, expected a positive integer"
                                .to_string(),
                        )
                    })?;
                    (prec, 0)
                }
                (None, Some(s)) => {
                    // Scale provided, precision set to maximum (38)
                    let scl = i8::try_convert(s).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid scale value for decimal type, expected an integer".to_string(),
                        )
                    })?;
                    (38, scl)
                }
                (Some(p), Some(s)) => {
                    // Both provided
                    let prec = u8::try_convert(p).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid precision value for decimal type, expected a positive integer"
                                .to_string(),
                        )
                    })?;
                    let scl = i8::try_convert(s).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid scale value for decimal type, expected an integer".to_string(),
                        )
                    })?;
                    (prec, scl)
                }
            };

            // Validate precision is in a valid range
            if precision < 1 {
                return Err(MagnusError::new(
                    ruby.exception_arg_error(),
                    format!(
                        "Precision for decimal type must be at least 1, got {}",
                        precision
                    ),
                ));
            }

            if precision > 38 {
                return Err(MagnusError::new(
                    ruby.exception_arg_error(),
                    format!(
                        "Precision for decimal type cannot exceed 38, got {}",
                        precision
                    ),
                ));
            }

            Ok(SchemaNode::Primitive {
                name,
                parquet_type: PrimitiveType::Decimal128(precision, scale),
                nullable,
                format,
            })
        }
        // For primitives, provide better error messages when type isn't recognized
        other => {
            if let Some(parquet_type) = parse_primitive_type(other) {
                Ok(SchemaNode::Primitive {
                    name,
                    parquet_type,
                    nullable,
                    format,
                })
            } else {
                Err(MagnusError::new(
                    magnus::exception::arg_error(),
                    format!(
                        "Unknown type: '{}'. Supported types are: struct, list, map, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, boolean, string, binary, date32, timestamp_millis, timestamp_micros, decimal",
                        other
                    )
                ))
            }
        }
    }
}

/// Convert a type string like "int32" to a PrimitiveType
fn parse_primitive_type(s: &str) -> Option<PrimitiveType> {
    match s.to_lowercase().as_str() {
        "int8" | "i8" => Some(PrimitiveType::Int8),
        "int16" | "i16" => Some(PrimitiveType::Int16),
        "int32" | "i32" | "int" => Some(PrimitiveType::Int32),
        "int64" | "i64" | "long" | "bigint" => Some(PrimitiveType::Int64),
        "uint8" | "u8" | "byte" => Some(PrimitiveType::UInt8),
        "uint16" | "u16" => Some(PrimitiveType::UInt16),
        "uint32" | "u32" | "uint" => Some(PrimitiveType::UInt32),
        "uint64" | "u64" | "ulong" => Some(PrimitiveType::UInt64),
        "float" | "float32" | "f32" => Some(PrimitiveType::Float32),
        "double" | "float64" | "f64" => Some(PrimitiveType::Float64),
        "bool" | "boolean" => Some(PrimitiveType::Boolean),
        "string" | "utf8" | "str" | "text" => Some(PrimitiveType::String),
        "binary" | "bytes" | "blob" => Some(PrimitiveType::Binary),
        "date" | "date32" => Some(PrimitiveType::Date32),
        "timestamp_millis" | "timestamp_ms" => Some(PrimitiveType::TimestampMillis),
        "timestamp_micros" | "timestamp_us" => Some(PrimitiveType::TimestampMicros),
        "decimal" => Some(PrimitiveType::Decimal128(38, 0)), // Maximum precision, scale 0
        "decimal256" => Some(PrimitiveType::Decimal256(38, 0)), // Maximum precision, scale 0
        _ => None,
    }
}

/// Convert a SchemaNode to an Arrow field
pub fn schema_node_to_arrow_field(node: &SchemaNode) -> ArrowField {
    match node {
        SchemaNode::Primitive {
            name,
            parquet_type,
            nullable,
            format: _,
        } => {
            let dt = match parquet_type {
                PrimitiveType::Int8 => ArrowDataType::Int8,
                PrimitiveType::Int16 => ArrowDataType::Int16,
                PrimitiveType::Int32 => ArrowDataType::Int32,
                PrimitiveType::Int64 => ArrowDataType::Int64,
                PrimitiveType::UInt8 => ArrowDataType::UInt8,
                PrimitiveType::UInt16 => ArrowDataType::UInt16,
                PrimitiveType::UInt32 => ArrowDataType::UInt32,
                PrimitiveType::UInt64 => ArrowDataType::UInt64,
                PrimitiveType::Float32 => ArrowDataType::Float32,
                PrimitiveType::Float64 => ArrowDataType::Float64,
                PrimitiveType::Decimal128(precision, scale) => {
                    ArrowDataType::Decimal128(*precision, *scale)
                }
                PrimitiveType::Decimal256(precision, scale) => {
                    ArrowDataType::Decimal256(*precision, *scale)
                }
                PrimitiveType::Boolean => ArrowDataType::Boolean,
                PrimitiveType::String => ArrowDataType::Utf8,
                PrimitiveType::Binary => ArrowDataType::Binary,
                PrimitiveType::Date32 => ArrowDataType::Date32,
                PrimitiveType::TimestampMillis => {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
                }
                PrimitiveType::TimestampMicros => {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
                }
            };
            ArrowField::new(name, dt, *nullable)
        }

        SchemaNode::List {
            name,
            nullable,
            item,
        } => {
            let child_field = schema_node_to_arrow_field(item);
            let list_type = ArrowDataType::List(Arc::new(child_field));
            ArrowField::new(name, list_type, *nullable)
        }

        SchemaNode::Map {
            name,
            nullable,
            key,
            value,
        } => {
            // A Map is basically: Map( Struct([key, value]), keysSorted=false )
            let key_field = schema_node_to_arrow_field(key);
            let value_field = schema_node_to_arrow_field(value);

            let entries_struct = ArrowDataType::Struct(ArrowFields::from(vec![
                ArrowField::new("key", key_field.data_type().clone(), false),
                ArrowField::new(
                    "value",
                    value_field.data_type().clone(),
                    value_field.is_nullable(),
                ),
            ]));

            let map_data_type = ArrowDataType::Map(
                Arc::new(ArrowField::new("entries", entries_struct, false)),
                false, // not sorted
            );
            ArrowField::new(name, map_data_type, *nullable)
        }

        SchemaNode::Struct {
            name,
            nullable,
            fields,
        } => {
            // Field validation happens earlier - no empty structs allowed
            let mut arrow_subfields = Vec::with_capacity(fields.len());
            for f in fields {
                arrow_subfields.push(schema_node_to_arrow_field(f));
            }
            let struct_type = ArrowDataType::Struct(ArrowFields::from(arrow_subfields));
            ArrowField::new(name, struct_type, *nullable)
        }
    }
}

/// Build an Arrow schema from the top-level Node, which must be a Struct
pub fn build_arrow_schema(
    root: &SchemaNode,
    logger: &RubyLogger,
) -> Result<Arc<ArrowSchema>, MagnusError> {
    match root {
        SchemaNode::Struct { fields, .. } => {
            // Fields debug output removed - we've fixed the empty struct issue

            let arrow_fields: Vec<ArrowField> =
                fields.iter().map(schema_node_to_arrow_field).collect();
            let arrow_schema = ArrowSchema::new(arrow_fields);
            logger.debug(|| format!("Constructed Arrow schema: {:?}", arrow_schema))?;
            Ok(Arc::new(arrow_schema))
        }
        _ => Err(MagnusError::new(
            magnus::exception::arg_error(),
            "Top-level schema must be a Struct".to_owned(),
        )),
    }
}
