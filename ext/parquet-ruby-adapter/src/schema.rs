use magnus::value::ReprValue;
use magnus::{Error as MagnusError, RArray, RHash, Ruby, Symbol, TryConvert, Value};
use parquet_core::{ParquetError, PrimitiveType, Schema, SchemaNode};

use crate::utils::parse_string_or_symbol;
use crate::RubyAdapterError;

/// Ruby schema builder that converts Ruby hash/array representations to Parquet schemas
pub struct RubySchemaBuilder;

impl RubySchemaBuilder {
    pub fn new() -> Self {
        Self
    }

    /// Parse a Ruby schema definition (hash) into a SchemaNode
    fn parse_schema_node(
        &self,
        name: String,
        schema_def: Value,
    ) -> Result<SchemaNode, RubyAdapterError> {
        // If it's a Hash, parse it as a complex type
        if let Ok(hash) = <RHash as TryConvert>::try_convert(schema_def) {
            return self.parse_hash_schema_node(name, hash);
        }

        // Otherwise, try to parse as a simple type symbol
        if let Ok(type_str) = schema_def.to_r_string()?.to_string() {
            // Check if it's a complex type with angle brackets
            if type_str.contains('<') {
                return self.parse_complex_type_string(name, type_str.to_string(), true);
            }

            let primitive_type =
                self.parse_primitive_type(type_str.to_string(), None, None, None)?;
            return Ok(SchemaNode::Primitive {
                name,
                primitive_type,
                nullable: true, // Default to nullable for simple types
                format: None,
            });
        }

        Err(RubyAdapterError::InvalidInput(format!(
            "Expected Hash or Symbol for schema definition, got {}",
            schema_def.class()
        )))
    }

    /// Parse a Ruby hash schema node
    fn parse_hash_schema_node(
        &self,
        name: String,
        hash: RHash,
    ) -> Result<SchemaNode, RubyAdapterError> {
        let ruby = Ruby::get().map_err(|e| RubyAdapterError::Ruby(e.to_string()))?;
        // Get the type field
        let type_sym: Value = hash
            .fetch::<_, Value>(ruby.to_symbol("type"))
            .map_err(|e| ParquetError::Schema(format!("Schema missing 'type' field: {}", e)))?;

        let type_str = type_sym.to_r_string()?.to_string()?;

        // Get nullable field (default to true)
        let nullable = hash
            .fetch::<_, Value>(ruby.to_symbol("nullable"))
            .ok()
            .and_then(|v| <bool as TryConvert>::try_convert(v).ok())
            .unwrap_or(true);

        // Get format field if present
        let format = hash
            .fetch::<_, Value>(ruby.to_symbol("format"))
            .ok()
            .and_then(|v| <String as TryConvert>::try_convert(v).ok());

        match type_str.to_string().as_str() {
            "struct" => {
                let fields_array: RArray = hash
                    .fetch(ruby.to_symbol("fields"))
                    .map_err(|e| ParquetError::Schema(format!("Struct missing 'fields': {}", e)))?;

                let mut fields = Vec::new();
                for field_value in fields_array.into_iter() {
                    let field_hash: RHash = <RHash as TryConvert>::try_convert(field_value)
                        .map_err(|e: MagnusError| {
                            ParquetError::Schema(format!("Invalid field definition: {}", e))
                        })?;

                    let _field_name: String =
                        field_hash.fetch(ruby.to_symbol("name")).map_err(|e| {
                            ParquetError::Schema(format!("Field missing 'name': {}", e))
                        })?;

                    let field_node = self.parse_field_definition(field_hash)?;
                    fields.push(field_node);
                }

                Ok(SchemaNode::Struct {
                    name,
                    nullable,
                    fields,
                })
            }

            "list" => {
                let item_def = hash
                    .fetch::<_, Value>(ruby.to_symbol("item"))
                    .map_err(|e| ParquetError::Schema(format!("List missing 'item': {}", e)))?;

                let item_name = format!("{}_item", name);
                let item_node = self.parse_schema_node(item_name, item_def)?;

                Ok(SchemaNode::List {
                    name,
                    nullable,
                    item: Box::new(item_node),
                })
            }

            "map" => {
                // Parse key definition. Parquet requires map keys to be
                // required (non-nullable), so enforce that invariant here
                // regardless of what the key hash specifies. This matches the
                // schema DSL (lib/parquet/schema.rb) and the `map<...>` string
                // form, both of which already build required keys.
                let key_def = hash
                    .fetch::<_, Value>(ruby.to_symbol("key"))
                    .map_err(|e| ParquetError::Schema(format!("Map missing 'key': {}", e)))?;
                let key_node = into_required(self.parse_schema_node("key".to_string(), key_def)?);

                // Parse value definition
                let value_def = hash
                    .fetch::<_, Value>(ruby.to_symbol("value"))
                    .map_err(|e| ParquetError::Schema(format!("Map missing 'value': {}", e)))?;
                let value_node = self.parse_schema_node("value".to_string(), value_def)?;

                Ok(SchemaNode::Map {
                    name,
                    nullable,
                    key: Box::new(key_node),
                    value: Box::new(value_node),
                })
            }

            // Check if it's a complex type with angle brackets
            type_str if type_str.contains('<') => {
                self.parse_complex_type_string(name, type_str.to_string(), nullable)
            }

            // Primitive types
            primitive_type => {
                if format.as_deref() == Some("uuid") {
                    return Ok(SchemaNode::Primitive {
                        name,
                        primitive_type: PrimitiveType::FixedLenByteArray(16),
                        nullable,
                        format,
                    });
                }

                // Get precision and scale for decimal types
                let precision = hash
                    .fetch::<_, Value>(ruby.to_symbol("precision"))
                    .ok()
                    .and_then(|v| <u8 as TryConvert>::try_convert(v).ok());

                let scale = hash
                    .fetch::<_, Value>(ruby.to_symbol("scale"))
                    .ok()
                    .and_then(|v| <i8 as TryConvert>::try_convert(v).ok());

                // Handle timezone for timestamp types
                // Support both new has_timezone (preferred) and legacy timezone parameters
                let timezone =
                    if let Ok(has_tz) = hash.fetch::<_, Value>(ruby.to_symbol("has_timezone")) {
                        // New approach: has_timezone boolean
                        if let Ok(has_timezone) = <bool as TryConvert>::try_convert(has_tz) {
                            if has_timezone {
                                Some("UTC".to_string()) // Presence means UTC storage
                            } else {
                                None // Absence means local/unzoned storage
                            }
                        } else {
                            None
                        }
                    } else {
                        hash.fetch::<_, Value>(ruby.to_symbol("timezone"))
                            .ok()
                            .map(|_| "UTC".to_string()) // Any value -> UTC
                    };

                let primitive = self.parse_primitive_type(
                    primitive_type.to_string(),
                    precision,
                    scale,
                    timezone,
                )?;

                Ok(SchemaNode::Primitive {
                    name,
                    primitive_type: primitive,
                    nullable,
                    format,
                })
            }
        }
    }

    /// Parse a complex type string like "list<string>" or "map<string,int32>"
    fn parse_complex_type_string(
        &self,
        name: String,
        type_str: String,
        nullable: bool,
    ) -> Result<SchemaNode, RubyAdapterError> {
        if type_str.starts_with("list<") && type_str.ends_with('>') {
            let inner_type = &type_str[5..type_str.len() - 1];
            let item_name = format!("{}_item", name);

            // Create a simple type node for the item
            let item_node = if inner_type.contains('<') {
                // Nested complex type
                self.parse_complex_type_string(item_name, inner_type.to_string(), true)?
            } else {
                // Simple primitive type
                SchemaNode::Primitive {
                    name: item_name,
                    primitive_type: self.parse_primitive_type(
                        inner_type.to_string(),
                        None,
                        None,
                        None,
                    )?,
                    nullable: true,
                    format: None,
                }
            };

            Ok(SchemaNode::List {
                name,
                nullable,
                item: Box::new(item_node),
            })
        } else if type_str.starts_with("map<") && type_str.ends_with('>') {
            let inner = &type_str[4..type_str.len() - 1];
            let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
            if parts.len() != 2 {
                return Err(RubyAdapterError::InvalidInput(format!(
                    "Invalid map type: {}",
                    type_str
                )));
            }

            let key_type = self.parse_primitive_type(parts[0].to_string(), None, None, None)?;
            let value_type = self.parse_primitive_type(parts[1].to_string(), None, None, None)?;

            Ok(SchemaNode::Map {
                name,
                nullable,
                key: Box::new(SchemaNode::Primitive {
                    name: "key".to_string(),
                    primitive_type: key_type,
                    nullable: false,
                    format: None,
                }),
                value: Box::new(SchemaNode::Primitive {
                    name: "value".to_string(),
                    primitive_type: value_type,
                    nullable: true,
                    format: None,
                }),
            })
        } else {
            Err(RubyAdapterError::InvalidInput(format!(
                "Unknown complex type: {}",
                type_str
            )))
        }
    }

    /// Parse a field definition from a Ruby hash
    fn parse_field_definition(&self, field_hash: RHash) -> Result<SchemaNode, RubyAdapterError> {
        let ruby = Ruby::get().map_err(|e| RubyAdapterError::Ruby(e.to_string()))?;
        let name: String = field_hash
            .fetch(ruby.to_symbol("name"))
            .map_err(|e| ParquetError::Schema(format!("Field missing 'name': {}", e)))?;

        // Check if there's a 'type' field - if so, parse as full definition
        if let Ok(_type_value) = field_hash.fetch::<_, Value>(ruby.to_symbol("type")) {
            // This is a full field definition
            self.parse_schema_node(name, field_hash.as_value())
        } else {
            // This might be a simplified definition - look for known field patterns
            Err(RubyAdapterError::InvalidInput(format!(
                "Field '{}' missing 'type' definition",
                name
            )))
        }
    }

    /// Parse a primitive type string to PrimitiveType enum
    fn parse_primitive_type(
        &self,
        type_str: String,
        precision: Option<u8>,
        scale: Option<i8>,
        timezone: Option<String>,
    ) -> Result<PrimitiveType, RubyAdapterError> {
        // Check if it's a decimal type with parentheses notation like "decimal(5,2)"
        if type_str.starts_with("decimal(") && type_str.ends_with(')') {
            let params = &type_str[8..type_str.len() - 1]; // Extract "5,2" from "decimal(5,2)"
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let p = parts[0].parse::<u8>().map_err(|_| {
                    ParquetError::Schema(format!("Invalid decimal precision: {}", parts[0]))
                })?;
                let s = parts[1].parse::<i8>().map_err(|_| {
                    ParquetError::Schema(format!("Invalid decimal scale: {}", parts[1]))
                })?;

                // Choose decimal type based on precision
                if p <= 38 {
                    return Ok(PrimitiveType::Decimal128(p, s));
                } else {
                    return Ok(PrimitiveType::Decimal256(p, s));
                }
            }
        }
        // Check for decimal256 with parentheses notation
        if type_str.starts_with("decimal256(") && type_str.ends_with(')') {
            let params = &type_str[11..type_str.len() - 1];
            let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();
            if parts.len() == 2 {
                let p = parts[0].parse::<u8>().map_err(|_| {
                    ParquetError::Schema(format!("Invalid decimal256 precision: {}", parts[0]))
                })?;
                let s = parts[1].parse::<i8>().map_err(|_| {
                    ParquetError::Schema(format!("Invalid decimal256 scale: {}", parts[1]))
                })?;
                return Ok(PrimitiveType::Decimal256(p, s));
            }
        }

        if type_str.starts_with("fixed_len_byte_array(") && type_str.ends_with(')') {
            let params = &type_str[21..type_str.len() - 1];
            let len = params.parse::<i32>().map_err(|_| {
                ParquetError::Schema(format!("Invalid fixed_len_byte_array length: {}", params))
            })?;
            return Ok(PrimitiveType::FixedLenByteArray(len));
        }

        match type_str.as_str() {
            "boolean" | "bool" => Ok(PrimitiveType::Boolean),
            "int8" => Ok(PrimitiveType::Int8),
            "int16" => Ok(PrimitiveType::Int16),
            "int32" => Ok(PrimitiveType::Int32),
            "int64" => Ok(PrimitiveType::Int64),
            "uint8" => Ok(PrimitiveType::UInt8),
            "uint16" => Ok(PrimitiveType::UInt16),
            "uint32" => Ok(PrimitiveType::UInt32),
            "uint64" => Ok(PrimitiveType::UInt64),
            "float" | "float32" => Ok(PrimitiveType::Float32),
            "double" | "float64" => Ok(PrimitiveType::Float64),
            "string" => Ok(PrimitiveType::String),
            "binary" => Ok(PrimitiveType::Binary),
            "date32" | "date" => Ok(PrimitiveType::Date32),
            "date64" => Ok(PrimitiveType::Date64),
            "timestamp" | "timestamp_millis" => {
                // PARQUET SPEC: timezone presence means UTC storage (isAdjustedToUTC = true)
                Ok(PrimitiveType::TimestampMillis(timezone.map(Into::into)))
            }
            "timestamp_second" => {
                // PARQUET SPEC: timezone presence means UTC storage (isAdjustedToUTC = true)
                Ok(PrimitiveType::TimestampSecond(timezone.map(Into::into)))
            }
            "timestamp_micros" => {
                // PARQUET SPEC: timezone presence means UTC storage (isAdjustedToUTC = true)
                Ok(PrimitiveType::TimestampMicros(timezone.map(Into::into)))
            }
            "timestamp_nanos" => {
                // PARQUET SPEC: timezone presence means UTC storage (isAdjustedToUTC = true)
                Ok(PrimitiveType::TimestampNanos(timezone.map(Into::into)))
            }
            "time_millis" => Ok(PrimitiveType::TimeMillis),
            "time_micros" => Ok(PrimitiveType::TimeMicros),
            "time_nanos" => Ok(PrimitiveType::TimeNanos),
            "decimal" => {
                // Use provided precision/scale or defaults
                let p = precision.unwrap_or(38);
                let s = scale.unwrap_or(0);

                // Choose decimal type based on precision
                if p <= 38 {
                    Ok(PrimitiveType::Decimal128(p, s))
                } else {
                    Ok(PrimitiveType::Decimal256(p, s))
                }
            }
            "decimal128" => {
                let p = precision.unwrap_or(38);
                let s = scale.unwrap_or(0);
                Ok(PrimitiveType::Decimal128(p, s))
            }
            "decimal256" => {
                let p = precision.unwrap_or(76);
                let s = scale.unwrap_or(0);
                Ok(PrimitiveType::Decimal256(p, s))
            }
            _ => Err(RubyAdapterError::InvalidInput(format!(
                "Unknown primitive type: {}",
                type_str
            ))),
        }
    }
}

impl Default for RubySchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Return `node` with its nullability forced to required (non-nullable).
///
/// Parquet maps store keys with `Repetition::Required`; a nullable map key is
/// an illegal state that the core schema validator rejects. Map keys reach this
/// helper from a user-supplied key hash whose `nullable` field defaults to
/// `true`, so forcing required here keeps the raw-hash path consistent with the
/// schema DSL and the `map<...>` string form.
fn into_required(node: SchemaNode) -> SchemaNode {
    match node {
        SchemaNode::Struct { name, fields, .. } => SchemaNode::Struct {
            name,
            nullable: false,
            fields,
        },
        SchemaNode::List { name, item, .. } => SchemaNode::List {
            name,
            nullable: false,
            item,
        },
        SchemaNode::Map {
            name, key, value, ..
        } => SchemaNode::Map {
            name,
            nullable: false,
            key,
            value,
        },
        SchemaNode::Primitive {
            name,
            primitive_type,
            format,
            ..
        } => SchemaNode::Primitive {
            name,
            primitive_type,
            nullable: false,
            format,
        },
    }
}

/// Wrapper functions for Ruby FFI since SchemaBuilderTrait requires Send + Sync
/// and Ruby Value is not Send/Sync
pub fn ruby_schema_to_parquet(schema_def: Value) -> Result<Schema, RubyAdapterError> {
    let ruby = Ruby::get().map_err(|e| RubyAdapterError::Ruby(e.to_string()))?;
    let builder = RubySchemaBuilder::new();

    // The Ruby schema should be a hash with a root struct
    let hash: RHash = <RHash as TryConvert>::try_convert(schema_def)
        .map_err(|e: MagnusError| ParquetError::Schema(format!("Schema must be a hash: {}", e)))?;

    // Check if it's already in the expected format (with type: :struct)
    let root_node = if hash.get(ruby.to_symbol("type")).is_some() {
        // It's a complete schema definition
        builder.parse_hash_schema_node("root".to_string(), hash)?
    } else if let Ok(fields) = hash.fetch::<_, RArray>(ruby.to_symbol("fields")) {
        // It's a simplified format with just fields array
        let mut field_nodes = Vec::new();
        for field_value in fields.into_iter() {
            let field_hash: RHash = <RHash as TryConvert>::try_convert(field_value)
                .map_err(|e: MagnusError| ParquetError::Schema(format!("Invalid field: {}", e)))?;
            field_nodes.push(builder.parse_field_definition(field_hash)?);
        }

        // Check for duplicate field names
        let field_names: Vec<String> = field_nodes
            .iter()
            .map(|node| match node {
                SchemaNode::Primitive { name, .. } => name.clone(),
                SchemaNode::List { name, .. } => name.clone(),
                SchemaNode::Map { name, .. } => name.clone(),
                SchemaNode::Struct { name, .. } => name.clone(),
            })
            .collect();

        let mut unique_names = std::collections::HashSet::new();
        for name in &field_names {
            if !unique_names.insert(name) {
                return Err(RubyAdapterError::InvalidInput(format!(
                    "Duplicate field names in root level schema: {:?}",
                    field_names
                )));
            }
        }

        SchemaNode::Struct {
            name: "root".to_string(),
            nullable: false,
            fields: field_nodes,
        }
    } else {
        return Err(RubyAdapterError::InvalidInput(
            "Schema must have 'type' or 'fields' key".to_string(),
        ));
    };

    // Build the schema
    parquet_core::SchemaBuilder::new()
        .with_root(root_node)
        .build()
        .map_err(|e| RubyAdapterError::InvalidInput(e.to_string()))
}

/// Convert a Parquet schema back to Ruby representation
pub fn parquet_schema_to_ruby(schema: &Schema) -> Result<Value, RubyAdapterError> {
    let ruby = Ruby::get()
        .map_err(|e| ParquetError::Conversion(format!("Failed to get Ruby runtime: {}", e)))?;

    schema_node_to_ruby(&schema.root, &ruby)
}

fn schema_node_to_ruby(node: &SchemaNode, ruby: &Ruby) -> Result<Value, RubyAdapterError> {
    let hash = ruby.hash_new();

    match node {
        SchemaNode::Struct {
            name,
            nullable,
            fields,
        } => {
            hash.aset(ruby.to_symbol("type"), ruby.to_symbol("struct"))
                .map_err(|e| ParquetError::Conversion(format!("Failed to set type: {}", e)))?;
            hash.aset(ruby.to_symbol("name"), name.as_str())
                .map_err(|e| ParquetError::Conversion(format!("Failed to set name: {}", e)))?;
            hash.aset(ruby.to_symbol("nullable"), *nullable)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set nullable: {}", e)))?;

            let fields_array = ruby.ary_new();
            for field in fields {
                fields_array
                    .push(schema_node_to_ruby(field, ruby)?)
                    .map_err(|e| {
                        ParquetError::Conversion(format!("Failed to push field: {}", e))
                    })?;
            }
            hash.aset(ruby.to_symbol("fields"), fields_array)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set fields: {}", e)))?;
        }

        SchemaNode::List {
            name,
            nullable,
            item,
        } => {
            hash.aset(ruby.to_symbol("type"), ruby.to_symbol("list"))
                .map_err(|e| ParquetError::Conversion(format!("Failed to set type: {}", e)))?;
            hash.aset(ruby.to_symbol("name"), name.as_str())
                .map_err(|e| ParquetError::Conversion(format!("Failed to set name: {}", e)))?;
            hash.aset(ruby.to_symbol("nullable"), *nullable)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set nullable: {}", e)))?;
            hash.aset(ruby.to_symbol("item"), schema_node_to_ruby(item, ruby)?)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set item: {}", e)))?;
        }

        SchemaNode::Map {
            name,
            nullable,
            key,
            value,
        } => {
            hash.aset(ruby.to_symbol("type"), ruby.to_symbol("map"))
                .map_err(|e| ParquetError::Conversion(format!("Failed to set type: {}", e)))?;
            hash.aset(ruby.to_symbol("name"), name.as_str())
                .map_err(|e| ParquetError::Conversion(format!("Failed to set name: {}", e)))?;
            hash.aset(ruby.to_symbol("nullable"), *nullable)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set nullable: {}", e)))?;
            hash.aset(ruby.to_symbol("key"), schema_node_to_ruby(key, ruby)?)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set key: {}", e)))?;
            hash.aset(ruby.to_symbol("value"), schema_node_to_ruby(value, ruby)?)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set value: {}", e)))?;
        }

        SchemaNode::Primitive {
            name,
            primitive_type,
            nullable,
            format,
        } => {
            let type_sym = match primitive_type {
                PrimitiveType::Boolean => ruby.to_symbol("boolean"),
                PrimitiveType::Int8 => ruby.to_symbol("int8"),
                PrimitiveType::Int16 => ruby.to_symbol("int16"),
                PrimitiveType::Int32 => ruby.to_symbol("int32"),
                PrimitiveType::Int64 => ruby.to_symbol("int64"),
                PrimitiveType::UInt8 => ruby.to_symbol("uint8"),
                PrimitiveType::UInt16 => ruby.to_symbol("uint16"),
                PrimitiveType::UInt32 => ruby.to_symbol("uint32"),
                PrimitiveType::UInt64 => ruby.to_symbol("uint64"),
                PrimitiveType::Float32 => ruby.to_symbol("float32"),
                PrimitiveType::Float64 => ruby.to_symbol("float64"),
                PrimitiveType::String => ruby.to_symbol("string"),
                PrimitiveType::Binary => ruby.to_symbol("binary"),
                PrimitiveType::Date32 => ruby.to_symbol("date32"),
                PrimitiveType::Date64 => ruby.to_symbol("date64"),
                PrimitiveType::TimestampSecond(_) => ruby.to_symbol("timestamp_second"),
                PrimitiveType::TimestampMillis(_) => ruby.to_symbol("timestamp_millis"),
                PrimitiveType::TimestampMicros(_) => ruby.to_symbol("timestamp_micros"),
                PrimitiveType::TimestampNanos(_) => ruby.to_symbol("timestamp_nanos"),
                PrimitiveType::TimeMillis => ruby.to_symbol("time_millis"),
                PrimitiveType::TimeMicros => ruby.to_symbol("time_micros"),
                PrimitiveType::TimeNanos => ruby.to_symbol("time_nanos"),
                PrimitiveType::Decimal128(_, _) => ruby.to_symbol("decimal128"),
                PrimitiveType::Decimal256(_, _) => ruby.to_symbol("decimal256"),
                PrimitiveType::FixedLenByteArray(_) => ruby.to_symbol("fixed_len_byte_array"),
            };

            hash.aset(ruby.to_symbol("type"), type_sym)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set type: {}", e)))?;
            hash.aset(ruby.to_symbol("name"), name.as_str())
                .map_err(|e| ParquetError::Conversion(format!("Failed to set name: {}", e)))?;
            hash.aset(ruby.to_symbol("nullable"), *nullable)
                .map_err(|e| ParquetError::Conversion(format!("Failed to set nullable: {}", e)))?;

            if let Some(fmt) = format {
                hash.aset(ruby.to_symbol("format"), fmt.as_str())
                    .map_err(|e| {
                        ParquetError::Conversion(format!("Failed to set format: {}", e))
                    })?;
            }

            // Add precision/scale for decimal types
            match primitive_type {
                PrimitiveType::Decimal128(p, s) | PrimitiveType::Decimal256(p, s) => {
                    hash.aset(ruby.to_symbol("precision"), *p).map_err(|e| {
                        ParquetError::Conversion(format!("Failed to set precision: {}", e))
                    })?;
                    hash.aset(ruby.to_symbol("scale"), *s).map_err(|e| {
                        ParquetError::Conversion(format!("Failed to set scale: {}", e))
                    })?;
                }
                PrimitiveType::FixedLenByteArray(len) => {
                    hash.aset(ruby.to_symbol("length"), *len).map_err(|e| {
                        ParquetError::Conversion(format!("Failed to set length: {}", e))
                    })?;
                }
                _ => {}
            }
        }
    }

    Ok(hash.as_value())
}

/// Convert old schema format to new format
/// Old: [{ "column_name" => "type" }, ...]
/// New: [{ name: "column_name", type: :type }, ...]
pub fn convert_legacy_schema(ruby: &Ruby, schema: RArray) -> Result<RArray, RubyAdapterError> {
    let new_schema = ruby.ary_new();

    for item in schema.into_iter() {
        let hash: RHash = TryConvert::try_convert(item).map_err(|e: MagnusError| {
            ParquetError::Schema(format!("Invalid schema item: {}", e))
        })?;
        let new_field = ruby.hash_new();

        // The old format has a single key-value pair per hash
        let process_result = hash.foreach(
            |key: Value,
             value: Value|
             -> std::result::Result<magnus::r_hash::ForEach, MagnusError> {
                let key_str: String = parse_string_or_symbol(ruby, key)?.ok_or_else(|| {
                    MagnusError::new(ruby.exception_arg_error(), "Nil keys not allowed in schema")
                })?;
                let type_str: String = TryConvert::try_convert(value)?;

                new_field.aset(ruby.to_symbol("name"), key_str)?;
                new_field.aset(ruby.to_symbol("type"), ruby.to_symbol(&type_str))?;
                if type_str.contains("timestamp") {
                    new_field.aset(ruby.to_symbol("has_timezone"), true)?;
                }

                Ok(magnus::r_hash::ForEach::Continue)
            },
        );

        if let Err(e) = process_result {
            return Err(RubyAdapterError::InvalidInput(format!(
                "Failed to process field: {}",
                e
            )));
        }

        new_schema
            .push(new_field)
            .map_err(|e| ParquetError::Schema(format!("Failed to push field: {}", e)))?;
    }

    Ok(new_schema)
}

/// Check if schema is in new DSL format (hash with type: :struct)
pub fn is_dsl_schema(ruby: &Ruby, schema_value: Value) -> Result<bool, RubyAdapterError> {
    if !schema_value.is_kind_of(ruby.class_hash()) {
        return Ok(false);
    }

    let schema_hash: RHash = TryConvert::try_convert(schema_value).map_err(|e: MagnusError| {
        ParquetError::Schema(format!("Failed to convert to hash: {}", e))
    })?;
    if let Some(type_val) = schema_hash.get(ruby.to_symbol("type")) {
        if type_val.is_kind_of(ruby.class_symbol()) {
            let type_sym: Symbol =
                TryConvert::try_convert(type_val).map_err(|e: MagnusError| {
                    ParquetError::Schema(format!("Failed to convert to symbol: {}", e))
                })?;
            return Ok(type_sym.name().map_err(|e: MagnusError| {
                ParquetError::Schema(format!("Failed to get symbol name: {}", e))
            })? == "struct");
        } else if type_val.is_kind_of(ruby.class_string()) {
            let type_str: String =
                TryConvert::try_convert(type_val).map_err(|e: MagnusError| {
                    ParquetError::Schema(format!("Failed to convert to string: {}", e))
                })?;
            return Ok(type_str == "struct");
        }
    }
    Ok(false)
}

/// Process schema value and convert to format expected by ruby_schema_to_parquet
pub fn process_schema_value(
    ruby: &Ruby,
    schema_value: Value,
    data_array: Option<&RArray>,
) -> Result<Value, RubyAdapterError> {
    // Check if it's the new DSL format
    if is_dsl_schema(ruby, schema_value)? {
        // For DSL format, pass it directly to ruby_schema_to_parquet
        // which should handle the conversion
        return Ok(schema_value);
    }

    // Handle array format or hash with fields
    let mut schema_array = if schema_value.is_nil() {
        ruby.ary_new()
    } else if schema_value.is_kind_of(ruby.class_array()) {
        let array: RArray = TryConvert::try_convert(schema_value).map_err(|e: MagnusError| {
            ParquetError::Schema(format!("Failed to convert to array: {}", e))
        })?;

        // Check if it's in old format (array of single-key hashes)
        if !array.is_empty() {
            let first_item: Value = array
                .entry(0)
                .map_err(|e| ParquetError::Schema(format!("Failed to get first item: {}", e)))?;

            if first_item.is_kind_of(ruby.class_hash()) {
                let first_hash: RHash =
                    TryConvert::try_convert(first_item).map_err(|e: MagnusError| {
                        ParquetError::Schema(format!("Failed to convert first item to hash: {}", e))
                    })?;
                // Check if it has the new format keys
                if first_hash.get(ruby.to_symbol("name")).is_some()
                    && first_hash.get(ruby.to_symbol("type")).is_some()
                {
                    // Already in new format
                    array
                } else {
                    // Old format, convert it
                    convert_legacy_schema(ruby, array)?
                }
            } else {
                return Err(RubyAdapterError::InvalidInput(
                    "schema array must contain hashes".to_string(),
                ));
            }
        } else {
            array
        }
    } else if schema_value.is_kind_of(ruby.class_hash()) {
        // Hash format with fields key
        let hash: RHash = TryConvert::try_convert(schema_value).map_err(|e: MagnusError| {
            ParquetError::Schema(format!("Failed to convert to hash: {}", e))
        })?;
        if let Some(fields) = hash.get(ruby.to_symbol("fields")) {
            TryConvert::try_convert(fields).map_err(|e: MagnusError| {
                ParquetError::Schema(format!("Failed to convert fields to array: {}", e))
            })?
        } else {
            return Err(RubyAdapterError::InvalidInput(
                "schema hash must have 'fields' key or be in DSL format with 'type' key"
                    .to_string(),
            ));
        }
    } else {
        return Err(RubyAdapterError::InvalidInput(
            "schema must be nil, an array, or a hash".to_string(),
        ));
    };

    // Check if we need to infer schema from data
    if schema_array.is_empty() {
        if let Some(data) = data_array {
            if data.is_empty() {
                return Err(RubyAdapterError::InvalidInput(
                    "Cannot infer schema from empty data".to_string(),
                ));
            }

            // Get first row/batch to determine column count
            let first_item: Value = data.entry(0).map_err(|e| {
                ParquetError::Schema(format!("Failed to get first data item: {}", e))
            })?;
            let num_columns = if first_item.is_kind_of(ruby.class_array()) {
                let first_array: RArray =
                    TryConvert::try_convert(first_item).map_err(|e: MagnusError| {
                        ParquetError::Schema(format!(
                            "Failed to convert first data item to array: {}",
                            e
                        ))
                    })?;
                first_array.len()
            } else {
                return Err(RubyAdapterError::InvalidInput(
                    "First data item must be an array".to_string(),
                ));
            };

            // Generate default schema with String types
            let new_schema = ruby.ary_new();
            for i in 0..num_columns {
                let field = ruby.hash_new();
                field
                    .aset(ruby.to_symbol("name"), format!("f{}", i))
                    .map_err(|e| {
                        ParquetError::Schema(format!("Failed to set field name: {}", e))
                    })?;
                field
                    .aset(ruby.to_symbol("type"), ruby.to_symbol("string"))
                    .map_err(|e| {
                        ParquetError::Schema(format!("Failed to set field type: {}", e))
                    })?;
                new_schema
                    .push(field)
                    .map_err(|e| ParquetError::Schema(format!("Failed to push field: {}", e)))?;
            }

            schema_array = new_schema;
        } else {
            return Err(RubyAdapterError::InvalidInput(
                "Schema is required when data is not provided for inference".to_string(),
            ));
        }
    }

    // Convert schema to the format expected by ruby_schema_to_parquet
    let schema_hash = ruby.hash_new();
    schema_hash
        .aset(ruby.to_symbol("fields"), schema_array)
        .map_err(|e| ParquetError::Schema(format!("Failed to set fields: {}", e)))?;
    Ok(schema_hash.as_value())
}

/// Extract schema nodes from schema fields
pub fn extract_field_schemas(schema: &Schema) -> Vec<SchemaNode> {
    if let SchemaNode::Struct { fields, .. } = &schema.root {
        fields.to_vec()
    } else {
        Vec::new()
    }
}
