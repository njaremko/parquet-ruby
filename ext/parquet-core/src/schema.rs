use std::collections::HashSet;
use triomphe::Arc;

const DECIMAL128_MAX_PRECISION: u8 = 38;
const DECIMAL256_MAX_PRECISION: u8 = 76;

/// Core schema representation for Parquet files
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub root: SchemaNode,
}

impl Schema {
    pub fn validate(&self) -> Result<(), String> {
        validate_root(&self.root)
    }
}

/// Represents a node in the Parquet schema tree
#[derive(Debug, Clone, PartialEq)]
pub enum SchemaNode {
    /// A struct with named fields
    Struct {
        name: String,
        nullable: bool,
        fields: Vec<SchemaNode>,
    },
    /// A list containing items of a single type
    List {
        name: String,
        nullable: bool,
        item: Box<SchemaNode>,
    },
    /// A map with key-value pairs
    Map {
        name: String,
        nullable: bool,
        key: Box<SchemaNode>,
        value: Box<SchemaNode>,
    },
    /// A primitive/leaf type
    Primitive {
        name: String,
        primitive_type: PrimitiveType,
        nullable: bool,
        format: Option<String>,
    },
}

/// Primitive data types supported by Parquet
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    // Integer types
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,

    // Floating point types
    Float32,
    Float64,

    // Decimal types (precision, scale)
    Decimal128(u8, i8),
    Decimal256(u8, i8),

    // Other basic types
    Boolean,
    String,
    Binary,

    // Date/Time types
    Date32,
    Date64,
    TimestampSecond(Option<Arc<str>>),
    TimestampMillis(Option<Arc<str>>),
    TimestampMicros(Option<Arc<str>>),
    TimestampNanos(Option<Arc<str>>),
    TimeMillis,
    TimeMicros,
    TimeNanos,

    // Fixed-length byte array
    FixedLenByteArray(i32),
}

/// Represents how values are repeated in Parquet
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Repetition {
    /// Field must have exactly one value
    Required,
    /// Field can have 0 or 1 value
    Optional,
    /// Field can have 0 or more values
    Repeated,
}

impl SchemaNode {
    /// Get the name of this schema node
    pub fn name(&self) -> &str {
        match self {
            SchemaNode::Struct { name, .. } => name,
            SchemaNode::List { name, .. } => name,
            SchemaNode::Map { name, .. } => name,
            SchemaNode::Primitive { name, .. } => name,
        }
    }

    /// Check if this node is nullable
    pub fn is_nullable(&self) -> bool {
        match self {
            SchemaNode::Struct { nullable, .. } => *nullable,
            SchemaNode::List { nullable, .. } => *nullable,
            SchemaNode::Map { nullable, .. } => *nullable,
            SchemaNode::Primitive { nullable, .. } => *nullable,
        }
    }

    /// Get the repetition level based on nullability
    pub fn repetition(&self) -> Repetition {
        if self.is_nullable() {
            Repetition::Optional
        } else {
            Repetition::Required
        }
    }
}

impl PrimitiveType {
    /// Get the logical type name for display
    pub fn type_name(&self) -> &'static str {
        match self {
            PrimitiveType::Int8 => "Int8",
            PrimitiveType::Int16 => "Int16",
            PrimitiveType::Int32 => "Int32",
            PrimitiveType::Int64 => "Int64",
            PrimitiveType::UInt8 => "UInt8",
            PrimitiveType::UInt16 => "UInt16",
            PrimitiveType::UInt32 => "UInt32",
            PrimitiveType::UInt64 => "UInt64",
            PrimitiveType::Float32 => "Float32",
            PrimitiveType::Float64 => "Float64",
            PrimitiveType::Decimal128(_, _) => "Decimal128",
            PrimitiveType::Decimal256(_, _) => "Decimal256",
            PrimitiveType::Boolean => "Boolean",
            PrimitiveType::String => "String",
            PrimitiveType::Binary => "Binary",
            PrimitiveType::Date32 => "Date32",
            PrimitiveType::Date64 => "Date64",
            PrimitiveType::TimestampSecond(_) => "TimestampSecond",
            PrimitiveType::TimestampMillis(_) => "TimestampMillis",
            PrimitiveType::TimestampMicros(_) => "TimestampMicros",
            PrimitiveType::TimestampNanos(_) => "TimestampNanos",
            PrimitiveType::TimeMillis => "TimeMillis",
            PrimitiveType::TimeMicros => "TimeMicros",
            PrimitiveType::TimeNanos => "TimeNanos",
            PrimitiveType::FixedLenByteArray(_) => "FixedLenByteArray",
        }
    }

    /// Check if this type requires a format specifier
    pub fn requires_format(&self) -> bool {
        matches!(
            self,
            PrimitiveType::Date32
                | PrimitiveType::Date64
                | PrimitiveType::TimestampSecond(_)
                | PrimitiveType::TimestampMillis(_)
                | PrimitiveType::TimestampMicros(_)
                | PrimitiveType::TimestampNanos(_)
                | PrimitiveType::TimeMillis
                | PrimitiveType::TimeMicros
                | PrimitiveType::TimeNanos
        )
    }
}

/// Builder for creating schemas
pub struct SchemaBuilder {
    root: Option<SchemaNode>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self { root: None }
    }

    pub fn with_root(mut self, root: SchemaNode) -> Self {
        self.root = Some(root);
        self
    }

    pub fn build(self) -> Result<Schema, String> {
        match self.root {
            Some(root) => {
                validate_root(&root)?;
                Ok(Schema { root })
            }
            None => Err("Schema must have a root node".to_string()),
        }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_root(root: &SchemaNode) -> Result<(), String> {
    match root {
        SchemaNode::Struct { name, fields, .. } => {
            if fields.is_empty() {
                return Err(format!(
                    "Root struct '{}' must contain at least one field",
                    name
                ));
            }
            validate_unique_field_names(fields, name)?;
            for field in fields {
                validate_schema_node(field, name)?;
            }
            Ok(())
        }
        _ => Err("Root schema node must be a struct".to_string()),
    }
}

fn validate_schema_node(node: &SchemaNode, parent_path: &str) -> Result<(), String> {
    let path = format!("{}.{}", parent_path, node.name());
    match node {
        SchemaNode::Struct { fields, .. } => {
            if fields.is_empty() {
                return Err(format!(
                    "Struct field '{}' must contain at least one field",
                    path
                ));
            }
            validate_unique_field_names(fields, &path)?;
            for field in fields {
                validate_schema_node(field, &path)?;
            }
        }
        SchemaNode::List { item, .. } => {
            validate_schema_node(item, &path)?;
        }
        SchemaNode::Map { key, value, .. } => {
            if key.is_nullable() {
                return Err(format!(
                    "Map key field '{}.{}' must be required",
                    path,
                    key.name()
                ));
            }
            validate_schema_node(key, &path)?;
            validate_schema_node(value, &path)?;
        }
        SchemaNode::Primitive {
            primitive_type,
            format,
            ..
        } => {
            validate_primitive_type(primitive_type, format.as_deref(), &path)?;
        }
    }
    Ok(())
}

fn validate_unique_field_names(fields: &[SchemaNode], path: &str) -> Result<(), String> {
    let mut names = HashSet::with_capacity(fields.len());
    for field in fields {
        let name = field.name();
        if !names.insert(name) {
            return Err(format!(
                "Struct field '{}' contains duplicate field '{}'",
                path, name
            ));
        }
    }
    Ok(())
}

fn validate_primitive_type(
    primitive_type: &PrimitiveType,
    format: Option<&str>,
    path: &str,
) -> Result<(), String> {
    match primitive_type {
        PrimitiveType::Decimal128(precision, scale) => validate_decimal_type(
            "Decimal128",
            *precision,
            *scale,
            DECIMAL128_MAX_PRECISION,
            path,
        )?,
        PrimitiveType::Decimal256(precision, scale) => validate_decimal_type(
            "Decimal256",
            *precision,
            *scale,
            DECIMAL256_MAX_PRECISION,
            path,
        )?,
        PrimitiveType::FixedLenByteArray(length) => {
            if *length <= 0 {
                return Err(format!(
                    "FixedLenByteArray field '{}' must have a positive length",
                    path
                ));
            }
            if format == Some("uuid") && *length != 16 {
                return Err(format!(
                    "UUID field '{}' must use FixedLenByteArray(16)",
                    path
                ));
            }
        }
        _ => {
            if format == Some("uuid") {
                return Err(format!(
                    "UUID field '{}' must use FixedLenByteArray(16)",
                    path
                ));
            }
        }
    }
    Ok(())
}

fn validate_decimal_type(
    type_name: &str,
    precision: u8,
    scale: i8,
    max_precision: u8,
    path: &str,
) -> Result<(), String> {
    if precision == 0 {
        return Err(format!(
            "{} field '{}' precision must be at least 1",
            type_name, path
        ));
    }
    if precision > max_precision {
        return Err(format!(
            "{} field '{}' precision {} exceeds maximum precision {}",
            type_name, path, precision, max_precision
        ));
    }
    if scale < 0 {
        return Err(format!(
            "{} field '{}' scale must be non-negative",
            type_name, path
        ));
    }
    if scale as u8 > precision {
        return Err(format!(
            "{} field '{}' scale {} cannot exceed precision {}",
            type_name, path, scale, precision
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let schema = SchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![
                    SchemaNode::Primitive {
                        name: "id".to_string(),
                        primitive_type: PrimitiveType::Int64,
                        nullable: false,
                        format: None,
                    },
                    SchemaNode::Primitive {
                        name: "name".to_string(),
                        primitive_type: PrimitiveType::String,
                        nullable: true,
                        format: None,
                    },
                ],
            })
            .build()
            .unwrap();

        assert_eq!(schema.root.name(), "root");
        assert!(!schema.root.is_nullable());
    }

    #[test]
    fn test_primitive_types() {
        let decimal = PrimitiveType::Decimal128(10, 2);
        assert_eq!(decimal.type_name(), "Decimal128");

        let timestamp = PrimitiveType::TimestampMicros(None);
        assert!(timestamp.requires_format());

        let integer = PrimitiveType::Int32;
        assert!(!integer.requires_format());
    }

    #[test]
    fn test_nested_schema() {
        let list_node = SchemaNode::List {
            name: "items".to_string(),
            nullable: true,
            item: Box::new(SchemaNode::Primitive {
                name: "item".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }),
        };

        assert_eq!(list_node.name(), "items");
        assert!(list_node.is_nullable());
        assert_eq!(list_node.repetition(), Repetition::Optional);
    }

    #[test]
    fn test_map_schema() {
        let map_node = SchemaNode::Map {
            name: "metadata".to_string(),
            nullable: false,
            key: Box::new(SchemaNode::Primitive {
                name: "key".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: false,
                format: None,
            }),
            value: Box::new(SchemaNode::Primitive {
                name: "value".to_string(),
                primitive_type: PrimitiveType::String,
                nullable: true,
                format: None,
            }),
        };

        assert_eq!(map_node.name(), "metadata");
        assert!(!map_node.is_nullable());
        assert_eq!(map_node.repetition(), Repetition::Required);
    }
}
