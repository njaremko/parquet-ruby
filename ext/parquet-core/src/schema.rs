use std::sync::Arc;

/// Core schema representation for Parquet files
#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub root: SchemaNode,
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

    pub fn build(self) -> Result<Schema, &'static str> {
        match self.root {
            Some(root) => Ok(Schema { root }),
            None => Err("Schema must have a root node"),
        }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
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
