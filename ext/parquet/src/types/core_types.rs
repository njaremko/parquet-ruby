#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ParserResultType {
    Hash,
    Array,
}

impl ParserResultType {
    pub fn iter() -> impl Iterator<Item = Self> {
        [Self::Hash, Self::Array].into_iter()
    }
}

impl TryFrom<&str> for ParserResultType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hash" => Ok(ParserResultType::Hash),
            "array" => Ok(ParserResultType::Array),
            _ => Err(format!("Invalid parser result type: {}", value)),
        }
    }
}

impl TryFrom<String> for ParserResultType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl std::fmt::Display for ParserResultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParserResultType::Hash => write!(f, "hash"),
            ParserResultType::Array => write!(f, "array"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListField<'a> {
    pub item_type: ParquetSchemaType<'a>,
    pub format: Option<&'a str>,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct MapField<'a> {
    pub key_type: ParquetSchemaType<'a>,
    pub value_type: ParquetSchemaType<'a>,
    pub key_format: Option<&'a str>,
    pub value_format: Option<&'a str>,
    pub value_nullable: bool,
}

#[derive(Debug, Clone)]
pub struct StructField<'a> {
    pub fields: Vec<super::writer_types::SchemaField<'a>>,
}

#[derive(Clone, Debug)]
pub enum ParquetSchemaType<'a> {
    Primitive(PrimitiveType),
    List(Box<ListField<'a>>),
    Map(Box<MapField<'a>>),
    Struct(Box<StructField<'a>>),
}

// New schema representation for the DSL-based approach
#[derive(Debug, Clone)]
pub enum SchemaNode {
    Struct {
        name: String,
        nullable: bool,
        fields: Vec<SchemaNode>,
    },
    List {
        name: String,
        nullable: bool,
        item: Box<SchemaNode>,
    },
    Map {
        name: String,
        nullable: bool,
        key: Box<SchemaNode>,
        value: Box<SchemaNode>,
    },
    Primitive {
        name: String,
        parquet_type: PrimitiveType,
        nullable: bool,
        format: Option<String>,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PrimitiveType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Boolean,
    String,
    Binary,
    Date32,
    TimestampMillis,
    TimestampMicros,
}
