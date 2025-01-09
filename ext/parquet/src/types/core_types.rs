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
pub struct ListField {
    pub item_type: ParquetSchemaType,
}

#[derive(Debug, Clone)]
pub struct MapField {
    pub key_type: ParquetSchemaType,
    pub value_type: ParquetSchemaType,
}

#[derive(Debug, Clone)]
pub enum ParquetSchemaType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float,
    Double,
    String,
    Binary,
    Boolean,
    Date32,
    TimestampMillis,
    TimestampMicros,
    List(Box<ListField>),
    Map(Box<MapField>),
}