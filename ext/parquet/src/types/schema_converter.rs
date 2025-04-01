use magnus::value::ReprValue; // Add ReprValue trait to scope
use magnus::{Error as MagnusError, IntoValue, RArray, Ruby, TryConvert, Value};

use crate::types::{ParquetSchemaType as PST, PrimitiveType, SchemaField, SchemaNode};
use crate::utils::parse_string_or_symbol;

/// Recursively converts a SchemaField to a SchemaNode for any level of nesting
fn convert_schema_field_to_node(field: &SchemaField) -> SchemaNode {
    match &field.type_ {
        PST::Primitive(primative) => SchemaNode::Primitive {
            name: field.name.clone(),
            nullable: field.nullable,
            parquet_type: *primative,
            format: field.format.clone(),
        },
        PST::List(list_field) => {
            // Create item node by recursively converting the list item type to a node
            let item_node = match &list_field.item_type {
                // For primitive types, create a primitive node with name "item"
                PST::Primitive(_) => {
                    // Use a temporary SchemaField to convert item type
                    let item_field = SchemaField {
                        name: "item".to_string(),
                        type_: list_field.item_type.clone(),
                        format: list_field.format.map(String::from),
                        nullable: list_field.nullable,
                    };
                    convert_schema_field_to_node(&item_field)
                }
                // For nested types (List, Map, Struct), recursively convert them
                PST::List(_) | PST::Map(_) | PST::Struct(_) => {
                    // Use a temporary SchemaField to convert item type
                    let item_field = SchemaField {
                        name: "item".to_string(),
                        type_: list_field.item_type.clone(),
                        format: list_field.format.map(String::from),
                        nullable: list_field.nullable,
                    };
                    convert_schema_field_to_node(&item_field)
                }
            };

            SchemaNode::List {
                name: field.name.clone(),
                nullable: field.nullable,
                item: Box::new(item_node),
            }
        }
        PST::Map(map_field) => {
            let key_field = SchemaField {
                name: "key".to_string(),
                type_: map_field.key_type.clone(),
                format: map_field.key_format.map(String::from),
                nullable: false, // Map keys can never be null in Parquet
            };
            let value_field = SchemaField {
                name: "value".to_string(),
                type_: map_field.value_type.clone(),
                format: map_field.value_format.map(String::from),
                nullable: map_field.value_nullable,
            };

            let key_node = convert_schema_field_to_node(&key_field);
            let value_node = convert_schema_field_to_node(&value_field);

            SchemaNode::Map {
                name: field.name.clone(),
                nullable: field.nullable,
                key: Box::new(key_node),
                value: Box::new(value_node),
            }
        }
        PST::Struct(struct_field) => {
            // Convert each subfield recursively
            let mut field_nodes = Vec::with_capacity(struct_field.fields.len());

            for subfield in struct_field.fields.iter() {
                // Recursively convert each subfield, supporting any level of nesting
                field_nodes.push(convert_schema_field_to_node(subfield));
            }

            SchemaNode::Struct {
                name: field.name.clone(),
                nullable: field.nullable,
                fields: field_nodes,
            }
        }
    }
}

/// Converts the legacy schema format (array of field hashes) to the new DSL format (SchemaNode)
pub fn legacy_schema_to_dsl(
    _ruby: &Ruby,
    schema_fields: Vec<SchemaField>,
) -> Result<SchemaNode, MagnusError> {
    // Create a top-level struct node with fields for each schema field
    let mut field_nodes = Vec::with_capacity(schema_fields.len());

    for field in schema_fields {
        // Use our recursive converter to handle any level of nesting
        field_nodes.push(convert_schema_field_to_node(&field));
    }

    Ok(SchemaNode::Struct {
        name: "".to_string(), // Top level has no name
        nullable: false,      // Top level is not nullable
        fields: field_nodes,
    })
}

/// Parses the legacy format schema (array of field hashes)
pub fn parse_legacy_schema(
    ruby: &Ruby,
    schema_value: Value,
) -> Result<Vec<SchemaField>, MagnusError> {
    if schema_value.is_nil()
        || (schema_value.is_kind_of(ruby.class_array())
            && RArray::from_value(schema_value)
                .ok_or_else(|| {
                    MagnusError::new(
                        ruby.exception_type_error(),
                        "Schema must be an array of field definitions or nil",
                    )
                })?.is_empty())
    {
        // If schema is nil or an empty array, we'll handle this in the caller
        return Ok(Vec::new());
    }

    if schema_value.is_kind_of(ruby.class_array()) {
        let schema_array = RArray::from_value(schema_value).ok_or_else(|| {
            MagnusError::new(
                ruby.exception_type_error(),
                "Schema must be an array of field definitions or nil",
            )
        })?;
        let mut schema = Vec::with_capacity(schema_array.len());

        for (idx, field_hash) in schema_array.into_iter().enumerate() {
            if !field_hash.is_kind_of(ruby.class_hash()) {
                return Err(MagnusError::new(
                    ruby.exception_type_error(),
                    format!("schema[{}] must be a hash", idx),
                ));
            }

            let entries: Vec<(Value, Value)> = field_hash.funcall("to_a", ())?;
            if entries.len() != 1 {
                return Err(MagnusError::new(
                    ruby.exception_type_error(),
                    format!("schema[{}] must contain exactly one key-value pair", idx),
                ));
            }

            let (name, type_value) = &entries[0];
            let name_option = parse_string_or_symbol(ruby, *name)?;
            let name = name_option.ok_or_else(|| {
                MagnusError::new(ruby.exception_runtime_error(), "Field name cannot be nil")
            })?;

            let (type_, format, nullable) = if type_value.is_kind_of(ruby.class_hash()) {
                let type_hash: Vec<(Value, Value)> = type_value.funcall("to_a", ())?;
                let mut type_str = None;
                let mut format_str = None;
                let mut nullable = true; // Default to true if not specified

                let mut precision: Option<Value> = None;
                let mut scale: Option<Value> = None;

                for (key, value) in type_hash {
                    let key_option = parse_string_or_symbol(ruby, key)?;
                    let key = key_option.ok_or_else(|| {
                        MagnusError::new(ruby.exception_runtime_error(), "Type key cannot be nil")
                    })?;
                    match key.as_str() {
                        "type" => type_str = Some(value),
                        "format" => {
                            let format_option = parse_string_or_symbol(ruby, value)?;
                            format_str = format_option;
                        }
                        "nullable" => {
                            // Extract nullable if present - convert to boolean
                            nullable = bool::try_convert(value).unwrap_or(true);
                        }
                        "precision" => {
                            precision = Some(value);
                        }
                        "scale" => {
                            scale = Some(value);
                        }
                        _ => {
                            return Err(MagnusError::new(
                                ruby.exception_type_error(),
                                format!("Unknown key '{}' in type definition", key),
                            ))
                        }
                    }
                }

                let type_str = type_str.ok_or_else(|| {
                    MagnusError::new(
                        ruby.exception_type_error(),
                        "Missing 'type' in type definition",
                    )
                })?;

                // Handle decimal type with precision and scale
                let mut type_result = PST::try_convert(type_str)?;
                
                // If it's a decimal type and we have precision and scale, override the type
                if let PST::Primitive(PrimitiveType::Decimal128(_, _)) = type_result {
                    let precision_value = precision.unwrap_or_else(|| {
                        let val: u8 = 18;
                        val.into_value_with(ruby)
                    });
                    let scale_value = scale.unwrap_or_else(|| {
                        let val: i8 = 2;
                        val.into_value_with(ruby)
                    });
                    
                    let precision_u8 = u8::try_convert(precision_value).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid precision value for decimal type, expected a positive integer".to_string(),
                        )
                    })?;
                    
                    // Validate precision is in a valid range
                    if precision_u8 < 1 {
                        return Err(MagnusError::new(
                            ruby.exception_arg_error(),
                            format!(
                                "Precision for decimal type must be at least 1, got {}", 
                                precision_u8
                            ),
                        ));
                    }
                    
                    if precision_u8 > 38 {
                        return Err(MagnusError::new(
                            ruby.exception_arg_error(),
                            format!(
                                "Precision for decimal type cannot exceed 38, got {}", 
                                precision_u8
                            ),
                        ));
                    }
                    
                    let scale_i8 = i8::try_convert(scale_value).map_err(|_| {
                        MagnusError::new(
                            ruby.exception_type_error(),
                            "Invalid scale value for decimal type, expected an integer".to_string(),
                        )
                    })?;
                    
                    // Validate scale is in a valid range relative to precision
                    if scale_i8 < 0 {
                        return Err(MagnusError::new(
                            ruby.exception_arg_error(),
                            format!(
                                "Scale for decimal type cannot be negative, got {}", 
                                scale_i8
                            ),
                        ));
                    }
                    
                    if scale_i8 as u8 > precision_u8 {
                        return Err(MagnusError::new(
                            ruby.exception_arg_error(),
                            format!(
                                "Scale ({}) cannot be larger than precision ({}) for decimal type", 
                                scale_i8, precision_u8
                            ),
                        ));
                    }
                    
                    type_result = PST::Primitive(PrimitiveType::Decimal128(precision_u8, scale_i8));
                } else if let Some(type_name) = parse_string_or_symbol(ruby, type_str)? {
                    if type_name == "decimal" {
                        let precision_value = precision.unwrap_or_else(|| {
                            let val: u8 = 18;
                            val.into_value_with(ruby)
                        });
                        let scale_value = scale.unwrap_or_else(|| {
                            let val: i8 = 2;
                            val.into_value_with(ruby)
                        });
                        
                        let precision_u8 = u8::try_convert(precision_value).map_err(|_| {
                            MagnusError::new(
                                ruby.exception_type_error(),
                                "Invalid precision value for decimal type, expected a positive integer".to_string(),
                            )
                        })?;
                        
                        let scale_i8 = i8::try_convert(scale_value).map_err(|_| {
                            MagnusError::new(
                                ruby.exception_type_error(),
                                "Invalid scale value for decimal type, expected an integer".to_string(),
                            )
                        })?;
                        
                        type_result = PST::Primitive(PrimitiveType::Decimal128(precision_u8, scale_i8));
                    }
                }

                (type_result, format_str, nullable)
            } else {
                (PST::try_convert(*type_value)?, None, true)
            };

            schema.push(SchemaField {
                name,
                type_,
                format,
                nullable,
            });
        }

        Ok(schema)
    } else {
        Err(MagnusError::new(
            ruby.exception_type_error(),
            "Schema must be an array of field definitions or nil",
        ))
    }
}

/// Generates schema fields by inferring from the first row
pub fn infer_schema_from_first_row(
    ruby: &Ruby,
    first_value: Value,
    nullable: bool,
) -> Result<Vec<SchemaField>, MagnusError> {
    let array = RArray::from_value(first_value).ok_or_else(|| {
        MagnusError::new(
            ruby.exception_type_error(),
            "First value must be an array when schema is not provided",
        )
    })?;

    // Generate field names f0, f1, f2, etc.
    Ok((0..array.len())
        .map(|i| SchemaField {
            name: format!("f{}", i),
            type_: PST::Primitive(PrimitiveType::String), // Default to String type when inferring
            format: None,
            nullable,
        })
        .collect())
}
