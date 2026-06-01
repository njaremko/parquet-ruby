use crate::SchemaNode;

/// Trait for schema introspection
///
/// This trait provides methods for examining and querying schemas
/// without modifying them.
pub trait SchemaInspector {
    /// Get the total number of fields (including nested)
    fn field_count(&self) -> usize;

    /// Get field by path (e.g., "address.city")
    fn get_field_by_path(&self, path: &str) -> Option<&SchemaNode>;

    /// Check if schema contains a specific field
    fn has_field(&self, name: &str) -> bool;

    /// Get all field paths in the schema
    fn all_field_paths(&self) -> Vec<String>;
}

impl SchemaInspector for crate::Schema {
    fn field_count(&self) -> usize {
        count_fields(&self.root)
    }

    fn get_field_by_path(&self, path: &str) -> Option<&SchemaNode> {
        get_field_by_path(&self.root, path)
    }

    fn has_field(&self, name: &str) -> bool {
        self.get_field_by_path(name).is_some()
    }

    fn all_field_paths(&self) -> Vec<String> {
        let mut paths = Vec::new();
        collect_field_paths(&self.root, String::new(), &mut paths);
        paths
    }
}

// Helper functions for schema inspection
fn count_fields(node: &SchemaNode) -> usize {
    match node {
        SchemaNode::Struct { fields, .. } => 1 + fields.iter().map(count_fields).sum::<usize>(),
        SchemaNode::List { item, .. } => 1 + count_fields(item),
        SchemaNode::Map { key, value, .. } => 1 + count_fields(key) + count_fields(value),
        SchemaNode::Primitive { .. } => 1,
    }
}

fn get_field_by_path<'a>(node: &'a SchemaNode, path: &str) -> Option<&'a SchemaNode> {
    if path.is_empty() {
        return None;
    }

    let mut parts: Vec<&str> = path.split('.').collect();
    // Strip a leading segment equal to the root's own name (so a path may carry
    // the root name as a prefix or omit it), but not when the root actually has a
    // child of that name — there the segment refers to the child, not the root.
    if parts.first().copied() == Some(node.name()) && !has_child_named(node, node.name()) {
        parts.remove(0);
    }
    get_field_by_path_parts(node, &parts)
}

fn has_child_named(node: &SchemaNode, name: &str) -> bool {
    matches!(node, SchemaNode::Struct { fields, .. } if fields.iter().any(|f| f.name() == name))
}

fn get_field_by_path_parts<'a>(node: &'a SchemaNode, parts: &[&str]) -> Option<&'a SchemaNode> {
    if parts.is_empty() {
        return Some(node);
    }

    let first = parts[0];
    let rest = &parts[1..];

    match node {
        SchemaNode::Struct { fields, .. } => fields
            .iter()
            .find(|f| f.name() == first)
            .and_then(|f| get_field_by_path_parts(f, rest)),
        SchemaNode::List { item, .. } if first == "item" || first == item.name() => {
            get_field_by_path_parts(item, rest)
        }
        SchemaNode::Map { key, value, .. } => match first {
            name if name == "key" || name == key.name() => get_field_by_path_parts(key, rest),
            name if name == "value" || name == value.name() => get_field_by_path_parts(value, rest),
            _ => None,
        },
        _ => None,
    }
}

fn collect_field_paths(node: &SchemaNode, prefix: String, paths: &mut Vec<String>) {
    let current_path = if prefix.is_empty() {
        node.name().to_string()
    } else {
        format!("{}.{}", prefix, node.name())
    };

    paths.push(current_path.clone());

    match node {
        SchemaNode::Struct { fields, .. } => {
            for field in fields {
                collect_field_paths(field, current_path.clone(), paths);
            }
        }
        SchemaNode::List { item, .. } => {
            collect_field_paths(item, current_path, paths);
        }
        SchemaNode::Map { key, value, .. } => {
            collect_field_paths(key, current_path.clone(), paths);
            collect_field_paths(value, current_path, paths);
        }
        SchemaNode::Primitive { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PrimitiveType, SchemaBuilder as CoreSchemaBuilder};

    #[test]
    fn test_schema_inspector() {
        let schema = CoreSchemaBuilder::new()
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
                    SchemaNode::Struct {
                        name: "address".to_string(),
                        nullable: true,
                        fields: vec![SchemaNode::Primitive {
                            name: "city".to_string(),
                            primitive_type: PrimitiveType::String,
                            nullable: true,
                            format: None,
                        }],
                    },
                ],
            })
            .build()
            .unwrap();

        // Test field count
        assert_eq!(schema.field_count(), 4); // root, id, address, city

        // Test field lookup
        assert!(schema.has_field("id"));
        assert!(schema.has_field("address"));
        assert!(schema.has_field("address.city"));
        assert!(!schema.has_field("missing"));

        // Test get field by path
        let city = schema.get_field_by_path("address.city").unwrap();
        assert_eq!(city.name(), "city");
    }

    #[test]
    fn leading_root_segment_resolves_to_child_when_root_has_such_a_child() {
        // When the root struct has a child sharing the root's own name, a leading
        // "root" segment must refer to that child, not be stripped as the root.
        let schema = CoreSchemaBuilder::new()
            .with_root(SchemaNode::Struct {
                name: "root".to_string(),
                nullable: false,
                fields: vec![SchemaNode::Primitive {
                    name: "root".to_string(),
                    primitive_type: PrimitiveType::Int64,
                    nullable: false,
                    format: None,
                }],
            })
            .build()
            .unwrap();

        let resolved = schema.get_field_by_path("root").unwrap();
        assert!(matches!(resolved, SchemaNode::Primitive { .. }));
        assert_eq!(resolved.name(), "root");
    }
}
