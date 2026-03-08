use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// A document is a JSON object with a unique ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    #[serde(rename = "_id")]
    pub id: String,
    #[serde(flatten)]
    pub data: Value,
}

impl Document {
    /// Create a new document from a JSON value.
    /// If the value contains an `_id` field, it is used; otherwise a UUID is generated.
    pub fn new(mut value: Value) -> Self {
        let id = match value.get("_id").and_then(|v| v.as_str()) {
            Some(existing) => {
                let id = existing.to_string();
                // Remove _id from the data so it doesn't duplicate under flatten
                if let Some(obj) = value.as_object_mut() {
                    obj.remove("_id");
                }
                id
            }
            None => {
                // Remove _id if it exists but isn't a string
                if let Some(obj) = value.as_object_mut() {
                    obj.remove("_id");
                }
                Uuid::new_v4().to_string()
            }
        };

        Document { id, data: value }
    }

    /// Get a field value by dot-notation path (e.g., "address.city").
    pub fn get_field(&self, path: &str) -> Option<&Value> {
        if path == "_id" {
            return None; // _id is handled separately
        }

        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &self.data;

        for part in &parts {
            match current {
                Value::Object(map) => {
                    current = map.get(*part)?;
                }
                Value::Array(arr) => {
                    let index: usize = part.parse().ok()?;
                    current = arr.get(index)?;
                }
                _ => return None,
            }
        }

        Some(current)
    }

    /// Convert to a full JSON value including the _id field.
    pub fn to_value(&self) -> Value {
        let mut obj = match &self.data {
            Value::Object(map) => map.clone(),
            _ => serde_json::Map::new(),
        };
        obj.insert("_id".to_string(), Value::String(self.id.clone()));
        Value::Object(obj)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_document_generates_id() {
        let doc = Document::new(json!({"name": "Alice"}));
        assert!(!doc.id.is_empty());
        assert_eq!(doc.get_field("name").unwrap(), "Alice");
    }

    #[test]
    fn test_new_document_preserves_id() {
        let doc = Document::new(json!({"_id": "custom-id", "name": "Bob"}));
        assert_eq!(doc.id, "custom-id");
        assert_eq!(doc.get_field("name").unwrap(), "Bob");
    }

    #[test]
    fn test_dot_notation() {
        let doc = Document::new(json!({
            "address": {
                "city": "Portland",
                "zip": "97201"
            }
        }));
        assert_eq!(doc.get_field("address.city").unwrap(), "Portland");
        assert_eq!(doc.get_field("address.zip").unwrap(), "97201");
    }

    #[test]
    fn test_array_indexing() {
        let doc = Document::new(json!({
            "tags": ["rust", "database", "nosql"]
        }));
        assert_eq!(doc.get_field("tags.1").unwrap(), "database");
    }

    #[test]
    fn test_to_value_includes_id() {
        let doc = Document::new(json!({"_id": "test", "x": 1}));
        let val = doc.to_value();
        assert_eq!(val["_id"], "test");
        assert_eq!(val["x"], 1);
    }
}
