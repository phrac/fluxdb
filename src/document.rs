use serde_json::Value;
use uuid::Uuid;

/// A document is a JSON object with a unique ID.
///
/// Internally stores only the pre-serialized JSON bytes (including `_id`).
/// The `Value` tree is parsed on demand from raw bytes — this cuts per-document
/// memory by ~60-80 % compared to keeping a live `serde_json::Value` tree.
#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    /// Pre-serialized JSON bytes (including _id). This is the single source of truth.
    raw: Box<[u8]>,
}

impl Document {
    /// Create a new document from a JSON value.
    /// If the value contains an `_id` field, it is used; otherwise a UUID is generated.
    pub fn new(mut value: Value) -> Self {
        let id = match value.get("_id").and_then(|v| v.as_str()) {
            Some(existing) => existing.to_string(),
            None => {
                // Remove non-string _id if present
                if let Some(obj) = value.as_object_mut() {
                    obj.remove("_id");
                }
                let id = Uuid::new_v4().to_string();
                if let Some(obj) = value.as_object_mut() {
                    obj.insert("_id".to_string(), Value::String(id.clone()));
                }
                id
            }
        };

        let raw = serde_json::to_vec(&value)
            .expect("Document::new: serde_json::to_vec failed on valid Value")
            .into_boxed_slice();
        Document { id, raw }
    }

    /// Create a document from an explicit ID and data (which may or may not contain `_id`).
    /// Used during WAL replay where the ID is stored separately.
    pub fn with_id(id: String, mut data: Value) -> Self {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("_id".to_string(), Value::String(id.clone()));
        }
        let raw = serde_json::to_vec(&data)
            .expect("Document::with_id: serde_json::to_vec failed on valid Value")
            .into_boxed_slice();
        Document { id, raw }
    }

    /// Create a document from pre-serialized JSON bytes.
    /// The bytes **must** be valid JSON containing an `_id` field matching `id`.
    /// Used on the fast WAL replay path to skip the Value intermediate entirely.
    #[inline]
    pub fn from_raw_parts(id: String, raw: Box<[u8]>) -> Self {
        Document { id, raw }
    }

    /// Get a field value by dot-notation path (e.g., "address.city", "_id").
    /// Parses the raw bytes on each call — callers that need multiple fields
    /// should use [`to_value`] once and query the returned Value instead.
    pub fn get_field(&self, path: &str) -> Option<Value> {
        let data: Value = serde_json::from_slice(&self.raw).ok()?;
        get_nested_field(&data, path).cloned()
    }

    /// Convert to a full JSON value including the _id field.
    /// Parses from the raw byte cache.
    #[inline]
    pub fn to_value(&self) -> Value {
        serde_json::from_slice(&self.raw)
            .expect("Document::to_value: raw bytes are invalid JSON")
    }

    /// Return pre-serialized JSON bytes (zero-copy reference).
    #[inline]
    pub fn raw_bytes(&self) -> &[u8] {
        &self.raw
    }

    /// Replace the document data, preserving the ID and rebuilding the byte cache.
    pub fn set_data(&mut self, mut data: Value) {
        if let Some(obj) = data.as_object_mut() {
            obj.insert("_id".to_string(), Value::String(self.id.clone()));
        }
        self.raw = serde_json::to_vec(&data)
            .expect("Document::set_data: serde_json::to_vec failed on valid Value")
            .into_boxed_slice();
    }

    /// Return data without the `_id` field (for WAL storage).
    pub fn data_without_id(&self) -> Value {
        match self.to_value() {
            Value::Object(mut map) => {
                map.remove("_id");
                Value::Object(map)
            }
            other => other,
        }
    }
}

/// Navigate a parsed JSON value by dot-notation path.
/// Returns a reference into the value tree.
pub fn get_nested_field<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in path.split('.') {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_document_generates_id() {
        let doc = Document::new(json!({"name": "Alice"}));
        assert!(!doc.id.is_empty());
        assert_eq!(doc.get_field("name").unwrap(), "Alice");
        // _id should be accessible via get_field
        assert_eq!(doc.get_field("_id").unwrap(), doc.id.as_str());
    }

    #[test]
    fn test_new_document_preserves_id() {
        let doc = Document::new(json!({"_id": "custom-id", "name": "Bob"}));
        assert_eq!(doc.id, "custom-id");
        assert_eq!(doc.get_field("name").unwrap(), "Bob");
        assert_eq!(doc.get_field("_id").unwrap(), "custom-id");
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

    #[test]
    fn test_raw_bytes_valid_json() {
        let doc = Document::new(json!({"_id": "r1", "val": 42}));
        let parsed: Value = serde_json::from_slice(doc.raw_bytes()).unwrap();
        assert_eq!(parsed["_id"], "r1");
        assert_eq!(parsed["val"], 42);
    }

    #[test]
    fn test_with_id_adds_id() {
        let doc = Document::with_id("w1".into(), json!({"x": 1}));
        assert_eq!(doc.id, "w1");
        assert_eq!(doc.get_field("_id").unwrap(), "w1");
        assert_eq!(doc.get_field("x").unwrap(), 1);
    }

    #[test]
    fn test_set_data_rebuilds_raw() {
        let mut doc = Document::new(json!({"_id": "s1", "v": 1}));
        doc.set_data(json!({"v": 2}));
        assert_eq!(doc.get_field("v").unwrap(), 2);
        assert_eq!(doc.get_field("_id").unwrap(), "s1");
        let parsed: Value = serde_json::from_slice(doc.raw_bytes()).unwrap();
        assert_eq!(parsed["v"], 2);
        assert_eq!(parsed["_id"], "s1");
    }

    #[test]
    fn test_from_raw_parts() {
        let raw = serde_json::to_vec(&json!({"_id": "rp1", "x": 42}))
            .unwrap()
            .into_boxed_slice();
        let doc = Document::from_raw_parts("rp1".into(), raw);
        assert_eq!(doc.id, "rp1");
        assert_eq!(doc.get_field("x").unwrap(), 42);
        assert_eq!(doc.to_value()["_id"], "rp1");
    }

    #[test]
    fn test_get_nested_field() {
        let val = json!({"a": {"b": {"c": 42}}});
        assert_eq!(get_nested_field(&val, "a.b.c"), Some(&json!(42)));
        assert_eq!(get_nested_field(&val, "a.b"), Some(&json!({"c": 42})));
        assert_eq!(get_nested_field(&val, "a.x"), None);
    }
}
