//! Conversions between `serde_json::Value` and the protobuf well-known `Struct`/`Value` types,
//! so the gRPC API can carry structured data natively instead of JSON-encoded strings.

use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value as PValue};

/// serde_json::Value → protobuf `Value`.
pub fn json_to_pvalue(v: serde_json::Value) -> PValue {
    let kind = match v {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(b) => Kind::BoolValue(b),
        serde_json::Value::Number(n) => Kind::NumberValue(n.as_f64().unwrap_or(0.0)),
        serde_json::Value::String(s) => Kind::StringValue(s),
        serde_json::Value::Array(a) => Kind::ListValue(ListValue {
            values: a.into_iter().map(json_to_pvalue).collect(),
        }),
        serde_json::Value::Object(o) => Kind::StructValue(json_obj_to_struct(o)),
    };
    PValue { kind: Some(kind) }
}

/// serde_json object map → protobuf `Struct`.
pub fn json_obj_to_struct(o: serde_json::Map<String, serde_json::Value>) -> Struct {
    Struct {
        fields: o.into_iter().map(|(k, v)| (k, json_to_pvalue(v))).collect(),
    }
}

/// Any serde_json::Value → `Struct`. Objects map directly; a non-object is wrapped under `value`
/// so the field always carries a valid Struct (rows/events are expected to be objects).
pub fn json_to_struct(v: serde_json::Value) -> Struct {
    match v {
        serde_json::Value::Object(o) => json_obj_to_struct(o),
        other => {
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("value".to_string(), json_to_pvalue(other));
            Struct { fields }
        }
    }
}

/// protobuf `Value` → serde_json::Value.
pub fn pvalue_to_json(v: PValue) -> serde_json::Value {
    match v.kind {
        None | Some(Kind::NullValue(_)) => serde_json::Value::Null,
        Some(Kind::BoolValue(b)) => serde_json::Value::Bool(b),
        Some(Kind::NumberValue(n)) => serde_json::json!(n),
        Some(Kind::StringValue(s)) => serde_json::Value::String(s),
        Some(Kind::ListValue(l)) => {
            serde_json::Value::Array(l.values.into_iter().map(pvalue_to_json).collect())
        }
        Some(Kind::StructValue(s)) => struct_to_json(s),
    }
}

/// protobuf `Struct` → serde_json object value.
pub fn struct_to_json(s: Struct) -> serde_json::Value {
    serde_json::Value::Object(
        s.fields
            .into_iter()
            .map(|(k, v)| (k, pvalue_to_json(v)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_nested_json() {
        let v = serde_json::json!({
            "name": "alice",
            "age": 30,
            "tags": ["a", "b"],
            "active": true,
            "meta": { "k": null },
        });
        let back = struct_to_json(json_to_struct(v.clone()));
        // Numbers come back as floats (Struct has no int type); compare structurally otherwise.
        assert_eq!(back["name"], "alice");
        assert_eq!(back["tags"], serde_json::json!(["a", "b"]));
        assert_eq!(back["active"], true);
        assert_eq!(back["age"], 30.0);
        assert!(back["meta"]["k"].is_null());
    }

    #[test]
    fn non_object_wrapped_under_value() {
        let s = json_to_struct(serde_json::json!("hi"));
        assert_eq!(struct_to_json(s)["value"], "hi");
    }
}
