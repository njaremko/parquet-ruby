use magnus::{IntoValue, Ruby, Value};
use parquet_ruby_adapter::{Result, RubyAdapterError, TryIntoValue};

/// Example struct that can fail during conversion to Ruby
struct ComplexData {
    name: String,
    values: Vec<i32>,
    metadata: std::collections::HashMap<String, String>,
}

impl TryIntoValue for ComplexData {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        let hash = handle.hash_new();

        // Set name
        hash.aset("name", self.name)
            .map_err(|e| RubyAdapterError::type_conversion(format!("Failed to set name: {}", e)))?;

        // Convert values array
        let values_array = handle.ary_new();
        for value in self.values {
            values_array.push(value).map_err(|e| {
                RubyAdapterError::type_conversion(format!("Failed to push value: {}", e))
            })?;
        }
        hash.aset("values", values_array).map_err(|e| {
            RubyAdapterError::type_conversion(format!("Failed to set values: {}", e))
        })?;

        // Convert metadata hash
        let metadata_hash = handle.hash_new();
        for (key, value) in self.metadata {
            metadata_hash.aset(key.clone(), value).map_err(|e| {
                RubyAdapterError::type_conversion(format!(
                    "Failed to set metadata key {}: {}",
                    key, e
                ))
            })?;
        }
        hash.aset("metadata", metadata_hash).map_err(|e| {
            RubyAdapterError::type_conversion(format!("Failed to set metadata: {}", e))
        })?;

        Ok(handle.into_value(hash))
    }
}

// Example of a type that might fail validation during conversion
struct ValidatedNumber {
    value: i32,
}

impl TryIntoValue for ValidatedNumber {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        // Validate the number is positive
        if self.value < 0 {
            return Err(RubyAdapterError::type_conversion(format!(
                "ValidatedNumber must be positive, got {}",
                self.value
            )));
        }

        // If valid, convert to Ruby
        Ok(self.value.into_value_with(handle))
    }
}

fn main() -> Result<()> {
    // Example usage:
    let ruby = Ruby::get().map_err(|_| RubyAdapterError::runtime("Failed to get Ruby runtime"))?;

    // Success case
    let data = ComplexData {
        name: "example".to_string(),
        values: vec![1, 2, 3],
        metadata: std::collections::HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]),
    };

    let _ruby_value = data.try_into_value(&ruby)?;
    println!("Successfully converted ComplexData to Ruby value");

    // Validation failure case
    let invalid_number = ValidatedNumber { value: -5 };
    match invalid_number.try_into_value(&ruby) {
        Ok(_) => println!("This shouldn't happen"),
        Err(e) => println!("Expected validation error: {}", e),
    }

    // Using the convenience method
    let valid_number = ValidatedNumber { value: 42 };
    let _ruby_value = valid_number.try_into_value_with_current_thread()?;
    println!("Successfully converted ValidatedNumber to Ruby value");

    Ok(())
}
