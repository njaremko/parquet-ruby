use crate::{error::Result, RubyAdapterError};
use magnus::{value::ReprValue, IntoValue, Ruby, Value};

/// Trait for converting Rust values to Ruby values with error handling
///
/// This is similar to Magnus's `IntoValue` trait but allows for returning errors
/// instead of panicking or returning invalid values.
pub trait TryIntoValue: Sized {
    /// Convert `self` to a Ruby value with error handling
    fn try_into_value(self, handle: &Ruby) -> Result<Value>;

    /// Convert `self` to a Ruby value with error handling, using the Ruby runtime from the current thread
    fn try_into_value_with_current_thread(self) -> Result<Value> {
        let ruby =
            Ruby::get().map_err(|_| RubyAdapterError::runtime("Failed to get Ruby runtime"))?;
        self.try_into_value(&ruby)
    }
}

// Note: We don't provide a blanket implementation for all IntoValue types
// because some types may want to provide custom error handling.
// Types that need TryIntoValue should implement it explicitly.

// Convenience implementations for common types
impl TryIntoValue for String {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for &str {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for i32 {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for i64 {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for f32 {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for f64 {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl TryIntoValue for bool {
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        Ok(self.into_value_with(handle))
    }
}

impl<T> TryIntoValue for Vec<T>
where
    T: TryIntoValue,
{
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        let array = handle.ary_new();
        for item in self {
            let ruby_value = item.try_into_value(handle)?;
            array.push(ruby_value)?;
        }
        Ok(handle.into_value(array))
    }
}

impl<T> TryIntoValue for Option<T>
where
    T: TryIntoValue,
{
    fn try_into_value(self, handle: &Ruby) -> Result<Value> {
        match self {
            Some(value) => value.try_into_value(handle),
            None => Ok(handle.qnil().as_value()),
        }
    }
}
