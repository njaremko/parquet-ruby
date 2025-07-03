use magnus::value::ReprValue;
use magnus::{Error as MagnusError, Value};

pub struct RubyLogger {
    logger: Option<Value>,
}

impl RubyLogger {
    pub fn new(logger: Option<Value>) -> Result<Self, MagnusError> {
        // Validate logger has required methods if provided
        if let Some(ref log) = logger {
            for method in &["debug", "info", "warn", "error"] {
                if !log.respond_to(*method, false)? {
                    return Err(MagnusError::new(
                        magnus::exception::arg_error(),
                        format!("Logger must respond to {}", method),
                    ));
                }
            }
        }
        Ok(Self { logger })
    }

    pub fn debug<F: FnOnce() -> String>(&self, msg_fn: F) -> Result<(), MagnusError> {
        if let Some(ref logger) = self.logger {
            logger.funcall::<_, _, Value>("debug", (msg_fn(),))?;
        }
        Ok(())
    }

    pub fn info<F: FnOnce() -> String>(&self, msg_fn: F) -> Result<(), MagnusError> {
        if let Some(ref logger) = self.logger {
            logger.funcall::<_, _, Value>("info", (msg_fn(),))?;
        }
        Ok(())
    }

    pub fn warn<F: FnOnce() -> String>(&self, msg_fn: F) -> Result<(), MagnusError> {
        if let Some(ref logger) = self.logger {
            logger.funcall::<_, _, Value>("warn", (msg_fn(),))?;
        }
        Ok(())
    }

    pub fn error<F: FnOnce() -> String>(&self, msg_fn: F) -> Result<(), MagnusError> {
        if let Some(ref logger) = self.logger {
            logger.funcall::<_, _, Value>("error", (msg_fn(),))?;
        }
        Ok(())
    }

    pub fn inner(&self) -> Option<Value> {
        self.logger
    }
}

// Make RubyLogger cloneable for passing to multiple functions
impl Clone for RubyLogger {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger,
        }
    }
}
