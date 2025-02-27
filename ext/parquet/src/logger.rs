// Logger module for Parquet gem
// Provides a Rust wrapper for Ruby logger objects

use std::str::FromStr;

use magnus::{exception::runtime_error, value::ReprValue, Error as MagnusError, Ruby, Value};

use crate::{reader::ReaderError, utils::parse_string_or_symbol};

/// Severity levels that match Ruby's Logger levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl FromStr for LogLevel {
    type Err = MagnusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warn" => LogLevel::Warn,
            "error" => LogLevel::Error,
            "fatal" => LogLevel::Fatal,
            _ => {
                return Err(MagnusError::new(
                    runtime_error(),
                    format!("Invalid log level: {}", s),
                ))
            }
        })
    }
}
/// A wrapper around a Ruby logger object
#[derive(Debug, Clone)]
pub struct RubyLogger {
    logger: Option<Value>,
    level: LogLevel,
}

#[allow(dead_code)]
impl RubyLogger {
    pub fn new(ruby: &Ruby, logger_value: Option<Value>) -> Result<Self, ReaderError> {
        let environment_level = std::env::var("PARQUET_GEM_LOG_LEVEL")
            .unwrap_or_else(|_| "warn".to_string())
            .parse::<LogLevel>()
            .unwrap_or(LogLevel::Warn);

        match logger_value {
            Some(logger) => {
                if logger.is_nil() {
                    return Ok(Self {
                        logger: None,
                        level: environment_level,
                    });
                }

                let level_value = logger.funcall::<_, _, Value>("level", ())?;
                let level = parse_string_or_symbol(ruby, level_value)?;
                let level = level
                    .map(|s| s.parse::<LogLevel>())
                    .transpose()?
                    .unwrap_or(environment_level);

                Ok(Self {
                    logger: Some(logger),
                    level,
                })
            }
            None => Ok(Self {
                logger: None,
                level: environment_level,
            }),
        }
    }

    /// Log a message at the given level
    pub fn log(&self, level: LogLevel, message: &str) -> Result<(), MagnusError> {
        let method = match level {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
            LogLevel::Fatal => "fatal",
        };

        match self.logger {
            Some(logger) => {
                logger.funcall::<_, _, Value>(method, (message,))?;
            }
            None => eprintln!("{}", message),
        }

        Ok(())
    }

    /// Log a debug message
    pub fn debug<F, S>(&self, message_fn: F) -> Result<(), MagnusError>
    where
        F: FnOnce() -> S,
        S: AsRef<str>,
    {
        if self.level <= LogLevel::Debug {
            let message = message_fn();
            self.log(LogLevel::Debug, message.as_ref())
        } else {
            Ok(())
        }
    }

    /// Log an info message
    pub fn info<F, S>(&self, message_fn: F) -> Result<(), MagnusError>
    where
        F: FnOnce() -> S,
        S: AsRef<str>,
    {
        if self.level <= LogLevel::Info {
            let message = message_fn();
            self.log(LogLevel::Info, message.as_ref())
        } else {
            Ok(())
        }
    }

    /// Log a warning message
    pub fn warn<F, S>(&self, message_fn: F) -> Result<(), MagnusError>
    where
        F: FnOnce() -> S,
        S: AsRef<str>,
    {
        if self.level <= LogLevel::Warn {
            let message = message_fn();
            self.log(LogLevel::Warn, message.as_ref())
        } else {
            Ok(())
        }
    }

    /// Log an error message
    pub fn error<F, S>(&self, message_fn: F) -> Result<(), MagnusError>
    where
        F: FnOnce() -> S,
        S: AsRef<str>,
    {
        if self.level <= LogLevel::Error {
            let message = message_fn();
            self.log(LogLevel::Error, message.as_ref())
        } else {
            Ok(())
        }
    }

    /// Log a fatal message
    pub fn fatal<F, S>(&self, message_fn: F) -> Result<(), MagnusError>
    where
        F: FnOnce() -> S,
        S: AsRef<str>,
    {
        if self.level <= LogLevel::Fatal {
            let message = message_fn();
            self.log(LogLevel::Fatal, message.as_ref())
        } else {
            Ok(())
        }
    }
}
