#![allow(unknown_lints)]
#![warn(clippy::all, rust_2018_idioms, rust_2018_compatibility)]
#![allow(clippy::rc_buffer)] // https://github.com/rust-lang/rust-clippy/issues/6170

mod artifact_cache;
mod asset_hub;
mod asset_hub_service;
mod capnp_db;
mod daemon;
mod error;
mod file_asset_source;
mod file_tracker;
mod serialized_asset;
mod source_pair_import;
mod watcher;

pub use crate::{
    daemon::{default_importer_contexts, default_importers, AssetDaemon, ImporterMap},
    error::{Error, Result},
};

#[cfg(debug_assertions)]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Debug;
#[cfg(not(debug_assertions))]
const DEFAULT_LOGGING_LEVEL: log::LevelFilter = log::LevelFilter::Info;

mod simple_logger {
    use log::{Level, Metadata, Record};

    pub struct SimpleLogger;

    impl log::Log for SimpleLogger {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() <= Level::Info
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                println!("{} - {}", record.level(), record.args());
            }
        }

        fn flush(&self) {}
    }
}
#[cfg(not(feature = "pretty_log"))]
static LOGGER: simple_logger::SimpleLogger = simple_logger::SimpleLogger;

#[cfg(not(feature = "pretty_log"))]
pub fn init_logging() -> Result<()> {
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "".to_string());
    let log_level = <log::LevelFilter as std::str::FromStr>::from_str(&rust_log)
        .unwrap_or(DEFAULT_LOGGING_LEVEL);
    log::set_logger(&LOGGER)
        .map(|()| log::set_max_level(log_level))
        .map_err(Error::SetLoggerError)
}
#[cfg(feature = "pretty_log")]
pub fn init_logging() -> Result<()> {
    use chrono::Local;
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "".to_string());
    let log_level = <log::LevelFilter as std::str::FromStr>::from_str(&rust_log)
        .unwrap_or(DEFAULT_LOGGING_LEVEL);
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{timestamp}][{level}][{target}] {message}",
                level = record.level(),
                timestamp = Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                target = record.target(),
                message = message,
            ))
        })
        .chain(std::io::stdout())
        .level(log_level)
        // .chain(fern::log_file("output.log")?)
        .apply()?;
    Ok(())
}
