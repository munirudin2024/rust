//! Data Imputation Strategies
//! Methods untuk mengisi missing values dengan berbagai strategi

pub mod strategies;
pub mod confidence;
pub mod fallback;

pub use strategies::{select_imputation_method, ColumnType, ImputationStrategy};
pub use confidence::ConfidenceScore;
pub use fallback::FallbackStrategy;

/// Trait untuk imputation method
pub trait Imputer {
    fn impute(&self, values: &[Option<f64>]) -> Vec<f64>;
    fn confidence(&self) -> ConfidenceScore;
}
