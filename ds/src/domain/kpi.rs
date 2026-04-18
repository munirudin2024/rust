#[derive(Debug, Clone)]
pub struct KpiDimension {
    pub dimension:     String,
    pub score:         f64,
    pub total_checked: usize,
    pub passed:        usize,
    pub failed:        usize,
    pub notes:         String,
}