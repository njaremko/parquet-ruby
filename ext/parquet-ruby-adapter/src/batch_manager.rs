/// Default constants for batch processing
pub const SAMPLE_SIZE: usize = 100;
pub const MIN_BATCH_SIZE: usize = 10;
pub const INITIAL_BATCH_SIZE: usize = 100;
pub const DEFAULT_MEMORY_THRESHOLD: usize = 64 * 1024 * 1024;

/// Manages dynamic batch sizing based on memory usage
pub struct BatchSizeManager {
    pub fixed_batch_size: Option<usize>,
    pub memory_threshold: usize,
    pub sample_size: usize,
    pub row_size_samples: Vec<usize>,
    pub current_batch_size: usize,
    pub rows_processed: usize,
    pub recent_row_sizes: Vec<usize>,
}

impl BatchSizeManager {
    pub fn new(
        fixed_batch_size: Option<usize>,
        memory_threshold: Option<usize>,
        sample_size: Option<usize>,
    ) -> Self {
        Self {
            fixed_batch_size,
            memory_threshold: memory_threshold.unwrap_or(DEFAULT_MEMORY_THRESHOLD),
            sample_size: sample_size.unwrap_or(SAMPLE_SIZE),
            row_size_samples: Vec::new(),
            current_batch_size: fixed_batch_size.unwrap_or(INITIAL_BATCH_SIZE),
            rows_processed: 0,
            recent_row_sizes: Vec::with_capacity(10),
        }
    }

    pub fn record_row_size(&mut self, size: usize) {
        self.rows_processed += 1;

        // Always track recent row sizes for current memory estimation
        if self.recent_row_sizes.len() >= 10 {
            self.recent_row_sizes.remove(0);
        }
        self.recent_row_sizes.push(size);

        // Sample for batch size calculation
        if self.row_size_samples.len() < self.sample_size {
            self.row_size_samples.push(size);

            // Recalculate batch size after enough samples
            if self.row_size_samples.len() >= MIN_BATCH_SIZE {
                self.recalculate_batch_size();
            }
        } else if self.rows_processed % 50 == 0 {
            // Periodically adjust based on recent data
            self.adjust_for_recent_sizes();
        }
    }

    pub fn average_row_size(&self) -> usize {
        if self.row_size_samples.is_empty() {
            1024 // Default estimate
        } else {
            self.row_size_samples.iter().sum::<usize>() / self.row_size_samples.len()
        }
    }

    pub fn recent_average_size(&self) -> usize {
        if self.recent_row_sizes.is_empty() {
            self.average_row_size()
        } else {
            self.recent_row_sizes.iter().sum::<usize>() / self.recent_row_sizes.len()
        }
    }

    fn adjust_for_recent_sizes(&mut self) {
        if self.fixed_batch_size.is_some() {
            return;
        }

        let recent_avg = self.recent_average_size();
        let overall_avg = self.average_row_size();

        // If recent rows are significantly different from the sample average, adjust
        if recent_avg > overall_avg * 2 || recent_avg < overall_avg / 2 {
            let target_memory = (self.memory_threshold as f64 * 0.8) as usize;
            self.current_batch_size = (target_memory / recent_avg).max(MIN_BATCH_SIZE);
        }
    }

    fn recalculate_batch_size(&mut self) {
        if self.fixed_batch_size.is_some() {
            return; // User specified fixed size
        }

        let avg_size = self.average_row_size();
        if avg_size > 0 {
            // Target 80% of memory threshold to leave headroom
            let target_memory = (self.memory_threshold as f64 * 0.8) as usize;
            self.current_batch_size = (target_memory / avg_size).max(MIN_BATCH_SIZE);
        }
    }

    pub fn should_flush(&self, batch_size: usize, current_batch_memory: usize) -> bool {
        if batch_size >= self.current_batch_size {
            return true;
        }

        // Use actual memory size if available, otherwise estimate
        let memory_usage = if current_batch_memory > 0 {
            current_batch_memory
        } else {
            batch_size * self.recent_average_size()
        };

        memory_usage >= self.memory_threshold
    }
}
