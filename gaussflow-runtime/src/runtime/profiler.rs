//! Memory and performance profiling for the runtime.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

/// Global memory tracker
pub struct MemoryProfiler {
    pub allocated: AtomicUsize,
    pub deallocated: AtomicUsize,
    pub peak: AtomicUsize,
}

impl MemoryProfiler {
    pub const fn new() -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            deallocated: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    pub fn allocated(&self, size: usize) {
        let allocated = self.allocated.fetch_add(size, Ordering::Relaxed) + size;
        self.peak.fetch_max(allocated, Ordering::Relaxed);
    }

    pub fn deallocated(&self, size: usize) {
        self.deallocated.fetch_add(size, Ordering::Relaxed);
    }

    pub fn current_usage(&self) -> usize {
        self.allocated.load(Ordering::Relaxed) - self.deallocated.load(Ordering::Relaxed)
    }

    pub fn peak_usage(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

/// Global memory profiler instance
pub static MEMORY_PROFILER: MemoryProfiler = MemoryProfiler::new();

/// Custom allocator that tracks memory usage
pub struct ProfilingAllocator;

unsafe impl GlobalAlloc for ProfilingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if !ptr.is_null() {
            MEMORY_PROFILER.allocated(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        MEMORY_PROFILER.deallocated(layout.size());
        System.dealloc(ptr, layout);
    }
}

/// Performance profiler for tracking execution times
pub struct PerformanceProfiler {
    start: Instant,
    pub total_duration: Duration,
    pub call_count: u64,
}

impl PerformanceProfiler {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            total_duration: Duration::default(),
            call_count: 0,
        }
    }

    pub fn start(&mut self) {
        self.start = Instant::now();
    }

    pub fn stop(&mut self) -> Duration {
        let duration = self.start.elapsed();
        self.total_duration += duration;
        self.call_count += 1;
        duration
    }

    pub fn avg_duration(&self) -> Duration {
        if self.call_count > 0 {
            self.total_duration / self.call_count
        } else {
            Duration::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::alloc::{alloc, dealloc, Layout};

    #[test]
    fn test_memory_profiler() {
        let profiler = MemoryProfiler::new();
        
        let layout = Layout::new::<u64>();
        unsafe {
            let ptr = alloc(layout) as *mut u64;
            profiler.allocated(layout.size());
            
            assert_eq!(profiler.current_usage(), layout.size());
            
            dealloc(ptr as *mut u8, layout);
            profiler.deallocated(layout.size());
            
            assert_eq!(profiler.current_usage(), 0);
        }
    }

    #[test]
    fn test_performance_profiler() {
        let mut profiler = PerformanceProfiler::new();
        
        profiler.start();
        std::thread::sleep(Duration::from_millis(10));
        let duration = profiler.stop();
        
        assert!(duration >= Duration::from_millis(10));
        assert_eq!(profiler.call_count, 1);
        assert!(profiler.avg_duration() >= Duration::from_millis(10));
    }
}
