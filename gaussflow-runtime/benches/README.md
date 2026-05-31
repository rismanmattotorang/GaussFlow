# GaussFlow Runtime Benchmarks

This directory contains comprehensive benchmarks for the GaussFlow runtime components, designed to measure performance, scalability, and resource efficiency.

## Benchmarks Overview

### 1. Runtime Benchmark (`runtime_benchmark.rs`)
**Purpose**: Core runtime performance measurement
- Task spawning and execution performance
- Memory pool allocation/deallocation
- Basic throughput measurements

**Key Metrics**:
- Tasks per second
- Memory allocation efficiency
- Worker thread utilization

### 2. Scheduler Benchmark (`scheduler_benchmark.rs`)
**Purpose**: Work-stealing scheduler performance
- Task scheduling efficiency
- Work-stealing queue performance
- Multi-threaded scheduling patterns

**Key Metrics**:
- Scheduling throughput
- Work-stealing efficiency
- Load balancing effectiveness

### 3. Comprehensive Benchmark (`comprehensive_benchmark.rs`)
**Purpose**: End-to-end performance analysis
- Multiple component integration
- Stress testing scenarios
- Performance regression detection

**Key Metrics**:
- End-to-end throughput
- Resource utilization
- Scalability characteristics

## Running Benchmarks

### Prerequisites
- Rust 1.70+ installed
- Criterion benchmark framework
- Sufficient system resources
- Stable system environment

### Basic Execution
```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench runtime_benchmark
cargo bench --bench scheduler_benchmark
cargo bench --bench comprehensive_bench

# Run with specific parameters
cargo bench --bench comprehensive_bench -- --measurement-time 30
```

### Advanced Configuration
```bash
# Run with custom sample size
cargo bench --bench comprehensive_bench -- --sample-size 50

# Run with specific warm-up time
cargo bench --bench comprehensive_bench -- --warm-up-time 10

# Run with noise threshold
cargo bench --bench comprehensive_bench -- --noise-threshold 0.05
```

## Benchmark Categories

### 1. Task Spawning Performance
Measures the efficiency of creating and scheduling tasks:

```rust
fn benchmark_task_spawning(c: &mut Criterion) {
    for num_tasks in [100, 1_000, 10_000, 100_000] {
        // Measure task spawning performance
    }
}
```

**Key Metrics**:
- Tasks spawned per second
- Memory overhead per task
- Scheduling latency

### 2. Memory Pool Performance
Evaluates memory allocation efficiency:

```rust
fn benchmark_memory_pool(c: &mut Criterion) {
    for pool_size in [1_024, 10_240, 102_400, 1_048_576] {
        // Measure allocation/deallocation performance
    }
}
```

**Key Metrics**:
- Allocation throughput
- Memory fragmentation
- Pool efficiency

### 3. Scheduler Performance
Tests work-stealing scheduler efficiency:

```rust
fn benchmark_scheduler(c: &mut Criterion) {
    for num_tasks in [1_000, 10_000, 100_000] {
        // Measure scheduling performance
    }
}
```

**Key Metrics**:
- Scheduling throughput
- Work-stealing efficiency
- Load balancing quality

### 4. Steal Queue Performance
Evaluates work-stealing queue operations:

```rust
fn benchmark_steal_queue(c: &mut Criterion) {
    for num_tasks in [1_000, 10_000, 100_000] {
        // Measure push/pop and steal operations
    }
}
```

**Key Metrics**:
- Push/pop throughput
- Steal operation efficiency
- Queue contention

### 5. Batch Processing Performance
Measures batch processing efficiency:

```rust
fn benchmark_batch_processing(c: &mut Criterion) {
    for batch_size in [10, 100, 1_000] {
        // Measure batch processing performance
    }
}
```

**Key Metrics**:
- Batch processing throughput
- Batch size optimization
- Memory efficiency

### 6. Concurrent Access Performance
Tests concurrent access patterns:

```rust
fn benchmark_concurrent_access(c: &mut Criterion) {
    for num_threads in [2, 4, 8, 16] {
        // Measure concurrent access performance
    }
}
```

**Key Metrics**:
- Concurrent throughput
- Scalability characteristics
- Resource contention

## Performance Targets

### Throughput Targets
- **Task Spawning**: >100K tasks/second
- **Memory Allocation**: >1M allocations/second
- **Scheduling**: >50K tasks/second
- **Work Stealing**: >100K operations/second

### Latency Targets
- **Task Creation**: <1μs per task
- **Memory Allocation**: <100ns per allocation
- **Scheduling**: <10μs per task
- **Work Stealing**: <1μs per operation

### Scalability Targets
- **Linear Scaling**: Performance should scale linearly with CPU cores
- **Memory Efficiency**: <1KB overhead per task
- **Resource Utilization**: >80% CPU utilization under load

## Benchmark Configuration

### Criterion Configuration
```rust
criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(5));
    targets = 
        benchmark_task_spawning,
        benchmark_memory_pool,
        benchmark_scheduler,
        benchmark_steal_queue,
        benchmark_batch_processing,
        benchmark_concurrent_access
);
```

### Environment Variables
```bash
# Set number of CPU cores for testing
export RUSTFLAGS="-C target-cpu=native"
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_LTO=true
```

## Interpreting Results

### Performance Analysis
1. **Throughput**: Higher is better
2. **Latency**: Lower is better
3. **Scalability**: Should scale with resources
4. **Efficiency**: Resource usage vs. performance

### Regression Detection
```bash
# Compare with previous results
cargo bench --bench comprehensive_bench -- --save-baseline previous
cargo bench --bench comprehensive_bench -- --baseline previous
```

### Performance Profiling
```bash
# Generate flamegraph
cargo bench --bench comprehensive_bench -- --profile-time 30
```

## Benchmark Best Practices

### 1. Consistent Environment
- Use dedicated benchmark machine
- Disable power management
- Close unnecessary applications
- Use consistent system load

### 2. Statistical Significance
- Use adequate sample sizes
- Account for system noise
- Run multiple iterations
- Validate results

### 3. Resource Monitoring
```rust
// Monitor system resources during benchmarks
use sysinfo::{System, SystemExt};

let mut sys = System::new_all();
sys.refresh_all();
println!("CPU usage: {}%", sys.global_cpu_info().cpu_usage());
println!("Memory usage: {} MB", sys.used_memory() / 1024 / 1024);
```

### 4. Benchmark Isolation
- Run benchmarks in isolation
- Avoid interference from other processes
- Use consistent input data
- Control external factors

## Continuous Benchmarking

### Automated Benchmarking
```bash
# Run benchmarks in CI/CD
cargo bench --bench comprehensive_bench -- --output-format=json > results.json

# Compare with baseline
cargo bench --bench comprehensive_bench -- --baseline main --output-format=json
```

### Performance Regression Testing
```yaml
# GitHub Actions example
- name: Run Benchmarks
  run: |
    cargo bench --bench comprehensive_bench -- --save-baseline main
    cargo bench --bench comprehensive_bench -- --baseline main --output-format=json > comparison.json
```

### Benchmark Reporting
```bash
# Generate HTML reports
cargo bench --bench comprehensive_bench -- --output-format=html

# Generate CSV reports
cargo bench --bench comprehensive_bench -- --output-format=csv
```

## Troubleshooting

### Common Issues

1. **High Variance**: Increase sample size or measurement time
2. **System Noise**: Use dedicated benchmark environment
3. **Memory Pressure**: Monitor system memory usage
4. **CPU Throttling**: Disable power management features

### Debugging Tips

1. **Enable Debug Logging**:
```rust
use tracing::Level;
tracing_subscriber::fmt()
    .with_max_level(Level::DEBUG)
    .init();
```

2. **Profile Memory Usage**:
```rust
use memory_stats::memory_stats;
let mem_stats = memory_stats().unwrap();
println!("Memory usage: {} MB", mem_stats.physical_mem / 1024 / 1024);
```

3. **Monitor System Resources**:
```bash
# Monitor during benchmark execution
htop
iotop
```

## Contributing

When adding new benchmarks:

1. Follow existing naming conventions
2. Include comprehensive documentation
3. Use appropriate sample sizes
4. Add performance assertions
5. Test with different configurations
6. Update this README

## Performance Optimization

### Code Optimization
```rust
// Use optimized configurations
let config = OptimizedExecutorConfig {
    num_workers: num_cpus::get(),
    enable_batching: true,
    enable_work_stealing: true,
    memory_pool_size: 1024 * 1024,
    ..Default::default()
};
```

### System Optimization
```bash
# Optimize for benchmarking
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
echo 0 | sudo tee /proc/sys/kernel/numa_balancing
```

## Next Steps

After running benchmarks:

1. Analyze performance characteristics
2. Identify bottlenecks
3. Optimize critical paths
4. Monitor for regressions
5. Share results with the community 