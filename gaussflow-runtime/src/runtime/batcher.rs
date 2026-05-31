//! Batching of operations for improved throughput.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::error::{Result, RuntimeError};

/// A batcher that groups multiple operations into batches for more efficient processing.
pub struct Batcher<Input, Output> {
    /// Maximum number of items per batch.
    max_batch_size: usize,
    /// Maximum time to wait before processing a batch.
    max_wait_time: Duration,
    /// Pending operations waiting to be batched.
    pending: Arc<Mutex<VecDeque<PendingOp<Input, Output>>>>,
    /// The last time a batch was processed.
    last_batch_time: Arc<Mutex<Instant>>,
    /// Waker to notify when the batch should be processed.
    waker: Arc<Mutex<Option<Waker>>>,
}

/// A pending operation waiting to be batched.
struct PendingOp<Input, Output> {
    /// The input to process.
    input: Input,
    /// Channel to send the result back to the caller.
    sender: oneshot::Sender<Result<Output>>,
}

impl<Input, Output> Batcher<Input, Output>
where
    Input: Send + 'static,
    Output: Send + 'static,
{
    /// Create a new batcher with the given configuration.
    pub fn new(max_batch_size: usize, max_wait_time: Duration) -> Self {
        Self {
            max_batch_size,
            max_wait_time,
            pending: Arc::new(Mutex::new(VecDeque::new())),
            last_batch_time: Arc::new(Mutex::new(Instant::now())),
            waker: Arc::new(Mutex::new(None)),
        }
    }

    /// Submit an operation to be batched.
    pub async fn submit(&self, input: Input) -> Result<Output> {
        let (sender, receiver) = oneshot::channel();
        let pending_op = PendingOp { input, sender };
        
        // Add to pending operations
        let should_schedule = {
            let mut pending = self.pending.lock();
            pending.push_back(pending_op);
            
            // Check if we've reached the batch size
            pending.len() >= self.max_batch_size
        };
        
        // Wake the batch processor if we've reached the batch size
        if should_schedule {
            if let Some(waker) = self.waker.lock().take() {
                waker.wake();
            }
        }
        
        // Wait for the result
        receiver.await.map_err(|_| RuntimeError::new(
            super::error::ErrorKind::InternalError,
            "Batch processor dropped the receiver"
        ))?
    }
    
    /// Get the next batch of operations to process.
    /// 
    /// This will wait until either:
    /// 1. The maximum batch size is reached, or
    /// 2. The maximum wait time has elapsed since the last batch
    pub async fn next_batch(&self) -> Vec<Input> {
        BatchFuture {
            batcher: self,
            started: false,
        }
        .await
    }
    
    /// Complete a batch of operations with the given results.
    /// 
    /// The results should be in the same order as the inputs.
    pub fn complete_batch(&self, results: Vec<Result<Output>>) {
        let mut pending = self.pending.lock();
        *self.last_batch_time.lock() = Instant::now();
        
        // Take up to the number of results from the front of the queue
        let count = results.len().min(pending.len());
        let batch = pending.drain(..count);
        
        // Send the results
        for (op, result) in batch.zip(results) {
            let _ = op.sender.send(result);
        }
    }
}

/// Future that resolves when a batch is ready to be processed.
struct BatchFuture<'a, Input, Output> {
    batcher: &'a Batcher<Input, Output>,
    started: bool,
}

impl<'a, Input, Output> Future for BatchFuture<'a, Input, Output> {
    type Output = Vec<Input>;
    
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        
        // Check if we should process a batch now
        let now = Instant::now();
        let last_batch_time = *this.batcher.last_batch_time.lock();
        let time_since_last_batch = now.duration_since(last_batch_time);
        
        let mut pending = this.batcher.pending.lock();
        
        // Check if we should process a batch now
        if pending.len() >= this.batcher.max_batch_size || 
           (!this.started && time_since_last_batch >= this.batcher.max_wait_time) {
            // Take all pending operations up to the max batch size
            let batch_size = pending.len().min(this.batcher.max_batch_size);
            let batch = pending.drain(..batch_size).collect::<Vec<_>>();
            
            // Extract the inputs and save the senders
            let (inputs, senders): (Vec<_>, Vec<_>) = batch.into_iter()
                .map(|op| (op.input, op.sender))
                .unzip();
            
            // Store the senders so we can send the results later
            *this.batcher.last_batch_time.lock() = now;
            
            return Poll::Ready(inputs);
        }
        
        // Not ready yet, store the waker
        *this.batcher.waker.lock() = Some(cx.waker().clone());
        this.started = true;
        
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_batching() {
        let batcher = Arc::new(Batcher::<usize, usize>::new(
            3, // max batch size
            Duration::from_millis(100), // max wait time
        ));
        
        let batcher_clone = batcher.clone();
        let processor = tokio::spawn(async move {
            // Process batches in a loop
            loop {
                let batch = batcher_clone.next_batch().await;
                if batch.is_empty() {
                    break;
                }
                
                // Process the batch
                let results = batch.into_iter().map(|x| Ok(x * 2)).collect();
                batcher_clone.complete_batch(results);
            }
        });
        
        // Submit some operations
        let results = futures::future::join_all(vec![
            batcher.submit(1),
            batcher.submit(2),
            batcher.submit(3),
            batcher.submit(4),
            batcher.submit(5),
        ]).await;
        
        // Check the results
        let results: Vec<_> = results.into_iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(results, vec![2, 4, 6, 8, 10]);
        
        // Shut down the processor
        batcher.complete_batch(Vec::new());
        processor.await.unwrap();
    }
    
    #[tokio::test]
    async fn test_timeout() {
        let batcher = Arc::new(Batcher::<usize, usize>::new(
            10, // max batch size (won't be reached)
            Duration::from_millis(50), // max wait time
        ));
        
        let batcher_clone = batcher.clone();
        let processor = tokio::spawn(async move {
            let start = Instant::now();
            
            // First batch should trigger by timeout
            let batch = batcher_clone.next_batch().await;
            assert_eq!(batch, vec![1]);
            batcher_clone.complete_batch(vec![Ok(100)]);
            
            // Should have waited at least the timeout
            assert!(start.elapsed() >= Duration::from_millis(50));
            
            // Second batch should also trigger by timeout
            let batch = batcher_clone.next_batch().await;
            assert_eq!(batch, vec![2]);
            batcher_clone.complete_batch(vec![Ok(200)]);
        });
        
        // Submit one operation
        let result1 = batcher.submit(1);
        
        // Wait for the first batch to process
        assert_eq!(result1.await.unwrap(), 100);
        
        // Submit another operation after a delay
        tokio::time::sleep(Duration::from_millis(10)).await;
        let result2 = batcher.submit(2);
        
        // Wait for the second batch to process
        assert_eq!(result2.await.unwrap(), 200);
        
        processor.await.unwrap();
    }
    
    #[tokio::test]
    async fn test_concurrent_submits() {
        let batcher = Arc::new(Batcher::<usize, usize>::new(
            10, // max batch size
            Duration::from_millis(100), // max wait time
        ));
        
        let batcher_clone = batcher.clone();
        let processor = tokio::spawn(async move {
            let mut total = 0;
            
            for _ in 0..10 {
                let batch = batcher_clone.next_batch().await;
                if batch.is_empty() {
                    break;
                }
                
                total += batch.len();
                let results = batch.into_iter().map(|x| Ok(x)).collect();
                batcher_clone.complete_batch(results);
            }
            
            assert_eq!(total, 100);
        });
        
        // Submit 100 operations from multiple tasks
        let mut handles = vec![];
        for i in 0..100 {
            let batcher = batcher.clone();
            handles.push(tokio::spawn(async move {
                batcher.submit(i).await.unwrap()
            }));
        }
        
        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Shut down the processor
        batcher.complete_batch(Vec::new());
        processor.await.unwrap();
    }
}
