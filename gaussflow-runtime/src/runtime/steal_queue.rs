//! A work-stealing queue for distributing tasks among worker threads.

use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;

/// A work-stealing queue implementation.
///
/// This is a lock-free, concurrent queue that allows multiple workers to push and pop tasks,
/// with support for stealing tasks from other workers' queues.
pub struct StealQueue<T> {
    /// The inner queue data.
    inner: Arc<CachePadded<Inner<T>>>,
    /// Phantom data to mark the queue as send and sync.
    _marker: PhantomData<Box<[MaybeUninit<T>>]>>,
}

/// Inner data for the work-stealing queue.
struct Inner<T> {
    /// The buffer containing the queue elements.
    buffer: *mut T,
    /// The capacity of the buffer.
    capacity: usize,
    /// The front of the queue (owned by the worker).
    front: AtomicIsize,
    /// The back of the queue (owned by the worker).
    back: CachePadded<AtomicIsize>,
    /// The array of stealers that can steal from this queue.
    stealers: CachePadded<AtomicPtr<AtomicIsize>>,
}

/// A handle to a work-stealing queue that can be used to steal tasks.
pub struct Stealer<T> {
    /// The inner queue data.
    inner: Arc<CachePadded<Inner<T>>>,
}

/// A handle to a work-stealing queue that can be used to push and pop tasks.
pub struct Worker<T> {
    /// The inner queue data.
    inner: Arc<CachePadded<Inner<T>>>,
    /// The index of the next element to pop.
    front: isize,
    /// The index of the next element to push.
    back: isize,
}

// Safety: The queue is thread-safe as long as T is Send.
unsafe impl<T: Send> Send for StealQueue<T> {}
unsafe impl<T: Send> Sync for StealQueue<T> {}
unsafe impl<T: Send> Send for Stealer<T> {}
unsafe impl<T: Send> Sync for Stealer<T> {}
unsafe impl<T: Send> Send for Worker<T> {}

impl<T> StealQueue<T> {
    /// Creates a new work-stealing queue with the given capacity.
    ///
    /// The capacity must be a power of two.
    pub fn new(capacity: usize) -> (Worker<T>, Stealer<T>) {
        assert!(capacity > 0, "capacity must be positive");
        assert!(capacity.is_power_of_two(), "capacity must be a power of two");
        
        // Allocate the buffer with one extra slot to distinguish between empty and full.
        let buffer = {
            let mut v = Vec::with_capacity(capacity + 1);
            let ptr = v.as_mut_ptr();
            std::mem::forget(v);
            ptr
        };
        
        let inner = Arc::new(CachePadded::new(Inner {
            buffer,
            capacity,
            front: AtomicIsize::new(0),
            back: CachePadded::new(AtomicIsize::new(0)),
            stealers: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
        }));
        
        let worker = Worker {
            inner: inner.clone(),
            front: 0,
            back: 0,
        };
        
        let stealer = Stealer { inner };
        
        (worker, stealer)
    }
    
    /// Creates a stealer for this queue.
    pub fn stealer(&self) -> Stealer<T> {
        Stealer {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Worker<T> {
    /// Pushes a task onto the queue.
    ///
    /// Returns `Ok(())` if the task was pushed, or `Err(task)` if the queue is full.
    pub fn push(&mut self, task: T) -> Result<(), T> {
        let inner = &*self.inner;
        
        // Check if the queue is full.
        if self.back - self.front >= inner.capacity as isize {
            return Err(task);
        }
        
        // Write the task to the buffer.
        unsafe {
            let index = (self.back as usize) & (inner.capacity - 1);
            ptr::write(inner.buffer.add(index), task);
        }
        
        // Increment the back index.
        self.back += 1;
        inner.back.store(self.back, Ordering::Release);
        
        Ok(())
    }
    
    /// Pops a task from the queue.
    ///
    /// Returns `Some(task)` if a task was available, or `None` if the queue is empty.
    pub fn pop(&mut self) -> Option<T> {
        // Try to pop from the local queue.
        if self.front < self.back {
            let inner = &*self.inner;
            let index = (self.front as usize) & (inner.capacity - 1);
            self.front += 1;
            
            // Safety: We've checked that the queue is not empty, and we're the only thread
            // that can pop from the front of the queue.
            unsafe {
                let task = ptr::read(inner.buffer.add(index));
                inner.front.store(self.front, Ordering::Release);
                Some(task)
            }
        } else {
            // The queue is empty, try to steal from other workers.
            self.steal()
        }
    }
    
    /// Steals a task from another worker.
    fn steal(&mut self) -> Option<T> {
        let inner = &*self.inner;
        
        // Get the list of stealers.
        let stealers = unsafe { &*inner.stealers.load(Ordering::Acquire) };
        
        // Try to steal from each stealer in turn.
        for stealer in stealers {
            if let Some(task) = stealer.steal() {
                return Some(task);
            }
        }
        
        None
    }
    
    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.front >= self.back
    }
    
    /// Returns the number of tasks in the queue.
    pub fn len(&self) -> usize {
        (self.back - self.front) as usize
    }
    
    /// Returns the capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl<T> Stealer<T> {
    /// Steals a task from the queue.
    ///
    /// Returns `Some(task)` if a task was stolen, or `None` if the queue is empty.
    pub fn steal(&self) -> Option<T> {
        let inner = &*self.inner;
        
        // Load the front and back indices.
        let front = inner.front.load(Ordering::Acquire);
        let back = inner.back.load(Ordering::Acquire);
        
        // Check if the queue is empty.
        if front >= back {
            return None;
        }
        
        // Try to increment the front index to claim a task.
        if inner
            .front
            .compare_exchange(front, front + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_ok()
        {
            // Successfully claimed a task, read it from the buffer.
            let index = (front as usize) & (inner.capacity - 1);
            
            // Safety: We've successfully claimed this index, so it's safe to read.
            unsafe {
                let task = ptr::read(inner.buffer.add(index));
                Some(task)
            }
        } else {
            // Failed to claim a task, the queue might be empty or another thread stole it.
            None
        }
    }
    
    /// Returns `true` if the queue is empty.
    pub fn is_empty(&self) -> bool {
        let inner = &*self.inner;
        let front = inner.front.load(Ordering::Acquire);
        let back = inner.back.load(Ordering::Acquire);
        front >= back
    }
    
    /// Returns the number of tasks in the queue.
    pub fn len(&self) -> usize {
        let inner = &*self.inner;
        let front = inner.front.load(Ordering::Acquire);
        let back = inner.back.load(Ordering::Acquire);
        (back - front).max(0) as usize
    }
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // Drop any remaining elements in the buffer.
        let front = self.front.load(Ordering::Relaxed);
        let back = self.back.load(Ordering::Relaxed);
        
        for i in front..back {
            let index = (i as usize) & (self.capacity - 1);
            unsafe {
                ptr::drop_in_place(self.buffer.add(index));
            }
        }
        
        // Free the buffer.
        unsafe {
            Vec::from_raw_parts(self.buffer, 0, self.capacity + 1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    
    #[test]
    fn test_push_pop() {
        let (mut worker, _) = StealQueue::new(4);
        
        assert!(worker.is_empty());
        assert_eq!(worker.len(), 0);
        
        assert!(worker.push(1).is_ok());
        assert!(!worker.is_empty());
        assert_eq!(worker.len(), 1);
        
        assert_eq!(worker.pop(), Some(1));
        assert!(worker.is_empty());
        assert_eq!(worker.len(), 0);
        assert_eq!(worker.pop(), None);
    }
    
    #[test]
    fn test_steal() {
        let (mut worker, stealer) = StealQueue::new(4);
        
        worker.push(1).unwrap();
        worker.push(2).unwrap();
        
        assert_eq!(stealer.steal(), Some(1));
        assert_eq!(worker.pop(), Some(2));
        assert_eq!(stealer.steal(), None);
    }
    
    #[test]
    fn test_concurrent() {
        const NUM_TASKS: usize = 10000;
        const NUM_WORKERS: usize = 4;
        
        let (mut workers, stealers): (Vec<_>, Vec<_>) = (0..NUM_WORKERS)
            .map(|_| StealQueue::new(1024))
            .unzip();
        
        // Share the stealers with all workers.
        let stealers = Arc::new(stealers);
        
        // Each worker will push tasks to its own queue and try to steal from others.
        let handles: Vec<_> = (0..NUM_WORKERS)
            .map(|i| {
                let mut worker = workers.remove(0);
                let stealers = stealers.clone();
                
                thread::spawn(move || {
                    let mut count = 0;
                    
                    // Push some tasks.
                    for j in 0..NUM_TASKS / NUM_WORKERS {
                        if worker.push(i * 1000 + j).is_err() {
                            panic!("Queue full");
                        }
                    }
                    
                    // Process tasks.
                    while count < NUM_TASKS / NUM_WORKERS {
                        if let Some(_) = worker.pop() {
                            count += 1;
                        } else {
                            // Try to steal from other workers.
                            for stealer in stealers.iter() {
                                if stealer.steal().is_some() {
                                    count += 1;
                                    break;
                                }
                            }
                        }
                    }
                    
                    count
                })
            })
            .collect();
        
        // Wait for all workers to finish.
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(total, NUM_TASKS);
    }
}
