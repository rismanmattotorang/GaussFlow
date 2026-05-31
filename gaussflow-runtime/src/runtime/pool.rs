//! Memory pooling for efficient allocation and reuse of objects.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::error::RuntimeError;
use super::Result;

/// A pool of reusable objects of type T.
#[derive(Debug)]
pub struct Pool<T: Default + Send> {
    /// Pooled objects.
    objects: Mutex<VecDeque<T>>,
    /// Maximum number of objects to keep in the pool.
    max_size: usize,
    /// Number of objects currently in the pool.
    size: AtomicUsize,
    /// Name of the pool for diagnostics.
    name: &'static str,
    /// Phantom data to mark the type parameter as used.
    _phantom: PhantomData<T>,
}

impl<T: Default + Send> Pool<T> {
    /// Create a new pool with the given maximum size.
    pub fn new(max_size: usize, name: &'static str) -> Self {
        Self {
            objects: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
            size: AtomicUsize::new(0),
            name,
            _phantom: PhantomData,
        }
    }

    /// Get an object from the pool, creating a new one if necessary.
    pub fn get(&self) -> Pooled<T> {
        let mut objects = self.objects.lock().unwrap();
        
        if let Some(obj) = objects.pop_front() {
            return Pooled {
                inner: Some(obj),
                pool: Some(self),
            };
        }

        // No objects available, create a new one
        let current_size = self.size.fetch_add(1, Ordering::Relaxed);
        if current_size >= self.max_size {
            // We've reached max size, so create a temporary object that won't be returned to the pool
            return Pooled {
                inner: Some(T::default()),
                pool: None, // This object won't be returned to the pool
            };
        }

        Pooled {
            inner: Some(T::default()),
            pool: Some(self),
        }
    }

    /// Return an object to the pool.
    fn put(&self, mut obj: T) {
        // Reset the object to its default state
        obj = T::default();
        
        let mut objects = match self.objects.lock() {
            Ok(guard) => guard,
            Err(_) => return, // If we can't acquire the lock, just drop the object
        };
        
        if objects.len() < self.max_size {
            objects.push_back(obj);
        }
        // If the pool is full, the object will be dropped
    }

    /// Get the number of objects currently in the pool.
    pub fn len(&self) -> usize {
        self.objects.lock().unwrap().len()
    }

    /// Check if the pool is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the maximum number of objects the pool can hold.
    pub fn capacity(&self) -> usize {
        self.max_size
    }
}

/// A handle to a pooled object that will be returned to the pool when dropped.
pub struct Pooled<'a, T: Default + Send> {
    inner: Option<T>,
    pool: Option<&'a Pool<T>>,
}

impl<'a, T: Default + Send> std::ops::Deref for Pooled<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().expect("Pooled value is None")
    }
}

impl<'a, T: Default + Send> std::ops::DerefMut for Pooled<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().expect("Pooled value is None")
    }
}

impl<'a, T: Default + Send> Drop for Pooled<'a, T> {
    fn drop(&mut self) {
        if let (Some(inner), Some(pool)) = (self.inner.take(), self.pool) {
            pool.put(inner);
        }
    }
}

/// A specialized pool for byte buffers.
pub type BufferPool = Pool<Vec<u8>>;

impl BufferPool {
    /// Create a new buffer pool with the given maximum size and buffer capacity.
    pub fn with_capacity(max_size: usize, buffer_capacity: usize) -> Self {
        let pool = Pool::new(max_size, "buffer_pool");
        
        // Pre-allocate some buffers
        for _ in 0..max_size.min(4) {
            let mut buf = Vec::with_capacity(buffer_capacity);
            unsafe { buf.set_len(buffer_capacity) }; // Safe because we just allocated
            pool.put(buf);
        }
        
        pool
    }
    
    /// Get a buffer with at least the requested capacity.
    pub fn get_with_capacity(&self, capacity: usize) -> Pooled<Vec<u8>> {
        let mut buf = self.get();
        if buf.capacity() < capacity {
            // If the pooled buffer is too small, create a new one
            buf.inner = Some(Vec::with_capacity(capacity));
        } else {
            // Otherwise, clear the existing buffer
            buf.clear();
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_pool_basics() {
        let pool = Pool::<String>::new(2, "test_pool");
        
        // First get should create a new string
        let mut s1 = pool.get();
        s1.push_str("hello");
        assert_eq!(&*s1, "hello");
        
        // Dropping should return it to the pool
        drop(s1);
        
        // Next get should reuse the same string
        let s2 = pool.get();
        assert_eq!(&*s2, ""); // Should be reset
    }
    
    #[test]
    fn test_pool_max_size() {
        let pool = Pool::<Vec<u8>>::new(2, "test_pool");
        
        let v1 = pool.get();
        let v2 = pool.get();
        
        // Pool is now at max size
        let v3 = pool.get(); // This will be a temporary object
        
        drop(v1);
        drop(v2);
        
        // Now the pool has 2 objects again
        let v4 = pool.get(); // Should reuse one from the pool
        drop(v4);
        
        drop(v3); // This one wasn't from the pool, so it will be dropped
        
        assert_eq!(pool.len(), 2);
    }
    
    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::with_capacity(2, 1024);
        
        let mut buf1 = pool.get_with_capacity(512);
        buf1.extend_from_slice(b"hello");
        
        let buf2 = pool.get_with_capacity(2048);
        assert!(buf2.capacity() >= 2048);
    }
    
    #[test]
    fn test_thread_safety() {
        let pool = Arc::new(Pool::<Vec<u8>>::new(10, "thread_safe_pool"));
        let mut handles = vec![];
        
        for _ in 0..4 {
            let pool = pool.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut buf = pool.get();
                    buf.extend_from_slice(b"test");
                    // The buffer will be returned to the pool when dropped
                }
            }));
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // The pool should have some buffers now
        assert!(pool.len() > 0);
        assert!(pool.len() <= 10); // Shouldn't exceed max size
    }
}
