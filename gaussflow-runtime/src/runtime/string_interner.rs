//! String interning for efficient string storage and comparison.

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use dashmap::DashMap;
use once_cell::sync::Lazy;
use parking_lot::RwLock as ParkingRwLock;

/// A globally interned string.
///
/// This is a handle to a string that has been interned in the global string interner.
/// It can be cloned and compared cheaply.
#[derive(Debug, Clone)]
pub struct InternedString {
    /// The interned string data.
    data: Arc<str>,
    /// The hash of the string.
    hash: u64,
}

impl PartialEq for InternedString {
    fn eq(&self, other: &Self) -> bool {
        // Fast path: compare hashes first
        self.hash == other.hash && self.data.as_ptr() == other.data.as_ptr()
    }
}

impl Eq for InternedString {}

impl Hash for InternedString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl Deref for InternedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl fmt::Display for InternedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.data.fmt(f)
    }
}

impl From<&str> for InternedString {
    fn from(s: &str) -> Self {
        INTERNER.get_or_intern(s)
    }
}

impl From<String> for InternedString {
    fn from(s: String) -> Self {
        INTERNER.get_or_intern(s)
    }
}

/// A string interner that stores strings in a global pool.
///
/// This allows for efficient storage and comparison of strings by storing each unique string only once.
/// It uses a two-level locking strategy with a read-heavy design for high concurrency.
struct StringInterner {
    /// The actual string storage.
    strings: DashMap<u64, Arc<str>>,
    /// A reverse mapping from string pointers to their hashes.
    /// This allows us to look up the hash of a string without hashing it again.
    hashes: ParkingRwLock<HashMap<*const str, u64>>,
}

impl StringInterner {
    /// Create a new string interner.
    fn new() -> Self {
        Self {
            strings: DashMap::new(),
            hashes: ParkingRwLock::new(HashMap::new()),
        }
    }

    /// Intern a string, returning an `InternedString`.
    ///
    /// If the string is already interned, this returns the existing `InternedString`.
    /// Otherwise, it adds the string to the interner and returns a new `InternedString`.
    fn get_or_intern<T: AsRef<str>>(&self, s: T) -> InternedString {
        let s = s.as_ref();
        let hash = self.hash(s);
        
        // Fast path: check if we already have this string
        if let Some(existing) = self.strings.get(&hash) {
            let ptr = existing.as_ptr() as *const str;
            return InternedString {
                data: existing.clone(),
                hash: *self.hashes.read().get(&ptr).unwrap_or(&0),
            };
        }
        
        // Slow path: need to add the string
        self.intern(s.to_string(), hash)
    }
    
    /// Intern a string that we know is not already in the interner.
    fn intern(&self, s: String, hash: u64) -> InternedString {
        // Check again in case another thread added it while we were waiting
        if let Some(existing) = self.strings.get(&hash) {
            let ptr = existing.as_ptr() as *const str;
            return InternedString {
                data: existing.clone(),
                hash: *self.hashes.read().get(&ptr).unwrap_or(&0),
            };
        }
        
        // Still not there, add it
        let arc = Arc::from(s);
        let ptr = arc.as_ptr() as *const str;
        
        // Store the string and its hash
        self.strings.insert(hash, arc.clone());
        self.hashes.write().insert(ptr, hash);
        
        InternedString {
            data: arc,
            hash,
        }
    }
    
    /// Hash a string.
    fn hash(&self, s: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Get the number of strings in the interner.
    fn len(&self) -> usize {
        self.strings.len()
    }
    
    /// Check if the interner is empty.
    fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }
}

/// Global string interner instance.
static INTERNER: Lazy<StringInterner> = Lazy::new(|| {
    StringInterner::new()
});

/// A thread-local string interner.
///
/// This provides a per-thread cache of interned strings to reduce contention on the global interner.
#[derive(Default)]
pub struct ThreadLocalInterner {
    /// Local cache of interned strings.
    cache: dashmap::DashMap<u64, InternedString>,
}

impl ThreadLocalInterner {
    /// Create a new thread-local interner.
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Intern a string, using the thread-local cache if possible.
    pub fn get_or_intern<T: AsRef<str>>(&self, s: T) -> InternedString {
        let s = s.as_ref();
        let hash = INTERNER.hash(s);
        
        // Try the local cache first
        if let Some(interned) = self.cache.get(&hash) {
            return interned.clone();
        }
        
        // Not in the local cache, try the global interner
        let interned = INTERNER.get_or_intern(s);
        
        // Cache the result
        self.cache.insert(hash, interned.clone());
        
        interned
    }
    
    /// Get the number of strings in the thread-local cache.
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    
    /// Check if the thread-local cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
}

/// Get the number of strings in the global string interner.
pub fn num_interned_strings() -> usize {
    INTERNER.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Barrier;
    use std::thread;
    
    #[test]
    fn test_interned_string() {
        let s1: InternedString = "hello".into();
        let s2: InternedString = "hello".to_string().into();
        let s3: InternedString = "world".into();
        
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
        assert_eq!(s1.as_ptr(), s2.as_ptr());
        assert_ne!(s1.as_ptr(), s3.as_ptr());
        
        assert_eq!(&*s1, "hello");
        assert_eq!(&*s2, "hello");
        assert_eq!(&*s3, "world");
    }
    
    #[test]
    fn test_thread_local_interner() {
        let interner = ThreadLocalInterner::new();
        
        let s1 = interner.get_or_intern("hello");
        let s2 = interner.get_or_intern("hello".to_string());
        let s3 = interner.get_or_intern("world");
        
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
        assert_eq!(s1.as_ptr(), s2.as_ptr());
        
        assert_eq!(interner.len(), 2);
    }
    
    #[test]
    fn test_concurrent_intern() {
        let num_threads = 8;
        let num_strings = 1000;
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let counter = Arc::new(AtomicUsize::new(0));
        
        let mut handles = vec![];
        
        for _ in 0..num_threads {
            let barrier = barrier.clone();
            let counter = counter.clone();
            
            handles.push(thread::spawn(move || {
                let interner = ThreadLocalInterner::new();
                barrier.wait();
                
                for i in 0..num_strings {
                    let s = format!("string_{}", i);
                    let _ = interner.get_or_intern(&s);
                }
                
                counter.fetch_add(interner.len(), Ordering::SeqCst);
            }));
        }
        
        barrier.wait();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        // The total number of strings in thread-local caches should be at least num_strings
        // (each string was interned at least once) and at most num_threads * num_strings
        // (each string was interned by each thread).
        let total = counter.load(Ordering::SeqCst);
        assert!(total >= num_strings);
        assert!(total <= num_threads * num_strings);
        
        // The global interner should have exactly num_strings strings
        assert_eq!(num_interned_strings(), num_strings);
    }
}
