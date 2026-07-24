use std::collections::VecDeque;
use std::sync::Mutex;

use tokio::sync::Semaphore;

/// A bounded, drop-oldest line buffer between the reader and the writer.
///
/// Semaphore permits track the number of queued lines, so `pop` can await
/// without polling. On overflow the oldest line is discarded and the item
/// count is unchanged, so no permit accounting is needed for the drop.
pub struct LineQueue {
    inner: Mutex<VecDeque<String>>,
    items: Semaphore,
    capacity: usize,
}

impl LineQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(VecDeque::with_capacity(capacity)),
            items: Semaphore::new(0),
            capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.items.available_permits()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Enqueue a line, dropping the oldest if the buffer is full.
    /// Returns true if a line was dropped to make room.
    pub fn push(&self, line: String) -> bool {
        let mut q = self.inner.lock().unwrap();
        if q.len() >= self.capacity {
            q.pop_front();
            q.push_back(line);
            true
        } else {
            q.push_back(line);
            drop(q);
            self.items.add_permits(1);
            false
        }
    }

    /// Await and remove the oldest line.
    pub async fn pop(&self) -> String {
        let permit = self
            .items
            .acquire()
            .await
            .expect("line queue semaphore never closed");
        permit.forget();
        self.inner
            .lock()
            .unwrap()
            .pop_front()
            .expect("permit implies a queued line")
    }

    /// Remove the oldest line if one is immediately available.
    pub fn try_pop(&self) -> Option<String> {
        match self.items.try_acquire() {
            Ok(permit) => {
                permit.forget();
                Some(
                    self.inner
                        .lock()
                        .unwrap()
                        .pop_front()
                        .expect("permit implies a queued line"),
                )
            }
            Err(_) => None,
        }
    }

    /// Await one line, then drain whatever else is already buffered, up to the
    /// given bounds. Adds no latency when the queue is sparse.
    pub async fn recv_batch(&self, max_lines: usize, max_bytes: usize) -> Vec<String> {
        let first = self.pop().await;
        let mut bytes = first.len() + 1;
        let mut batch = Vec::with_capacity(max_lines.min(64));
        batch.push(first);
        while batch.len() < max_lines && bytes < max_bytes {
            match self.try_pop() {
                Some(line) => {
                    bytes += line.len() + 1;
                    batch.push(line);
                }
                None => break,
            }
        }
        batch
    }
}
