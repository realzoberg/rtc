use crate::chunk::chunk_payload_data::ChunkPayloadData;

use std::collections::VecDeque;

/// pendingBaseQueue
pub(crate) type PendingBaseQueue = VecDeque<ChunkPayloadData>;

/// Identifies a queued fragment even when its SID, SSN and timestamp are reused.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct PendingPosition {
    unordered: bool,
    position: u64,
}

/// pendingQueue
#[derive(Debug, Default)]
pub(crate) struct PendingQueue {
    unordered_queue: PendingBaseQueue,
    ordered_queue: PendingBaseQueue,
    ordered_popped: u64,
    unordered_popped: u64,
    queue_len: usize,
    n_bytes: usize,
    selected: bool,
    unordered_is_selected: bool,
}

impl PendingQueue {
    pub(crate) fn new() -> Self {
        PendingQueue::default()
    }

    pub(crate) fn push(&mut self, c: ChunkPayloadData) {
        self.n_bytes += c.user_data.len();
        if c.unordered {
            self.unordered_queue.push_back(c);
        } else {
            self.ordered_queue.push_back(c);
        }
        self.queue_len += 1;
    }

    pub(crate) fn peek(&self) -> Option<&ChunkPayloadData> {
        if self.selected {
            if self.unordered_is_selected {
                return self.unordered_queue.front();
            } else {
                return self.ordered_queue.front();
            }
        }

        let c = self.unordered_queue.front();

        if c.is_some() {
            return c;
        }

        self.ordered_queue.front()
    }

    pub(crate) fn front_position(&self) -> Option<PendingPosition> {
        let unordered = self.peek()?.unordered;
        Some(PendingPosition {
            unordered,
            position: if unordered {
                self.unordered_popped
            } else {
                self.ordered_popped
            },
        })
    }

    /// Remove the rest of this message once. A repeated call cannot consume
    /// the next message, even when it has the same stream and policy metadata.
    pub(crate) fn drain_message(&mut self, position: PendingPosition) -> Vec<ChunkPayloadData> {
        if self.front_position() != Some(position) {
            return vec![];
        }
        let mut chunks = vec![];
        loop {
            let c = self
                .peek()
                .expect("pending message must have an ending fragment");
            let c = self.pop(c.beginning_fragment, c.unordered).unwrap();
            let last = c.ending_fragment;
            chunks.push(c);
            if last {
                break;
            }
        }
        chunks
    }

    pub(crate) fn pop(
        &mut self,
        beginning_fragment: bool,
        unordered: bool,
    ) -> Option<ChunkPayloadData> {
        let popped = if self.selected {
            let popped = if self.unordered_is_selected {
                self.unordered_queue.pop_front()
            } else {
                self.ordered_queue.pop_front()
            };
            if let Some(p) = &popped
                && p.ending_fragment
            {
                self.selected = false;
            }
            popped
        } else {
            if !beginning_fragment {
                return None;
            }
            if unordered {
                let popped = { self.unordered_queue.pop_front() };
                if let Some(p) = &popped
                    && !p.ending_fragment
                {
                    self.selected = true;
                    self.unordered_is_selected = true;
                }
                popped
            } else {
                let popped = { self.ordered_queue.pop_front() };
                if let Some(p) = &popped
                    && !p.ending_fragment
                {
                    self.selected = true;
                    self.unordered_is_selected = false;
                }
                popped
            }
        };

        if let Some(p) = &popped {
            self.n_bytes -= p.user_data.len();
            self.queue_len -= 1;
            if p.unordered {
                self.unordered_popped += 1;
            } else {
                self.ordered_popped += 1;
            }
        }

        popped
    }

    pub(crate) fn get_num_bytes(&self) -> usize {
        self.n_bytes
    }

    pub(crate) fn len(&self) -> usize {
        self.queue_len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
