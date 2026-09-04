use std::collections::VecDeque;

use thiserror::Error;

#[derive(Debug)]
pub struct DeterministicRng {
    values: VecDeque<u64>,
    capacity: usize,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeterministicRngError {
    #[error("deterministic RNG capacity must be nonzero")]
    ZeroCapacity,
    #[error("{provided} deterministic values exceed capacity {capacity}")]
    TooManyValues { provided: usize, capacity: usize },
    #[error("deterministic RNG fixture exhausted")]
    Exhausted,
}

impl DeterministicRng {
    pub fn new(
        values: impl IntoIterator<Item = u64>,
        capacity: usize,
    ) -> Result<Self, DeterministicRngError> {
        if capacity == 0 {
            return Err(DeterministicRngError::ZeroCapacity);
        }
        let values: VecDeque<_> = values.into_iter().collect();
        if values.len() > capacity {
            return Err(DeterministicRngError::TooManyValues {
                provided: values.len(),
                capacity,
            });
        }
        Ok(Self { values, capacity })
    }

    #[must_use]
    pub fn remaining(&self) -> usize {
        self.values.len()
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn next_u64(&mut self) -> Result<u64, DeterministicRngError> {
        self.values
            .pop_front()
            .ok_or(DeterministicRngError::Exhausted)
    }

    pub fn next_u32(&mut self) -> Result<u32, DeterministicRngError> {
        Ok(self.next_u64()? as u32)
    }

    pub fn fill_bytes(&mut self, output: &mut [u8]) -> Result<(), DeterministicRngError> {
        for chunk in output.chunks_mut(8) {
            let bytes = self.next_u64()?.to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_and_exhaustion_are_reproducible() {
        let mut rng = DeterministicRng::new([0x0807_0605_0403_0201], 1).unwrap();
        let mut bytes = [0_u8; 6];
        rng.fill_bytes(&mut bytes).unwrap();
        assert_eq!(bytes, [1, 2, 3, 4, 5, 6]);
        assert_eq!(rng.next_u64(), Err(DeterministicRngError::Exhausted));
    }

    #[test]
    fn input_is_bounded() {
        assert!(matches!(
            DeterministicRng::new([1, 2], 1),
            Err(DeterministicRngError::TooManyValues { .. })
        ));
    }
}
