use anyhow::{bail, Result};
use std::time::{Duration, Instant};

pub fn wait_for_condition<F>(timeout: Duration, condition: F) -> Result<()>
where
    F: Fn() -> Result<bool>,
{
    let start = Instant::now();
    while Instant::now().duration_since(start) < timeout {
        if condition()? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!("timeout waiting condition")
}
