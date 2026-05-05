mod record;

use anyhow::Result;

#[derive(clap::Parser, Debug)]
#[command(about = "a transaction program")]
pub struct TransactArgs {
    /// path to the input file
    pub name: std::path::PathBuf,
}

impl TransactArgs {
    pub fn run(&self) -> Result<()> {
        Ok(())
    }
}
