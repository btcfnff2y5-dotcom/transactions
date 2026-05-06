mod convert;
mod db;
mod process;
mod record;

use anyhow::{Context, Result};
use db::StateStore;
use db::rocks::RocksStore;
use futures::StreamExt;
use process::{process_csv, run_engine};
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::io::{BufWriter, stdout};

#[derive(clap::Parser, Debug)]
#[command(about = "a transaction program")]
pub struct TransactArgs {
    pub input_file: std::path::PathBuf,
}

impl TransactArgs {
    pub async fn run(self) -> Result<()> {
        let file = File::open(&self.input_file)
            .await
            .with_context(|| format!("Failed to open input file: {:?}", self.input_file))?;

        let out = BufWriter::new(stdout());

        let db_path = {
            const TRANSACTIONS_DB: &str = "transactions_db";
            let mut db_path = std::env::current_dir()?;
            db_path.push(TRANSACTIONS_DB);
            db_path
        };

        orchestrate_transactions(file, out, db_path).await
    }
}

pub async fn orchestrate_transactions<R, W>(
    input: R,
    mut output: W,
    db_path: PathBuf,
) -> anyhow::Result<()>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    W: tokio::io::AsyncWrite + Unpin + Send,
{
    if db_path.exists() {
        tokio::fs::remove_dir_all(&db_path)
            .await
            .with_context(|| format!("Failed to clear DB at {:?}", db_path))?;
    }

    let store = RocksStore::new(db_path).await?;
    let stream = process_csv(input).await;
    run_engine(&store, Box::pin(stream)).await?;

    // print report
    {
        output
            .write_all(b"client,available,held,total,locked\n")
            .await?;
        let mut reports = store.get_client_report().await;
        while let Some(r) = reports.next().await {
            let line = format!(
                "{},{:.4},{:.4},{:.4},{}\n",
                r.client, r.available, r.held, r.total, r.locked
            );
            output.write_all(line.as_bytes()).await?;
        }

        output.flush().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_orchestrate_full_flow() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().to_path_buf();
        let input = Cursor::new("type,client,tx,amount\ndeposit,1,1,10.0".as_bytes());
        let mut output = Cursor::new(Vec::new());

        let result = orchestrate_transactions(input, &mut output, db_path).await;

        assert!(result.is_ok());
        let report = String::from_utf8(output.into_inner())?;
        assert!(report.contains("1,10.0000,0.0000,10.0000,false"));
        Ok(())
    }

    #[tokio::test]
    async fn test_orchestrate_with_invalid_csv() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().to_path_buf();
        let csv_data = "type, client, tx, amount\nmagic_trick, 1, 1, 100.0";
        let input = Cursor::new(csv_data.as_bytes());
        let mut output = Cursor::new(Vec::new());

        let result = orchestrate_transactions(input, &mut output, db_path).await;

        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn test_orchestrate_frozen_stop() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path().to_path_buf();
        let csv_data = r#"type, client, tx, amount
deposit, 1, 1, 100.0
dispute, 1, 1,
chargeback, 1, 1,
withdrawal, 1, 2, 10.0
"#;
        let input = Cursor::new(csv_data.as_bytes());
        let mut output = Cursor::new(Vec::new());

        let result = orchestrate_transactions(input, &mut output, db_path).await;
        assert!(result.is_ok());

        output.set_position(0);
        let mut output_str = String::new();
        std::io::Read::read_to_string(&mut output, &mut output_str)?;

        let expected_output = r#"client,available,held,total,locked
1,0.0000,0.0000,0.0000,true"#;

        assert!(
            output_str.contains(expected_output),
            "Report mismatch. Found: {}",
            output_str
        );

        Ok(())
    }
}
