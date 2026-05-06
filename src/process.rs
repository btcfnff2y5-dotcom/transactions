use crate::db::{Chargeback, Deposit, Dispute, Resolve, StateStore, Withdrawal};
use crate::record::Transaction;

use csv_async::{AsyncReaderBuilder, Trim};
use futures::stream::{Stream, StreamExt, TryStreamExt};
use std::convert::TryFrom;
use tokio::io::AsyncRead;

pub(crate) async fn process_csv<'a, R>(
    input: R,
) -> impl Stream<Item = anyhow::Result<Transaction>> + Send + 'a
where
    R: AsyncRead + Unpin + Send + 'a,
{
    AsyncReaderBuilder::new()
        .trim(Trim::All)
        .create_deserializer(input)
        .into_deserialize::<Transaction>()
        .map(|result| match result {
            Ok(raw) => Transaction::try_from(raw).map_err(anyhow::Error::msg),
            Err(e) => Err(anyhow::Error::from(e)),
        })
}

pub(crate) async fn run_engine<S: StateStore>(
    store: &S,
    mut transactions: impl Stream<Item = anyhow::Result<Transaction>> + Unpin,
) -> anyhow::Result<()> {
    while let Some(tx) = transactions.try_next().await? {
        let result = match &tx {
            Transaction::Deposit { .. } => store.deposit(Deposit::try_from(&tx)?).await,
            Transaction::Withdrawal { .. } => store.withdrawal(Withdrawal::try_from(&tx)?).await,
            Transaction::Dispute { .. } => store.dispute(Dispute::try_from(&tx)?).await,
            Transaction::Resolve { .. } => store.resolve(Resolve::try_from(&tx)?).await,
            Transaction::Chargeback { .. } => store.chargeback(Chargeback::try_from(&tx)?).await,
        };
        if let Err(e) = result {
            eprintln!("Transaction failed: {}", e);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::ClientReport;
    use anyhow::Result;
    use async_trait::async_trait;
    use futures::stream::BoxStream;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct MockStore {
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl StateStore for MockStore {
        async fn deposit(&self, cmd: Deposit) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("dep-{}-{}", cmd.client, cmd.amount));
            Ok(())
        }
        async fn withdrawal(&self, cmd: Withdrawal) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("wit-{}-{}", cmd.client, cmd.amount));
            Ok(())
        }
        async fn dispute(&self, cmd: Dispute) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("dis-{}-{}", cmd.client, cmd.tx));
            Ok(())
        }
        async fn resolve(&self, cmd: Resolve) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("res-{}-{}", cmd.client, cmd.tx));
            Ok(())
        }
        async fn chargeback(&self, cmd: Chargeback) -> Result<()> {
            self.calls
                .lock()
                .await
                .push(format!("cha-{}-{}", cmd.client, cmd.tx));
            Ok(())
        }
        async fn get_client_report(&self) -> BoxStream<'_, ClientReport> {
            futures::stream::empty().boxed()
        }
    }

    #[tokio::test]
    async fn test_process_csv_valid_input() {
        let csv = "type, client, tx, amount\ndeposit, 1, 101, 10.5";
        let stream = process_csv(csv.as_bytes()).await;
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 1);
        let tx = results[0].as_ref().unwrap();
        if let Transaction::Deposit { client_id, .. } = tx {
            assert_eq!(*client_id, 1);
        }
    }

    #[tokio::test]
    async fn test_run_engine_error_handling() {
        let store = MockStore::default();
        let csv = "type, client, tx, amount\ndeposit, 1, 1, ";

        let stream = process_csv(csv.as_bytes()).await;
        let result = run_engine(&store, Box::pin(stream)).await;

        assert!(result.is_err());
    }
}
