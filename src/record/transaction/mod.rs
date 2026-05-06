mod raw_transaction;

use super::{Amount, Client, Tx};
use raw_transaction::RawTransaction;

use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(try_from = "RawTransaction")]
pub(crate) enum Transaction {
    Deposit {
        client_id: Client,
        tx_id: Tx,
        amount: Amount,
    },
    Withdrawal {
        client_id: Client,
        tx_id: Tx,
        amount: Amount,
    },
    Dispute {
        client_id: Client,
        tx_id: Tx,
    },
    Resolve {
        client_id: Client,
        tx_id: Tx,
    },
    Chargeback {
        client_id: Client,
        tx_id: Tx,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv_async::{AsyncReaderBuilder, Trim};
    use futures::stream::StreamExt;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_deposit() {
        let csv_data = r#"
        type, client, tx, amount
        deposit, 1, 101, 10.12349
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Deposit {
                client_id: 1,
                tx_id: 101,
                amount: Amount(dec!(10.1234))
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_withdrawal() {
        let csv_data = r#"
        type, client, tx, amount
        withdrawal, 1, 101, 10.12349
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Withdrawal {
                client_id: 1,
                tx_id: 101,
                amount: Amount(dec!(10.1234))
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_dispute() {
        let csv_data = r#"
        type, client, tx, amount
        dispute, 1, 101,
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Dispute {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_dispute_ignore_amount() {
        let csv_data = r#"
        type, client, tx, amount
        dispute, 1, 101, 123.00
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Dispute {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_resolve() {
        let csv_data = r#"
        type, client, tx, amount
        resolve, 1, 101,
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Resolve {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_resolve_ignore_amount() {
        let csv_data = r#"
        type, client, tx, amount
        resolve, 1, 101, 99.00
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Resolve {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_chargeback() {
        let csv_data = r#"
        type, client, tx, amount
        chargeback, 1, 101,
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Chargeback {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }

    #[tokio::test]
    async fn test_chargeback_ignore_amount() {
        let csv_data = r#"
        type, client, tx, amount
        chargeback, 1, 101, 99.00
        "#
        .trim();
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());
        let mut records = rdr.deserialize::<Transaction>();

        assert_eq!(
            records.next().await.unwrap().unwrap(),
            Transaction::Chargeback {
                client_id: 1,
                tx_id: 101,
            }
        );
        assert!(records.next().await.is_none());
    }
}
