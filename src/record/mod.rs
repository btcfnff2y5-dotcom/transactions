mod amount;

use amount::Amount;
use serde::Deserialize;
use std::convert::TryFrom;

type Client = u16;
type Tx = u32;

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(try_from = "RawTransaction")]
pub enum Transaction {
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

#[derive(Deserialize)]
struct RawTransaction {
    #[serde(rename = "type")]
    tx_type: String,
    #[serde(rename = "client")]
    client: Client,
    #[serde(rename = "tx")]
    tx: Tx,
    amount: Option<Amount>,
}

impl TryFrom<RawTransaction> for Transaction {
    type Error = String;

    fn try_from(raw: RawTransaction) -> Result<Self, Self::Error> {
        match raw.tx_type.as_str() {
            "deposit" => Ok(Transaction::Deposit {
                client_id: raw.client,
                tx_id: raw.tx,
                amount: raw.amount.ok_or("Deposit missing amount")?,
            }),
            "withdrawal" => Ok(Transaction::Withdrawal {
                client_id: raw.client,
                tx_id: raw.tx,
                amount: raw.amount.ok_or("Withdrawal missing amount")?,
            }),
            "dispute" => Ok(Transaction::Dispute {
                client_id: raw.client,
                tx_id: raw.tx,
            }),
            "resolve" => Ok(Transaction::Resolve {
                client_id: raw.client,
                tx_id: raw.tx,
            }),
            "chargeback" => Ok(Transaction::Chargeback {
                client_id: raw.client,
                tx_id: raw.tx,
            }),
            other => Err(format!("Unknown transaction type: {}", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv_async::{AsyncReaderBuilder, Trim};
    use futures::stream::StreamExt;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_async_csv_deposit() {
        let csv_data = "type, client, tx, amount\ndeposit, 1, 101, 10.12349";
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());

        let mut records = rdr.deserialize::<Transaction>();
        let tx = records
            .next()
            .await
            .unwrap()
            .expect("Failed to parse deposit");

        assert_eq!(
            tx,
            Transaction::Deposit {
                client_id: 1,
                tx_id: 101,
                amount: Amount(dec!(10.1234))
            }
        );
    }

    #[tokio::test]
    async fn test_async_csv_dispute() {
        let csv_data = "type, client, tx, amount\ndispute, 5, 202, ";
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .flexible(true)
            .create_deserializer(csv_data.as_bytes());

        let mut records = rdr.deserialize::<Transaction>();
        let tx = records
            .next()
            .await
            .unwrap()
            .expect("Failed to parse dispute");

        assert_eq!(
            tx,
            Transaction::Dispute {
                client_id: 5,
                tx_id: 202
            }
        );
    }

    #[tokio::test]
    async fn test_async_csv_invalid_type() {
        let csv_data = "type, client, tx, amount\nunknown, 1, 1, 10.0";
        let mut rdr = AsyncReaderBuilder::new()
            .trim(Trim::All)
            .create_deserializer(csv_data.as_bytes());

        let mut records = rdr.deserialize::<Transaction>();
        let result = records.next().await.unwrap();
        assert!(result.is_err());
    }
}
