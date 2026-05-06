use super::{Amount, Client, Transaction, Tx};
use serde::Deserialize;
use std::convert::TryFrom;

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum RawType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Deserialize)]
pub(super) struct RawTransaction {
    #[serde(rename = "type")]
    tx_type: RawType,
    #[serde(rename = "client")]
    client_id: Client,
    #[serde(rename = "tx")]
    tx_id: Tx,
    amount: Option<Amount>,
}

impl TryFrom<RawTransaction> for Transaction {
    type Error = String;

    fn try_from(raw: RawTransaction) -> Result<Self, Self::Error> {
        let client_id = raw.client_id;
        let tx_id = raw.tx_id;

        match raw.tx_type {
            RawType::Deposit => Ok(Transaction::Deposit {
                client_id,
                tx_id,
                amount: raw.amount.ok_or("Deposit missing amount")?,
            }),
            RawType::Withdrawal => Ok(Transaction::Withdrawal {
                client_id,
                tx_id,
                amount: raw.amount.ok_or("Withdrawal missing amount")?,
            }),
            RawType::Dispute => Ok(Transaction::Dispute { client_id, tx_id }),
            RawType::Resolve => Ok(Transaction::Resolve { client_id, tx_id }),
            RawType::Chargeback => Ok(Transaction::Chargeback { client_id, tx_id }),
        }
    }
}
