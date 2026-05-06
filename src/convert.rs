use anyhow::{Context, anyhow};
use rust_decimal::prelude::ToPrimitive;
use std::convert::TryFrom;

use crate::db::{Chargeback, Deposit, Dispute, Resolve, Withdrawal};
use crate::record::Transaction;

impl<'a> TryFrom<&'a Transaction> for Deposit {
    type Error = anyhow::Error;

    fn try_from(tx: &'a Transaction) -> anyhow::Result<Self> {
        if let Transaction::Deposit {
            client_id,
            tx_id,
            amount,
        } = tx
        {
            Ok(Deposit {
                client: *client_id,
                tx: *tx_id,
                amount: amount.to_f32().context("Invalid amount conversion")?,
            })
        } else {
            Err(anyhow!("Transaction is not a Deposit"))
        }
    }
}

impl<'a> TryFrom<&'a Transaction> for Withdrawal {
    type Error = anyhow::Error;

    fn try_from(tx: &'a Transaction) -> anyhow::Result<Self> {
        if let Transaction::Withdrawal {
            client_id,
            tx_id,
            amount,
        } = tx
        {
            Ok(Withdrawal {
                client: *client_id,
                tx: *tx_id,
                amount: amount.to_f32().context("Invalid amount conversion")?,
            })
        } else {
            anyhow::bail!("Transaction is not a Withdrawal");
        }
    }
}

impl<'a> TryFrom<&'a Transaction> for Dispute {
    type Error = anyhow::Error;

    fn try_from(tx: &'a Transaction) -> anyhow::Result<Self> {
        if let Transaction::Dispute { client_id, tx_id } = tx {
            Ok(Dispute {
                client: *client_id,
                tx: *tx_id,
            })
        } else {
            anyhow::bail!("Transaction is not a Dispute");
        }
    }
}

impl<'a> TryFrom<&'a Transaction> for Resolve {
    type Error = anyhow::Error;

    fn try_from(tx: &'a Transaction) -> anyhow::Result<Self> {
        if let Transaction::Resolve { client_id, tx_id } = tx {
            Ok(Resolve {
                client: *client_id,
                tx: *tx_id,
            })
        } else {
            anyhow::bail!("Transaction is not a Resolve");
        }
    }
}

impl<'a> TryFrom<&'a Transaction> for Chargeback {
    type Error = anyhow::Error;

    fn try_from(tx: &'a Transaction) -> anyhow::Result<Self> {
        if let Transaction::Chargeback { client_id, tx_id } = tx {
            Ok(Chargeback {
                client: *client_id,
                tx: *tx_id,
            })
        } else {
            anyhow::bail!("Transaction is not a Chargeback");
        }
    }
}
