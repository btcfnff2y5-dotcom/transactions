use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use sled::transaction::{ConflictableTransactionError, TransactionResult};
use sled::{Config, Db, Transactional, Tree};
use std::convert::{TryFrom, TryInto};
use std::path::PathBuf;

use super::{Chargeback, ClientReport, Deposit, Dispute, Resolve, StateStore, Withdrawal};

const CF_BALANCES: &str = "balances";
const CF_LEDGER: &str = "ledger";

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum LedgerStatus {
    Normal = 0,
    Disputed = 1,
    ChargedBack = 3,
}

impl From<LedgerStatus> for u8 {
    fn from(status: LedgerStatus) -> Self {
        status as u8
    }
}

impl TryFrom<u8> for LedgerStatus {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(LedgerStatus::Normal),
            1 => Ok(LedgerStatus::Disputed),
            3 => Ok(LedgerStatus::ChargedBack),
            _ => bail!("Invalid ledger status: {}", value),
        }
    }
}

pub(crate) struct SledStore {
    #[allow(dead_code)]
    db: Db,
    balances: Tree,
    ledger: Tree,
}

impl SledStore {
    pub async fn new(path: Option<PathBuf>) -> Result<Self> {
        let config = match path {
            Some(p) => Config::default().path(p),
            None => Config::default().temporary(true), // In-memory for containers
        };

        let db = config.open().context("Failed to open sled")?;
        let balances = db.open_tree(CF_BALANCES)?;
        let ledger = db.open_tree(CF_LEDGER)?;

        Ok(Self {
            db,
            balances,
            ledger,
        })
    }

    // --- Byte Packing Helpers ---
    fn is_frozen(bal: &[u8]) -> bool {
        bal[0] == 1
    }

    fn set_frozen(bal: &mut [u8], frozen: bool) {
        bal[0] = if frozen { 1 } else { 0 };
    }

    fn get_balance_val(bal: &[u8]) -> f32 {
        f32::from_be_bytes(bal[1..5].try_into().unwrap_or([0u8; 4]))
    }

    fn set_balance_val(bal: &mut [u8], amount: f32) {
        bal[1..5].copy_from_slice(&amount.to_be_bytes());
    }

    fn get_held_val(bal: &[u8]) -> f32 {
        f32::from_be_bytes(bal[5..9].try_into().unwrap_or([0u8; 4]))
    }

    fn set_held_val(bal: &mut [u8], amount: f32) {
        bal[5..9].copy_from_slice(&amount.to_be_bytes());
    }

    fn create_ledger_key(client: u16, tx: u32) -> [u8; 6] {
        let mut key = [0u8; 6];
        key[0..2].copy_from_slice(&client.to_be_bytes());
        key[2..6].copy_from_slice(&tx.to_be_bytes());
        key
    }

    fn create_ledger_val(amount: f32, status: LedgerStatus) -> [u8; 5] {
        let mut led_val = [0u8; 5];
        led_val[0..4].copy_from_slice(&amount.to_be_bytes());
        led_val[4] = status.into();
        led_val
    }

    fn get_ledger_amount(led_val: &[u8]) -> f32 {
        f32::from_be_bytes(led_val[0..4].try_into().unwrap_or([0u8; 4]))
    }

    fn get_ledger_status(led_val: &[u8]) -> LedgerStatus {
        LedgerStatus::try_from(led_val[4]).unwrap_or(LedgerStatus::Normal)
    }

    fn set_ledger_status(led_val: &mut [u8], status: LedgerStatus) {
        led_val[4] = status.into();
    }
}

#[async_trait]
impl StateStore for SledStore {
    async fn deposit(&self, cmd: Deposit) -> Result<()> {
        let client_key = cmd.client.to_be_bytes();
        let led_key = Self::create_ledger_key(cmd.client, cmd.tx);
        let led_val = Self::create_ledger_val(cmd.amount, LedgerStatus::Normal);

        let result: TransactionResult<(), anyhow::Error> = (&self.balances, &self.ledger)
            .transaction(|(bal_t, led_t)| {
                let mut bal = bal_t
                    .get(client_key)?
                    .map(|v| v.to_vec())
                    .unwrap_or_else(|| vec![0; 9]);

                if Self::is_frozen(&bal) {
                    return Err(ConflictableTransactionError::Abort(anyhow::anyhow!(
                        "Account is frozen"
                    )));
                }

                let new_val = Self::get_balance_val(&bal) + cmd.amount;
                Self::set_balance_val(&mut bal, new_val);

                bal_t.insert(&client_key, bal)?;
                led_t.insert(&led_key, &led_val[..])?;
                Ok(())
            });

        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    async fn withdrawal(&self, cmd: Withdrawal) -> Result<()> {
        let client_key = cmd.client.to_be_bytes();

        let result: TransactionResult<(), anyhow::Error> = self.balances.transaction(|bal_t| {
            if let Some(mut bal) = bal_t.get(client_key)?.map(|v| v.to_vec()) {
                if Self::is_frozen(&bal) {
                    return Ok(());
                }
                let avail = Self::get_balance_val(&bal);
                if avail >= cmd.amount {
                    Self::set_balance_val(&mut bal, avail - cmd.amount);
                    bal_t.insert(&client_key, bal)?;
                }
            }
            Ok(())
        });

        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    async fn dispute(&self, cmd: Dispute) -> Result<()> {
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);
        let client_key = cmd.client.to_be_bytes();

        let result: TransactionResult<(), anyhow::Error> = (&self.balances, &self.ledger)
            .transaction(|(bal_t, led_t)| {
                if let Some(mut l_val) = led_t.get(l_key)?.map(|v| v.to_vec()) {
                    if Self::get_ledger_status(&l_val) != LedgerStatus::Normal {
                        return Ok(());
                    }

                    if let Some(mut b_val) = bal_t.get(client_key)?.map(|v| v.to_vec()) {
                        let amt = Self::get_ledger_amount(&l_val);
                        let avail = Self::get_balance_val(&b_val);
                        let held = Self::get_held_val(&b_val);

                        Self::set_balance_val(&mut b_val, avail - amt);
                        Self::set_held_val(&mut b_val, held + amt);
                        Self::set_ledger_status(&mut l_val, LedgerStatus::Disputed);

                        bal_t.insert(&client_key, b_val)?;
                        led_t.insert(&l_key, l_val)?;
                    }
                }
                Ok(())
            });

        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    async fn resolve(&self, cmd: Resolve) -> Result<()> {
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);
        let client_key = cmd.client.to_be_bytes();

        let result: TransactionResult<(), anyhow::Error> = (&self.balances, &self.ledger)
            .transaction(|(bal_t, led_t)| {
                if let Some(mut l_val) = led_t.get(l_key)?.map(|v| v.to_vec()) {
                    if Self::get_ledger_status(&l_val) != LedgerStatus::Disputed {
                        return Ok(());
                    }

                    if let Some(mut b_val) = bal_t.get(client_key)?.map(|v| v.to_vec()) {
                        let amt = Self::get_ledger_amount(&l_val);
                        let avail = Self::get_balance_val(&b_val);
                        let held = Self::get_held_val(&b_val);

                        Self::set_balance_val(&mut b_val, avail + amt);
                        Self::set_held_val(&mut b_val, held - amt);
                        Self::set_ledger_status(&mut l_val, LedgerStatus::Normal);

                        bal_t.insert(&client_key, b_val)?;
                        led_t.insert(&l_key, l_val)?;
                    }
                }
                Ok(())
            });

        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    async fn chargeback(&self, cmd: Chargeback) -> Result<()> {
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);
        let client_key = cmd.client.to_be_bytes();

        let result: TransactionResult<(), anyhow::Error> = (&self.balances, &self.ledger)
            .transaction(|(bal_t, led_t)| {
                if let Some(mut l_val) = led_t.get(l_key)?.map(|v| v.to_vec()) {
                    if Self::get_ledger_status(&l_val) != LedgerStatus::Disputed {
                        return Ok(());
                    }

                    if let Some(mut b_val) = bal_t.get(client_key)?.map(|v| v.to_vec()) {
                        let amt = Self::get_ledger_amount(&l_val);
                        let held = Self::get_held_val(&b_val);

                        Self::set_held_val(&mut b_val, held - amt);
                        Self::set_frozen(&mut b_val, true);
                        Self::set_ledger_status(&mut l_val, LedgerStatus::ChargedBack);

                        bal_t.insert(&client_key, b_val)?;
                        led_t.insert(&l_key, l_val)?;
                    }
                }
                Ok(())
            });

        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    async fn get_client_report(&self) -> BoxStream<'_, ClientReport> {
        let iter = self.balances.iter();
        let stream = futures::stream::iter(iter).map(|item| {
            let (c_key, c_val) = item.expect("Sled database error during report generation");
            let cid = u16::from_be_bytes(c_key[..].try_into().unwrap());
            let avail = Self::get_balance_val(&c_val);
            let held = Self::get_held_val(&c_val);
            let locked = Self::is_frozen(&c_val);

            ClientReport {
                client: cid,
                available: avail,
                held,
                total: avail + held,
                locked,
            }
        });
        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup_store() -> SledStore {
        // Passing None triggers Config::default().temporary(true)
        SledStore::new(None)
            .await
            .expect("Failed to create SledStore")
    }

    #[tokio::test]
    async fn test_deposit_new_account() -> Result<()> {
        let store = setup_store().await;

        let cmd = Deposit {
            client: 1,
            tx: 1,
            amount: 100.0,
        };
        store.deposit(cmd).await?;

        // Verify balance record
        let bal_bytes = store
            .balances
            .get(1u16.to_be_bytes())?
            .context("Account should exist")?;

        assert_eq!(SledStore::get_balance_val(&bal_bytes), 100.0);
        assert!(!SledStore::is_frozen(&bal_bytes));
        Ok(())
    }

    #[tokio::test]
    async fn test_deposit_existing_account_accumulation() -> Result<()> {
        let store = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store
            .deposit(Deposit {
                client: 1,
                tx: 2,
                amount: 50.5,
            })
            .await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 150.5);
        Ok(())
    }

    #[tokio::test]
    async fn test_deposit_frozen_account_fails() -> Result<()> {
        let store = setup_store().await;

        // Manually freeze the account
        let mut bal = vec![0u8; 9];
        SledStore::set_frozen(&mut bal, true);
        store.balances.insert(1u16.to_be_bytes(), bal)?;

        let result = store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("frozen"));
        Ok(())
    }

    #[tokio::test]
    async fn test_withdraw_non_existent_no_error() -> Result<()> {
        let store = setup_store().await;
        let cmd = Withdrawal {
            client: 99,
            tx: 1,
            amount: 50.0,
        };
        let result = store.withdrawal(cmd).await;
        assert!(
            result.is_ok(),
            "Withdrawal from non-existent should be silent"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_withdraw_insufficient_funds_silent() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 10.0,
            })
            .await?;

        let cmd = Withdrawal {
            client: 1,
            tx: 2,
            amount: 50.0,
        };
        store.withdrawal(cmd).await?; // Your impl returns Ok(()) if funds are low

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 10.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_withdraw_success() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        store
            .withdrawal(Withdrawal {
                client: 1,
                tx: 2,
                amount: 40.0,
            })
            .await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 60.0);
        Ok(())
    }

    // --- DISPUTE / RESOLVE TESTS ---

    #[tokio::test]
    async fn test_dispute_normal_effective() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 0.0);
        assert_eq!(SledStore::get_held_val(&bal_bytes), 100.0);

        let led_bytes = store
            .ledger
            .get(SledStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            SledStore::get_ledger_status(&led_bytes),
            LedgerStatus::Disputed
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_disputed_effective() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        // Resolve the dispute
        store.resolve(Resolve { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 100.0);
        assert_eq!(SledStore::get_held_val(&bal_bytes), 0.0);

        let led_bytes = store
            .ledger
            .get(SledStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            SledStore::get_ledger_status(&led_bytes),
            LedgerStatus::Normal
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_dispute_against_disputed_ignored() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?; // Available 0, Held 100

        // Duplicate dispute
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 0.0);
        assert_eq!(SledStore::get_held_val(&bal_bytes), 100.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_chargeback_against_dispute_effective() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        // Chargeback the dispute
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert!(SledStore::is_frozen(&bal_bytes));
        assert_eq!(SledStore::get_held_val(&bal_bytes), 0.0);

        let led_bytes = store
            .ledger
            .get(SledStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            SledStore::get_ledger_status(&led_bytes),
            LedgerStatus::ChargedBack
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_chargeback_against_normal_ignored() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        // Chargeback without a prior dispute should be ignored
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert!(!SledStore::is_frozen(&bal_bytes));
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 100.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_resolve_against_chargeback_ignored() -> Result<()> {
        let store = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        // Attempt to resolve after a chargeback has finalized
        store.resolve(Resolve { client: 1, tx: 1 }).await?;

        let bal_bytes = store.balances.get(1u16.to_be_bytes())?.unwrap();
        assert!(SledStore::is_frozen(&bal_bytes));
        assert_eq!(SledStore::get_balance_val(&bal_bytes), 0.0);
        Ok(())
    }
}
