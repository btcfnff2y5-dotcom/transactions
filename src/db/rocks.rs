use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use rocksdb::{ColumnFamilyDescriptor, DB, Options, WriteBatch};
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

pub(crate) struct RocksStore {
    db: DB,
}

impl RocksStore {
    pub async fn new(path: PathBuf) -> Result<Self> {
        let db_opts = {
            let mut db_opts = Options::default();
            db_opts.create_if_missing(true);
            db_opts.create_missing_column_families(true);
            db_opts
        };
        let descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(CF_BALANCES, Options::default()),
            ColumnFamilyDescriptor::new(CF_LEDGER, {
                let mut led_opts = Options::default();
                led_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(2));
                led_opts
            }),
        ];

        Ok(Self {
            db: DB::open_cf_descriptors(&db_opts, path, descriptors)?,
        })
    }

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

    // New Helper: Get held value from bytes 5-8
    fn get_held_val(bal: &[u8]) -> f32 {
        f32::from_be_bytes(bal[5..9].try_into().unwrap_or([0u8; 4]))
    }

    // New Helper: Set held value in bytes 5-8
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
impl StateStore for RocksStore {
    async fn deposit(&self, cmd: Deposit) -> Result<()> {
        let cf_bal = self
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let cf_led = self.db.cf_handle(CF_LEDGER).context("Ledger CF missing")?;

        let client_key = cmd.client.to_be_bytes();
        let mut bal = self
            .db
            .get_cf(&cf_bal, client_key)?
            .unwrap_or_else(|| vec![0; 9]); // Changed to 9 bytes

        if Self::is_frozen(&bal) {
            bail!("Account is frozen");
        }

        let val = Self::get_balance_val(&bal) + cmd.amount;
        Self::set_balance_val(&mut bal, val);

        let led_key = Self::create_ledger_key(cmd.client, cmd.tx);
        let led_val = Self::create_ledger_val(cmd.amount, LedgerStatus::Normal);

        let mut batch = WriteBatch::default();
        batch.put_cf(&cf_bal, client_key, bal);
        batch.put_cf(&cf_led, led_key, led_val);
        self.db.write(batch)?;
        Ok(())
    }

    async fn withdrawal(&self, cmd: Withdrawal) -> Result<()> {
        let cf_bal = self
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let client_key = cmd.client.to_be_bytes();

        if let Some(mut bal) = self.db.get_cf(&cf_bal, client_key)? {
            if Self::is_frozen(&bal) {
                bail!("Account is frozen");
            }
            let avail = Self::get_balance_val(&bal);
            if avail >= cmd.amount {
                Self::set_balance_val(&mut bal, avail - cmd.amount);
                self.db.put_cf(&cf_bal, client_key, bal)?;
            } else {
                bail!("Insufficient funds");
            }
        }
        Ok(())
    }

    async fn dispute(&self, cmd: Dispute) -> Result<()> {
        let cf_bal = self
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let cf_led = self.db.cf_handle(CF_LEDGER).context("Ledger CF missing")?;
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);

        if let Some(mut l_val) = self.db.get_cf(&cf_led, l_key)? {
            if Self::get_ledger_status(&l_val) != LedgerStatus::Normal {
                return Ok(());
            }

            let amt = Self::get_ledger_amount(&l_val);
            let mut b_val = self
                .db
                .get_cf(&cf_bal, cmd.client.to_be_bytes())?
                .context("Balance not found for dispute")?;

            let avail = Self::get_balance_val(&b_val);
            let held = Self::get_held_val(&b_val);

            // Move funds from available to held
            Self::set_balance_val(&mut b_val, avail - amt);
            Self::set_held_val(&mut b_val, held + amt);
            Self::set_ledger_status(&mut l_val, LedgerStatus::Disputed);

            let mut batch = WriteBatch::default();
            batch.put_cf(&cf_bal, cmd.client.to_be_bytes(), b_val);
            batch.put_cf(&cf_led, l_key, l_val);
            self.db.write(batch)?;
        }
        Ok(())
    }

    async fn resolve(&self, cmd: Resolve) -> Result<()> {
        let cf_bal = self
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let cf_led = self.db.cf_handle(CF_LEDGER).context("Ledger CF missing")?;
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);

        if let Some(mut l_val) = self.db.get_cf(&cf_led, l_key)? {
            if Self::get_ledger_status(&l_val) != LedgerStatus::Disputed {
                return Ok(());
            }

            let amt = Self::get_ledger_amount(&l_val);
            let mut b_val = self
                .db
                .get_cf(&cf_bal, cmd.client.to_be_bytes())?
                .context("Balance not found for resolve")?;

            let avail = Self::get_balance_val(&b_val);
            let held = Self::get_held_val(&b_val);

            // Move funds back from held to available
            Self::set_balance_val(&mut b_val, avail + amt);
            Self::set_held_val(&mut b_val, held - amt);
            Self::set_ledger_status(&mut l_val, LedgerStatus::Normal);

            let mut batch = WriteBatch::default();
            batch.put_cf(&cf_bal, cmd.client.to_be_bytes(), b_val);
            batch.put_cf(&cf_led, l_key, l_val);
            self.db.write(batch)?;
        }
        Ok(())
    }

    async fn chargeback(&self, cmd: Chargeback) -> Result<()> {
        let cf_bal = self
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let cf_led = self.db.cf_handle(CF_LEDGER).context("Ledger CF missing")?;
        let l_key = Self::create_ledger_key(cmd.client, cmd.tx);

        if let Some(mut l_val) = self.db.get_cf(&cf_led, l_key)? {
            if Self::get_ledger_status(&l_val) != LedgerStatus::Disputed {
                return Ok(());
            }

            let amt = Self::get_ledger_amount(&l_val);
            let mut b_val = self
                .db
                .get_cf(&cf_bal, cmd.client.to_be_bytes())?
                .context("Balance not found for chargeback")?;

            let held = Self::get_held_val(&b_val);

            // Remove funds from held and freeze
            Self::set_held_val(&mut b_val, held - amt);
            Self::set_frozen(&mut b_val, true);
            Self::set_ledger_status(&mut l_val, LedgerStatus::ChargedBack);

            let mut batch = WriteBatch::default();
            batch.put_cf(&cf_bal, cmd.client.to_be_bytes(), b_val);
            batch.put_cf(&cf_led, l_key, l_val);
            self.db.write(batch)?;
        }
        Ok(())
    }

    async fn get_client_report(&self) -> BoxStream<'_, ClientReport> {
        let cf_bal = self.db.cf_handle(CF_BALANCES).expect("Balances CF missing");
        let iter = self.db.iterator_cf(&cf_bal, rocksdb::IteratorMode::Start);

        let stream = futures::stream::iter(iter).map(move |item| {
            let (c_key, c_val) = item.expect("DB Read Error");
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
    use tempfile::tempdir;

    async fn setup_store() -> (RocksStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let store = RocksStore::new(dir.path().to_path_buf()).await.unwrap();
        (store, dir)
    }

    // 1. Can deposit into a new, non-existing account
    #[tokio::test]
    async fn test_deposit_new_account() -> Result<()> {
        let (store, _dir) = setup_store().await;

        let cmd = Deposit {
            client: 1,
            tx: 1,
            amount: 100.0,
        };
        store.deposit(cmd).await?;

        // Verify the account was created with the correct balance
        let cf_bal = store
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let bal_bytes = store
            .db
            .get_cf(&cf_bal, 1u16.to_be_bytes())?
            .context("Account should exist")?;

        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 100.0);
        assert!(!RocksStore::is_frozen(&bal_bytes));
        Ok(())
    }

    // 2. Can deposit into existing account and it correctly accumulates
    #[tokio::test]
    async fn test_deposit_existing_account_accumulation() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Initial deposit
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        // Secondary deposit to the same account
        store
            .deposit(Deposit {
                client: 1,
                tx: 2,
                amount: 50.5,
            })
            .await?;

        let cf_bal = store
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();

        // Final balance should be 150.5
        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 150.5);
        Ok(())
    }

    // 3. Cannot deposit into a frozen account
    #[tokio::test]
    async fn test_deposit_frozen_account_fails() -> Result<()> {
        let (store, _dir) = setup_store().await;
        let cf_bal = store
            .db
            .cf_handle(CF_BALANCES)
            .context("Balances CF missing")?;

        // FIX: Must be 9 bytes now
        let mut bal = vec![0u8; 9];
        RocksStore::set_frozen(&mut bal, true);
        store.db.put_cf(&cf_bal, 1u16.to_be_bytes(), bal)?;

        let result = store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Account is frozen");
        Ok(())
    }

    // 1. Cannot withdraw from non-existent account, no error returned
    #[tokio::test]
    async fn test_withdraw_non_existent_no_error() -> Result<()> {
        let (store, _dir) = setup_store().await;

        let cmd = Withdrawal {
            client: 99,
            tx: 1,
            amount: 50.0,
        };
        let result = store.withdrawal(cmd).await;

        assert!(
            result.is_ok(),
            "Should not return error for non-existent account"
        );
        Ok(())
    }

    // 2. Cannot withdraw (insufficient funds) from an existing account, error returned
    #[tokio::test]
    async fn test_withdraw_insufficient_funds_fails() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Setup existing account with 10.0
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 10.0,
            })
            .await?;

        // Attempt to withdraw 50.0
        let cmd = Withdrawal {
            client: 1,
            tx: 2,
            amount: 50.0,
        };
        let result = store.withdrawal(cmd).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Insufficient funds");
        Ok(())
    }

    // 3. Can withdraw from an existing account with appropriate funds
    #[tokio::test]
    async fn test_withdraw_success() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Setup existing account with 100.0
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        // Withdraw 40.0
        let cmd = Withdrawal {
            client: 1,
            tx: 2,
            amount: 40.0,
        };
        store.withdrawal(cmd).await?;

        // Verify remaining balance is 60.0
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 60.0);
        Ok(())
    }

    // 4. Cannot withdraw from frozen account
    #[tokio::test]
    async fn test_withdraw_frozen_fails_even_with_funds() -> Result<()> {
        let (store, _dir) = setup_store().await;
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let mut bal = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();

        // Ensure this buffer is 9 bytes before calling set_frozen (it will be if returned from DB)
        assert_eq!(bal.len(), 9);
        RocksStore::set_frozen(&mut bal, true);
        store.db.put_cf(&cf_bal, 1u16.to_be_bytes(), bal)?;

        let result = store
            .withdrawal(Withdrawal {
                client: 1,
                tx: 2,
                amount: 10.0,
            })
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Account is frozen");
        Ok(())
    }

    #[tokio::test]
    async fn test_dispute_non_existent_silent() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Transaction 99 doesn't exist in the ledger
        let cmd = Dispute { client: 1, tx: 99 };
        let result = store.dispute(cmd).await;

        assert!(
            result.is_ok(),
            "Dispute against non-existent record should be silent"
        );
        Ok(())
    }

    // 2. Dispute against normal transaction is effective
    #[tokio::test]
    async fn test_dispute_normal_effective() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();

        // NEW ASSERTIONS: Verify both available AND held
        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 0.0);
        assert_eq!(RocksStore::get_held_val(&bal_bytes), 100.0);

        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::Disputed
        );

        Ok(())
    }

    // 3. Dispute against marked chargeback is nothing (idempotent/ignored)
    #[tokio::test]
    async fn test_dispute_against_chargeback_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?; // Status is now ChargedBack

        // Snapshot available balance before the duplicate dispute
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_before = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        let avail_before = RocksStore::get_balance_val(&bal_before);

        // Attempt another dispute
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        // Verify balance hasn't changed further
        let bal_after = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert_eq!(RocksStore::get_balance_val(&bal_after), avail_before);
        Ok(())
    }

    // 4. Dispute against marked dispute is nothing (no double-deduction)
    #[tokio::test]
    async fn test_dispute_against_disputed_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?; // First dispute: Avail becomes 0.0

        // Attempt second dispute on same transaction
        store.dispute(Dispute { client: 1, tx: 1 }).await?;

        // Balance should still be 0.0, not -100.0
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 0.0);
        Ok(())
    }

    // 1. Resolve against nothing (non-existent transaction) is nothing
    #[tokio::test]
    async fn test_resolve_non_existent_silent() -> Result<()> {
        let (store, _dir) = setup_store().await;

        let cmd = Resolve { client: 1, tx: 99 };
        let result = store.resolve(cmd).await;

        assert!(
            result.is_ok(),
            "Resolve against non-existent record should be silent"
        );
        Ok(())
    }

    // 2. Resolve against disputed takes effect (returns funds to available)
    #[tokio::test]
    async fn test_resolve_disputed_effective() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Setup: Deposit 100, then Dispute it (Available becomes 0.0)
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

        // Verify balance: Available should be back to 100.0
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert_eq!(RocksStore::get_balance_val(&bal_bytes), 100.0);

        // Verify ledger status returned to Normal
        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::Normal
        );

        Ok(())
    }

    // 3. Resolve against chargeback is nothing
    #[tokio::test]
    async fn test_resolve_against_chargeback_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        // Setup: Deposit -> Dispute -> Chargeback
        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        // Snapshot available balance (should be 0.0)
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let avail_before =
            RocksStore::get_balance_val(&store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap());

        // Attempt to resolve a chargeback
        store.resolve(Resolve { client: 1, tx: 1 }).await?;

        // Balance should NOT have increased
        let avail_after =
            RocksStore::get_balance_val(&store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap());
        assert_eq!(avail_after, avail_before);

        // Status should still be ChargedBack
        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::ChargedBack
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_chargeback_against_normal_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;

        // Attempt chargeback on a normal transaction (not disputed)
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        // Verify account is NOT frozen and status is still Normal
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert!(!RocksStore::is_frozen(&bal_bytes));

        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::Normal
        );
        Ok(())
    }

    // 2. Chargeback against chargeback is nothing (idempotent)
    #[tokio::test]
    async fn test_chargeback_against_chargeback_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?; // First chargeback

        // Attempt second chargeback
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::ChargedBack
        );
        Ok(())
    }

    // 3. Chargeback against resolved is nothing
    #[tokio::test]
    async fn test_chargeback_against_resolved_ignored() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?;
        store.resolve(Resolve { client: 1, tx: 1 }).await?; // Status is back to Normal

        // Attempt chargeback
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert!(!RocksStore::is_frozen(&bal_bytes));
        Ok(())
    }

    // 4. Chargeback against dispute takes effect along with frozen
    #[tokio::test]
    async fn test_chargeback_against_dispute_effective() -> Result<()> {
        let (store, _dir) = setup_store().await;

        store
            .deposit(Deposit {
                client: 1,
                tx: 1,
                amount: 100.0,
            })
            .await?;
        store.dispute(Dispute { client: 1, tx: 1 }).await?; // Available: 0, Held: 100

        // Chargeback the dispute
        store.chargeback(Chargeback { client: 1, tx: 1 }).await?;

        // Verify account IS frozen
        let cf_bal = store.db.cf_handle(CF_BALANCES).unwrap();
        let bal_bytes = store.db.get_cf(&cf_bal, 1u16.to_be_bytes())?.unwrap();
        assert!(RocksStore::is_frozen(&bal_bytes));

        // Verify ledger status is ChargedBack
        let cf_led = store.db.cf_handle(CF_LEDGER).unwrap();
        let led_bytes = store
            .db
            .get_cf(&cf_led, RocksStore::create_ledger_key(1, 1))?
            .unwrap();
        assert_eq!(
            RocksStore::get_ledger_status(&led_bytes),
            LedgerStatus::ChargedBack
        );

        Ok(())
    }
}
