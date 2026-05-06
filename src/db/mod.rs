pub(crate) mod rocks;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;

#[async_trait]
pub(crate) trait StateStore {
    async fn deposit(&self, cmd: Deposit) -> Result<()>;
    async fn withdrawal(&self, cmd: Withdrawal) -> Result<()>;
    async fn dispute(&self, cmd: Dispute) -> Result<()>;
    async fn resolve(&self, cmd: Resolve) -> Result<()>;
    async fn chargeback(&self, cmd: Chargeback) -> Result<()>;
    async fn get_client_report(&self) -> BoxStream<'_, ClientReport>;
}

#[derive(Debug, PartialEq)]
pub(crate) struct ClientReport {
    pub(crate) client: u16,
    pub(crate) available: f32,
    pub(crate) held: f32,
    pub(crate) total: f32,
    pub(crate) locked: bool,
}

type ClientId = u16;
type TxId = u32;
type Amount = f32;
pub(crate) struct Deposit {
    pub(crate) client: ClientId,
    pub(crate) tx: TxId,
    pub(crate) amount: Amount,
}
pub(crate) struct Withdrawal {
    pub(crate) client: ClientId,
    #[allow(dead_code)]
    pub(crate) tx: TxId,
    pub(crate) amount: Amount,
}
pub(crate) struct Dispute {
    pub(crate) client: ClientId,
    pub(crate) tx: TxId,
}
pub(crate) struct Resolve {
    pub(crate) client: ClientId,
    pub(crate) tx: TxId,
}
pub(crate) struct Chargeback {
    pub(crate) client: ClientId,
    pub(crate) tx: TxId,
}
