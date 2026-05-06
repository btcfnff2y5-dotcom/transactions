mod amount;
mod transaction;

pub(crate) type Client = u16;
pub(crate) type Tx = u32;
pub(crate) use amount::Amount;
pub(crate) use transaction::Transaction;
