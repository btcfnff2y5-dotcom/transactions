use transactions::TransactArgs;

use anyhow::Result;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    use clap::Parser;
    TransactArgs::try_parse()?.run()
}
