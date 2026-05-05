use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Deserializer};
use std::ops::Deref;

const PRECISION: u32 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Amount(pub Decimal);

impl<'de> Deserialize<'de> for Amount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let d = rust_decimal::serde::str::deserialize(deserializer)?;
        Amount::try_from(d).map_err(serde::de::Error::custom)
    }
}

impl Deref for Amount {
    type Target = Decimal;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<Decimal> for Amount {
    type Error = anyhow::Error;

    fn try_from(d: Decimal) -> Result<Self, Self::Error> {
        if d < Decimal::ZERO {
            anyhow::bail!("amount must be non-negative");
        }
        Ok(Self(d.round_dp_with_strategy(
            PRECISION,
            RoundingStrategy::ToZero,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use serde_json;

    #[test]
    fn test_boundaries() {
        assert_eq!(
            *serde_json::from_str::<Amount>("\"0.0\"").unwrap(),
            dec!(0.0)
        );

        assert_eq!(
            *serde_json::from_str::<Amount>("\"-0.0\"").unwrap(),
            dec!(0.0)
        );

        assert_eq!(
            serde_json::from_str::<Amount>("\"-0.1\"")
                .unwrap_err()
                .to_string(),
            "amount must be non-negative"
        );
    }

    #[test]
    fn test_no_rounding() {
        assert_eq!(
            *serde_json::from_str::<Amount>("\"12.34\"").unwrap(),
            dec!(12.34)
        );
        assert_eq!(
            *serde_json::from_str::<Amount>("\"12.3456\"").unwrap(),
            dec!(12.3456)
        );

        assert_eq!(
            *serde_json::from_str::<Amount>("\"12.34561\"").unwrap(),
            dec!(12.3456)
        );
        assert_eq!(
            *serde_json::from_str::<Amount>("\"12.34567\"").unwrap(),
            dec!(12.3456)
        );
    }
}
