use crate::{
    error::KnightBusError,
    runtime::{MmapWalkRuntime, WalkQueryRuntime},
    truth::TruthGraphIndex,
    types::{VerificationFamilySummary, VerificationSummary},
};

pub fn run_parity_verification(
    truth_index: &TruthGraphIndex,
    runtime: &MmapWalkRuntime,
) -> Result<VerificationSummary, KnightBusError> {
    let mut family_summaries = Vec::new();
    let mut total_checked_queries = 0_usize;

    for family in crate::types::QueryFamily::ALL {
        let mut checked_queries = 0_usize;
        for seed_key in truth_index.seed_keys_for_family(family) {
            let expected = truth_index
                .neighbors_within(&seed_key, family.direction(), family.hops())?
                .into_iter()
                .map(|key| key.to_string())
                .collect::<Vec<_>>();
            let actual = runtime.query_keys_for_family(&seed_key, family)?;

            if expected != actual {
                return Err(KnightBusError::ParityMismatch {
                    family: family.label().to_owned(),
                    entity: seed_key.to_string(),
                    expected,
                    actual,
                });
            }

            checked_queries += 1;
        }

        total_checked_queries += checked_queries;
        family_summaries.push(VerificationFamilySummary {
            family,
            checked_queries,
        });
    }

    Ok(VerificationSummary {
        total_checked_queries,
        families: family_summaries,
    })
}
