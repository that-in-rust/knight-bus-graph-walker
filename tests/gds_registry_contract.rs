use std::collections::HashSet;

use knight_bus::{
    GdsEntryKind, GdsProcedureFamily, GdsProcedureMode, GdsSupportStatus, KnightBusError,
    find_gds_entry_spec, gds_inventory_row_count, gds_procedure_specs,
    require_registered_gds_procedure, require_supported_gds_procedure,
};

#[test]
fn gds_inventory_rows_start_with_gds_prefix_now() {
    let specs = gds_procedure_specs();
    assert_eq!(specs.len(), gds_inventory_row_count());
    assert!(!specs.is_empty(), "inventory must not be empty");
    assert!(specs.iter().all(|spec| spec.name.starts_with("gds.")));
}

#[test]
fn gds_inventory_contains_pagerank_rows_now() {
    let stream_spec = find_gds_entry_spec(GdsEntryKind::Procedure, "gds.pageRank.stream")
        .expect("pageRank stream must exist");
    let estimate_spec =
        find_gds_entry_spec(GdsEntryKind::Procedure, "gds.pageRank.stream.estimate")
            .expect("pageRank stream estimate must exist");

    assert_eq!(stream_spec.procedure_mode, GdsProcedureMode::Stream);
    assert_eq!(estimate_spec.procedure_mode, GdsProcedureMode::Estimate);
    assert_eq!(
        stream_spec.estimate_name.as_deref(),
        Some("gds.pageRank.stream.estimate")
    );
}

#[test]
fn gds_inventory_covers_major_families_now() {
    let families = gds_procedure_specs()
        .iter()
        .map(|spec| spec.family)
        .collect::<HashSet<_>>();

    for required_family in [
        GdsProcedureFamily::Catalog,
        GdsProcedureFamily::Centrality,
        GdsProcedureFamily::Community,
        GdsProcedureFamily::Embeddings,
        GdsProcedureFamily::MachineLearning,
        GdsProcedureFamily::Misc,
        GdsProcedureFamily::PathFinding,
        GdsProcedureFamily::PipelineCatalog,
        GdsProcedureFamily::Similarity,
        GdsProcedureFamily::Sysinfo,
    ] {
        assert!(
            families.contains(&required_family),
            "missing family {}",
            required_family.label()
        );
    }
}

#[test]
fn gds_registry_keys_are_unique_now() {
    let mut seen = HashSet::new();

    for spec in gds_procedure_specs() {
        assert!(
            seen.insert((spec.entry_kind, spec.name.clone())),
            "duplicate registry key for {} `{}`",
            spec.entry_kind.label(),
            spec.name
        );
    }
}

#[test]
fn gds_estimate_links_are_consistent_now() {
    for spec in gds_procedure_specs()
        .iter()
        .filter(|spec| spec.entry_kind == GdsEntryKind::Procedure)
    {
        match spec.procedure_mode {
            GdsProcedureMode::Estimate => assert!(spec.estimate_name.is_none()),
            _ => {
                if let Some(estimate_name) = spec.estimate_name.as_deref() {
                    let estimate_spec = find_gds_entry_spec(GdsEntryKind::Procedure, estimate_name)
                        .expect("linked estimate row must exist");
                    assert_eq!(estimate_spec.procedure_mode, GdsProcedureMode::Estimate);
                }
            }
        }
    }
}

#[test]
fn gds_registered_and_unknown_errors_differ_now() {
    let registered_error =
        require_supported_gds_procedure("gds.pageRank.stream").expect_err("not implemented yet");
    match registered_error {
        KnightBusError::UnsupportedRegisteredGdsEntry {
            entry_kind,
            name,
            support_status,
        } => {
            assert_eq!(entry_kind, "procedure");
            assert_eq!(name, "gds.pageRank.stream");
            assert_eq!(
                support_status,
                GdsSupportStatus::NeedsArchitectureSpike.label()
            );
        }
        other => panic!("expected registered unsupported error, got {other:?}"),
    }

    let unknown_error = require_supported_gds_procedure("gds.thisDoesNotExist.stream")
        .expect_err("unknown procedure must fail");
    match unknown_error {
        KnightBusError::UnknownGdsEntry { entry_kind, name } => {
            assert_eq!(entry_kind, "procedure");
            assert_eq!(name, "gds.thisDoesNotExist.stream");
        }
        other => panic!("expected unknown GDS entry error, got {other:?}"),
    }
}

#[test]
fn gds_registered_lookup_preserves_inventory_count_now() {
    let registered_rows = gds_procedure_specs()
        .iter()
        .filter(|spec| spec.entry_kind == GdsEntryKind::Procedure)
        .count();
    assert!(registered_rows > 0);

    let resolved_rows = gds_procedure_specs()
        .iter()
        .filter(|spec| spec.entry_kind == GdsEntryKind::Procedure)
        .map(|spec| require_registered_gds_procedure(&spec.name).expect("row resolves"))
        .count();

    assert_eq!(registered_rows, resolved_rows);
}
