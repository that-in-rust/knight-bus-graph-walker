use std::collections::HashSet;

use knight_bus::{
    GdsEntryKind, GdsProcedureFamily, GdsProcedureMode, GdsSupportStatus, KnightBusError,
    effective_gds_support_status, find_gds_entry_spec, gds_inventory_row_count,
    gds_procedure_specs, require_registered_gds_procedure, require_supported_gds_entry,
    require_supported_gds_procedure,
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

#[test]
fn built_in_catalog_entries_are_supported_now() {
    let project_estimate = require_supported_gds_procedure("gds.graph.project.estimate")
        .expect("graph.project.estimate");
    let node_props = require_supported_gds_procedure("gds.graph.nodeProperties.stream")
        .expect("nodeProperties.stream");
    let node_prop = require_supported_gds_procedure("gds.graph.nodeProperty.stream")
        .expect("nodeProperty.stream");
    let rel_props = require_supported_gds_procedure("gds.graph.relationshipProperties.stream")
        .expect("relationshipProperties.stream");
    let rel_prop = require_supported_gds_procedure("gds.graph.relationshipProperty.stream")
        .expect("relationshipProperty.stream");
    let exists_proc =
        require_supported_gds_procedure("gds.graph.exists").expect("graph.exists procedure");
    let exists_func = require_supported_gds_entry(GdsEntryKind::UserFunction, "gds.graph.exists")
        .expect("graph.exists function");
    let list_proc = require_supported_gds_procedure("gds.graph.list").expect("graph.list");
    let drop_proc = require_supported_gds_procedure("gds.graph.drop").expect("graph.drop");
    let size_proc =
        require_supported_gds_procedure("gds.internal.graph.sizeOf").expect("graph.sizeOf");

    assert_eq!(
        effective_gds_support_status(project_estimate),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(node_props),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(node_prop),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(rel_props),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(rel_prop),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(exists_proc),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(exists_func),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(list_proc),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(drop_proc),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
    assert_eq!(
        effective_gds_support_status(size_proc),
        GdsSupportStatus::P1ImplementedExactLowRam
    );
}
