use std::{collections::HashSet, sync::OnceLock};

use serde::Deserialize;

use crate::error::KnightBusError;

pub const GDS_PUBLIC_SURFACE_INVENTORY_PATH: &str =
    "docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GdsEntryKind {
    Procedure,
    UserFunction,
}

impl GdsEntryKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Procedure => "procedure",
            Self::UserFunction => "user_function",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum GdsProcedureFamily {
    Catalog,
    Centrality,
    Common,
    Community,
    Embeddings,
    MachineLearning,
    Misc,
    PathFinding,
    PipelineCatalog,
    Similarity,
    Sysinfo,
    Test,
}

impl GdsProcedureFamily {
    pub fn label(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Centrality => "centrality",
            Self::Common => "common",
            Self::Community => "community",
            Self::Embeddings => "embeddings",
            Self::MachineLearning => "machine-learning",
            Self::Misc => "misc",
            Self::PathFinding => "path-finding",
            Self::PipelineCatalog => "pipeline-catalog",
            Self::Similarity => "similarity",
            Self::Sysinfo => "sysinfo",
            Self::Test => "test",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GdsAnnotationMode {
    Read,
    Write,
    MissingEvidence,
}

impl GdsAnnotationMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Read => "READ",
            Self::Write => "WRITE",
            Self::MissingEvidence => "MissingEvidence",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GdsProcedureMode {
    Stream,
    Stats,
    Mutate,
    Write,
    Estimate,
    Other,
}

impl GdsProcedureMode {
    pub fn label(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::Stats => "stats",
            Self::Mutate => "mutate",
            Self::Write => "write",
            Self::Estimate => "estimate",
            Self::Other => "other",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GdsSupportStatus {
    P0RegisteredCompatible,
    P1ImplementedExactLowRam,
    P2ImplementedLater,
    NeedsArchitectureSpike,
    UnsupportedButRegistered,
    ExplicitlyOutOfScope,
}

impl GdsSupportStatus {
    pub fn label(self) -> &'static str {
        match self {
            Self::P0RegisteredCompatible => "P0-RegisteredCompatible",
            Self::P1ImplementedExactLowRam => "P1-ImplementedExactLowRam",
            Self::P2ImplementedLater => "P2-ImplementedLater",
            Self::NeedsArchitectureSpike => "NeedsArchitectureSpike",
            Self::UnsupportedButRegistered => "UnsupportedButRegistered",
            Self::ExplicitlyOutOfScope => "ExplicitlyOutOfScope",
        }
    }

    pub fn is_supported_now(self) -> bool {
        matches!(self, Self::P1ImplementedExactLowRam)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GdsProcedureSpec {
    pub entry_kind: GdsEntryKind,
    pub name: String,
    pub family: GdsProcedureFamily,
    pub annotation_mode: GdsAnnotationMode,
    pub procedure_mode: GdsProcedureMode,
    pub estimate_name: Option<String>,
    pub config_inputs: String,
    pub result_columns: String,
    pub source_file: String,
    pub source_line: usize,
    pub support_status: GdsSupportStatus,
    pub architecture_needs: String,
    pub memory_needs: String,
    pub test_oracle: String,
    pub prd_impact: String,
    pub deprecated_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GdsRegistryKey {
    pub entry_kind: GdsEntryKind,
    pub name: String,
}

impl GdsRegistryKey {
    pub fn new(entry_kind: GdsEntryKind, name: impl Into<String>) -> Self {
        Self {
            entry_kind,
            name: name.into(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawInventoryRow {
    entry_kind: String,
    name: String,
    family: String,
    mode: String,
    estimate_variant: String,
    config_inputs: String,
    result_columns: String,
    source_file: String,
    source_line: usize,
    support_status: String,
    architecture_needs: String,
    memory_needs: String,
    test_oracle: String,
    prd_impact: String,
    deprecated_by: String,
}

static GDS_PROCEDURE_SPECS: OnceLock<Vec<GdsProcedureSpec>> = OnceLock::new();

pub fn gds_procedure_specs() -> &'static [GdsProcedureSpec] {
    GDS_PROCEDURE_SPECS
        .get_or_init(load_gds_procedure_specs_now)
        .as_slice()
}

pub fn gds_inventory_row_count() -> usize {
    gds_procedure_specs().len()
}

pub fn find_gds_entry_spec(
    entry_kind: GdsEntryKind,
    name: &str,
) -> Option<&'static GdsProcedureSpec> {
    gds_procedure_specs()
        .iter()
        .find(|spec| spec.entry_kind == entry_kind && spec.name == name)
}

pub fn require_registered_gds_entry(
    entry_kind: GdsEntryKind,
    name: &str,
) -> Result<&'static GdsProcedureSpec, KnightBusError> {
    find_gds_entry_spec(entry_kind, name).ok_or_else(|| KnightBusError::UnknownGdsEntry {
        entry_kind: entry_kind.label().to_owned(),
        name: name.to_owned(),
    })
}

pub fn require_supported_gds_entry(
    entry_kind: GdsEntryKind,
    name: &str,
) -> Result<&'static GdsProcedureSpec, KnightBusError> {
    let spec = require_registered_gds_entry(entry_kind, name)?;
    if spec.support_status.is_supported_now() {
        Ok(spec)
    } else {
        Err(KnightBusError::UnsupportedRegisteredGdsEntry {
            entry_kind: entry_kind.label().to_owned(),
            name: name.to_owned(),
            support_status: spec.support_status.label().to_owned(),
        })
    }
}

pub fn require_registered_gds_procedure(
    name: &str,
) -> Result<&'static GdsProcedureSpec, KnightBusError> {
    require_registered_gds_entry(GdsEntryKind::Procedure, name)
}

pub fn require_supported_gds_procedure(
    name: &str,
) -> Result<&'static GdsProcedureSpec, KnightBusError> {
    require_supported_gds_entry(GdsEntryKind::Procedure, name)
}

fn load_gds_procedure_specs_now() -> Vec<GdsProcedureSpec> {
    let inventory_bytes =
        include_str!("../docs_PRD03/reference-learning/GDS-Public-Surface-Inventory.tsv");
    let mut reader = csv::ReaderBuilder::new()
        .delimiter(b'\t')
        .from_reader(inventory_bytes.as_bytes());

    let raw_rows = reader
        .deserialize::<RawInventoryRow>()
        .collect::<Result<Vec<_>, _>>()
        .expect("GDS public-surface inventory TSV must remain parseable");

    let known_procedure_names = raw_rows
        .iter()
        .filter(|row| row.entry_kind == "procedure")
        .map(|row| row.name.clone())
        .collect::<HashSet<_>>();

    let mut seen_keys = HashSet::new();
    let mut specs = Vec::with_capacity(raw_rows.len());

    for raw_row in raw_rows {
        let entry_kind = parse_gds_entry_kind_now(&raw_row.entry_kind);
        let family = parse_gds_family_now(&raw_row.family);
        let annotation_mode = parse_gds_annotation_mode_now(&raw_row.mode);
        let procedure_mode =
            derive_gds_procedure_mode_now(&raw_row.name, &raw_row.estimate_variant);
        let key = GdsRegistryKey::new(entry_kind, raw_row.name.clone());

        if !seen_keys.insert((key.entry_kind, key.name.clone())) {
            panic!(
                "duplicate GDS registry key detected for {} `{}`",
                key.entry_kind.label(),
                key.name
            );
        }

        let estimate_name = match (entry_kind, procedure_mode) {
            (GdsEntryKind::Procedure, GdsProcedureMode::Estimate) => None,
            (GdsEntryKind::Procedure, _) => {
                let candidate = format!("{}.estimate", raw_row.name);
                known_procedure_names
                    .contains(&candidate)
                    .then_some(candidate)
            }
            (GdsEntryKind::UserFunction, _) => None,
        };

        specs.push(GdsProcedureSpec {
            entry_kind,
            name: raw_row.name,
            family,
            annotation_mode,
            procedure_mode,
            estimate_name,
            config_inputs: raw_row.config_inputs,
            result_columns: raw_row.result_columns,
            source_file: raw_row.source_file,
            source_line: raw_row.source_line,
            support_status: parse_gds_support_status_now(&raw_row.support_status),
            architecture_needs: raw_row.architecture_needs,
            memory_needs: raw_row.memory_needs,
            test_oracle: raw_row.test_oracle,
            prd_impact: raw_row.prd_impact,
            deprecated_by: normalize_optional_field_now(raw_row.deprecated_by),
        });
    }

    specs
}

fn parse_gds_entry_kind_now(value: &str) -> GdsEntryKind {
    match value {
        "procedure" => GdsEntryKind::Procedure,
        "user_function" => GdsEntryKind::UserFunction,
        other => panic!("unsupported GDS entry kind `{other}` in inventory"),
    }
}

fn parse_gds_family_now(value: &str) -> GdsProcedureFamily {
    match value {
        "catalog" => GdsProcedureFamily::Catalog,
        "centrality" => GdsProcedureFamily::Centrality,
        "common" => GdsProcedureFamily::Common,
        "community" => GdsProcedureFamily::Community,
        "embeddings" => GdsProcedureFamily::Embeddings,
        "machine-learning" => GdsProcedureFamily::MachineLearning,
        "misc" => GdsProcedureFamily::Misc,
        "path-finding" => GdsProcedureFamily::PathFinding,
        "pipeline-catalog" => GdsProcedureFamily::PipelineCatalog,
        "similarity" => GdsProcedureFamily::Similarity,
        "sysinfo" => GdsProcedureFamily::Sysinfo,
        "test" => GdsProcedureFamily::Test,
        other => panic!("unsupported GDS family `{other}` in inventory"),
    }
}

fn parse_gds_annotation_mode_now(value: &str) -> GdsAnnotationMode {
    match value {
        "READ" => GdsAnnotationMode::Read,
        "WRITE" => GdsAnnotationMode::Write,
        "MissingEvidence" | "M" => GdsAnnotationMode::MissingEvidence,
        _ => GdsAnnotationMode::MissingEvidence,
    }
}

fn derive_gds_procedure_mode_now(name: &str, estimate_variant: &str) -> GdsProcedureMode {
    if estimate_variant == "yes" || name.ends_with(".estimate") {
        return GdsProcedureMode::Estimate;
    }
    if name.ends_with(".stream") {
        return GdsProcedureMode::Stream;
    }
    if name.ends_with(".stats") {
        return GdsProcedureMode::Stats;
    }
    if name.ends_with(".mutate") {
        return GdsProcedureMode::Mutate;
    }
    if name.ends_with(".write") {
        return GdsProcedureMode::Write;
    }
    GdsProcedureMode::Other
}

fn parse_gds_support_status_now(value: &str) -> GdsSupportStatus {
    match value {
        "P0Registered" | "P0-RegisteredCompatible" => GdsSupportStatus::P0RegisteredCompatible,
        "P1ExactLowRam" | "P1-ImplementedExactLowRam" => GdsSupportStatus::P1ImplementedExactLowRam,
        "P2Later" | "P2-ImplementedLater" => GdsSupportStatus::P2ImplementedLater,
        "NeedsArchitectureSpike" => GdsSupportStatus::NeedsArchitectureSpike,
        "UnsupportedButRegistered" => GdsSupportStatus::UnsupportedButRegistered,
        "ExplicitlyOutOfScope" => GdsSupportStatus::ExplicitlyOutOfScope,
        other => panic!("unsupported GDS support status `{other}` in inventory"),
    }
}

fn normalize_optional_field_now(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() || trimmed == "none" {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
