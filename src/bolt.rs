use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File},
    io::Read,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use boltr::{
    error::BoltError,
    server::{
        AuthCredentials, AuthInfo, AuthValidator, BoltBackend, BoltRecord, ResultMetadata,
        ResultStream, SessionConfig, SessionHandle, SessionProperty, TransactionHandle,
    },
    types::{BoltDict, BoltValue},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::{
    cypher::{
        CypherParameterValue, CypherWalkError, NeighborhoodExecutionLimits,
        NeighborhoodTerminationReason, compile_neighborhood_walk_plan,
        execute_neighborhood_walk_with_limits, hash_canonical_plan_bytes,
    },
    runtime::{GraphAdjacencyRuntime, MmapWalkRuntime},
    types::SnapshotManifest,
};

pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAXIMUM_RESULT_ROWS: usize = 1_000_000;
pub const NEIGHBORHOOD_GRAPH_PROFILE_FILE: &str = "compatibility-profile.json";

#[derive(Debug, Error)]
pub enum BoltCompatibilityStartupError {
    #[error("failed to read snapshot identity file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse snapshot manifest {path}: {source}")]
    Manifest {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse graph profile {path}: {source}")]
    GraphProfileManifest {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid graph profile {path}: {message}")]
    InvalidGraphProfile { path: PathBuf, message: String },
    #[error("invalid Bolt compatibility configuration: {message}")]
    InvalidConfiguration { message: String },
}

#[derive(Debug, Deserialize)]
struct NeighborhoodGraphProfileManifest {
    schema_version: u32,
    profile_version: String,
    node_label: String,
    start_node_id_property: String,
    result_node_id_property: String,
    relationship_type: String,
    minimum_hops: u32,
    maximum_hops: u32,
    node_count: u64,
    relationship_count: u64,
}

pub struct KnightBusBasicAuthValidator {
    username: String,
    username_digest: [u8; 32],
    password_digest: [u8; 32],
}

impl KnightBusBasicAuthValidator {
    pub fn new(username: String, password: String) -> Self {
        Self {
            username_digest: digest_optional_credential(Some(&username)),
            password_digest: digest_optional_credential(Some(&password)),
            username,
        }
    }
}

#[async_trait]
impl AuthValidator for KnightBusBasicAuthValidator {
    async fn validate(&self, credentials: &AuthCredentials) -> Result<AuthInfo, BoltError> {
        if credentials.scheme != "basic" {
            return Err(BoltError::Authentication(
                "Knight Bus accepts only Bolt basic authentication".to_owned(),
            ));
        }
        let supplied_username = digest_optional_credential(credentials.principal.as_deref());
        let supplied_password = digest_optional_credential(credentials.credentials.as_deref());
        let username_matches = bool::from(self.username_digest.ct_eq(&supplied_username));
        let password_matches = bool::from(self.password_digest.ct_eq(&supplied_password));
        if !username_matches || !password_matches {
            return Err(BoltError::Authentication(
                "invalid username or password".to_owned(),
            ));
        }
        Ok(AuthInfo {
            principal: self.username.clone(),
            credentials_expired: false,
        })
    }
}

pub struct KnightBusBoltBackend {
    runtime: Arc<MmapWalkRuntime>,
    next_session_id: AtomicU64,
    snapshot_generation: i64,
    snapshot_hash: String,
    query_timeout: Duration,
    maximum_result_rows: usize,
}

struct AdmittedFailureReceiptContext<'a> {
    query: &'a str,
    canonical_plan_hash: &'a str,
    parameters: &'a BTreeMap<String, CypherParameterValue>,
    snapshot_generation: i64,
    snapshot_hash: &'a str,
    parse_compile_micros: i64,
    execution_micros: i64,
}

impl KnightBusBoltBackend {
    pub fn new(runtime: MmapWalkRuntime) -> Result<Self, BoltCompatibilityStartupError> {
        Self::new_with_execution_limits(runtime, DEFAULT_QUERY_TIMEOUT, DEFAULT_MAXIMUM_RESULT_ROWS)
    }

    pub fn new_with_execution_limits(
        runtime: MmapWalkRuntime,
        query_timeout: Duration,
        maximum_result_rows: usize,
    ) -> Result<Self, BoltCompatibilityStartupError> {
        if Instant::now().checked_add(query_timeout).is_none() {
            return Err(BoltCompatibilityStartupError::InvalidConfiguration {
                message: "query timeout exceeds the platform Instant range".to_owned(),
            });
        }
        if maximum_result_rows > i64::MAX as usize {
            return Err(BoltCompatibilityStartupError::InvalidConfiguration {
                message: "maximum result rows exceeds the Bolt integer range".to_owned(),
            });
        }
        validate_graph_profile_once(&runtime)?;
        let (snapshot_generation, snapshot_hash) = hash_snapshot_identity_once(&runtime)?;
        let snapshot_generation = i64::try_from(snapshot_generation).map_err(|_| {
            BoltCompatibilityStartupError::InvalidConfiguration {
                message: "snapshot generation exceeds the Bolt integer range".to_owned(),
            }
        })?;
        Ok(Self {
            runtime: Arc::new(runtime),
            next_session_id: AtomicU64::new(0),
            snapshot_generation,
            snapshot_hash,
            query_timeout,
            maximum_result_rows,
        })
    }

    pub fn shared_runtime(&self) -> &Arc<MmapWalkRuntime> {
        &self.runtime
    }
}

#[async_trait]
impl BoltBackend for KnightBusBoltBackend {
    async fn create_session(&self, _config: &SessionConfig) -> Result<SessionHandle, BoltError> {
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        Ok(SessionHandle(format!("knight-bus-{session_id}")))
    }

    async fn close_session(&self, _session: &SessionHandle) -> Result<(), BoltError> {
        Ok(())
    }

    async fn configure_session(
        &self,
        _session: &SessionHandle,
        property: SessionProperty,
    ) -> Result<(), BoltError> {
        match property {
            SessionProperty::Database(database) if database == "neo4j" => Ok(()),
            SessionProperty::Database(database) => Err(BoltError::Protocol(format!(
                "database selection `{database}` is outside the neighborhood-walk-v1 profile"
            ))),
        }
    }

    async fn reset_session(&self, _session: &SessionHandle) -> Result<(), BoltError> {
        Ok(())
    }

    async fn execute(
        &self,
        _session: &SessionHandle,
        query: &str,
        parameters: &HashMap<String, BoltValue>,
        extra: &BoltDict,
        transaction: Option<&TransactionHandle>,
    ) -> Result<ResultStream, BoltError> {
        let request_started = Instant::now();
        let deadline = request_started
            .checked_add(self.query_timeout)
            .ok_or_else(|| BoltError::Protocol("query deadline overflow".to_owned()))?;
        let parse_started = request_started;
        if transaction.is_some() {
            return Err(BoltError::Transaction(
                "explicit transactions are outside the read-only auto-commit profile".to_owned(),
            ));
        }
        validate_auto_commit_metadata(extra)?;
        let cypher_parameters = parameters
            .iter()
            .map(|(name, value)| (name.clone(), convert_bolt_parameter_value(value)))
            .collect::<BTreeMap<_, _>>();
        let plan = compile_neighborhood_walk_plan(query, &cypher_parameters)
            .map_err(map_cypher_failure_metadata)?;
        let parse_compile_micros = duration_micros_as_i64(parse_started);
        let canonical_plan_hash = hash_canonical_plan_bytes(&plan);
        let execution_started = Instant::now();
        let execution_limits = NeighborhoodExecutionLimits {
            deadline: Some(deadline),
            maximum_result_rows: Some(self.maximum_result_rows),
            ..NeighborhoodExecutionLimits::default()
        };
        let result =
            match execute_neighborhood_walk_with_limits(&self.runtime, &plan, &execution_limits) {
                Ok(result) => result,
                Err(error) => {
                    let execution_micros = duration_micros_as_i64(execution_started);
                    return Err(map_admitted_failure_metadata(
                        error,
                        AdmittedFailureReceiptContext {
                            query,
                            canonical_plan_hash: &canonical_plan_hash,
                            parameters: &cypher_parameters,
                            snapshot_generation: self.snapshot_generation,
                            snapshot_hash: &self.snapshot_hash,
                            parse_compile_micros,
                            execution_micros,
                        },
                    ));
                }
            };
        let execution_micros = duration_micros_as_i64(execution_started);
        let result_row_count = result.records.len();
        let result_hash = hash_result_records_exact(&result.records);
        let records = result
            .records
            .into_iter()
            .map(|record| BoltRecord {
                values: vec![BoltValue::String(record.node_id)],
            })
            .collect();

        Ok(ResultStream {
            metadata: ResultMetadata {
                columns: result.columns,
                extra: BoltDict::new(),
            },
            records,
            summary: BoltDict::from([
                ("type".to_owned(), BoltValue::String("r".to_owned())),
                (
                    "t_last".to_owned(),
                    BoltValue::Integer((parse_compile_micros + execution_micros) / 1_000),
                ),
                (
                    "knight_bus_receipt".to_owned(),
                    BoltValue::Dict(BoltDict::from([
                        (
                            "query_hash".to_owned(),
                            BoltValue::String(hash_text_content_exact(query)),
                        ),
                        (
                            "canonical_plan_hash".to_owned(),
                            BoltValue::String(canonical_plan_hash),
                        ),
                        (
                            "profile_version".to_owned(),
                            BoltValue::String("knight-bus-neighborhood-walk-v1".to_owned()),
                        ),
                        (
                            "snapshot_generation".to_owned(),
                            BoltValue::Integer(self.snapshot_generation),
                        ),
                        (
                            "snapshot_hash".to_owned(),
                            BoltValue::String(self.snapshot_hash.clone()),
                        ),
                        (
                            "parameter_names".to_owned(),
                            BoltValue::List(
                                cypher_parameters
                                    .keys()
                                    .cloned()
                                    .map(BoltValue::String)
                                    .collect(),
                            ),
                        ),
                        (
                            "result_row_count".to_owned(),
                            BoltValue::Integer(i64::try_from(result_row_count).map_err(|_| {
                                BoltError::Protocol(
                                    "result row count exceeds the Bolt integer range".to_owned(),
                                )
                            })?),
                        ),
                        ("result_hash".to_owned(), BoltValue::String(result_hash)),
                        (
                            "parse_compile_micros".to_owned(),
                            BoltValue::Integer(parse_compile_micros),
                        ),
                        (
                            "execution_micros".to_owned(),
                            BoltValue::Integer(execution_micros),
                        ),
                        (
                            "termination_status".to_owned(),
                            BoltValue::String("success".to_owned()),
                        ),
                        (
                            "resource_high_water_status".to_owned(),
                            BoltValue::String("unavailable".to_owned()),
                        ),
                    ])),
                ),
            ]),
        })
    }

    async fn begin_transaction(
        &self,
        _session: &SessionHandle,
        _extra: &BoltDict,
    ) -> Result<TransactionHandle, BoltError> {
        Err(BoltError::Transaction(
            "explicit transactions are outside the read-only auto-commit profile".to_owned(),
        ))
    }

    async fn commit(
        &self,
        _session: &SessionHandle,
        _transaction: &TransactionHandle,
    ) -> Result<BoltDict, BoltError> {
        Err(BoltError::Transaction(
            "commit is outside the read-only auto-commit profile".to_owned(),
        ))
    }

    async fn rollback(
        &self,
        _session: &SessionHandle,
        _transaction: &TransactionHandle,
    ) -> Result<(), BoltError> {
        Err(BoltError::Transaction(
            "rollback is outside the read-only auto-commit profile".to_owned(),
        ))
    }

    async fn get_server_info(&self) -> Result<BoltDict, BoltError> {
        Ok(BoltDict::from([
            (
                "server".to_owned(),
                BoltValue::String(format!("knight-bus/{}", env!("CARGO_PKG_VERSION"))),
            ),
            (
                "bolt_agent".to_owned(),
                BoltValue::Dict(BoltDict::from([
                    (
                        "product".to_owned(),
                        BoltValue::String("knight-bus".to_owned()),
                    ),
                    (
                        "version".to_owned(),
                        BoltValue::String(env!("CARGO_PKG_VERSION").to_owned()),
                    ),
                ])),
            ),
        ]))
    }
}

fn digest_optional_credential(value: Option<&str>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    match value {
        Some(value) => {
            hasher.update([1]);
            hasher.update(value.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.finalize().into()
}

fn convert_bolt_parameter_value(value: &BoltValue) -> CypherParameterValue {
    match value {
        BoltValue::Null => CypherParameterValue::Null,
        BoltValue::Boolean(value) => CypherParameterValue::Boolean(*value),
        BoltValue::Integer(value) => CypherParameterValue::Integer(*value),
        BoltValue::String(value) => CypherParameterValue::String(value.clone()),
        _ => CypherParameterValue::Unsupported,
    }
}

fn validate_auto_commit_metadata(extra: &BoltDict) -> Result<(), BoltError> {
    for (name, value) in extra {
        match (name.as_str(), value) {
            ("db", BoltValue::String(database)) if database == "neo4j" => {}
            ("mode", BoltValue::String(mode)) if mode == "r" => {}
            ("bookmarks", BoltValue::List(bookmarks)) if bookmarks.is_empty() => {}
            ("bookmarks", _) => {
                return Err(BoltError::Protocol(
                    "bookmarks are outside the read-only auto-commit profile".to_owned(),
                ));
            }
            ("imp_user", _) => {
                return Err(BoltError::Protocol(
                    "impersonation is outside the read-only auto-commit profile".to_owned(),
                ));
            }
            ("db", _) => {}
            _ => {
                return Err(BoltError::Protocol(format!(
                    "Bolt RUN metadata `{name}` is outside the read-only auto-commit profile"
                )));
            }
        }
    }
    Ok(())
}

fn map_cypher_failure_metadata(error: CypherWalkError) -> BoltError {
    BoltError::Query {
        code: cypher_failure_code_now(&error).to_owned(),
        message: error.to_string(),
    }
}

fn map_admitted_failure_metadata(
    error: CypherWalkError,
    context: AdmittedFailureReceiptContext<'_>,
) -> BoltError {
    let parameter_names = context.parameters.keys().collect::<Vec<_>>();
    let message = serde_json::json!({
        "message": error.to_string(),
        "knight_bus_receipt": {
            "query_hash": hash_text_content_exact(context.query),
            "canonical_plan_hash": context.canonical_plan_hash,
            "profile_version": "knight-bus-neighborhood-walk-v1",
            "snapshot_generation": context.snapshot_generation,
            "snapshot_hash": context.snapshot_hash,
            "parameter_names": parameter_names,
            "result_row_count": 0,
            "result_hash": hash_result_records_exact(&[]),
            "parse_compile_micros": context.parse_compile_micros,
            "execution_micros": context.execution_micros,
            "termination_status": termination_status_text_now(&error),
            "resource_high_water_status": "unavailable",
        }
    })
    .to_string();
    BoltError::Query {
        code: cypher_failure_code_now(&error).to_owned(),
        message,
    }
}

fn cypher_failure_code_now(error: &CypherWalkError) -> &'static str {
    match error {
        CypherWalkError::Syntax { .. } => "Neo.ClientError.Statement.SyntaxError",
        CypherWalkError::UnsupportedFeature { .. } => {
            "Neo.ClientError.Statement.UnsupportedFeature"
        }
        CypherWalkError::InvalidParameter { .. } => "Neo.ClientError.Statement.ParameterMissing",
        CypherWalkError::Execution { .. } => "Neo.DatabaseError.General.UnknownError",
        CypherWalkError::Terminated { .. } => "Neo.TransientError.Transaction.Terminated",
    }
}

fn termination_status_text_now(error: &CypherWalkError) -> &'static str {
    match error {
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::DeadlineExceeded,
        } => "deadline_exceeded",
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ResultRowLimitExceeded,
        } => "result_row_limit_exceeded",
        CypherWalkError::Terminated {
            reason: NeighborhoodTerminationReason::ClientCancelled,
        } => "client_cancelled",
        CypherWalkError::Execution { .. } => "execution_failed",
        CypherWalkError::Syntax { .. }
        | CypherWalkError::UnsupportedFeature { .. }
        | CypherWalkError::InvalidParameter { .. } => "not_admitted",
    }
}

fn hash_snapshot_identity_once(
    runtime: &MmapWalkRuntime,
) -> Result<(u64, String), BoltCompatibilityStartupError> {
    let manifest_path = runtime.snapshot_dir().join("manifest.json");
    let manifest_bytes =
        fs::read(&manifest_path).map_err(|source| BoltCompatibilityStartupError::Io {
            path: manifest_path.clone(),
            source,
        })?;
    let manifest: SnapshotManifest = serde_json::from_slice(&manifest_bytes).map_err(|source| {
        BoltCompatibilityStartupError::Manifest {
            path: manifest_path,
            source,
        }
    })?;
    let snapshot_dir = runtime.snapshot_dir();
    let mut file_paths = fs::read_dir(snapshot_dir)
        .map_err(|source| BoltCompatibilityStartupError::Io {
            path: snapshot_dir.to_path_buf(),
            source,
        })?
        .map(|entry| {
            entry
                .map(|value| value.path())
                .map_err(|source| BoltCompatibilityStartupError::Io {
                    path: snapshot_dir.to_path_buf(),
                    source,
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    file_paths.retain(|path| path.is_file());
    file_paths.sort();

    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    for path in file_paths {
        let file_name = path
            .file_name()
            .ok_or_else(|| BoltCompatibilityStartupError::Io {
                path: path.clone(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "snapshot entry has no file name",
                ),
            })?;
        let file_name_bytes = file_name.as_encoded_bytes();
        hasher.update((file_name_bytes.len() as u64).to_le_bytes());
        hasher.update(file_name_bytes);
        let mut file = File::open(&path).map_err(|source| BoltCompatibilityStartupError::Io {
            path: path.clone(),
            source,
        })?;
        loop {
            let bytes_read =
                file.read(&mut buffer)
                    .map_err(|source| BoltCompatibilityStartupError::Io {
                        path: path.clone(),
                        source,
                    })?;
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }
    }
    Ok((
        manifest.snapshot_generation,
        format!("{:x}", hasher.finalize()),
    ))
}

fn validate_graph_profile_once(
    runtime: &MmapWalkRuntime,
) -> Result<(), BoltCompatibilityStartupError> {
    let profile_path = runtime.snapshot_dir().join(NEIGHBORHOOD_GRAPH_PROFILE_FILE);
    let profile_bytes =
        fs::read(&profile_path).map_err(|source| BoltCompatibilityStartupError::Io {
            path: profile_path.clone(),
            source,
        })?;
    let profile: NeighborhoodGraphProfileManifest = serde_json::from_slice(&profile_bytes)
        .map_err(
            |source| BoltCompatibilityStartupError::GraphProfileManifest {
                path: profile_path.clone(),
                source,
            },
        )?;
    let expected_values = [
        (profile.schema_version == 1, "schema_version must equal 1"),
        (
            profile.profile_version == "knight-bus-neighborhood-walk-v1",
            "profile_version must equal knight-bus-neighborhood-walk-v1",
        ),
        (
            profile.node_label == "Entity",
            "node_label must equal Entity",
        ),
        (
            profile.start_node_id_property == "node_id",
            "start_node_id_property must equal node_id",
        ),
        (
            profile.result_node_id_property == "node_id",
            "result_node_id_property must equal node_id",
        ),
        (
            profile.relationship_type == "DEPENDS_ON",
            "relationship_type must equal DEPENDS_ON",
        ),
        (profile.minimum_hops == 1, "minimum_hops must equal 1"),
        (profile.maximum_hops == 2, "maximum_hops must equal 2"),
        (
            profile.node_count == u64::from(runtime.node_count()),
            "node_count must match the opened snapshot",
        ),
        (
            profile.relationship_count == runtime.relationship_count(),
            "relationship_count must match the opened snapshot",
        ),
    ];
    if let Some((_, message)) = expected_values.iter().find(|(valid, _)| !valid) {
        return Err(BoltCompatibilityStartupError::InvalidGraphProfile {
            path: profile_path,
            message: (*message).to_owned(),
        });
    }
    Ok(())
}

fn hash_text_content_exact(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

fn hash_result_records_exact(records: &[crate::cypher::ProjectedBoltResultRecord]) -> String {
    let mut hasher = Sha256::new();
    for record in records {
        hasher.update((record.node_id.len() as u64).to_le_bytes());
        hasher.update(record.node_id.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn duration_micros_as_i64(started: Instant) -> i64 {
    i64::try_from(started.elapsed().as_micros()).map_or(i64::MAX, |duration| duration)
}
