use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};

use serde::{Deserialize, Serialize};

use crate::contracts::{
    CommandResult, DeleteEnvironmentResponse, EnvironmentAuthMode, ListSavedEnvironmentsResponse,
    MilenaCommandError, RuntimeAuthConfig, SaveEnvironmentRequest, SavedEnvironment,
};

const ENVIRONMENT_SCHEMA_VERSION: u16 = 1;
const KEYCHAIN_SERVICE: &str = "milena.kafka.environment";

pub trait EnvironmentSecretStore {
    fn save_password(&self, account: &str, password: &str) -> CommandResult<()>;
    fn load_password(&self, account: &str) -> CommandResult<Option<String>>;
    fn remove_password(&self, account: &str) -> CommandResult<()>;
}

#[derive(Default)]
pub struct InMemoryEnvironmentSecretStore {
    secrets: Mutex<HashMap<String, String>>,
}

impl EnvironmentSecretStore for InMemoryEnvironmentSecretStore {
    fn save_password(&self, account: &str, password: &str) -> CommandResult<()> {
        self.secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .insert(account.to_string(), password.to_string());
        Ok(())
    }

    fn load_password(&self, account: &str) -> CommandResult<Option<String>> {
        Ok(self
            .secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .get(account)
            .cloned())
    }

    fn remove_password(&self, account: &str) -> CommandResult<()> {
        self.secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .remove(account);
        Ok(())
    }
}

pub struct MacosKeychainEnvironmentSecretStore;

impl EnvironmentSecretStore for MacosKeychainEnvironmentSecretStore {
    fn save_password(&self, account: &str, password: &str) -> CommandResult<()> {
        let output = Command::new("security")
            .arg("add-generic-password")
            .arg("-a")
            .arg(account)
            .arg("-s")
            .arg(KEYCHAIN_SERVICE)
            .arg("-w")
            .arg(password)
            .arg("-U")
            .output()
            .map_err(|error| MilenaCommandError::environment_secret_store_failed(error))?;

        if output.status.success() {
            Ok(())
        } else {
            Err(MilenaCommandError::environment_secret_store_failed(
                String::from_utf8_lossy(&output.stderr).trim(),
            ))
        }
    }

    fn load_password(&self, account: &str) -> CommandResult<Option<String>> {
        let output = Command::new("security")
            .arg("find-generic-password")
            .arg("-a")
            .arg(account)
            .arg("-s")
            .arg(KEYCHAIN_SERVICE)
            .arg("-w")
            .output()
            .map_err(|error| MilenaCommandError::environment_secret_store_failed(error))?;

        if output.status.success() {
            let password = String::from_utf8_lossy(&output.stdout)
                .trim_end_matches(['\r', '\n'])
                .to_string();
            Ok(Some(password))
        } else if output.status.code() == Some(44) {
            Ok(None)
        } else {
            Err(MilenaCommandError::environment_secret_store_failed(
                String::from_utf8_lossy(&output.stderr).trim(),
            ))
        }
    }

    fn remove_password(&self, account: &str) -> CommandResult<()> {
        let output = Command::new("security")
            .arg("delete-generic-password")
            .arg("-a")
            .arg(account)
            .arg("-s")
            .arg(KEYCHAIN_SERVICE)
            .output()
            .map_err(|error| MilenaCommandError::environment_secret_store_failed(error))?;

        if output.status.success() || output.status.code() == Some(44) {
            Ok(())
        } else {
            Err(MilenaCommandError::environment_secret_store_failed(
                String::from_utf8_lossy(&output.stderr).trim(),
            ))
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
struct EnvironmentMetadata {
    schema_version: u16,
    name: String,
    brokers: Vec<String>,
    auth_mode: EnvironmentAuthMode,
    username: Option<String>,
    advanced_properties: String,
}

impl From<EnvironmentMetadata> for SavedEnvironment {
    fn from(metadata: EnvironmentMetadata) -> Self {
        Self {
            schema_version: metadata.schema_version,
            name: metadata.name,
            brokers: metadata.brokers,
            auth_mode: metadata.auth_mode,
            username: metadata.username,
            advanced_properties: metadata.advanced_properties,
        }
    }
}

pub fn save_environment(
    config_dir: &Path,
    request: SaveEnvironmentRequest,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<SavedEnvironment> {
    let metadata = validate_environment(config_dir, request)?;

    match metadata.auth_mode {
        EnvironmentAuthMode::Plaintext => {
            let _ = secrets.remove_password(&keychain_account_for_environment(&metadata.name));
        }
        EnvironmentAuthMode::SaslSslScramSha512 => {
            let account = keychain_account_for_environment(&metadata.name);
            if let Some(password) = metadata.password.as_deref() {
                secrets.save_password(&account, password)?;
            } else if secrets.load_password(&account)?.is_none() {
                return Err(MilenaCommandError::environment_password_required());
            }
        }
    }

    write_metadata(config_dir, &metadata.without_password())?;

    Ok(metadata.without_password().into())
}

pub fn list_environments(config_dir: &Path) -> CommandResult<ListSavedEnvironmentsResponse> {
    let mut environments = Vec::new();
    let environments_dir = environments_dir(config_dir);
    if !environments_dir.exists() {
        return Ok(ListSavedEnvironmentsResponse { environments });
    }

    for entry in fs::read_dir(environments_dir)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?
    {
        let entry = entry.map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        environments.push(read_metadata_file(&entry.path())?.into());
    }

    environments
        .sort_by_key(|environment: &SavedEnvironment| environment.name.to_ascii_lowercase());
    Ok(ListSavedEnvironmentsResponse { environments })
}

pub fn load_environment(config_dir: &Path, name: &str) -> CommandResult<SavedEnvironment> {
    Ok(read_metadata(config_dir, name)?.into())
}

pub fn delete_environment(
    config_dir: &Path,
    name: &str,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<DeleteEnvironmentResponse> {
    let metadata = read_metadata(config_dir, name)?;
    let path = environment_file_path(config_dir, &metadata.name);
    fs::remove_file(path).map_err(|error| MilenaCommandError::environment_storage_failed(error))?;

    let warning = if metadata.auth_mode == EnvironmentAuthMode::SaslSslScramSha512 {
        secrets
            .remove_password(&keychain_account_for_environment(&metadata.name))
            .err()
            .map(|error| {
                format!(
                    "environment metadata was deleted, but password cleanup failed: {}",
                    error.message
                )
            })
    } else {
        None
    };

    Ok(DeleteEnvironmentResponse {
        name: metadata.name,
        warning,
    })
}

pub fn materialize_runtime_auth_config(
    config_dir: &Path,
    name: &str,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<RuntimeAuthConfig> {
    let metadata = read_metadata(config_dir, name)?;
    let mut properties = parse_advanced_properties(&metadata.advanced_properties)?;

    match metadata.auth_mode {
        EnvironmentAuthMode::Plaintext => {
            properties.insert("security.protocol".to_string(), "PLAINTEXT".to_string());
        }
        EnvironmentAuthMode::SaslSslScramSha512 => {
            let username = metadata
                .username
                .clone()
                .ok_or_else(|| MilenaCommandError::environment_username_required())?;
            let account = keychain_account_for_environment(&metadata.name);
            let password = secrets
                .load_password(&account)?
                .ok_or_else(|| MilenaCommandError::environment_secret_missing(&account))?;

            properties.insert("security.protocol".to_string(), "SASL_SSL".to_string());
            properties.insert("sasl.mechanism".to_string(), "SCRAM-SHA-512".to_string());
            properties.insert("sasl.username".to_string(), username);
            properties.insert("sasl.password".to_string(), password);
        }
    }

    Ok(RuntimeAuthConfig {
        environment: metadata.name,
        brokers: metadata.brokers,
        properties,
    })
}

pub fn materialize_temporary_runtime_auth_config(
    config_dir: &Path,
    request: SaveEnvironmentRequest,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<RuntimeAuthConfig> {
    let metadata = validate_environment(config_dir, request)?;
    let mut properties = parse_advanced_properties(&metadata.advanced_properties)?;

    match metadata.auth_mode {
        EnvironmentAuthMode::Plaintext => {
            properties.insert("security.protocol".to_string(), "PLAINTEXT".to_string());
        }
        EnvironmentAuthMode::SaslSslScramSha512 => {
            let username = metadata
                .username
                .clone()
                .ok_or_else(|| MilenaCommandError::environment_username_required())?;
            let password = match metadata.password {
                Some(password) => password,
                None => {
                    let account = keychain_account_for_environment(&metadata.name);
                    secrets
                        .load_password(&account)?
                        .ok_or_else(|| MilenaCommandError::environment_secret_missing(&account))?
                }
            };

            properties.insert("security.protocol".to_string(), "SASL_SSL".to_string());
            properties.insert("sasl.mechanism".to_string(), "SCRAM-SHA-512".to_string());
            properties.insert("sasl.username".to_string(), username);
            properties.insert("sasl.password".to_string(), password);
        }
    }

    Ok(RuntimeAuthConfig {
        environment: metadata.name,
        brokers: metadata.brokers,
        properties,
    })
}

fn validate_environment(
    config_dir: &Path,
    request: SaveEnvironmentRequest,
) -> CommandResult<EnvironmentMetadataDraft> {
    let name = request.name.trim();
    if name.is_empty() {
        return Err(MilenaCommandError::environment_name_required());
    }

    ensure_no_case_insensitive_duplicate(config_dir, name)?;

    let brokers = request
        .brokers
        .into_iter()
        .map(|broker| broker.trim().to_string())
        .filter(|broker| !broker.is_empty())
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        return Err(MilenaCommandError::environment_brokers_required());
    }

    validate_advanced_properties(&request.advanced_properties)?;

    let (username, password) = match request.auth_mode {
        EnvironmentAuthMode::Plaintext => (None, None),
        EnvironmentAuthMode::SaslSslScramSha512 => {
            let username = request
                .username
                .as_deref()
                .map(str::trim)
                .filter(|username| !username.is_empty())
                .ok_or_else(|| MilenaCommandError::environment_username_required())?;
            let password = request
                .password
                .as_deref()
                .filter(|password| !password.trim().is_empty());
            if password.is_none() && !existing_environment_uses_scram(config_dir, name)? {
                return Err(MilenaCommandError::environment_password_required());
            }
            (Some(username.to_string()), password.map(str::to_string))
        }
    };

    Ok(EnvironmentMetadataDraft {
        schema_version: ENVIRONMENT_SCHEMA_VERSION,
        name: name.to_string(),
        brokers,
        auth_mode: request.auth_mode,
        username,
        password,
        advanced_properties: request.advanced_properties,
    })
}

fn ensure_no_case_insensitive_duplicate(config_dir: &Path, name: &str) -> CommandResult<()> {
    let environments_dir = environments_dir(config_dir);
    if !environments_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(environments_dir)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?
    {
        let entry = entry.map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let metadata = read_metadata_file(&entry.path())?;
        if metadata.name.eq_ignore_ascii_case(name) && metadata.name != name {
            return Err(MilenaCommandError::environment_duplicate_name(name));
        }
    }

    Ok(())
}

fn existing_environment_uses_scram(config_dir: &Path, name: &str) -> CommandResult<bool> {
    let Some(path) = find_environment_file_path(config_dir, name)? else {
        return Ok(false);
    };
    let metadata = read_metadata_file(&path)?;
    Ok(metadata.name == name && metadata.auth_mode == EnvironmentAuthMode::SaslSslScramSha512)
}

fn write_metadata(config_dir: &Path, metadata: &EnvironmentMetadata) -> CommandResult<()> {
    let environments_dir = environments_dir(config_dir);
    fs::create_dir_all(&environments_dir)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    let bytes = serde_json::to_vec_pretty(metadata)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    fs::write(environment_file_path(config_dir, &metadata.name), bytes)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))
}

fn read_metadata(config_dir: &Path, name: &str) -> CommandResult<EnvironmentMetadata> {
    let name = name.trim();
    if name.is_empty() {
        return Err(MilenaCommandError::environment_name_required());
    }

    let path = find_environment_file_path(config_dir, name)?
        .ok_or_else(|| MilenaCommandError::environment_not_found(name))?;
    read_metadata_file(&path)
}

fn read_metadata_file(path: &Path) -> CommandResult<EnvironmentMetadata> {
    let bytes =
        fs::read(path).map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    let metadata = serde_json::from_slice::<EnvironmentMetadata>(&bytes)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    validate_persisted_metadata(metadata)
}

fn validate_persisted_metadata(
    metadata: EnvironmentMetadata,
) -> CommandResult<EnvironmentMetadata> {
    if metadata.schema_version != ENVIRONMENT_SCHEMA_VERSION {
        return Err(MilenaCommandError::environment_storage_failed(format!(
            "unsupported environment schema version {}; expected {}",
            metadata.schema_version, ENVIRONMENT_SCHEMA_VERSION
        )));
    }
    if metadata.name.trim().is_empty() {
        return Err(MilenaCommandError::environment_name_required());
    }
    if metadata
        .brokers
        .iter()
        .map(|broker| broker.trim())
        .filter(|broker| !broker.is_empty())
        .count()
        == 0
    {
        return Err(MilenaCommandError::environment_brokers_required());
    }
    if metadata.auth_mode == EnvironmentAuthMode::SaslSslScramSha512
        && metadata
            .username
            .as_deref()
            .map(str::trim)
            .filter(|username| !username.is_empty())
            .is_none()
    {
        return Err(MilenaCommandError::environment_username_required());
    }
    validate_advanced_properties(&metadata.advanced_properties)?;
    Ok(metadata)
}

fn validate_advanced_properties(contents: &str) -> CommandResult<()> {
    let properties = parse_advanced_properties(contents)?;
    for key in properties.keys() {
        if is_generated_auth_key(key) {
            return Err(MilenaCommandError::environment_advanced_properties_invalid(
                format!("advanced properties cannot override generated Kafka auth key '{key}'"),
            ));
        }
    }
    Ok(())
}

fn parse_advanced_properties(contents: &str) -> CommandResult<BTreeMap<String, String>> {
    let mut properties = BTreeMap::new();

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }

        let separator_index = line.find('=').or_else(|| line.find(':')).ok_or_else(|| {
            MilenaCommandError::environment_advanced_properties_invalid(format!(
                "advanced properties line {} is missing a key/value separator",
                line_index + 1
            ))
        })?;
        let key = line[..separator_index].trim();
        if key.is_empty() {
            return Err(MilenaCommandError::environment_advanced_properties_invalid(
                format!(
                    "advanced properties line {} has an empty key",
                    line_index + 1
                ),
            ));
        }

        properties.insert(
            key.to_string(),
            line[separator_index + 1..].trim().to_string(),
        );
    }

    Ok(properties)
}

fn is_generated_auth_key(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "security.protocol"
            | "sasl.mechanism"
            | "sasl.username"
            | "sasl.password"
            | "sasl.jaas.config"
    )
}

fn find_environment_file_path(config_dir: &Path, name: &str) -> CommandResult<Option<PathBuf>> {
    let exact_path = environment_file_path(config_dir, name);
    if exact_path.exists() {
        return Ok(Some(exact_path));
    }

    let environments_dir = environments_dir(config_dir);
    if !environments_dir.exists() {
        return Ok(None);
    }

    for entry in fs::read_dir(environments_dir)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?
    {
        let entry = entry.map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let metadata = read_metadata_file(&entry.path())?;
        if metadata.name == name {
            return Ok(Some(entry.path()));
        }
    }

    Ok(None)
}

fn environments_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("environments")
}

fn environment_file_path(config_dir: &Path, name: &str) -> PathBuf {
    environments_dir(config_dir).join(format!("{}.json", hex_encode(name.trim().as_bytes())))
}

fn keychain_account_for_environment(name: &str) -> String {
    format!(
        "environment:{}:password",
        hex_encode(name.trim().as_bytes())
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

struct EnvironmentMetadataDraft {
    schema_version: u16,
    name: String,
    brokers: Vec<String>,
    auth_mode: EnvironmentAuthMode,
    username: Option<String>,
    password: Option<String>,
    advanced_properties: String,
}

impl EnvironmentMetadataDraft {
    fn without_password(&self) -> EnvironmentMetadata {
        EnvironmentMetadata {
            schema_version: self.schema_version,
            name: self.name.clone(),
            brokers: self.brokers.clone(),
            auth_mode: self.auth_mode.clone(),
            username: self.username.clone(),
            advanced_properties: self.advanced_properties.clone(),
        }
    }
}
