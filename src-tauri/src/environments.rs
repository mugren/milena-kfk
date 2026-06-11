use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Mutex,
};

use serde::{Deserialize, Serialize};

use crate::contracts::{
    CommandResult, MilenaCommandError, RuntimeAuthConfig, SaveEnvironmentRequest, SavedEnvironment,
};

const KEYCHAIN_SERVICE: &str = "milena.kafka.environment";
const SECRET_REF_PREFIX: &str = "macos-keychain://milena.kafka.environment/";

pub trait EnvironmentSecretStore {
    fn save_password(&self, secret_ref: &str, password: &str) -> CommandResult<()>;
    fn load_password(&self, secret_ref: &str) -> CommandResult<Option<String>>;
}

#[derive(Default)]
pub struct InMemoryEnvironmentSecretStore {
    secrets: Mutex<HashMap<String, String>>,
}

impl InMemoryEnvironmentSecretStore {
    pub fn remove_password(&self, secret_ref: &str) {
        self.secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .remove(secret_ref);
    }
}

impl EnvironmentSecretStore for InMemoryEnvironmentSecretStore {
    fn save_password(&self, secret_ref: &str, password: &str) -> CommandResult<()> {
        self.secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .insert(secret_ref.to_string(), password.to_string());
        Ok(())
    }

    fn load_password(&self, secret_ref: &str) -> CommandResult<Option<String>> {
        Ok(self
            .secrets
            .lock()
            .expect("in-memory secret store lock should not be poisoned")
            .get(secret_ref)
            .cloned())
    }
}

pub struct MacosKeychainEnvironmentSecretStore;

impl EnvironmentSecretStore for MacosKeychainEnvironmentSecretStore {
    fn save_password(&self, secret_ref: &str, password: &str) -> CommandResult<()> {
        let keychain_ref = KeychainSecretReference::parse(secret_ref)?;
        let output = Command::new("security")
            .arg("add-generic-password")
            .arg("-a")
            .arg(&keychain_ref.account)
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

    fn load_password(&self, secret_ref: &str) -> CommandResult<Option<String>> {
        let keychain_ref = KeychainSecretReference::parse(secret_ref)?;
        let output = Command::new("security")
            .arg("find-generic-password")
            .arg("-a")
            .arg(&keychain_ref.account)
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
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
struct EnvironmentMetadata {
    name: String,
    brokers: Vec<String>,
    username: String,
    auth_properties_template: String,
    password_secret_ref: String,
}

impl From<EnvironmentMetadata> for SavedEnvironment {
    fn from(metadata: EnvironmentMetadata) -> Self {
        Self {
            name: metadata.name,
            brokers: metadata.brokers,
            username: metadata.username,
            auth_properties_template: metadata.auth_properties_template,
            password_secret_ref: metadata.password_secret_ref,
        }
    }
}

pub fn save_environment(
    config_dir: &Path,
    request: SaveEnvironmentRequest,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<SavedEnvironment> {
    let metadata = validate_environment(request)?;

    secrets.save_password(&metadata.password_secret_ref, &metadata.password)?;
    write_metadata(config_dir, &metadata.without_password())?;

    Ok(metadata.without_password().into())
}

pub fn load_environment(config_dir: &Path, name: &str) -> CommandResult<SavedEnvironment> {
    Ok(read_metadata(config_dir, name)?.into())
}

pub fn materialize_runtime_auth_config(
    config_dir: &Path,
    name: &str,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<RuntimeAuthConfig> {
    let metadata = read_metadata(config_dir, name)?;
    let password = secrets
        .load_password(&metadata.password_secret_ref)?
        .ok_or_else(|| {
            MilenaCommandError::environment_secret_missing(&metadata.password_secret_ref)
        })?;
    let substituted = metadata
        .auth_properties_template
        .replace("${KAFKA_USER}", &metadata.username)
        .replace("${KAFKA_PASS}", &password)
        .replace("$KAFKA_USER", &metadata.username)
        .replace("$KAFKA_PASS", &password);

    Ok(RuntimeAuthConfig {
        environment: metadata.name,
        brokers: metadata.brokers,
        properties: parse_auth_properties(&substituted)?,
    })
}

fn validate_environment(
    request: SaveEnvironmentRequest,
) -> CommandResult<EnvironmentMetadataDraft> {
    let name = request.name.trim();
    if name.is_empty() {
        return Err(MilenaCommandError::environment_name_required());
    }

    let brokers = request
        .brokers
        .into_iter()
        .map(|broker| broker.trim().to_string())
        .filter(|broker| !broker.is_empty())
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        return Err(MilenaCommandError::environment_brokers_required());
    }

    let username = request.username.trim();
    if username.is_empty() {
        return Err(MilenaCommandError::environment_username_required());
    }

    if request.password.is_empty() {
        return Err(MilenaCommandError::environment_password_required());
    }

    if request.auth_properties_template.trim().is_empty() {
        return Err(MilenaCommandError::environment_auth_template_required());
    }

    Ok(EnvironmentMetadataDraft {
        name: name.to_string(),
        brokers,
        username: username.to_string(),
        password: request.password,
        auth_properties_template: request.auth_properties_template,
        password_secret_ref: KeychainSecretReference::for_environment(name).display_ref,
    })
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

    let path = environment_file_path(config_dir, name);
    if !path.exists() {
        return Err(MilenaCommandError::environment_not_found(name));
    }

    let bytes =
        fs::read(path).map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))
}

fn parse_auth_properties(contents: &str) -> CommandResult<BTreeMap<String, String>> {
    let mut properties = BTreeMap::new();

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }

        let separator_index = line.find('=').or_else(|| line.find(':')).ok_or_else(|| {
            MilenaCommandError::environment_auth_template_invalid(format!(
                "auth.properties line {} is missing a key/value separator",
                line_index + 1
            ))
        })?;
        let key = line[..separator_index].trim();
        if key.is_empty() {
            return Err(MilenaCommandError::environment_auth_template_invalid(
                format!("auth.properties line {} has an empty key", line_index + 1),
            ));
        }

        properties.insert(
            key.to_string(),
            line[separator_index + 1..].trim().to_string(),
        );
    }

    Ok(properties)
}

fn environments_dir(config_dir: &Path) -> PathBuf {
    config_dir.join("environments")
}

fn environment_file_path(config_dir: &Path, name: &str) -> PathBuf {
    environments_dir(config_dir).join(format!("{}.json", hex_encode(name.trim().as_bytes())))
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>()
}

struct EnvironmentMetadataDraft {
    name: String,
    brokers: Vec<String>,
    username: String,
    password: String,
    auth_properties_template: String,
    password_secret_ref: String,
}

impl EnvironmentMetadataDraft {
    fn without_password(&self) -> EnvironmentMetadata {
        EnvironmentMetadata {
            name: self.name.clone(),
            brokers: self.brokers.clone(),
            username: self.username.clone(),
            auth_properties_template: self.auth_properties_template.clone(),
            password_secret_ref: self.password_secret_ref.clone(),
        }
    }
}

struct KeychainSecretReference {
    account: String,
    display_ref: String,
}

impl KeychainSecretReference {
    fn for_environment(name: &str) -> Self {
        let environment_id = hex_encode(name.trim().as_bytes());
        Self {
            account: format!("environment:{}:password", environment_id),
            display_ref: format!("{}{}/password", SECRET_REF_PREFIX, environment_id),
        }
    }

    fn parse(secret_ref: &str) -> CommandResult<Self> {
        let suffix = secret_ref.strip_prefix(SECRET_REF_PREFIX).ok_or_else(|| {
            MilenaCommandError::environment_secret_store_failed("invalid Keychain secret reference")
        })?;
        let environment_id = suffix.strip_suffix("/password").ok_or_else(|| {
            MilenaCommandError::environment_secret_store_failed("invalid Keychain secret reference")
        })?;

        Ok(Self {
            account: format!("environment:{}:password", environment_id),
            display_ref: secret_ref.to_string(),
        })
    }
}
