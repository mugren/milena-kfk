use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use milena_lib::{
    contracts::{
        CommandResult, MilenaCommandError, MilenaCommandErrorCode, SaveEnvironmentRequest,
    },
    environments::{
        load_environment, materialize_runtime_auth_config, save_environment,
        EnvironmentSecretStore, InMemoryEnvironmentSecretStore,
    },
};

#[test]
fn environment_can_be_saved_and_loaded_without_plaintext_password_metadata() {
    let config_dir = temp_config_dir("save-load");
    let secrets = InMemoryEnvironmentSecretStore::default();

    let saved = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "Local Dev".to_string(),
            brokers: vec![
                " localhost:9092 ".to_string(),
                "".to_string(),
                "localhost:9093".to_string(),
            ],
            username: " alice ".to_string(),
            password: "correct-horse-battery-staple".to_string(),
            auth_properties_template: auth_template(),
        },
        &secrets,
    )
    .expect("environment should save");

    assert_eq!(saved.name, "Local Dev");
    assert_eq!(saved.brokers, vec!["localhost:9092", "localhost:9093"]);
    assert_eq!(saved.username, "alice");
    assert_eq!(saved.auth_properties_template, auth_template());
    assert!(saved
        .password_secret_ref
        .starts_with("macos-keychain://milena.kafka.environment/"));

    let metadata_contents = persisted_metadata_contents(&config_dir);
    assert!(metadata_contents.contains(&saved.password_secret_ref));
    assert!(!metadata_contents.contains("correct-horse-battery-staple"));
    assert!(!metadata_contents.contains("\"password\""));

    let loaded =
        load_environment(&config_dir, "Local Dev").expect("environment metadata should load");
    assert_eq!(loaded, saved);

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn runtime_auth_config_substitutes_saved_username_and_password_into_properties() {
    let config_dir = temp_config_dir("materialize");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-a:9092".to_string(), "kafka-b:9092".to_string()],
            username: "service-user".to_string(),
            password: "service-pass".to_string(),
            auth_properties_template: auth_template(),
        },
        &secrets,
    )
    .expect("environment should save");

    let runtime = materialize_runtime_auth_config(&config_dir, "staging", &secrets)
        .expect("runtime auth config should materialize");

    assert_eq!(runtime.environment, "staging");
    assert_eq!(runtime.brokers, vec!["kafka-a:9092", "kafka-b:9092"]);
    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"SASL_SSL".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.jaas.config"),
        Some(
            &"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"service-user\" password=\"service-pass\";".to_string()
        )
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn missing_keychain_secret_fails_runtime_materialization() {
    let config_dir = temp_config_dir("missing-secret");
    let secrets = InMemoryEnvironmentSecretStore::default();

    let saved = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "qa".to_string(),
            brokers: vec!["qa-kafka:9092".to_string()],
            username: "qa-user".to_string(),
            password: "qa-pass".to_string(),
            auth_properties_template: auth_template(),
        },
        &secrets,
    )
    .expect("environment should save");
    secrets.remove_password(&saved.password_secret_ref);

    let error = materialize_runtime_auth_config(&config_dir, "qa", &secrets)
        .expect_err("runtime config should require the saved password secret");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentSecretMissing);
    assert!(error.message.contains(&saved.password_secret_ref));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn environment_save_validates_required_fields_and_drops_blank_brokers() {
    let config_dir = temp_config_dir("validation");
    let secrets = InMemoryEnvironmentSecretStore::default();

    for (request, code) in [
        (
            SaveEnvironmentRequest {
                name: " ".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                username: "alice".to_string(),
                password: "secret".to_string(),
                auth_properties_template: auth_template(),
            },
            MilenaCommandErrorCode::EnvironmentNameRequired,
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec![" ".to_string(), "".to_string()],
                username: "alice".to_string(),
                password: "secret".to_string(),
                auth_properties_template: auth_template(),
            },
            MilenaCommandErrorCode::EnvironmentBrokersRequired,
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                username: " ".to_string(),
                password: "secret".to_string(),
                auth_properties_template: auth_template(),
            },
            MilenaCommandErrorCode::EnvironmentUsernameRequired,
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                username: "alice".to_string(),
                password: "".to_string(),
                auth_properties_template: auth_template(),
            },
            MilenaCommandErrorCode::EnvironmentPasswordRequired,
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                username: "alice".to_string(),
                password: "secret".to_string(),
                auth_properties_template: " \n ".to_string(),
            },
            MilenaCommandErrorCode::EnvironmentAuthTemplateRequired,
        ),
    ] {
        let error = save_environment(&config_dir, request, &secrets)
            .expect_err("invalid environment should fail validation");
        assert_eq!(error.code, code);
    }

    let saved = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: " Trimmed Dev ".to_string(),
            brokers: vec![
                " kafka-a:9092 ".to_string(),
                " ".to_string(),
                "kafka-b:9092".to_string(),
            ],
            username: " alice ".to_string(),
            password: "secret".to_string(),
            auth_properties_template: auth_template(),
        },
        &secrets,
    )
    .expect("valid environment should save");
    assert_eq!(saved.name, "Trimmed Dev");
    assert_eq!(saved.brokers, vec!["kafka-a:9092", "kafka-b:9092"]);
    assert_eq!(saved.username, "alice");

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn missing_environment_lookup_returns_not_found() {
    let config_dir = temp_config_dir("missing-environment");

    let error = load_environment(&config_dir, "missing")
        .expect_err("missing environment should return a command error");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentNotFound);
    assert!(error.message.contains("missing"));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn secret_store_save_failure_does_not_write_metadata_or_leak_password() {
    let config_dir = temp_config_dir("secret-save-failure");

    let error = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "dev".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            username: "alice".to_string(),
            password: "super-secret".to_string(),
            auth_properties_template: auth_template(),
        },
        &FailingSaveSecretStore,
    )
    .expect_err("secret store failure should fail save");

    assert_eq!(
        error.code,
        MilenaCommandErrorCode::EnvironmentSecretStoreFailed
    );
    assert!(
        !config_dir.join("environments").exists(),
        "metadata should not be persisted when secret storage fails"
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn runtime_auth_parser_handles_comments_separators_and_duplicate_keys() {
    let config_dir = temp_config_dir("auth-parser");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "dev".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            username: "service-user".to_string(),
            password: "service-pass".to_string(),
            auth_properties_template: [
                "# comment",
                "! also a comment",
                "",
                "security.protocol = PLAINTEXT",
                "security.protocol: SASL_SSL",
                "sasl.username=$KAFKA_USER",
                "sasl.password:${KAFKA_PASS}",
            ]
            .join("\n"),
        },
        &secrets,
    )
    .expect("environment should save");

    let runtime = materialize_runtime_auth_config(&config_dir, "dev", &secrets)
        .expect("runtime auth config should parse");

    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"SASL_SSL".to_string()),
        "duplicate keys should use the last value, matching Java properties behavior"
    );
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"service-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"service-pass".to_string())
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn runtime_auth_parser_rejects_invalid_lines_and_empty_keys() {
    for (test_name, template, expected_message) in [
        (
            "invalid-line",
            "security.protocol SASL_SSL",
            "missing a key/value separator",
        ),
        ("empty-key", " = SASL_SSL", "empty key"),
    ] {
        let config_dir = temp_config_dir(test_name);
        let secrets = InMemoryEnvironmentSecretStore::default();

        save_environment(
            &config_dir,
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                username: "service-user".to_string(),
                password: "service-pass".to_string(),
                auth_properties_template: template.to_string(),
            },
            &secrets,
        )
        .expect("environment should save before runtime parsing");

        let error = materialize_runtime_auth_config(&config_dir, "dev", &secrets)
            .expect_err("invalid auth template should fail materialization");

        assert_eq!(
            error.code,
            MilenaCommandErrorCode::EnvironmentAuthTemplateInvalid
        );
        assert!(error.message.contains(expected_message));

        let _ = fs::remove_dir_all(config_dir);
    }
}

struct FailingSaveSecretStore;

impl EnvironmentSecretStore for FailingSaveSecretStore {
    fn save_password(&self, _secret_ref: &str, _password: &str) -> CommandResult<()> {
        Err(MilenaCommandError::environment_secret_store_failed(
            "keychain unavailable",
        ))
    }

    fn load_password(&self, _secret_ref: &str) -> CommandResult<Option<String>> {
        Ok(None)
    }
}

fn auth_template() -> String {
    [
        "security.protocol=SASL_SSL",
        "sasl.mechanism=PLAIN",
        "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_USER\" password=\"$KAFKA_PASS\";",
    ]
    .join("\n")
}

fn persisted_metadata_contents(config_dir: &PathBuf) -> String {
    let environments_dir = config_dir.join("environments");
    let metadata_path = fs::read_dir(environments_dir)
        .expect("environments directory should exist")
        .next()
        .expect("environment metadata file should exist")
        .expect("environment metadata entry should be readable")
        .path();

    fs::read_to_string(metadata_path).expect("environment metadata should be readable")
}

fn temp_config_dir(test_name: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "milena-{}-{}-{}",
        test_name,
        std::process::id(),
        timestamp
    ));
    fs::create_dir_all(&path).expect("temp config dir should be created");
    path
}
