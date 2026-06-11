use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use milena_lib::{
    contracts::{MilenaCommandErrorCode, SaveEnvironmentRequest},
    environments::{
        load_environment, materialize_runtime_auth_config, save_environment,
        InMemoryEnvironmentSecretStore,
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
