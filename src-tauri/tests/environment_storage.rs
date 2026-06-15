use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use milena_lib::{
    contracts::{
        CommandResult, EnvironmentAuthMode, MilenaCommandError, MilenaCommandErrorCode,
        SaveEnvironmentRequest,
    },
    environments::{
        delete_environment, list_environments, load_environment, materialize_runtime_auth_config,
        materialize_temporary_runtime_auth_config, save_environment, EnvironmentSecretStore,
        InMemoryEnvironmentSecretStore,
    },
};

#[test]
fn plaintext_environment_can_be_saved_loaded_and_listed_without_secret_metadata() {
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
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: "# local notes\nclient.id = milena-dev".to_string(),
        },
        &secrets,
    )
    .expect("environment should save");

    assert_eq!(saved.schema_version, 1);
    assert_eq!(saved.name, "Local Dev");
    assert_eq!(saved.brokers, vec!["localhost:9092", "localhost:9093"]);
    assert_eq!(saved.auth_mode, EnvironmentAuthMode::Plaintext);
    assert_eq!(saved.username, None);
    assert_eq!(
        saved.advanced_properties,
        "# local notes\nclient.id = milena-dev"
    );

    let metadata_contents = persisted_metadata_contents(&config_dir);
    assert!(metadata_contents.contains("\"schemaVersion\": 1"));
    assert!(metadata_contents.contains("\"authMode\": \"plaintext\""));
    assert!(!metadata_contents.contains("macos-keychain://"));
    assert!(!metadata_contents.contains("\"password\""));
    assert!(!metadata_contents.contains("passwordSecretRef"));

    let loaded =
        load_environment(&config_dir, "Local Dev").expect("environment metadata should load");
    assert_eq!(loaded, saved);

    let listed = list_environments(&config_dir).expect("environment list should load");
    assert_eq!(listed.environments, vec![saved]);

    let runtime = materialize_runtime_auth_config(&config_dir, "Local Dev", &secrets)
        .expect("plaintext runtime auth should materialize");
    assert_eq!(runtime.environment, "Local Dev");
    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"PLAINTEXT".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"milena-dev".to_string())
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn scram_environment_materializes_generated_auth_properties_from_keychain_secret() {
    let config_dir = temp_config_dir("scram-materialize");
    let secrets = InMemoryEnvironmentSecretStore::default();

    let saved = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-a:9092".to_string(), "kafka-b:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some(" service-user ".to_string()),
            password: Some("service-pass".to_string()),
            advanced_properties: "client.id = milena-staging".to_string(),
        },
        &secrets,
    )
    .expect("environment should save");

    assert_eq!(saved.schema_version, 1);
    assert_eq!(saved.auth_mode, EnvironmentAuthMode::SaslSslScramSha512);
    assert_eq!(saved.username, Some("service-user".to_string()));
    assert_eq!(
        secrets
            .load_password(&keychain_account("staging"))
            .expect("secret load should succeed"),
        Some("service-pass".to_string())
    );

    let metadata_contents = persisted_metadata_contents(&config_dir);
    assert!(!metadata_contents.contains("service-pass"));
    assert!(!metadata_contents.contains("macos-keychain://"));
    assert!(!metadata_contents.contains("passwordSecretRef"));

    let runtime = materialize_runtime_auth_config(&config_dir, "staging", &secrets)
        .expect("runtime auth config should materialize");

    assert_eq!(runtime.environment, "staging");
    assert_eq!(runtime.brokers, vec!["kafka-a:9092", "kafka-b:9092"]);
    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"SASL_SSL".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.mechanism"),
        Some(&"SCRAM-SHA-512".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"service-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"service-pass".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"milena-staging".to_string())
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn scram_environment_edit_with_blank_password_reuses_existing_keychain_secret() {
    let config_dir = temp_config_dir("scram-edit-reuse-password");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-a:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("old-user".to_string()),
            password: Some("existing-pass".to_string()),
            advanced_properties: "client.id = old-client".to_string(),
        },
        &secrets,
    )
    .expect("initial SCRAM environment should save");

    let edited = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-b:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("new-user".to_string()),
            password: None,
            advanced_properties: "client.id = new-client".to_string(),
        },
        &secrets,
    )
    .expect("SCRAM edit with omitted password should reuse existing secret");

    assert_eq!(edited.username, Some("new-user".to_string()));
    assert_eq!(edited.brokers, vec!["kafka-b:9092"]);
    assert_eq!(
        secrets
            .load_password(&keychain_account("staging"))
            .expect("secret load should succeed"),
        Some("existing-pass".to_string())
    );

    let runtime = materialize_runtime_auth_config(&config_dir, "staging", &secrets)
        .expect("edited SCRAM runtime should materialize");
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"new-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"existing-pass".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"new-client".to_string())
    );

    let edited_again = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-c:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("final-user".to_string()),
            password: Some(" \n ".to_string()),
            advanced_properties: "client.id = final-client".to_string(),
        },
        &secrets,
    )
    .expect("SCRAM edit with blank password should reuse existing secret");

    assert_eq!(edited_again.username, Some("final-user".to_string()));
    assert_eq!(
        secrets
            .load_password(&keychain_account("staging"))
            .expect("secret load should succeed"),
        Some("existing-pass".to_string())
    );

    let runtime = materialize_runtime_auth_config(&config_dir, "staging", &secrets)
        .expect("blank-password SCRAM edit should still materialize");
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"final-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"existing-pass".to_string())
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn temporary_plaintext_runtime_auth_materializes_without_persisting_environment() {
    let config_dir = temp_config_dir("temporary-plaintext-materialize");
    let secrets = InMemoryEnvironmentSecretStore::default();

    let runtime = materialize_temporary_runtime_auth_config(
        &config_dir,
        SaveEnvironmentRequest {
            name: "unsaved-local".to_string(),
            brokers: vec!["localhost:19092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: "client.id = milena-test".to_string(),
        },
        &secrets,
    )
    .expect("temporary plaintext runtime auth should materialize");

    assert_eq!(runtime.environment, "unsaved-local");
    assert_eq!(runtime.brokers, vec!["localhost:19092"]);
    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"PLAINTEXT".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"milena-test".to_string())
    );
    assert!(list_environments(&config_dir)
        .expect("environment list should load")
        .environments
        .is_empty());

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn temporary_scram_runtime_auth_uses_request_password_without_saving_it() {
    let config_dir = temp_config_dir("temporary-scram-materialize");
    let secrets = InMemoryEnvironmentSecretStore::default();

    let runtime = materialize_temporary_runtime_auth_config(
        &config_dir,
        SaveEnvironmentRequest {
            name: "unsaved-secure".to_string(),
            brokers: vec!["kafka-a:9094".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("service-user".to_string()),
            password: Some("request-secret".to_string()),
            advanced_properties: "client.id = milena-secure".to_string(),
        },
        &secrets,
    )
    .expect("temporary SCRAM runtime auth should materialize");

    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"SASL_SSL".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.mechanism"),
        Some(&"SCRAM-SHA-512".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"service-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"request-secret".to_string())
    );
    assert_eq!(
        secrets
            .load_password(&keychain_account("unsaved-secure"))
            .expect("secret load should succeed"),
        None
    );
    assert!(list_environments(&config_dir)
        .expect("environment list should load")
        .environments
        .is_empty());

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn temporary_scram_edit_with_blank_password_uses_existing_keychain_secret() {
    let config_dir = temp_config_dir("temporary-scram-edit-reuse-password");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-a:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("old-user".to_string()),
            password: Some("existing-pass".to_string()),
            advanced_properties: "client.id = old-client".to_string(),
        },
        &secrets,
    )
    .expect("initial SCRAM environment should save");

    let runtime = materialize_temporary_runtime_auth_config(
        &config_dir,
        SaveEnvironmentRequest {
            name: "staging".to_string(),
            brokers: vec!["kafka-b:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("new-user".to_string()),
            password: None,
            advanced_properties: "client.id = new-client".to_string(),
        },
        &secrets,
    )
    .expect("temporary SCRAM edit should reuse existing secret");

    assert_eq!(runtime.brokers, vec!["kafka-b:9092"]);
    assert_eq!(
        runtime.properties.get("sasl.username"),
        Some(&"new-user".to_string())
    );
    assert_eq!(
        runtime.properties.get("sasl.password"),
        Some(&"existing-pass".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"new-client".to_string())
    );

    let loaded =
        load_environment(&config_dir, "staging").expect("saved metadata should remain unchanged");
    assert_eq!(loaded.brokers, vec!["kafka-a:9092"]);
    assert_eq!(loaded.username, Some("old-user".to_string()));
    assert_eq!(loaded.advanced_properties, "client.id = old-client");

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn advanced_properties_comments_blank_lines_and_generated_key_overrides_are_handled() {
    let config_dir = temp_config_dir("advanced-properties");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: [
                "# local client settings",
                "",
                "! preserved only as metadata",
                "client.id = milena-local",
                "request.timeout.ms: 15000",
            ]
            .join("\n"),
        },
        &secrets,
    )
    .expect("environment with allowed advanced properties should save");

    let runtime = materialize_runtime_auth_config(&config_dir, "local", &secrets)
        .expect("runtime auth config should materialize allowed advanced properties");

    assert_eq!(
        runtime.properties.get("security.protocol"),
        Some(&"PLAINTEXT".to_string())
    );
    assert_eq!(
        runtime.properties.get("client.id"),
        Some(&"milena-local".to_string())
    );
    assert_eq!(
        runtime.properties.get("request.timeout.ms"),
        Some(&"15000".to_string())
    );
    assert_eq!(runtime.properties.len(), 3);

    let error = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "bad-local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: "sasl.username=mallory".to_string(),
        },
        &secrets,
    )
    .expect_err("advanced properties must not override generated auth keys");

    assert_eq!(
        error.code,
        MilenaCommandErrorCode::EnvironmentAdvancedPropertiesInvalid
    );
    assert!(error.message.contains("cannot override"));
    assert!(error.message.contains("sasl.username"));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn missing_keychain_secret_fails_runtime_materialization() {
    let config_dir = temp_config_dir("missing-secret");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "qa".to_string(),
            brokers: vec!["qa-kafka:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("qa-user".to_string()),
            password: Some("qa-pass".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("environment should save");
    secrets
        .remove_password(&keychain_account("qa"))
        .expect("secret remove should succeed");

    let error = materialize_runtime_auth_config(&config_dir, "qa", &secrets)
        .expect_err("runtime config should require the saved password secret");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentSecretMissing);
    assert!(error.message.contains(&keychain_account("qa")));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn scram_environment_edit_with_blank_password_requires_existing_keychain_secret() {
    let config_dir = temp_config_dir("scram-edit-missing-secret");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "qa".to_string(),
            brokers: vec!["qa-kafka:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("qa-user".to_string()),
            password: Some("qa-pass".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("initial SCRAM environment should save");
    secrets
        .remove_password(&keychain_account("qa"))
        .expect("secret remove should succeed");

    let error = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "qa".to_string(),
            brokers: vec!["qa-kafka:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("new-qa-user".to_string()),
            password: None,
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect_err("SCRAM edit with blank password should require a reusable secret");

    assert_eq!(
        error.code,
        MilenaCommandErrorCode::EnvironmentPasswordRequired
    );
    let loaded = load_environment(&config_dir, "qa")
        .expect("failed SCRAM edit should leave existing metadata intact");
    assert_eq!(loaded.username, Some("qa-user".to_string()));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn environment_save_validates_schema_fields_and_advanced_properties() {
    let config_dir = temp_config_dir("validation");
    let secrets = InMemoryEnvironmentSecretStore::default();

    for (request, code, expected_message) in [
        (
            SaveEnvironmentRequest {
                name: " ".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                auth_mode: EnvironmentAuthMode::Plaintext,
                username: None,
                password: None,
                advanced_properties: String::new(),
            },
            MilenaCommandErrorCode::EnvironmentNameRequired,
            "environment name",
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec![" ".to_string(), "".to_string()],
                auth_mode: EnvironmentAuthMode::Plaintext,
                username: None,
                password: None,
                advanced_properties: String::new(),
            },
            MilenaCommandErrorCode::EnvironmentBrokersRequired,
            "broker",
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
                username: Some(" ".to_string()),
                password: Some("secret".to_string()),
                advanced_properties: String::new(),
            },
            MilenaCommandErrorCode::EnvironmentUsernameRequired,
            "username",
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
                username: Some("alice".to_string()),
                password: Some("".to_string()),
                advanced_properties: String::new(),
            },
            MilenaCommandErrorCode::EnvironmentPasswordRequired,
            "password",
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                auth_mode: EnvironmentAuthMode::Plaintext,
                username: None,
                password: None,
                advanced_properties: "client.id milena".to_string(),
            },
            MilenaCommandErrorCode::EnvironmentAdvancedPropertiesInvalid,
            "separator",
        ),
        (
            SaveEnvironmentRequest {
                name: "dev".to_string(),
                brokers: vec!["localhost:9092".to_string()],
                auth_mode: EnvironmentAuthMode::Plaintext,
                username: None,
                password: None,
                advanced_properties: "security.protocol=SASL_SSL".to_string(),
            },
            MilenaCommandErrorCode::EnvironmentAdvancedPropertiesInvalid,
            "cannot override",
        ),
    ] {
        let error = save_environment(&config_dir, request, &secrets)
            .expect_err("invalid environment should fail validation");
        assert_eq!(error.code, code);
        assert!(error.message.contains(expected_message));
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
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: Some(" ignored ".to_string()),
            password: Some("ignored".to_string()),
            advanced_properties: "client.id: milena".to_string(),
        },
        &secrets,
    )
    .expect("valid environment should save");
    assert_eq!(saved.name, "Trimmed Dev");
    assert_eq!(saved.brokers, vec!["kafka-a:9092", "kafka-b:9092"]);
    assert_eq!(saved.username, None);

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn switching_plaintext_to_scram_requires_a_new_password() {
    let config_dir = temp_config_dir("plaintext-to-scram-password-required");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("plaintext environment should save");

    let error = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("alice".to_string()),
            password: Some(" ".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect_err("switching plaintext to SCRAM should require a password");

    assert_eq!(
        error.code,
        MilenaCommandErrorCode::EnvironmentPasswordRequired
    );

    let loaded = load_environment(&config_dir, "local")
        .expect("failed SCRAM switch should leave plaintext metadata intact");
    assert_eq!(loaded.auth_mode, EnvironmentAuthMode::Plaintext);

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn switching_scram_to_plaintext_removes_existing_keychain_secret() {
    let config_dir = temp_config_dir("scram-to-plaintext-removes-secret");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("SCRAM environment should save");

    let saved = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "local".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("SCRAM to plaintext switch should save");

    assert_eq!(saved.auth_mode, EnvironmentAuthMode::Plaintext);
    assert_eq!(
        secrets
            .load_password(&keychain_account("local"))
            .expect("secret load should succeed"),
        None
    );

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn persisted_metadata_schema_is_validated_on_load_and_list() {
    let config_dir = temp_config_dir("schema-validation");
    let environments_dir = config_dir.join("environments");
    fs::create_dir_all(&environments_dir).expect("environments dir should be created");
    fs::write(
        environments_dir.join("invalid.json"),
        r#"{
  "schemaVersion": 2,
  "name": "future",
  "brokers": ["localhost:9092"],
  "authMode": "plaintext",
  "username": null,
  "advancedProperties": ""
}"#,
    )
    .expect("invalid metadata should be written");

    let load_error = load_environment(&config_dir, "future")
        .expect_err("unsupported schema version should fail load");
    assert_eq!(
        load_error.code,
        MilenaCommandErrorCode::EnvironmentStorageFailed
    );
    assert!(load_error.message.contains("schema version 2"));

    let list_error =
        list_environments(&config_dir).expect_err("unsupported schema version should fail list");
    assert_eq!(
        list_error.code,
        MilenaCommandErrorCode::EnvironmentStorageFailed
    );
    assert!(list_error.message.contains("schema version 2"));

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn case_insensitive_duplicate_environment_names_are_rejected() {
    let config_dir = temp_config_dir("duplicate-name");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "Dev Cluster".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("first environment should save");

    let error = save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "dev cluster".to_string(),
            brokers: vec!["localhost:9093".to_string()],
            auth_mode: EnvironmentAuthMode::Plaintext,
            username: None,
            password: None,
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect_err("case-insensitive duplicate should fail");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentDuplicateName);
    assert!(error.message.contains("dev cluster"));

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
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("alice".to_string()),
            password: Some("super-secret".to_string()),
            advanced_properties: String::new(),
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
fn delete_environment_removes_metadata_and_keychain_secret() {
    let config_dir = temp_config_dir("delete");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "delete-me".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("environment should save");

    let response =
        delete_environment(&config_dir, "delete-me", &secrets).expect("environment should delete");

    assert_eq!(response.name, "delete-me");
    assert_eq!(response.warning, None);
    assert_eq!(
        secrets
            .load_password(&keychain_account("delete-me"))
            .expect("secret load should succeed"),
        None
    );
    let error = load_environment(&config_dir, "delete-me")
        .expect_err("deleted environment metadata should be removed");
    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentNotFound);

    let _ = fs::remove_dir_all(config_dir);
}

#[test]
fn delete_environment_returns_warning_when_keychain_cleanup_fails() {
    let config_dir = temp_config_dir("delete-warning");
    let secrets = InMemoryEnvironmentSecretStore::default();

    save_environment(
        &config_dir,
        SaveEnvironmentRequest {
            name: "warning".to_string(),
            brokers: vec!["localhost:9092".to_string()],
            auth_mode: EnvironmentAuthMode::SaslSslScramSha512,
            username: Some("alice".to_string()),
            password: Some("secret".to_string()),
            advanced_properties: String::new(),
        },
        &secrets,
    )
    .expect("environment should save");

    let response = delete_environment(&config_dir, "warning", &FailingRemoveSecretStore)
        .expect("metadata delete should still succeed when secret cleanup fails");

    assert_eq!(response.name, "warning");
    assert!(response
        .warning
        .as_deref()
        .expect("warning should be present")
        .contains("password cleanup failed"));
    let error = load_environment(&config_dir, "warning")
        .expect_err("metadata should still be deleted when cleanup fails");
    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentNotFound);

    let _ = fs::remove_dir_all(config_dir);
}

struct FailingSaveSecretStore;

impl EnvironmentSecretStore for FailingSaveSecretStore {
    fn save_password(&self, _account: &str, _password: &str) -> CommandResult<()> {
        Err(MilenaCommandError::environment_secret_store_failed(
            "keychain unavailable",
        ))
    }

    fn load_password(&self, _account: &str) -> CommandResult<Option<String>> {
        Ok(None)
    }

    fn remove_password(&self, _account: &str) -> CommandResult<()> {
        Ok(())
    }
}

struct FailingRemoveSecretStore;

impl EnvironmentSecretStore for FailingRemoveSecretStore {
    fn save_password(&self, _account: &str, _password: &str) -> CommandResult<()> {
        Ok(())
    }

    fn load_password(&self, _account: &str) -> CommandResult<Option<String>> {
        Ok(Some("secret".to_string()))
    }

    fn remove_password(&self, _account: &str) -> CommandResult<()> {
        Err(MilenaCommandError::environment_secret_store_failed(
            "keychain delete failed",
        ))
    }
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

fn keychain_account(name: &str) -> String {
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
