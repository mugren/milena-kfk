use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use milena_lib::{
    contracts::{ImportKafkaShellEnvironmentRequest, MilenaCommandErrorCode},
    environment_import::import_kafka_shell_environment,
    environments::{
        load_environment, materialize_runtime_auth_config, InMemoryEnvironmentSecretStore,
    },
};

#[test]
fn kafka_shell_environment_import_saves_metadata_and_secret_reference() {
    let fixture = ImportFixture::new("successful");
    let secrets = InMemoryEnvironmentSecretStore::default();
    fixture.write_envs_ini(
        r#"
[dev]
brokers = localhost:9092, localhost:9093
username = alice
password = correct-horse-battery-staple
"#,
    );
    fixture.write_auth_properties(auth_template());

    let imported =
        import_kafka_shell_environment(&fixture.config_dir, fixture.request("dev"), &secrets)
            .expect("kafka-shell environment should import");

    assert_eq!(imported.environment.name, "dev");
    assert_eq!(
        imported.environment.brokers,
        vec!["localhost:9092", "localhost:9093"]
    );
    assert_eq!(imported.environment.username, "alice");
    assert!(imported
        .environment
        .password_secret_ref
        .starts_with("macos-keychain://milena.kafka.environment/"));

    let persisted = persisted_metadata_contents(&fixture.config_dir);
    assert!(persisted.contains(&imported.environment.password_secret_ref));
    assert!(!persisted.contains("correct-horse-battery-staple"));
    assert!(!persisted.contains("\"password\""));

    let runtime = materialize_runtime_auth_config(&fixture.config_dir, "dev", &secrets)
        .expect("imported secret should materialize through the secret store");
    assert_eq!(
        runtime.properties.get("sasl.jaas.config"),
        Some(
            &"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"correct-horse-battery-staple\";".to_string()
        )
    );
}

#[test]
fn kafka_shell_environment_import_preserves_auth_template_text() {
    let fixture = ImportFixture::new("auth-template");
    let secrets = InMemoryEnvironmentSecretStore::default();
    let template = [
        "# kafka-shell auth template",
        "security.protocol=SASL_SSL",
        "sasl.mechanism=PLAIN",
        "sasl.jaas.config=module required username=\"${KAFKA_USER}\" password=\"${KAFKA_PASS}\";",
        "",
    ]
    .join("\n");
    fixture.write_envs_ini(
        r#"
[qa]
brokers = qa-kafka:9092
username = qa-user
password = qa-pass
"#,
    );
    fixture.write_auth_properties(&template);

    let imported =
        import_kafka_shell_environment(&fixture.config_dir, fixture.request("qa"), &secrets)
            .expect("kafka-shell environment should import");
    let loaded =
        load_environment(&fixture.config_dir, "qa").expect("imported environment should load");

    assert_eq!(imported.environment.auth_properties_template, template);
    assert_eq!(loaded.auth_properties_template, template);

    let runtime = materialize_runtime_auth_config(&fixture.config_dir, "qa", &secrets)
        .expect("braced envsubst variables should materialize");
    assert_eq!(
        runtime.properties.get("sasl.jaas.config"),
        Some(&"module required username=\"qa-user\" password=\"qa-pass\";".to_string())
    );
}

#[test]
fn kafka_shell_environment_import_rejects_unsupported_fields() {
    let fixture = ImportFixture::new("unsupported");
    let secrets = InMemoryEnvironmentSecretStore::default();
    fixture.write_envs_ini(
        r#"
[dev]
brokers = localhost:9092
username = alice
password = secret
mechanism = SCRAM-SHA-512
"#,
    );
    fixture.write_auth_properties(auth_template());

    let error =
        import_kafka_shell_environment(&fixture.config_dir, fixture.request("dev"), &secrets)
            .expect_err("unsupported fields should fail import");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentImportInvalid);
    assert!(error.message.contains("unsupported field 'mechanism'"));
    assert!(error
        .message
        .contains("supported fields are brokers, username, and password"));
}

#[test]
fn kafka_shell_environment_import_rejects_ambiguous_brokers() {
    let fixture = ImportFixture::new("ambiguous-brokers");
    let secrets = InMemoryEnvironmentSecretStore::default();
    fixture.write_envs_ini(
        r#"
[dev]
brokers = kafka-a:9092 kafka-b:9092
username = alice
password = secret
"#,
    );
    fixture.write_auth_properties(auth_template());

    let error =
        import_kafka_shell_environment(&fixture.config_dir, fixture.request("dev"), &secrets)
            .expect_err("ambiguous broker lists should fail import");

    assert_eq!(error.code, MilenaCommandErrorCode::EnvironmentImportInvalid);
    assert!(error
        .message
        .contains("comma-separated Kafka bootstrap servers"));
}

fn auth_template() -> &'static str {
    "security.protocol=SASL_SSL\nsasl.mechanism=PLAIN\nsasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$KAFKA_USER\" password=\"$KAFKA_PASS\";"
}

fn persisted_metadata_contents(config_dir: &Path) -> String {
    let environments_dir = config_dir.join("environments");
    let metadata_path = fs::read_dir(environments_dir)
        .expect("environments directory should exist")
        .next()
        .expect("environment metadata file should exist")
        .expect("environment metadata entry should be readable")
        .path();

    fs::read_to_string(metadata_path).expect("environment metadata should be readable")
}

struct ImportFixture {
    root: PathBuf,
    config_dir: PathBuf,
    envs_ini_path: PathBuf,
    auth_properties_path: PathBuf,
}

impl ImportFixture {
    fn new(test_name: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "milena-import-{}-{}-{}",
            test_name,
            std::process::id(),
            timestamp
        ));
        fs::create_dir_all(&root).expect("fixture root should be created");
        let config_dir = root.join("config");
        fs::create_dir_all(&config_dir).expect("config dir should be created");

        Self {
            envs_ini_path: root.join("envs.ini"),
            auth_properties_path: root.join("auth.properties"),
            root,
            config_dir,
        }
    }

    fn request(&self, environment_name: &str) -> ImportKafkaShellEnvironmentRequest {
        ImportKafkaShellEnvironmentRequest {
            environment_name: environment_name.to_string(),
            environment_file_path: self.envs_ini_path.to_string_lossy().to_string(),
            auth_properties_path: self.auth_properties_path.to_string_lossy().to_string(),
        }
    }

    fn write_envs_ini(&self, contents: &str) {
        fs::write(&self.envs_ini_path, contents).expect("envs.ini should be written");
    }

    fn write_auth_properties(&self, contents: &str) {
        fs::write(&self.auth_properties_path, contents).expect("auth.properties should be written");
    }
}

impl Drop for ImportFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
