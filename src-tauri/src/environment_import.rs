use std::{fs, path::Path};

use crate::{
    contracts::{
        CommandResult, ImportKafkaShellEnvironmentRequest, ImportKafkaShellEnvironmentResponse,
        MilenaCommandError, SaveEnvironmentRequest,
    },
    environments::{save_environment, EnvironmentSecretStore},
};

#[derive(Default)]
struct KafkaShellEnvironmentDraft {
    brokers: Option<FieldValue>,
    username: Option<FieldValue>,
    password: Option<FieldValue>,
}

struct FieldValue {
    value: String,
    line_number: usize,
}

pub fn import_kafka_shell_environment(
    config_dir: &Path,
    request: ImportKafkaShellEnvironmentRequest,
    secrets: &impl EnvironmentSecretStore,
) -> CommandResult<ImportKafkaShellEnvironmentResponse> {
    let environment_name = request.environment_name.trim();
    if environment_name.is_empty() {
        return Err(MilenaCommandError::environment_name_required());
    }

    let environment_file_path = required_path(
        &request.environment_file_path,
        "kafka-shell environment file path is required",
    )?;
    let auth_properties_path = required_path(
        &request.auth_properties_path,
        "auth.properties template path is required",
    )?;

    let environment_contents = fs::read_to_string(environment_file_path)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?;
    let auth_properties_template = fs::read_to_string(auth_properties_path)
        .map_err(|error| MilenaCommandError::environment_storage_failed(error))?;

    let imported = parse_kafka_shell_environment(&environment_contents, environment_name)?;
    let environment = save_environment(
        config_dir,
        SaveEnvironmentRequest {
            name: environment_name.to_string(),
            brokers: parse_brokers(&imported.required("brokers")?)?,
            username: imported.required("username")?,
            password: imported.required("password")?,
            auth_properties_template,
        },
        secrets,
    )?;

    Ok(ImportKafkaShellEnvironmentResponse { environment })
}

fn required_path<'a>(path: &'a str, message: &str) -> CommandResult<&'a Path> {
    let path = path.trim();
    if path.is_empty() {
        return Err(MilenaCommandError::environment_import_invalid(message));
    }

    Ok(Path::new(path))
}

fn parse_kafka_shell_environment(
    contents: &str,
    environment_name: &str,
) -> CommandResult<KafkaShellEnvironmentDraft> {
    let mut active_section: Option<String> = None;
    let mut matched_section = false;
    let mut draft = KafkaShellEnvironmentDraft::default();

    for (line_index, raw_line) in contents.lines().enumerate() {
        let line_number = line_index + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
            continue;
        }

        if line.starts_with('[') {
            let section_name = parse_section_header(line, line_number)?;
            matched_section = matched_section || section_name == environment_name;
            active_section = Some(section_name);
            continue;
        }

        if active_section.as_deref() != Some(environment_name) {
            continue;
        }

        let (key, value) = parse_key_value(line, line_number)?;
        match key {
            "brokers" => assign_unique(&mut draft.brokers, value, line_number, key)?,
            "username" => assign_unique(&mut draft.username, value, line_number, key)?,
            "password" => assign_unique(&mut draft.password, value, line_number, key)?,
            unsupported => {
                return Err(MilenaCommandError::environment_import_invalid(format!(
                    "unsupported field '{}' in section [{}] on line {}; supported fields are brokers, username, and password",
                    unsupported, environment_name, line_number
                )));
            }
        }
    }

    if !matched_section {
        return Err(MilenaCommandError::environment_import_invalid(format!(
            "section [{}] was not found in the kafka-shell environment file",
            environment_name
        )));
    }

    Ok(draft)
}

fn parse_section_header(line: &str, line_number: usize) -> CommandResult<String> {
    if !line.ends_with(']') {
        return Err(MilenaCommandError::environment_import_invalid(format!(
            "invalid section header on line {}; expected [environment-name]",
            line_number
        )));
    }

    let section_name = line[1..line.len() - 1].trim();
    if section_name.is_empty() {
        return Err(MilenaCommandError::environment_import_invalid(format!(
            "empty section name on line {}",
            line_number
        )));
    }

    Ok(section_name.to_string())
}

fn parse_key_value(line: &str, line_number: usize) -> CommandResult<(&str, String)> {
    let separator_index = line.find('=').ok_or_else(|| {
        MilenaCommandError::environment_import_invalid(format!(
            "invalid kafka-shell environment line {}; expected key=value",
            line_number
        ))
    })?;
    let key = line[..separator_index].trim();
    let value = line[separator_index + 1..].trim();

    if key.is_empty() {
        return Err(MilenaCommandError::environment_import_invalid(format!(
            "empty field name on line {}",
            line_number
        )));
    }

    Ok((key, value.to_string()))
}

fn assign_unique(
    target: &mut Option<FieldValue>,
    value: String,
    line_number: usize,
    field_name: &str,
) -> CommandResult<()> {
    if let Some(existing) = target {
        return Err(MilenaCommandError::environment_import_invalid(format!(
            "duplicate '{}' field in kafka-shell environment section; first seen on line {}, duplicated on line {}",
            field_name, existing.line_number, line_number
        )));
    }

    *target = Some(FieldValue { value, line_number });
    Ok(())
}

fn parse_brokers(value: &str) -> CommandResult<Vec<String>> {
    if value.split_whitespace().count() > 1 && !value.contains(',') {
        return Err(MilenaCommandError::environment_import_invalid(
            "brokers field is ambiguous; use comma-separated Kafka bootstrap servers",
        ));
    }

    let brokers = value
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        return Err(MilenaCommandError::environment_brokers_required());
    }

    Ok(brokers)
}

impl KafkaShellEnvironmentDraft {
    fn required(&self, field_name: &str) -> CommandResult<String> {
        let value = match field_name {
            "brokers" => self.brokers.as_ref(),
            "username" => self.username.as_ref(),
            "password" => self.password.as_ref(),
            _ => None,
        }
        .ok_or_else(|| {
            MilenaCommandError::environment_import_invalid(format!(
                "missing '{}' field in kafka-shell environment section",
                field_name
            ))
        })?;

        if value.value.is_empty() {
            return Err(MilenaCommandError::environment_import_invalid(format!(
                "'{}' field in kafka-shell environment section is empty",
                field_name
            )));
        }

        Ok(value.value.clone())
    }
}
