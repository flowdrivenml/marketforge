use std::{env, ffi::OsString, io, path::Path};

use super::config::DATABASE_URL_ENV;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseEnvironment {
    Production,
    Testing,
}

impl DatabaseEnvironment {
    fn key(self) -> &'static str {
        match self {
            Self::Production => "PRODUCTION_DATABASE_KEY",
            Self::Testing => "TEST_DATABASE_KEY",
        }
    }
}

pub(super) fn resolve(
    root: &Path,
    database: DatabaseEnvironment,
    explicit: Option<&str>,
) -> Result<String, String> {
    resolve_with(root, database, explicit, |key| env::var_os(key))
}

fn resolve_with(
    root: &Path,
    database: DatabaseEnvironment,
    explicit: Option<&str>,
    environment: impl Fn(&str) -> Option<OsString>,
) -> Result<String, String> {
    if let Some(value) = explicit {
        return validate(value, "explicit PostgreSQL override", database);
    }
    // Live's legacy override is unscoped. Only use it for production.
    if database == DatabaseEnvironment::Production {
        if let Some(value) = environment(DATABASE_URL_ENV) {
            return validate(
                value.to_str().unwrap_or_default(),
                DATABASE_URL_ENV,
                database,
            );
        }
    }
    let key = database.key();
    if let Some(value) = environment(key) {
        let value = value
            .to_str()
            .ok_or_else(|| format!("invalid UTF-8 in {key}"))?;
        if !value.trim().is_empty() {
            return validate(value, key, database);
        }
    }
    let source = ".secrets/postgres.env";
    let missing =
        || format!("set {key} in the environment or in a private workspace {source} file");
    let text = crate::shared::secret_file::read(&root.join(source), 65536).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            missing()
        } else {
            format!("cannot read {source} as a private regular UTF-8 file owned by the running user ({})", error.kind())
        }
    })?;
    let mut value = None;
    let text = text.strip_prefix('\u{feff}').unwrap_or(&text);
    for (index, line) in text.lines().enumerate() {
        let line = line.trim_start();
        let line = line
            .strip_prefix("export")
            .filter(|tail| tail.starts_with(char::is_whitespace))
            .unwrap_or(line)
            .trim_start();
        let Some((name, raw)) = line.split_once('=') else {
            if line.trim() == key {
                return Err(format!(
                    "invalid {key} assignment in {source} at line {}",
                    index + 1
                ));
            }
            continue;
        };
        if name.trim_end() != key {
            continue;
        }
        if value.is_some() {
            return Err(format!(
                "duplicate {key} assignment in {source} at line {}",
                index + 1
            ));
        }
        value = Some(parse_value(raw).ok_or_else(|| {
            format!("invalid {key} assignment in {source} at line {}", index + 1)
        })?);
    }
    validate(
        &value.ok_or_else(missing)?,
        &format!("{key} in {source}"),
        database,
    )
}

fn parse_value(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if raw.starts_with('#') {
        return Some(String::new());
    }
    let mut chars = raw.chars();
    let Some(quote @ ('\'' | '"')) = chars.next() else {
        let comment = raw
            .char_indices()
            .zip(raw.chars().skip(1))
            .find(|((_, ch), next)| ch.is_whitespace() && *next == '#');
        return Some(
            comment
                .map_or(raw, |((index, _), _)| &raw[..index])
                .trim()
                .into(),
        );
    };
    let mut value = String::new();
    while let Some(ch) = chars.next() {
        if ch == quote {
            let tail = chars.as_str().trim();
            return (tail.is_empty() || tail.starts_with('#')).then_some(value);
        }
        if ch != '\\' {
            value.push(ch);
            continue;
        }
        match chars.next()? {
            escaped if escaped == quote || escaped == '\\' => value.push(escaped),
            'n' if quote == '"' => value.push('\n'),
            'r' if quote == '"' => value.push('\r'),
            't' if quote == '"' => value.push('\t'),
            escaped => {
                value.push('\\');
                value.push(escaped);
            }
        }
    }
    None
}

pub(super) fn connect_options(
    value: &str,
) -> Result<sqlx::postgres::PgConnectOptions, sqlx::Error> {
    // SQLx warns with raw values for unrecognized connection parameters.
    tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
        value.parse()
    })
    .map_err(|_: sqlx::Error| {
        sqlx::Error::Configuration("invalid PostgreSQL URL (value redacted)".into())
    })
}

fn validate(value: &str, source: &str, database: DatabaseEnvironment) -> Result<String, String> {
    let value = value.trim();
    let invalid = || format!("invalid PostgreSQL URL in {source} (value redacted)");
    if value.len() > 65536 || value.chars().any(char::is_control) {
        return Err(invalid());
    }
    let url = url::Url::parse(value).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        return Err(invalid());
    }
    if database == DatabaseEnvironment::Testing {
        let selected = url.query_pairs().filter(|(key, _)| key == "dbname").last();
        let name = selected
            .as_ref()
            .map_or(url.path().trim_start_matches('/'), |(_, name)| {
                name.as_ref()
            });
        if name.trim().is_empty() {
            return Err(format!(
                "{source} must explicitly name the testing database"
            ));
        }
    }
    connect_options(value).map_err(|_| invalid())?;
    Ok(value.into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{fs, os::unix::fs::PermissionsExt};

    const PROD: &str = "postgresql://synthetic:fixture-password@localhost/live";
    const TEST: &str = "postgresql://synthetic:fixture-password@localhost/testing";
    const OVERRIDE: &str = "postgres://synthetic@localhost/explicit";

    fn file(root: &Path, text: &str) -> std::path::PathBuf {
        let directory = root.join(".secrets");
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("postgres.env");
        fs::write(&path, text).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        path
    }

    #[test]
    fn credential_precedence_and_database_identity() {
        let root = tempfile::tempdir().unwrap();
        file(
            root.path(),
            &format!(
                "\u{feff}export PRODUCTION_DATABASE_KEY='{PROD}' # comment\r\nTEST_DATABASE_KEY=\"{TEST}\"\r\n"
            ),
        );
        let environment = |key: &str| match key {
            DATABASE_URL_ENV => Some(OsString::from(OVERRIDE)),
            "PRODUCTION_DATABASE_KEY" => Some(OsString::from(PROD)),
            "TEST_DATABASE_KEY" => Some(OsString::from("  ")),
            _ => None,
        };
        use DatabaseEnvironment::{Production, Testing};
        assert_eq!(
            resolve_with(root.path(), Production, Some(TEST), environment)
                .ok()
                .as_deref(),
            Some(TEST)
        );
        assert_eq!(
            resolve_with(root.path(), Production, None, environment)
                .ok()
                .as_deref(),
            Some(OVERRIDE)
        );
        assert_eq!(
            resolve_with(root.path(), Testing, None, environment)
                .ok()
                .as_deref(),
            Some(TEST)
        );
        assert_eq!(
            resolve_with(root.path(), Production, None, |_| None)
                .ok()
                .as_deref(),
            Some(PROD)
        );
        assert_eq!(
            resolve_with(root.path(), Production, None, |key| (key
                == "PRODUCTION_DATABASE_KEY")
                .then(|| OsString::from(OVERRIDE)))
            .ok()
            .as_deref(),
            Some(OVERRIDE)
        );

        let unnamed = "postgresql://synthetic@localhost";
        assert!(
            resolve_with(root.path(), Testing, Some(unnamed), environment)
                .unwrap_err()
                .contains("explicitly name the testing database")
        );
        file(root.path(), &format!("PRODUCTION_DATABASE_KEY={PROD}\n"));
        assert!(
            resolve_with(root.path(), Testing, None, environment)
                .unwrap_err()
                .contains("TEST_DATABASE_KEY")
        );
    }

    #[test]
    fn invalid_selected_sources_fail_without_disclosure_or_fallback() {
        use DatabaseEnvironment::Production;
        let root = tempfile::tempdir().unwrap();
        let resolve = || resolve_with(root.path(), Production, None, |_| None);
        assert!(resolve().unwrap_err().contains(".secrets/postgres.env"));
        let path = file(root.path(), &format!("PRODUCTION_DATABASE_KEY={PROD}\n"));
        let invalid = "not-a-postgres-url-fixture";
        let error = resolve_with(root.path(), Production, Some(invalid), |_| None).unwrap_err();
        assert!(error.contains("override") && !error.contains(invalid));
        let error =
            resolve_with(root.path(), Production, None, |_| Some(invalid.into())).unwrap_err();
        assert!(error.contains(DATABASE_URL_ENV) && !error.contains(invalid));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(resolve().unwrap_err().contains("private regular"));
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        for (content, expected) in [
            (
                format!("PRODUCTION_DATABASE_KEY={PROD}\nPRODUCTION_DATABASE_KEY={TEST}"),
                "duplicate",
            ),
            (format!("PRODUCTION_DATABASE_KEY=\"{PROD}"), "assignment"),
            (
                "PRODUCTION_DATABASE_KEY=# empty".into(),
                "invalid PostgreSQL URL",
            ),
            (
                format!("PRODUCTION_DATABASE_KEY={invalid}"),
                "invalid PostgreSQL URL",
            ),
        ] {
            fs::write(&path, content).unwrap();
            let error = resolve().unwrap_err();
            assert!(
                error.contains(expected)
                    && !error.contains(invalid)
                    && !error.contains("fixture-password")
            );
        }
    }
}
