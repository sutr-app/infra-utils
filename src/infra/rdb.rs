use anyhow::{Context, Result};
use debug_stub_derive::DebugStub;

pub mod error_utils;
pub mod escape;
use log::LevelFilter;
use serde::{Deserialize, de::DeserializeOwned};
use sqlx::ConnectOptions;
use std::time::Duration;

const DEFAULT_MAX_CONNECTIONS: u32 = 5;

fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

// cannot make default future off: https://stackoverflow.com/questions/63782238/remove-crate-feature
// cannot select feature mutually exclusive: https://github.com/rust-lang/cargo/issues/2980
// so use cfg instead of feature (use sqlite3 as default, use mysql if specified, use postgres if only postgres specified(exclusive))

#[cfg(feature = "mysql")]
pub type Rdb = sqlx::MySql;
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub type Rdb = sqlx::Postgres;
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub type Rdb = sqlx::Sqlite;

#[cfg(feature = "mysql")]
pub type RdbPool = sqlx::MySqlPool;
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub type RdbPool = sqlx::PgPool;
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub type RdbPool = sqlx::SqlitePool;

#[cfg(feature = "mysql")]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::MySql>;
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;

#[cfg(feature = "mysql")]
pub type RdbConnectOptions = sqlx::mysql::MySqlConnectOptions;
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub type RdbConnectOptions = sqlx::postgres::PgConnectOptions;
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub type RdbConnectOptions = sqlx::sqlite::SqliteConnectOptions;

#[cfg(feature = "mysql")]
pub type RdbArguments = sqlx::mysql::MySqlArguments;
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub type RdbArguments = sqlx::postgres::PgArguments;
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub type RdbArguments = sqlx::sqlite::SqliteArguments;

pub trait RdbConfigTrait: Clone {
    fn rdb_url(&self) -> String;

    fn max_connections(&self) -> u32;
}
impl RdbConfigImpl {
    pub fn new(host: String, port: String, user: String, password: String, dbname: String) -> Self {
        RdbConfigImpl {
            host,
            port,
            user,
            password,
            dbname,
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

#[derive(Deserialize, Clone, DebugStub)]
pub struct RdbConfigImpl {
    pub host: String,
    pub port: String,
    #[debug_stub = "[USER]"]
    pub user: String,
    #[debug_stub = "[PASSWORD]"]
    pub password: String,
    pub dbname: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl RdbConfigTrait for RdbConfigImpl {
    #[cfg(feature = "mysql")]
    fn rdb_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    fn rdb_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    fn rdb_url(&self) -> String {
        format!("sqlite://{}", self.dbname)
    }
    fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

impl Default for RdbConfigImpl {
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    fn default() -> Self {
        tracing::info!("Use default RdbConfig (sqlite3).");
        RdbConfigImpl {
            host: "".to_string(),
            port: "".to_string(),
            user: "".to_string(),
            password: "".to_string(),
            dbname: "default.sqlite3".to_string(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
    #[cfg(feature = "mysql")]
    fn default() -> Self {
        tracing::info!("Use default RdbConfig (mysql).");
        RdbConfigImpl {
            host: "127.0.0.1".to_string(),
            port: "3306".to_string(),
            user: "mysql".to_string(),
            password: "mysql".to_string(),
            dbname: "default".to_string(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    fn default() -> Self {
        tracing::info!("Use default RdbConfig (postgres).");
        RdbConfigImpl {
            host: "127.0.0.1".to_string(),
            port: "5432".to_string(),
            user: "mysql".to_string(),
            password: "mysql".to_string(),
            dbname: "default".to_string(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

#[derive(Deserialize, Clone, DebugStub)]
pub struct RdbUrlConfigImpl {
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl RdbConfigTrait for RdbUrlConfigImpl {
    #[cfg(feature = "mysql")]
    fn rdb_url(&self) -> String {
        if self.url.starts_with("mysql") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    fn rdb_url(&self) -> String {
        if self.url.starts_with("postgres") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    fn rdb_url(&self) -> String {
        if self.url.starts_with("sqlite") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

impl Default for RdbUrlConfigImpl {
    fn default() -> Self {
        tracing::info!("Use default RDBConfig (sqlite3).");
        RdbUrlConfigImpl {
            url: "sqlite://default.sqlite3".to_string(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
        }
    }
}

#[cfg(test)]
mod config_tests {
    use super::{
        DEFAULT_MAX_CONNECTIONS, RdbConfig, RdbConfigImpl, RdbConfigTrait, RdbUrlConfigImpl,
    };

    #[cfg(feature = "mysql")]
    const CURRENT_DB_URL_ENV: (&str, &str) = ("MYSQL_", "mysql://user:password@db:3306/app");
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    const CURRENT_DB_URL_ENV: (&str, &str) = ("POSTGRES_", "postgres://user:password@db:5432/app");
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    const CURRENT_DB_URL_ENV: (&str, &str) = ("SQLITE_", "sqlite://configured.sqlite3");

    #[cfg(feature = "mysql")]
    const UNSUPPORTED_DB_ENV_PREFIX: &str = "POSTGRES_";
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    const UNSUPPORTED_DB_ENV_PREFIX: &str = "MYSQL_";
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    const UNSUPPORTED_DB_ENV_PREFIX: &str = "POSTGRES_";

    #[test]
    fn url_config_uses_default_max_connections_when_omitted() {
        let (db_prefix, url) = CURRENT_DB_URL_ENV;
        let config = RdbConfig::load_db_url_config_from_slice(
            "APP_",
            &[(format!("APP_{db_prefix}URL"), url.to_string())],
        )
        .expect("URL-only configuration should load");

        let RdbConfig::Url(config) = config else {
            panic!("expected URL configuration");
        };
        assert_eq!(config.url, url);
        assert_eq!(config.max_connections(), DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn url_config_ignores_unsupported_database_environment_variables() {
        let config = RdbConfig::load_db_url_config_from_slice(
            "APP_",
            &[(
                format!("APP_{UNSUPPORTED_DB_ENV_PREFIX}URL"),
                "unsupported://database".to_string(),
            )],
        );

        assert!(config.is_none());
    }

    #[test]
    fn separated_config_uses_default_max_connections_when_omitted() {
        let config = envy::from_iter::<_, RdbConfigImpl>(vec![
            ("HOST".to_string(), "db".to_string()),
            ("PORT".to_string(), "3306".to_string()),
            ("USER".to_string(), "user".to_string()),
            ("PASSWORD".to_string(), "password".to_string()),
            ("DBNAME".to_string(), "app".to_string()),
        ])
        .expect("separated configuration should deserialize");

        assert_eq!(config.max_connections(), DEFAULT_MAX_CONNECTIONS);
    }

    #[test]
    fn url_config_deserializes_explicit_max_connections() {
        let config = envy::from_iter::<_, RdbUrlConfigImpl>(vec![
            ("URL".to_string(), "sqlite://configured.sqlite3".to_string()),
            ("MAX_CONNECTIONS".to_string(), "42".to_string()),
        ])
        .expect("URL configuration should deserialize");

        assert_eq!(config.max_connections(), 42);
    }

    #[test]
    fn url_test() {
        let conf = RdbConfigImpl {
            host: "127.0.0.1".to_string(),
            port: "1111".to_string(),
            user: "hoge_user".to_string(),
            password: "pass".to_string(),
            dbname: "db".to_string(),
            max_connections: super::DEFAULT_MAX_CONNECTIONS,
        };
        #[cfg(feature = "mysql")]
        assert_eq!(conf.rdb_url(), "mysql://hoge_user:pass@127.0.0.1:1111/db");
        #[cfg(all(feature = "postgres", not(feature = "mysql")))]
        assert_eq!(
            conf.rdb_url(),
            "postgres://hoge_user:pass@127.0.0.1:1111/db"
        );
        #[cfg(not(any(feature = "mysql", feature = "postgres")))]
        assert_eq!(conf.rdb_url(), "sqlite://db");
    }
}

#[derive(Debug, Clone, Deserialize)]
pub enum RdbConfig {
    Separate(RdbConfigImpl),
    Url(RdbUrlConfigImpl),
}

impl RdbConfig {
    pub fn new_by_url(url: &str, max_connections: u32) -> Self {
        RdbConfig::Url(RdbUrlConfigImpl {
            url: url.to_string(),
            max_connections,
        })
    }
    pub fn new(
        host: String,
        port: String,
        user: String,
        password: String,
        dbname: String,
        max_connections: u32,
    ) -> Self {
        RdbConfig::Separate(RdbConfigImpl {
            host,
            port,
            user,
            password,
            dbname,
            max_connections,
        })
    }
    pub fn load_db_config_from_env(prefix: String) -> Option<RdbConfig> {
        let environment = std::env::vars().collect::<Vec<_>>();
        Self::load_separated_db_config_from_slice(&prefix, &environment)
            .or_else(|| Self::load_db_url_config_from_slice(&prefix, &environment))
    }
    fn load_separated_db_config_from_slice(
        prefix: &str,
        environment: &[(String, String)],
    ) -> Option<RdbConfig> {
        try_db_prefixes::<RdbConfigImpl>(prefix, environment).map(RdbConfig::Separate)
    }
    fn load_db_url_config_from_slice(
        prefix: &str,
        environment: &[(String, String)],
    ) -> Option<RdbConfig> {
        try_db_prefixes::<RdbUrlConfigImpl>(prefix, environment).map(RdbConfig::Url)
    }
}

#[cfg(feature = "mysql")]
const DB_CONFIG_ENV_PREFIXES: [&str; 1] = ["MYSQL_"];
#[cfg(all(feature = "postgres", not(feature = "mysql")))]
const DB_CONFIG_ENV_PREFIXES: [&str; 1] = ["POSTGRES_"];
#[cfg(not(any(feature = "mysql", feature = "postgres")))]
const DB_CONFIG_ENV_PREFIXES: [&str; 1] = ["SQLITE_"];

/// Deserializes `T` from environment variables for the database selected at build time.
fn try_db_prefixes<T: DeserializeOwned>(
    prefix: &str,
    environment: &[(String, String)],
) -> Option<T> {
    DB_CONFIG_ENV_PREFIXES.into_iter().find_map(|db_prefix| {
        envy::prefixed(format!("{prefix}{db_prefix}"))
            .from_iter(environment.iter().cloned())
            .ok()
    })
}

impl RdbConfigTrait for RdbConfig {
    fn rdb_url(&self) -> String {
        match self {
            RdbConfig::Separate(config) => config.rdb_url(),
            RdbConfig::Url(config) => config.rdb_url(),
        }
    }
    fn max_connections(&self) -> u32 {
        match self {
            RdbConfig::Separate(config) => config.max_connections,
            RdbConfig::Url(config) => config.max_connections,
        }
    }
}

#[cfg(not(any(feature = "mysql", feature = "postgres")))]
pub async fn new_rdb_pool(config: &RdbConfig, init_schema: Option<&String>) -> Result<RdbPool> {
    use anyhow::anyhow;
    use sqlx::{
        Sqlite,
        migrate::MigrateDatabase,
        sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    };
    tracing::debug!("new sqlite pool: {}", config.rdb_url());
    // create db file if not exists
    if !Sqlite::database_exists(&config.rdb_url()).await? {
        Sqlite::create_database(&config.rdb_url()).await?;
    }
    let options = SqliteConnectOptions::from_url(&url::Url::parse(&config.rdb_url())?)?
        .log_statements(LevelFilter::Trace)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1)); // TODO set from config

    let pr = SqlitePoolOptions::new()
        .max_connections(config.max_connections())
        //.min_connections(3)
        .connect_with(options)
        .await
        .context(format!(
            "cannot initialize sql connection. url:{:?}",
            config.rdb_url()
        ));
    match pr {
        Ok(p) => setup_sqlite(&p, init_schema).await.map(|_| p),
        Err(e) => Err(anyhow!("setup error: {:?}", e)),
    }
}

#[cfg(not(any(feature = "mysql", feature = "postgres")))]
async fn setup_sqlite(p: &RdbPool, init_schema: Option<&String>) -> Result<()> {
    // Use DELETE journal mode for tests to ensure cross-process visibility
    // WAL mode can cause issues when test process writes data that worker process needs to read
    let use_wal = std::env::var("SQLITE_DISABLE_WAL")
        .map(|v| v != "1" && v.to_lowercase() != "true")
        .unwrap_or(true);

    if use_wal {
        sqlx::query::<Rdb>("PRAGMA journal_mode = WAL;")
            .execute(p)
            .await?;
        sqlx::query::<Rdb>("PRAGMA synchronous = NORMAL;")
            .execute(p)
            .await?;
    } else {
        tracing::info!("SQLite WAL disabled (SQLITE_DISABLE_WAL=1)");
        sqlx::query::<Rdb>("PRAGMA journal_mode = DELETE;")
            .execute(p)
            .await?;
        sqlx::query::<Rdb>("PRAGMA synchronous = FULL;")
            .execute(p)
            .await?;
    }
    sqlx::query::<Rdb>("PRAGMA auto_vacuum = incremental")
        .execute(p)
        .await?;
    if let Some(init_schema) = init_schema {
        // AssertSqlSafe: schema text is statically bundled and contains no user input.
        // raw_sql needs a `'static` owner, hence the owned copy.
        sqlx::raw_sql(sqlx::AssertSqlSafe(init_schema.to_string()))
            .execute(p)
            .await?;
    }
    Ok(())
}

#[cfg(feature = "mysql")]
pub async fn new_rdb_pool(config: &RdbConfig, _sqlite_schema: Option<&String>) -> Result<RdbPool> {
    // let port = config.port.parse::<u16>()?;
    // from sqlx 0.7, mysql connection options not used (statement_cache_capacity)
    // ref. https://github.com/launchbadge/sqlx/issues/2773

    use sqlx::mysql::MySqlPoolOptions;
    tracing::debug!("new mysql pool: {}", config.rdb_url());
    let options: sqlx::mysql::MySqlConnectOptions = config
        .rdb_url()
        .parse()
        .context(format!("cannot parse url: {:?}", config.rdb_url()))?;

    let options = options
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1)); // TODO set from config

    // TODO set from config
    MySqlPoolOptions::new()
        .idle_timeout(Some(Duration::from_secs(10 * 60)))
        .max_lifetime(Some(Duration::from_secs(10 * 60))) // same as mysql server wait_timeout
        .acquire_timeout(Duration::from_secs(5))
        // .test_before_acquire(false)
        .max_connections(config.max_connections())
        // .min_connections(config.max_connections() / 5 + 1)
        .connect_with(options)
        .await
        .context(format!(
            "cannot initialize mysql connection:{:?}",
            config.rdb_url()
        ))
}

#[cfg(all(feature = "postgres", not(feature = "mysql")))]
pub async fn new_rdb_pool(config: &RdbConfig, _sqlite_schema: Option<&String>) -> Result<RdbPool> {
    // let port = config.port.parse::<u16>()?;
    // from sqlx 0.7, pgsql connection options not used (statement_cache_capacity)
    // ref. https://github.com/launchbadge/sqlx/issues/2773

    use sqlx::postgres::PgPoolOptions;
    tracing::debug!("new postgres pool: {}", config.rdb_url());
    let options: sqlx::postgres::PgConnectOptions = config
        .rdb_url()
        .parse()
        .context(format!("cannot parse url: {:?}", config.rdb_url()))?;

    let options = options
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(20)); // TODO set from config

    // TODO set from config
    PgPoolOptions::new()
        .idle_timeout(Some(Duration::from_secs(10 * 60))) // TODO set from config
        .max_lifetime(Some(Duration::from_secs(10 * 60))) // TODO set from config
        .acquire_timeout(Duration::from_secs(10))
        // .test_before_acquire(false)
        .max_connections(config.max_connections())
        // .min_connections(config.max_connections() / 5 + 1)
        .connect_with(options)
        .await
        .context(format!(
            "cannot initialize postgres connection:{:?}",
            config.rdb_url()
        ))
}

pub mod query_result {
    #[cfg(feature = "mysql")]
    use sqlx::mysql::MySqlQueryResult;
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    use sqlx::postgres::PgQueryResult;
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    use sqlx::sqlite::SqliteQueryResult;

    #[cfg(feature = "mysql")]
    pub fn last_insert_id(res: MySqlQueryResult) -> i64 {
        res.last_insert_id() as i64
    }
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    pub fn last_insert_id(_res: PgQueryResult) -> i64 {
        // TODO not available in postgres
        0
    }
    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    pub fn last_insert_id(res: SqliteQueryResult) -> i64 {
        res.last_insert_rowid()
    }
}

pub trait UseRdbPool {
    fn db_pool(&self) -> &RdbPool;
}

pub trait UseRdbOption {
    fn db_pool(&self) -> Option<&RdbPool>;
}

pub mod test {
    // use std::time::Duration;

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    #[sqlx::test]
    pub async fn test_sqlite() {
        use crate::infra::rdb::Rdb;

        use super::RdbConfig;
        use anyhow::anyhow;

        sqlx::any::install_default_drivers();
        // connection test for localhost
        let pool = crate::infra::rdb::new_rdb_pool(
            &RdbConfig::new(
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "".to_string(),
                "test.sqlite3".to_string(),
                super::DEFAULT_MAX_CONNECTIONS,
            ),
            None,
        )
        .await
        .unwrap();
        let rows = sqlx::query::<Rdb>("SELECT 1 as one")
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow!("db error: {:?}", e));
        assert!(rows.is_ok());
    }
    #[cfg(feature = "mysql")]
    #[sqlx::test]
    pub async fn test_mysql() {
        use crate::infra::rdb::Rdb;
        use anyhow::anyhow;

        sqlx::any::install_default_drivers();
        // connection test for localhost
        let pool = crate::infra::rdb::new_rdb_pool(&crate::infra::test::MYSQL_CONFIG, None)
            .await
            .unwrap();
        let rows = sqlx::query::<Rdb>("SELECT 1 as one")
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow!("db error: {:?}", e));
        assert!(rows.is_ok());
    }
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    #[sqlx::test]
    pub async fn test_postgres() {
        use crate::infra::rdb::Rdb;
        use anyhow::anyhow;

        sqlx::any::install_default_drivers();
        // connection test for localhost
        let pool = crate::infra::rdb::new_rdb_pool(&crate::infra::test::POSTGRES_CONFIG, None)
            .await
            .unwrap();
        let rows = sqlx::query::<Rdb>("SELECT 1 as one")
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow!("db error: {:?}", e));
        assert!(rows.is_ok());
    }
}
