use anyhow::{Context, Result};
use command_utils::util::result::ToOption;
use debug_stub_derive::DebugStub;
use log::LevelFilter;
use serde::Deserialize;
use sqlx::ConnectOptions;
use std::time::Duration;

#[cfg(feature = "mysql")]
pub type Rdb = sqlx::MySql;
#[cfg(feature = "sqlite")]
pub type Rdb = sqlx::Sqlite;
#[cfg(feature = "postgres")]
pub type Rdb = sqlx::Postgres;

#[cfg(feature = "mysql")]
pub type RdbPool = sqlx::MySqlPool;
#[cfg(feature = "sqlite")]
pub type RdbPool = sqlx::SqlitePool;
#[cfg(feature = "postgres")]
pub type RdbPool = sqlx::PgPool;

#[cfg(feature = "mysql")]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::MySql>;
#[cfg(feature = "sqlite")]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::Sqlite>;
#[cfg(feature = "postgres")]
pub type RdbTransaction<'a> = sqlx::Transaction<'a, sqlx::Postgres>;

#[cfg(feature = "mysql")]
pub type RdbConnectOptions = sqlx::mysql::MySqlConnectOptions;
#[cfg(feature = "sqlite")]
pub type RdbConnectOptions = sqlx::sqlite::SqliteConnectOptions;
#[cfg(feature = "postgres")]
pub type RdbConnectOptions = sqlx::postgres::PgConnectOptions;

#[cfg(feature = "mysql")]
pub type RdbArguments = sqlx::mysql::MySqlArguments;
#[cfg(feature = "sqlite")]
pub type RdbArguments<'a> = sqlx::sqlite::SqliteArguments<'a>;
#[cfg(feature = "postgres")]
pub type RdbArguments = sqlx::postgres::PgArguments;

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
            max_connections: 20,
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
    #[cfg(feature = "postgres")]
    fn rdb_url(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
    #[cfg(feature = "sqlite")]
    fn rdb_url(&self) -> String {
        format!("sqlite://{}", self.dbname)
    }
    fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

impl Default for RdbConfigImpl {
    #[cfg(feature = "sqlite")]
    fn default() -> Self {
        tracing::info!("Use default RdbConfig (sqlite3).");
        RdbConfigImpl {
            host: "".to_string(),
            port: "".to_string(),
            user: "".to_string(),
            password: "".to_string(),
            dbname: "jobworkerp.sqlite3".to_string(),
            max_connections: 20,
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
            dbname: "jobworker".to_string(),
            max_connections: 20,
        }
    }
    #[cfg(feature = "postgres")]
    fn default() -> Self {
        tracing::info!("Use default RdbConfig (postgres).");
        RdbConfigImpl {
            host: "127.0.0.1".to_string(),
            port: "5432".to_string(),
            user: "mysql".to_string(),
            password: "mysql".to_string(),
            dbname: "jobworker".to_string(),
            max_connections: 20,
        }
    }
}

#[derive(Deserialize, Clone, DebugStub)]
pub struct RdbUrlConfigImpl {
    pub url: String,
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
    #[cfg(feature = "postgres")]
    fn rdb_url(&self) -> String {
        if self.url.starts_with("postgres") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    #[cfg(feature = "sqlite")]
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
            url: "sqlite://jobworkerp.sqlite3".to_string(),
            max_connections: 20,
        }
    }
}

#[test]
fn url_test() {
    let conf = RdbConfigImpl {
        host: "127.0.0.1".to_string(),
        port: "1111".to_string(),
        user: "hoge_user".to_string(),
        password: "pass".to_string(),
        dbname: "db".to_string(),
        max_connections: 20,
    };
    #[cfg(feature = "mysql")]
    assert_eq!(conf.rdb_url(), "mysql://hoge_user:pass@127.0.0.1:1111/db");
    #[cfg(feature = "postgres")]
    assert_eq!(
        conf.rdb_url(),
        "postgres://hoge_user:pass@127.0.0.1:1111/db"
    );
    #[cfg(feature = "sqlite")]
    assert_eq!(conf.rdb_url(), "sqlite://db");
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
        Self::load_separated_db_config_from_env(prefix.clone())
            .or_else(|| Self::load_db_url_config_from_env(prefix))
    }
    fn load_separated_db_config_from_env(prefix: String) -> Option<RdbConfig> {
        envy::prefixed(format!("{}{}", &prefix, "SQLITE_"))
            .from_env::<RdbConfigImpl>()
            .or_else(|_| {
                envy::prefixed(format!("{}{}", &prefix, "MYSQL_")).from_env::<RdbConfigImpl>()
            })
            .to_option()
            .map(RdbConfig::Separate)
    }
    fn load_db_url_config_from_env(prefix: String) -> Option<RdbConfig> {
        envy::prefixed(format!("{}{}", &prefix, "SQLITE_"))
            .from_env::<RdbUrlConfigImpl>()
            .or_else(|_| {
                envy::prefixed(format!("{}{}", &prefix, "MYSQL_")).from_env::<RdbUrlConfigImpl>()
            })
            .to_option()
            .map(RdbConfig::Url)
    }
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

#[cfg(feature = "sqlite")]
pub async fn new_rdb_pool(config: &RdbConfig, init_schema: Option<&String>) -> Result<RdbPool> {
    use anyhow::anyhow;
    use sqlx::{
        migrate::MigrateDatabase,
        sqlite::{SqliteConnectOptions, SqlitePoolOptions},
        Sqlite,
    };
    tracing::debug!("new sqlite pool: {}", config.rdb_url());
    // create db file if not exists
    if !Sqlite::database_exists(&config.rdb_url()).await? {
        Sqlite::create_database(&config.rdb_url()).await?;
    }
    let options = SqliteConnectOptions::from_url(&url::Url::parse(&config.rdb_url())?)?
        .log_statements(LevelFilter::Trace)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1));

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

#[cfg(feature = "sqlite")]
async fn setup_sqlite(p: &RdbPool, init_schema: Option<&String>) -> Result<()> {
    sqlx::query::<Rdb>("PRAGMA journal_mode = WAL;")
        .execute(p)
        .await?;
    sqlx::query::<Rdb>("PRAGMA synchronous  = NORMAL;")
        .execute(p)
        .await?;
    sqlx::query::<Rdb>("PRAGMA auto_vacuum = incremental")
        .execute(p)
        .await?;
    if let Some(init_schema) = init_schema {
        sqlx::raw_sql(init_schema.as_str()).execute(p).await?;
    }
    Ok(())
}

#[cfg(feature = "mysql")]
pub async fn new_rdb_pool(config: &RdbConfig, _sqlite_schema: Option<&String>) -> Result<RdbPool> {
    // let port = config.port.parse::<u16>()?;
    // from sqlx 0.7, mysql connection options not used (statement_cache_capacity)
    // ref. https://github.com/launchbadge/sqlx/issues/2773

    use sqlx::mysql::MySqlPoolOptions;
    tracing::info!("new mysql pool: {}", config.rdb_url());
    let options: sqlx::mysql::MySqlConnectOptions = config
        .rdb_url()
        .parse()
        .context(format!("cannot parse url: {:?}", config.rdb_url()))?;

    let options = options
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1));

    // TODO set from config
    MySqlPoolOptions::new()
        .idle_timeout(Some(Duration::from_secs(10 * 60)))
        .max_lifetime(Some(Duration::from_secs(10 * 60))) // same as mariadb server wait_timeout
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

#[cfg(feature = "postgres")]
pub async fn new_rdb_pool(config: &RdbConfig, _sqlite_schema: Option<&String>) -> Result<RdbPool> {
    // let port = config.port.parse::<u16>()?;
    // from sqlx 0.7, mysql connection options not used (statement_cache_capacity)
    // ref. https://github.com/launchbadge/sqlx/issues/2773

    use sqlx::postgres::PgPoolOptions;
    tracing::info!("new mysql pool: {}", config.rdb_url());
    let options: sqlx::postgres::PgConnectOptions = config
        .rdb_url()
        .parse()
        .context(format!("cannot parse url: {:?}", config.rdb_url()))?;

    let options = options
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1));

    // TODO set from config
    PgPoolOptions::new()
        .idle_timeout(Some(Duration::from_secs(10 * 60)))
        .max_lifetime(Some(Duration::from_secs(10 * 60))) // same as mariadb server wait_timeout
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

pub mod query_result {
    #[cfg(feature = "mysql")]
    use sqlx::mysql::MySqlQueryResult;
    #[cfg(feature = "postgres")]
    use sqlx::postgres::PgQueryResult;
    #[cfg(feature = "sqlite")]
    use sqlx::sqlite::SqliteQueryResult;

    #[cfg(feature = "mysql")]
    pub fn last_insert_id(res: MySqlQueryResult) -> i64 {
        res.last_insert_id() as i64
    }
    #[cfg(feature = "postgres")]
    pub fn last_insert_id(res: PgQueryResult) -> i64 {
        // TODO not available in postgres
        0
    }
    #[cfg(feature = "sqlite")]
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

    #[cfg(feature = "sqlite")]
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
                20,
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
    #[cfg(feature = "postgres")]
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
