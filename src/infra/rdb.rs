use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use debug_stub_derive::DebugStub;
use log::LevelFilter;
use serde::Deserialize;
use sqlx::{
    any::{AnyConnectOptions, AnyPoolOptions},
    migrate::MigrateDatabase,
    Any, ConnectOptions, Pool,
};

pub trait RDBConfigTrait: Clone {
    fn mysql_url(&self) -> String;
    // TODO not implemeted for pgsql
    fn pgsql_url(&self) -> String;
    fn sqlite_url(&self) -> String;

    fn max_connections(&self) -> u32;
}

#[derive(Deserialize, Clone, DebugStub)]
pub struct RDBConfigImpl {
    pub host: String,
    pub port: String,
    #[debug_stub = "[USER]"]
    pub user: String,
    #[debug_stub = "[PASSWORD]"]
    pub password: String,
    pub dbname: String,
    pub max_connections: u32,
}

impl RDBConfigTrait for RDBConfigImpl {
    fn mysql_url(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
    // TODO not implemeted for pgsql
    fn pgsql_url(&self) -> String {
        format!(
            "pgsql://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.dbname
        )
    }
    fn sqlite_url(&self) -> String {
        format!("sqlite://{}", self.dbname)
    }
    fn max_connections(&self) -> u32 {
        self.max_connections
    }
}

impl Default for RDBConfigImpl {
    fn default() -> Self {
        tracing::info!("Use default RDBConfig (sqlite3).");
        RDBConfigImpl {
            host: "".to_string(),
            port: "".to_string(),
            user: "".to_string(),
            password: "".to_string(),
            dbname: "jobworkerp.sqlite3".to_string(),
            max_connections: 20,
        }
    }
}

#[derive(Deserialize, Clone, DebugStub)]
pub struct RDBUrlConfigImpl {
    pub url: String,
    pub max_connections: u32,
}

impl RDBConfigTrait for RDBUrlConfigImpl {
    fn mysql_url(&self) -> String {
        if self.url.starts_with("mysql") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    // TODO not implemeted for pgsql
    fn pgsql_url(&self) -> String {
        if self.url.starts_with("pgsql") {
            self.url.clone()
        } else {
            "".to_string()
        }
    }
    fn sqlite_url(&self) -> String {
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

impl Default for RDBUrlConfigImpl {
    fn default() -> Self {
        tracing::info!("Use default RDBConfig (sqlite3).");
        RDBUrlConfigImpl {
            url: "sqlite://jobworkerp.sqlite3".to_string(),
            max_connections: 20,
        }
    }
}

#[test]
fn url_test() {
    let conf = RDBConfigImpl {
        host: "127.0.0.1".to_string(),
        port: "1111".to_string(),
        user: "hoge_user".to_string(),
        password: "pass".to_string(),
        dbname: "db".to_string(),
        max_connections: 20,
    };
    assert_eq!(conf.mysql_url(), "mysql://hoge_user:pass@127.0.0.1:1111/db");
    assert_eq!(conf.sqlite_url(), "sqlite://db")
}

#[derive(Debug, Clone)]
pub enum RDBConfig {
    Separate(RDBConfigImpl),
    Url(RDBUrlConfigImpl),
}
impl RDBConfig {
    pub fn new_by_url(url: &str, max_connections: u32) -> Self {
        RDBConfig::Url(RDBUrlConfigImpl {
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
        RDBConfig::Separate(RDBConfigImpl {
            host,
            port,
            user,
            password,
            dbname,
            max_connections,
        })
    }
}

impl RDBConfigTrait for RDBConfig {
    fn mysql_url(&self) -> String {
        match self {
            RDBConfig::Separate(config) => config.mysql_url(),
            RDBConfig::Url(config) => config.mysql_url(),
        }
    }
    fn pgsql_url(&self) -> String {
        match self {
            RDBConfig::Separate(config) => config.pgsql_url(),
            RDBConfig::Url(config) => config.pgsql_url(),
        }
    }
    fn sqlite_url(&self) -> String {
        match self {
            RDBConfig::Separate(config) => config.sqlite_url(),
            RDBConfig::Url(config) => config.sqlite_url(),
        }
    }
    fn max_connections(&self) -> u32 {
        match self {
            RDBConfig::Separate(config) => config.max_connections,
            RDBConfig::Url(config) => config.max_connections,
        }
    }
}

pub async fn new_sqlite_pool(
    config: &RDBConfig,
    init_schema: Option<&String>,
) -> Result<Pool<Any>> {
    println!("new sqlite pool: {}", config.sqlite_url());
    // create db file if not exists
    if !sqlx::Sqlite::database_exists(&config.sqlite_url()).await? {
        sqlx::Sqlite::create_database(&config.sqlite_url()).await?;
    }
    let options = AnyConnectOptions::from_url(&url::Url::parse(&config.sqlite_url())?)?
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1));

    let pr = AnyPoolOptions::new()
        .max_connections(config.max_connections())
        //.min_connections(3)
        .connect_with(options)
        .await
        .context(format!(
            "cannot initialize sql connection. url:{:?}",
            config.sqlite_url()
        ));
    match pr {
        Ok(p) => setup_sqlite(&p, init_schema).await.map(|_| p),
        Err(e) => Err(anyhow!("setup error: {:?}", e)),
    }
}

async fn setup_sqlite(p: &Pool<Any>, init_schema: Option<&String>) -> Result<()> {
    sqlx::query::<Any>("PRAGMA journal_mode = WAL;")
        .execute(p)
        .await?;
    sqlx::query::<Any>("PRAGMA synchronous  = NORMAL;")
        .execute(p)
        .await?;
    sqlx::query::<Any>("PRAGMA auto_vacuum = incremental")
        .execute(p)
        .await?;
    if let Some(init_schema) = init_schema {
        sqlx::raw_sql(init_schema.as_str()).execute(p).await?;
    }
    Ok(())
}

pub async fn new_mysql_pool(config: &RDBConfig) -> Result<Pool<Any>> {
    // let port = config.port.parse::<u16>()?;
    // from sqlx 0.7, mysql connection options not used (statement_cache_capacity)
    // ref. https://github.com/launchbadge/sqlx/issues/2773
    //
    // let mut options = MySqlConnectOptions::new()
    //     .host(&config.host)
    //     .port(port)
    //     .username(&config.user)
    //     .password(&config.password)
    //     .database(&config.dbname)
    //     .charset("utf8mb4")
    //     .statement_cache_capacity(2048); // TODO setting
    println!("new mysql pool: {}", config.mysql_url());

    let options = AnyConnectOptions::from_url(&url::Url::parse(&config.mysql_url())?)?
        .log_statements(LevelFilter::Debug)
        .log_slow_statements(LevelFilter::Warn, Duration::from_secs(1));

    // TODO set from config
    AnyPoolOptions::new()
        .idle_timeout(Some(Duration::from_secs(10 * 60)))
        .max_lifetime(Some(Duration::from_secs(10 * 60))) // same as mariadb server wait_timeout
        .acquire_timeout(Duration::from_secs(2))
        // .test_before_acquire(false)
        .max_connections(config.max_connections())
        .min_connections(config.max_connections() / 5 + 1)
        .connect_with(options)
        // .connect(&config.mysql_url())
        .await
        .context(format!(
            "cannot initialize mysql connection:{:?}",
            config.mysql_url()
        ))
}

pub async fn new_rdb_pool(
    db_config: &RDBConfig,
    sqlite_schema: Option<&String>,
) -> Result<Pool<Any>> {
    sqlx::any::install_default_drivers();
    let p = new_mysql_pool(db_config).await;
    if p.is_err() {
        new_sqlite_pool(db_config, sqlite_schema).await
    } else {
        p
    }
}

pub trait UseRdbPool {
    fn db_pool(&self) -> &Pool<Any>;
}

pub trait UseRdbOption {
    fn db_pool(&self) -> Option<&Pool<Any>>;
}

pub mod test {
    // use std::time::Duration;

    #[sqlx::test]
    pub async fn test_sqlite() {
        use super::RDBConfig;
        use anyhow::anyhow;
        use sqlx::Any;

        sqlx::any::install_default_drivers();
        // connection test for localhost
        let pool = crate::infra::rdb::new_sqlite_pool(
            &RDBConfig::new(
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
        let rows = sqlx::query::<Any>("SELECT 1 as one")
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow!("db error: {:?}", e));
        assert!(rows.is_ok());
    }
    #[sqlx::test]
    pub async fn test_mysql() {
        use super::RDBConfig;
        use anyhow::anyhow;
        use sqlx::Any;

        sqlx::any::install_default_drivers();
        // connection test for localhost
        let pool = crate::infra::rdb::new_mysql_pool(&RDBConfig::new_by_url(
            "mysql://mysql:mysql@127.0.0.1:3306/test",
            20,
        ))
        .await
        .unwrap();
        let rows = sqlx::query::<Any>("SELECT 1 as one")
            .fetch_all(&pool)
            .await
            .map_err(|e| anyhow!("db error: {:?}", e));
        assert!(rows.is_ok());
    }
}
