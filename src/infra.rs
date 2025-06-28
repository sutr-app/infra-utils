pub mod cache;
pub mod chan;
pub mod lock;
pub mod memory;
pub mod net;
pub mod rdb;
pub mod redis;
pub mod redis_cluster;
pub mod trace;

// for test only
pub mod test {
    use super::{
        rdb::{RdbConfig, RdbPool},
        redis::{RedisClient, RedisConfig, RedisPool},
    };
    use anyhow::Result;
    use once_cell::sync::Lazy;
    use sqlx::migrate::Migrator;
    use tokio::{runtime::Runtime, sync::OnceCell};

    // destroy runtime, destroy connection pool, so use as static in all test
    // ref. https://qiita.com/autotaker1984/items/d0ae2d7feb148ffb8989
    // ref. https://github.com/launchbadge/sqlx/issues/2881
    // ref. https://github.com/kingwingfly/share_runtime_example
    pub static TEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });

    pub async fn setup_test_rdb() -> &'static RdbPool {
        if cfg!(feature = "mysql") {
            setup_test_rdb_from::<&str>("sql/mysql").await
        } else if cfg!(all(feature = "postgres", not(feature = "mysql"))) {
            setup_test_rdb_from::<&str>("sql/postgres").await
        } else {
            setup_test_rdb_from::<&str>("sql/sqlite").await
        }
    }

    // migrate once in initialization
    #[cfg(feature = "mysql")]
    static MYSQL_INIT: OnceCell<RdbPool> = OnceCell::const_new();

    #[cfg(feature = "mysql")]
    pub static MYSQL_CONFIG: Lazy<RdbConfig> = Lazy::new(|| {
        //let host = std::env::var("TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        //RdbConfig::new(
        //    host,
        //    "3306".to_string(),
        //    "mysql".to_string(),
        //    "mysql".to_string(),
        //    "test".to_string(),
        //    20,
        //)
        RdbConfig::new_by_url(
            std::env::var("TEST_MYSQL_URL")
                .unwrap_or_else(|_| "mysql://maria:maria@127.0.0.1:3306/test".to_string())
                .as_str(),
            20,
        )
    });

    #[cfg(feature = "mysql")]
    pub async fn setup_test_rdb_from<T: Into<String>>(dir: T) -> &'static RdbPool {
        MYSQL_INIT
            .get_or_init(|| async {
                let pool = crate::infra::rdb::new_rdb_pool(&MYSQL_CONFIG, None)
                    .await
                    .unwrap();
                Migrator::new(std::path::Path::new(&dir.into()))
                    .await
                    .unwrap()
                    .run(&pool)
                    .await
                    .unwrap();
                pool
            })
            .await
    }

    #[cfg(feature = "mysql")]
    pub async fn truncate_tables(pool: &RdbPool, tables: Vec<&str>) {
        let sql = tables
            .iter()
            .map(|t| format!("TRUNCATE TABLE {t};"))
            .collect::<Vec<String>>()
            .join(" ");
        sqlx::raw_sql(sql.as_str())
            .execute(pool)
            .await
            .expect("truncate all tables");
    }

    // migrate once in initialization
    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    static POSTGRES_INIT: OnceCell<RdbPool> = OnceCell::const_new();

    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    pub static POSTGRES_CONFIG: Lazy<RdbConfig> = Lazy::new(|| {
        //let host = std::env::var("TEST_POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        //RdbConfig::new(
        //    host,
        //    "3306".to_string(),
        //    "postgres".to_string(),
        //    "postgres".to_string(),
        //    "test".to_string(),
        //    20,
        //)
        RdbConfig::new_by_url(
            std::env::var("TEST_POSTGRES_URL")
                .unwrap_or_else(|_| "postgres://postgres:postgres@127.0.0.1:5432/test".to_string())
                .as_str(),
            20,
        )
    });

    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    pub async fn setup_test_rdb_from<T: Into<String>>(dir: T) -> &'static RdbPool {
        POSTGRES_INIT
            .get_or_init(|| async {
                let pool = crate::infra::rdb::new_rdb_pool(&POSTGRES_CONFIG, None)
                    .await
                    .unwrap();
                Migrator::new(std::path::Path::new(&dir.into()))
                    .await
                    .unwrap()
                    .run(&pool)
                    .await
                    .unwrap();
                pool
            })
            .await
    }

    #[cfg(all(feature = "postgres", not(feature = "mysql")))]
    pub async fn truncate_tables(pool: &RdbPool, tables: Vec<&str>) {
        let sql = tables
            .iter()
            .map(|t| format!("TRUNCATE TABLE {};", t))
            .collect::<Vec<String>>()
            .join(" ");
        sqlx::raw_sql(sql.as_str())
            .execute(pool)
            .await
            .expect("truncate all tables");
    }

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    static SQLITE_INIT: OnceCell<RdbPool> = OnceCell::const_new();

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    pub static SQLITE_CONFIG: Lazy<RdbConfig> = Lazy::new(|| {
        RdbConfig::new(
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "".to_string(),
            "./test_db.sqlite3".to_string(),
            20,
        )
    });

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    pub async fn setup_test_rdb_from<T: Into<String>>(dir: T) -> &'static RdbPool {
        SQLITE_INIT
            .get_or_init(|| async {
                _setup_sqlite_internal(dir)
                    .await
                    .inspect_err(|e| tracing::error!("error: {:?}", e))
                    .unwrap()
            })
            .await
    }

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    async fn _setup_sqlite_internal<T: Into<String>>(dir: T) -> Result<RdbPool> {
        let pool = crate::infra::rdb::new_rdb_pool(&SQLITE_CONFIG, None).await?;
        Migrator::new(std::path::Path::new(&dir.into()))
            .await?
            .run(&pool)
            .await?;
        Ok(pool)
    }

    #[cfg(not(any(feature = "mysql", feature = "postgres")))]
    pub async fn truncate_tables(pool: &RdbPool, tables: Vec<&str>) {
        let sql = tables
            .iter()
            .map(|t| format!("DELETE FROM '{t}'; DELETE FROM SQLITE_SEQUENCE WHERE name = '{t}'; "))
            .collect::<Vec<String>>()
            .join(" ");
        sqlx::raw_sql(sql.as_str())
            .execute(pool)
            .await
            .unwrap_or_else(|_| panic!("delete tables: {}", tables.join(",")));
    }

    pub static REDIS_CONFIG: Lazy<RedisConfig> = Lazy::new(|| {
        let url = std::env::var("TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        RedisConfig {
            username: None,
            password: None,
            url,
            pool_create_timeout_msec: None,
            pool_wait_timeout_msec: None,
            pool_recycle_timeout_msec: None,
            pool_size: 20,
        }
    });

    static REDIS: tokio::sync::OnceCell<RedisPool> = tokio::sync::OnceCell::const_new();

    pub async fn setup_test_redis_pool() -> &'static RedisPool {
        setup_redis_pool(REDIS_CONFIG.clone()).await
    }
    pub fn setup_test_redis_client() -> Result<RedisClient> {
        crate::infra::redis::new_redis_client(REDIS_CONFIG.clone())
    }
    pub async fn setup_redis_pool(config: RedisConfig) -> &'static RedisPool {
        REDIS
            .get_or_init(|| async {
                crate::infra::redis::new_redis_pool(config)
                    .await
                    .expect("msg")
            })
            .await
    }
}
