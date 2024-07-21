pub mod memory;
pub mod net;
pub mod rdb;
pub mod redis;
pub mod redis_cluster;

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
    pub static TEST_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    });

    pub async fn setup_test_rdb() -> &'static RdbPool {
        if cfg!(feature = "mysql") {
            setup_test_rdb_from::<&str>("sql/mysql").await
        } else {
            setup_test_rdb_from::<&str>("sql/sqlite").await
        }
    }

    // migrate once in initialization
    #[cfg(feature = "mysql")]
    static MYSQL_INIT: OnceCell<RdbPool> = OnceCell::const_new();

    #[cfg(feature = "mysql")]
    pub static MYSQL_CONFIG: Lazy<RdbConfig> = Lazy::new(|| {
        let host = std::env::var("TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        RdbConfig::new(
            host,
            "3306".to_string(),
            "mysql".to_string(),
            "mysql".to_string(),
            "test".to_string(),
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
                truncate_tables(&pool).await;
                pool
            })
            .await
    }

    #[cfg(feature = "mysql")]
    pub async fn truncate_tables(pool: &RdbPool) {
        sqlx::raw_sql("TRUNCATE TABLE job; TRUNCATE TABLE worker; TRUNCATE TABLE job_result;")
            .execute(pool)
            .await
            .expect("truncate all tables");
    }

    #[cfg(not(feature = "mysql"))]
    static SQLITE_INIT: OnceCell<RdbPool> = OnceCell::const_new();

    #[cfg(not(feature = "mysql"))]
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

    #[cfg(not(feature = "mysql"))]
    pub async fn setup_test_rdb_from<T: Into<String>>(dir: T) -> &'static RdbPool {
        use command_utils::util::result::TapErr;

        SQLITE_INIT
            .get_or_init(|| async {
                _setup_sqlite_internal(dir)
                    .await
                    .tap_err(|e| tracing::error!("error: {:?}", e))
                    .unwrap()
            })
            .await
    }

    #[cfg(not(feature = "mysql"))]
    async fn _setup_sqlite_internal<T: Into<String>>(dir: T) -> Result<RdbPool> {
        let pool = crate::infra::rdb::new_rdb_pool(&SQLITE_CONFIG, None).await?;
        Migrator::new(std::path::Path::new(&dir.into()))
            .await?
            .run(&pool)
            .await?;
        truncate_tables(&pool).await;
        Ok(pool)
    }

    #[cfg(not(feature = "mysql"))]
    pub async fn truncate_tables(pool: &RdbPool) {
        sqlx::raw_sql("DELETE FROM job; DELETE FROM worker; DELETE FROM job_result; DELETE FROM SQLITE_SEQUENCE WHERE name='job' OR name='worker' OR name='job_result';")
            .execute(pool)
            .await
            .expect("delete all tables");
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
            pool_size: 10,
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
