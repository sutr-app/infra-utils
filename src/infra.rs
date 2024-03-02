pub mod memory;
pub mod rdb;
pub mod redis;
pub mod redis_cluster;

// for test only
pub mod test {
    use command_utils::util::result::TapErr;

    use super::{
        rdb::RDBConfig,
        redis::{RedisClient, RedisConfig, RedisPool},
    };
    use anyhow::Result;
    use once_cell::sync::Lazy;
    use sqlx::{migrate::Migrator, Any, Pool};
    use tokio::sync::OnceCell;

    // migrate once in initialization
    static MYSQL_INIT: OnceCell<Pool<Any>> = OnceCell::const_new();
    static SQLITE_INIT: OnceCell<Pool<Any>> = OnceCell::const_new();

    pub static SQLITE_CONFIG: Lazy<RDBConfig> = Lazy::new(|| RDBConfig {
        host: "".to_string(),
        port: "".to_string(),
        user: "".to_string(),
        password: "".to_string(),
        dbname: "./test_db.sqlite3".to_string(),
        max_connections: 20,
    });

    pub static MYSQL_CONFIG: Lazy<RDBConfig> = Lazy::new(|| {
        let host = std::env::var("TEST_MYSQL_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
        RDBConfig {
            host,
            port: "3306".to_string(),
            user: "mysql".to_string(),
            password: "mysql".to_string(),
            dbname: "test".to_string(),
            max_connections: 20,
        }
    });

    pub async fn setup_test_sqlite<T: Into<String>>(dir: T) -> &'static Pool<Any> {
        SQLITE_INIT
            .get_or_init(|| async {
                _setup_sqlite_internal(dir)
                    .await
                    .tap_err(|e| tracing::error!("error: {:?}", e))
                    .unwrap()
            })
            .await
    }

    async fn _setup_sqlite_internal<T: Into<String>>(dir: T) -> Result<Pool<Any>> {
        let pool = crate::infra::rdb::new_rdb_pool(&SQLITE_CONFIG, None).await?;
        Migrator::new(std::path::Path::new(&dir.into()))
            .await?
            .run(&pool)
            .await?;
        Ok(pool)
    }

    pub async fn setup_test_mysql<T: Into<String>>(dir: T) -> &'static Pool<Any> {
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
