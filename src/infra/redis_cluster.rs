use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use bb8::ManageConnection;
use bb8::Pool;
use bb8::PooledConnection;
use debug_stub_derive::DebugStub;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::AsyncCommands;
use redis::RedisError;
use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize, Clone, DebugStub)]
pub struct RedisClusterConfig {
    #[debug_stub = "[USER]"]
    pub username: Option<String>,
    #[debug_stub = "[PASSWORD]"]
    pub password: Option<String>,
    pub urls: Vec<String>,
    pub pool_connection_timeout_msec: Option<u64>,
    pub pool_idle_timeout_msec: Option<u64>,
    pub pool_max_lifetime_msec: Option<u64>,
    pub pool_size: usize,
    pub pool_min_idle: Option<u32>,
    /// Set to `true` for blocking operations like BLPOP that may wait indefinitely.
    /// When `true`, disables the response timeout on connections.
    /// Default is `false` (uses redis-rs default timeout).
    #[serde(default)]
    pub blocking: bool,
}

/// Custom bb8 connection manager for Redis Cluster.
///
/// # Why not use redis-rs's built-in bb8 feature?
///
/// redis-rs 1.0.2 introduced default response timeouts in `get_async_connection()`.
/// The built-in bb8 feature uses this method directly without allowing configuration.
///
/// This causes blocking operations like BLPOP to timeout immediately instead of waiting
/// indefinitely. We implement our own `ManageConnection` to use `get_async_connection_with_config()`
/// with `response_timeout: None` when `blocking = true`.
///
/// When redis-rs's bb8 feature supports configuration, this can be replaced.
#[derive(Clone, DebugStub)]
pub struct RedisClusterConnectionManager {
    #[debug_stub = "ClusterClient"]
    client: ClusterClient,
    blocking: bool,
}

impl RedisClusterConnectionManager {
    pub fn new(client: ClusterClient, blocking: bool) -> Self {
        Self { client, blocking }
    }
}

impl ManageConnection for RedisClusterConnectionManager {
    type Connection = ClusterConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        // NOTE: For Redis Cluster, response_timeout is configured at ClusterClient creation time
        // via ClusterClientBuilder::response_timeout(). The blocking flag is used during
        // new_redis_cluster_pool() to create an appropriate ClusterClient.
        // Here we just use the client that was already configured with the right timeout.
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err(RedisError::from((
                redis::ErrorKind::Io,
                "ping validation failed",
                pong,
            ))),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

pub type RedisClusterPool = Pool<RedisClusterConnectionManager>;

// TODO looking forward to resolving issue: https://github.com/redis-rs/redis-rs/issues/492
//
// use redis::aio::Connection as RedisConnection;
// use redis::aio::PubSub;
// pub type RedisClient = redis::cluster::ClusterClient;
//#[async_trait]
//pub trait UseRedisClusterClient {
//    fn redis_cluster_client(&self) -> &RedisClient;
//
//    async fn subscribe(&self, channel: &str) -> Result<PubSub> {
//        let mut pubsub = self
//            .redis_cluster_client()
//            .get_async_connection()
//            .await?
//            .into_pubsub();
//        pubsub.subscribe(channel).await?;
//        Ok(pubsub)
//    }
//
//    async fn psubscribe(&self, pchannel: &String) -> Result<PubSub> {
//        let mut pubsub = self
//            .redis_cluster_client()
//            .get_async_connection()
//            .await?
//            .into_pubsub();
//        pubsub.psubscribe(pchannel).await?;
//        Ok(pubsub)
//    }
//
//    async fn publish(&self, channel: &str, message: &Vec<u8>) -> Result<()> {
//        let mut conn = self.redis_cluster_client().get_async_connection().await?;
//        conn.publish(channel, message).await?;
//        Ok(())
//    }
//}

#[async_trait]
pub trait UseRedisClusterConnection {
    fn redis_cluster_connection(&self) -> &ClusterConnection;
}

#[async_trait]
pub trait UseRedisClusterPool {
    fn redis_cluster_pool(&self) -> &RedisClusterPool;

    async fn connection(&self) -> Result<PooledConnection<'_, RedisClusterConnectionManager>> {
        self.redis_cluster_pool()
            .get()
            .await
            .map_err(|e| anyhow!("{:?}", e))
    }
}

// for normal use (single connection)
pub fn new_redis_cluster_client(conf: RedisClusterConfig) -> Result<ClusterClient> {
    tracing::info!("Connecting to {:?}", conf.urls);
    ClusterClient::new(conf.urls).map_err(|e| e.into())
}
pub async fn new_redis_cluster_connection(config: RedisClusterConfig) -> Result<ClusterConnection> {
    tracing::info!("Connecting to {:?}", config);

    let client = new_redis_cluster_client(config)?;
    client
        .get_async_connection()
        .await
        .map_err(|e| anyhow!("redis_cluster init error: {:?}", e))
}

// pooling for multiple connections (supports blocking operations via conf.blocking)
pub async fn new_redis_cluster_pool(conf: RedisClusterConfig) -> Result<RedisClusterPool> {
    tracing::info!(
        "Connecting to cluster with pool {:?}, blocking={}",
        conf.urls,
        conf.blocking
    );

    // Build ClusterClient with appropriate response_timeout based on blocking flag
    let mut client_builder = redis::cluster::ClusterClientBuilder::new(conf.urls.clone());
    if conf.blocking {
        // Use Duration::MAX for blocking operations (effectively no timeout)
        client_builder = client_builder.response_timeout(Duration::MAX);
    }
    let client = client_builder.build()?;
    let manager = RedisClusterConnectionManager::new(client, conf.blocking);

    let pool_size: u32 = conf.pool_size.try_into().map_err(|_| {
        anyhow!(
            "pool_size {} exceeds maximum value of {} for Redis cluster pool",
            conf.pool_size,
            u32::MAX
        )
    })?;
    let mut builder = Pool::builder().max_size(pool_size);

    if let Some(timeout) = conf.pool_connection_timeout_msec {
        builder = builder.connection_timeout(Duration::from_millis(timeout));
    }

    if let Some(timeout) = conf.pool_idle_timeout_msec {
        builder = builder.idle_timeout(Some(Duration::from_millis(timeout)));
    }

    if let Some(timeout) = conf.pool_max_lifetime_msec {
        builder = builder.max_lifetime(Some(Duration::from_millis(timeout)));
    }

    if let Some(min_idle) = conf.pool_min_idle {
        builder = builder.min_idle(Some(min_idle));
    }

    builder.build(manager).await.map_err(|e| {
        anyhow!(format!(
            "redis cluster pool init error: {:?}, urls={:?}",
            e, conf.urls
        ))
    })
}

pub trait UseRedisClusterLock: UseRedisClusterPool + Send + Sync {
    fn lock(
        &self,
        key: impl Into<String> + Send + Sync,
        expire_sec: i32,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let mut con = self.redis_cluster_pool().get().await?;
            // use redis_cluster set cmd with nx and ex option to lock
            let k = key.into();
            match redis::cmd("SET")
                .arg(&k)
                .arg(Self::lock_value())
                .arg("NX")
                .arg("EX")
                .arg(expire_sec)
                .query_async(&mut *con)
                .await
            {
                Ok(lock) => {
                    if Self::is_ok(lock) {
                        Ok(())
                    } else {
                        tracing::debug!("failed to lock:{:?}", &k);
                        Err(anyhow!("failed to lock:{:?}", &k))
                    }
                }
                Err(e) => {
                    // unlock if error? (comment out for pesimistic lock but may make process slow (locked until expire time, so set expiretime not too long))
                    // let _ = self.unlock(key).await;
                    Err(e.into())
                }
            }
        }
    }

    fn unlock(
        &self,
        key: impl Into<String> + Send + Sync,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async {
            let mut redis_cluster = self.redis_cluster_pool().get().await?;
            let k = key.into();
            redis_cluster.del::<String, ()>(k).await?;
            Ok(())
        }
    }

    #[inline]
    fn lock_value() -> &'static str {
        "1"
    }

    #[inline]
    fn is_ok(str: String) -> bool {
        str == "OK" // for redis_cluster OK response
    }

    #[inline]
    fn is_not_ok(str: String) -> bool {
        str != "OK" // for redis_cluster OK response
    }
}

#[cfg(feature = "redis-cluster-test")]
#[cfg(test)]
mod test {
    use crate::infra::redis_cluster::{UseRedisClusterLock, UseRedisClusterPool};
    use anyhow::Result;
    use redis::AsyncCommands;

    #[tokio::test]
    async fn single_test() {
        use serde::{Deserialize, Serialize};
        let host = std::env::var("TEST_REDIS_CLUSTER_HOST").unwrap_or("127.0.0.1".to_string());

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct User {
            id: u64,
            name: String,
            mail: String,
        }
        let config = super::RedisClusterConfig {
            username: None,
            password: None,
            urls: vec![
                format!("redis://{}:7000", host),
                format!("redis://{}:7001", host),
                format!("redis://{}:7002", host),
                format!("redis://{}:7003", host),
                format!("redis://{}:7004", host),
                format!("redis://{}:7005", host),
            ],
            pool_connection_timeout_msec: None,
            pool_idle_timeout_msec: None,
            pool_max_lifetime_msec: None,
            pool_size: 10,
            pool_min_idle: None,
            blocking: false,
        };
        let mut cli = super::new_redis_cluster_connection(config)
            .await
            .inspect_err(|e| println!("error: {e:?}"))
            .unwrap();
        cli.del::<&str, u32>("foo").await.unwrap();
        let v: Option<String> = cli.get("foo").await.unwrap();

        // use the pool like any other RedisClusterClient with the Deref trait
        assert_eq!(None, v);
        assert!(cli
            .set_ex::<&str, &str, bool>("foo", "bar", 30)
            .await
            .unwrap());
        assert_eq!(
            "bar".to_string(),
            cli.get::<&str, String>("foo").await.unwrap()
        );
        let user1 = User {
            id: 1,
            name: "Taro".to_string(),
            mail: "taro@example.com".to_string(),
        };
        // set serialized user1
        assert!(cli
            .set_ex::<&str, String, bool>("user1", serde_json::to_string(&user1).unwrap(), 30,)
            .await
            .unwrap());
        // get and deserialize user1 string
        if let Ok(st) = cli.get::<&str, Option<String>>("user1").await {
            assert_eq!(
                Some(user1),
                serde_json::from_str(st.unwrap().as_str()).unwrap()
            )
        } else {
            panic!("expected value not found for struct User");
        }
        // for end
    }

    #[tokio::test]
    async fn pool_test() -> Result<()> {
        let host = std::env::var("TEST_REDIS_CLUSTER_HOST").unwrap_or("127.0.0.1".to_string());

        #[derive(Clone)]
        struct TestRedisClusterPool {
            pool: super::RedisClusterPool,
        }
        impl super::UseRedisClusterPool for TestRedisClusterPool {
            fn redis_cluster_pool(&self) -> &super::RedisClusterPool {
                &self.pool
            }
        }
        let config = super::RedisClusterConfig {
            username: None,
            password: None,
            urls: vec![
                format!("redis://{}:7000", host),
                format!("redis://{}:7001", host),
                format!("redis://{}:7002", host),
                format!("redis://{}:7003", host),
                format!("redis://{}:7004", host),
                format!("redis://{}:7005", host),
            ],
            pool_connection_timeout_msec: None,
            pool_idle_timeout_msec: None,
            pool_max_lifetime_msec: None,
            pool_size: 5,
            pool_min_idle: None,
            blocking: true, // Enable blocking for BLPOP test
        };
        let p = super::new_redis_cluster_pool(config).await.unwrap();
        let client = TestRedisClusterPool { pool: p };
        let th_p = client.clone();
        // blocking (pull)
        let pull_jh = tokio::spawn(async move {
            while let Ok((k, v)) = th_p
                .connection()
                .await
                .unwrap()
                .blpop::<&str, (String, i64)>("foobl", 1.0)
                .await
            {
                println!("============ Blocking pop result on {k}: {v}");
            }
        });

        let key = "ffffoooo";
        // use the pool like any other RedisClusterClient with the Deref trait
        assert_eq!(
            Ok(None),
            client
                .connection()
                .await
                .unwrap()
                .get::<&str, Option<String>>(key)
                .await
        );
        assert_eq!(
            Ok(true),
            client
                .connection()
                .await
                .unwrap()
                .set_ex(key, "bar", 10)
                .await
        );
        assert_eq!(
            "bar".to_string(),
            client
                .connection()
                .await
                .unwrap()
                .get::<&str, String>(key)
                .await
                .unwrap()
        );
        // push for blpop
        for idx in 0..50i64 {
            assert!(client
                .connection()
                .await
                .unwrap()
                .rpush::<&str, i64, bool>("foobl", idx)
                .await
                .is_ok());
            // tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(Ok(true), client.connection().await.unwrap().del(key).await);
        assert_eq!(
            Ok(None),
            client
                .connection()
                .await
                .unwrap()
                .get::<&str, Option<String>>(key)
                .await
        );
        // for end
        // wait for blpop timeout
        pull_jh.await?;
        // pull_jh.abort();
        Ok(())
    }

    // lock and unlock test with UseRedisClusterLock
    #[tokio::test]
    async fn lock_unlock_test() -> Result<()> {
        #[derive(Clone)]
        struct TestRedisClusterPool {
            pool: super::RedisClusterPool,
        }
        impl UseRedisClusterPool for TestRedisClusterPool {
            fn redis_cluster_pool(&self) -> &super::RedisClusterPool {
                &self.pool
            }
        }
        impl super::UseRedisClusterLock for TestRedisClusterPool {}
        let host = std::env::var("TEST_REDIS_CLUSTER_HOST").unwrap_or("127.0.0.1".to_string());

        let config = super::RedisClusterConfig {
            username: None,
            password: None,
            urls: vec![
                format!("redis://{}:7000", host),
                format!("redis://{}:7001", host),
                format!("redis://{}:7002", host),
                format!("redis://{}:7003", host),
                format!("redis://{}:7004", host),
                format!("redis://{}:7005", host),
            ],
            pool_connection_timeout_msec: None,
            pool_idle_timeout_msec: None,
            pool_max_lifetime_msec: None,
            pool_size: 5,
            pool_min_idle: None,
            blocking: false, // Lock operations don't need blocking
        };
        let p = super::new_redis_cluster_pool(config).await.unwrap();
        let client = TestRedisClusterPool { pool: p };
        let key = "lock_test";
        // get lock
        client.lock(key, 10).await?;
        // try lock -> failure
        assert!(client.lock(key, 10).await.is_err());
        // unlock
        client.unlock(key).await?;
        // try lock again
        assert!(client.lock(key, 10).await.is_ok());
        // for end
        Ok(())
    }
}
