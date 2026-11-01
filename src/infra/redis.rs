use anyhow::Result;
use anyhow::anyhow;
use bb8::ManageConnection;
use bb8::Pool;
use bb8::PooledConnection;
use debug_stub_derive::DebugStub;
use futures::stream::BoxStream;
use redis::AsyncCommands;
use redis::AsyncConnectionConfig;
use redis::Client;
use redis::RedisError;
use redis::aio::MultiplexedConnection as RedisConnection;
use redis::aio::PubSub;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[derive(Deserialize, Clone, DebugStub)]
pub struct RedisConfig {
    #[debug_stub = "[USER]"]
    pub username: Option<String>,
    #[debug_stub = "[PASSWORD]"]
    pub password: Option<String>,
    pub url: String,
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

/// Custom bb8 connection manager for Redis.
///
/// # Why not use redis-rs's built-in bb8 feature?
///
/// redis-rs 1.0.2 introduced default response timeouts in `get_multiplexed_async_connection()`.
/// The built-in bb8 feature (`redis = { features = ["bb8"] }`) uses this method directly
/// without allowing `AsyncConnectionConfig` customization.
///
/// This causes blocking operations like BLPOP to timeout immediately instead of waiting
/// indefinitely. Since both redis-rs's bb8 feature and the bb8-redis crate have this limitation,
/// we implement our own `ManageConnection` to use `get_multiplexed_async_connection_with_config()`
/// with `response_timeout: None` when `blocking = true`.
///
/// When redis-rs's bb8 feature supports `AsyncConnectionConfig`, this can be replaced.
#[derive(Clone, Debug)]
pub struct RedisConnectionManager {
    client: Client,
    blocking: bool,
    /// Sent with `CLIENT SETNAME` on every new connection, so the server-side
    /// clients of this pool can be found (e.g. by [`kill_blocked_clients`]).
    client_name: Option<String>,
}

impl RedisConnectionManager {
    pub fn new(client: Client, blocking: bool) -> Self {
        Self {
            client,
            blocking,
            client_name: None,
        }
    }

    pub fn with_client_name(mut self, client_name: impl Into<String>) -> Self {
        self.client_name = Some(client_name.into());
        self
    }
}

impl ManageConnection for RedisConnectionManager {
    type Connection = RedisConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut connection = if self.blocking {
            let config = AsyncConnectionConfig::new().set_response_timeout(None);
            self.client
                .get_multiplexed_async_connection_with_config(&config)
                .await?
        } else {
            self.client.get_multiplexed_async_connection().await?
        };
        if let Some(name) = &self.client_name {
            connection.client_setname::<_, ()>(name).await?;
        }
        Ok(connection)
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

pub type RedisPool = Pool<RedisConnectionManager>;
pub type RedisClient = redis::Client;

pub trait UseRedisClient: Send + Sync {
    fn redis_client(&self) -> &RedisClient;

    fn subscribe(&self, channel: &str) -> impl std::future::Future<Output = Result<PubSub>> + Send {
        async move {
            let mut pubsub = self.redis_client().get_async_pubsub().await?;
            pubsub.subscribe(channel).await?;
            Ok(pubsub)
        }
    }

    /// Subscribe with timeout setting
    fn subscribe_with_timeout(
        &self,
        channel: &str,
        read_timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<PubSub>> + Send {
        async move {
            let mut pubsub = self.redis_client().get_async_pubsub().await?;

            // Note: We'll handle timeout at the application level since redis-rs PubSub
            // doesn't directly support read timeout. The timeout will be enforced
            // by the calling code using tokio::select! and tokio::time::timeout.
            if let Some(timeout) = read_timeout {
                tracing::debug!(
                    "Pubsub created with application-level timeout: {:?} for channel: {}",
                    timeout,
                    channel
                );
            }

            pubsub.subscribe(channel).await?;
            Ok(pubsub)
        }
    }

    fn unsubscribe(&self, channel: &str) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let mut pubsub = self.redis_client().get_async_pubsub().await?;
            pubsub.unsubscribe(channel).await?;
            Ok(())
        }
    }

    fn psubscribe(
        &self,
        pchannel: &String,
    ) -> impl std::future::Future<Output = Result<PubSub>> + Send {
        async move {
            let mut pubsub = self.redis_client().get_async_pubsub().await?;
            pubsub.psubscribe(pchannel).await?;
            Ok(pubsub)
        }
    }

    fn psubscribe_with_timeout(
        &self,
        pchannel: &String,
        read_timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<PubSub>> + Send {
        async move {
            let mut pubsub = self.redis_client().get_async_pubsub().await?;

            // Pattern subscribe timeout handled at application level
            if let Some(timeout) = read_timeout {
                tracing::debug!(
                    "Pubsub pattern created with application-level timeout: {:?} for pattern: {}",
                    timeout,
                    pchannel
                );
            }

            pubsub.psubscribe(pchannel).await?;
            Ok(pubsub)
        }
    }

    fn publish(
        &self,
        channel: &str,
        message: &Vec<u8>,
    ) -> impl std::future::Future<Output = Result<u32>> + Send {
        async move {
            let mut conn = self
                .redis_client()
                .get_multiplexed_async_connection()
                .await?;
            let r = conn
                .publish::<&str, &Vec<u8>, u32>(channel, message)
                .await?;
            Ok(r)
        }
    }

    fn publish_stream(
        &self,
        channel: &str,
        mut message: BoxStream<Vec<u8>>,
    ) -> impl std::future::Future<Output = Result<u32>> + Send {
        async move {
            tracing::debug!("publish_stream: {:?}", channel);
            let mut conn = self
                .redis_client()
                .get_multiplexed_async_connection()
                .await?;
            let r = Arc::new(Mutex::new(0));
            use futures::StreamExt;
            while let Some(msg) = message.next().await {
                let res = conn.publish::<&str, &Vec<u8>, u32>(channel, &msg).await;
                tracing::debug!("published: {:?}", res);
                if let Ok(n) = res {
                    *r.lock().unwrap() += n;
                }
            }
            let count = {
                let guard = r.lock().unwrap();
                *guard
            };
            Ok(count)
        }
    }

    fn publish_multi_if_listen(
        &self,
        channels: &[String],
        message: &Vec<u8>,
    ) -> impl std::future::Future<Output = Result<bool>> + Send {
        async move {
            let mut res = false;
            for ch in channels {
                let mut conn = self
                    .redis_client()
                    .get_multiplexed_async_connection()
                    .await?;
                let (channel, sub_count) = self.numsub(&mut conn, ch).await?;
                if sub_count > 0 {
                    conn.publish::<&str, &Vec<u8>, ()>(channel.as_str(), message)
                        .await?;
                    res = true;
                }
            }
            Ok(res)
        }
    }

    fn numsub(
        &self,
        conn: &mut RedisConnection,
        channel: &str,
    ) -> impl std::future::Future<Output = Result<(String, i64)>> + Send {
        async move {
            let subscriptions_counts: HashMap<String, u32> = redis::cmd("PUBSUB")
                .arg("NUMSUB")
                .arg(channel)
                .query_async(conn)
                .await?;
            let subscription_count = *subscriptions_counts.get(channel).unwrap();
            Ok((channel.to_string(), subscription_count as i64))
        }
    }
}

pub trait UseRedisConnection {
    fn redis_connection(&self) -> &RedisConnection;
}

pub trait UseRedisPool: Send + Sync {
    fn redis_pool(&self) -> &RedisPool;

    fn connection(
        &self,
    ) -> impl std::future::Future<Output = Result<PooledConnection<'_, RedisConnectionManager>>> + Send
    {
        async {
            self.redis_pool()
                .get()
                .await
                .map_err(|e| anyhow!("{:?}", e))
        }
    }
}

/// Trait for accessing a Redis pool configured for blocking operations like BLPOP.
/// This pool has response_timeout disabled to allow indefinite waiting.
pub trait UseRedisBlockingPool: Send + Sync {
    fn redis_blocking_pool(&self) -> &RedisPool;

    fn blocking_connection(
        &self,
    ) -> impl std::future::Future<Output = Result<PooledConnection<'_, RedisConnectionManager>>> + Send
    {
        async {
            self.redis_blocking_pool()
                .get()
                .await
                .map_err(|e| anyhow!("{:?}", e))
        }
    }
}

// for normal use (single connection)
pub fn new_redis_client(config: RedisConfig) -> Result<RedisClient> {
    tracing::info!("Connecting to {:?}", config.url);
    Client::open(config.url).map_err(|e| e.into())
}
pub async fn new_redis_connection(config: RedisConfig) -> Result<RedisConnection> {
    tracing::info!("Connecting to {:?}", config);

    let client = Client::open(config.url)?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| anyhow!("redis init error: {:?}", e))
}

// pooling for multiple connections (supports blocking operations via config.blocking)
pub async fn new_redis_pool(config: RedisConfig) -> Result<RedisPool> {
    build_redis_pool(config, None).await
}

/// [`new_redis_pool`] whose connections carry `client_name` on the server.
pub async fn new_named_redis_pool(config: RedisConfig, client_name: &str) -> Result<RedisPool> {
    build_redis_pool(config, Some(client_name)).await
}

async fn build_redis_pool(config: RedisConfig, client_name: Option<&str>) -> Result<RedisPool> {
    let client = Client::open(config.url.clone())?;
    let mut manager = RedisConnectionManager::new(client, config.blocking);
    if let Some(name) = client_name {
        manager = manager.with_client_name(name);
    }
    let pool_size: u32 = config.pool_size.try_into().map_err(|_| {
        anyhow!(
            "pool_size {} exceeds maximum value of {} for Redis pool",
            config.pool_size,
            u32::MAX
        )
    })?;
    let mut builder = Pool::builder().max_size(pool_size);

    if let Some(timeout) = config.pool_connection_timeout_msec {
        builder = builder.connection_timeout(Duration::from_millis(timeout));
    }

    if let Some(timeout) = config.pool_idle_timeout_msec {
        builder = builder.idle_timeout(Some(Duration::from_millis(timeout)));
    }

    if let Some(timeout) = config.pool_max_lifetime_msec {
        builder = builder.max_lifetime(Some(Duration::from_millis(timeout)));
    }

    if let Some(min_idle) = config.pool_min_idle {
        builder = builder.min_idle(Some(min_idle));
    }

    builder.build(manager).await.map_err(|e| {
        anyhow!(format!(
            "redis pool init error: config={:?}, {:?}",
            config, e
        ))
    })
}

/// Closes the server-side connections named `client_name` that are blocked in a
/// command such as BLPOP, and returns how many it closed.
///
/// A blocking command whose future was dropped keeps waiting on the server, and
/// it can still pop an element that nobody reads; closing its connection ends it.
/// Uses a connection of its own, so it works while the pool's are blocked.
pub async fn kill_blocked_clients(client: &RedisClient, client_name: &str) -> Result<usize> {
    let mut connection = client.get_multiplexed_async_connection().await?;
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .arg("TYPE")
        .arg("normal")
        .query_async(&mut connection)
        .await?;
    let mut closed = 0;
    for id in blocked_client_ids(&list, client_name) {
        // A client that is already gone is not an error: it is no longer blocked.
        let killed: redis::RedisResult<i64> = redis::cmd("CLIENT")
            .arg("KILL")
            .arg("ID")
            .arg(id)
            .query_async(&mut connection)
            .await;
        if matches!(killed, Ok(n) if n > 0) {
            closed += 1;
        }
    }
    Ok(closed)
}

/// Ids in a `CLIENT LIST` reply of the clients named `client_name` that are
/// blocked (`b` in their flags).
fn blocked_client_ids(client_list: &str, client_name: &str) -> Vec<u64> {
    client_list
        .lines()
        .filter_map(|line| {
            let field = |key: &str| {
                line.split_whitespace()
                    .find_map(|pair| pair.strip_prefix(key)?.strip_prefix('='))
            };
            let blocked = field("flags").is_some_and(|flags| flags.contains('b'));
            (blocked && field("name") == Some(client_name))
                .then(|| field("id")?.parse().ok())
                .flatten()
        })
        .collect()
}

pub trait UseRedisLock: UseRedisPool + Send + Sync {
    fn lock(
        &self,
        key: impl Into<String> + Send + Sync,
        expire_sec: i32,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async move {
            let mut con = self.redis_pool().get().await?;
            // use redis set cmd with nx and ex option to lock
            let k = key.into();
            // SET NX returns nil when key already exists (lock not acquired)
            // so use Option<String> to handle both "OK" and nil responses
            match redis::cmd("SET")
                .arg(&k)
                .arg(Self::lock_value())
                .arg("NX")
                .arg("EX")
                .arg(expire_sec)
                .query_async::<Option<String>>(&mut *con)
                .await
            {
                Ok(Some(lock)) => {
                    if Self::is_ok(lock) {
                        Ok(())
                    } else {
                        tracing::debug!("failed to lock:{:?}", &k);
                        Err(anyhow!("failed to lock:{:?}", k))
                    }
                }
                Ok(None) => {
                    // nil response means key already exists (lock held by another)
                    tracing::debug!("failed to lock (key exists):{:?}", &k);
                    Err(anyhow!("failed to lock:{:?}", k))
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
            let mut redis = self.redis_pool().get().await?;
            let k = key.into();
            redis.del::<String, ()>(k).await?;
            Ok(())
        }
    }

    #[inline]
    fn lock_value() -> &'static str {
        "1"
    }

    #[inline]
    fn is_ok(str: String) -> bool {
        str == "OK" // for redis OK response
    }

    #[inline]
    fn is_not_ok(str: String) -> bool {
        str != "OK" // for redis OK response
    }
}

// #[cfg(feature = "redis-test")]
#[cfg(test)]
mod test {
    use super::RedisPool;
    use crate::infra::{
        redis::{RedisClient, UseRedisLock, UseRedisPool, new_redis_connection, new_redis_pool},
        test::REDIS_CONFIG,
    };
    use anyhow::Result;
    use futures::StreamExt;
    use redis::AsyncCommands;
    use serde::{Deserialize, Serialize};

    #[tokio::test]
    async fn single_test() {
        use crate::infra::test::REDIS_CONFIG;

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct User {
            id: u64,
            name: String,
            mail: String,
        }
        let config = REDIS_CONFIG.clone();
        let mut cli = new_redis_connection(config).await.unwrap();
        cli.del::<&str, u32>("foo").await.unwrap();
        let v: Option<String> = cli.get("foo").await.unwrap();

        // use the pool like any other RedisClient with the Deref trait
        assert_eq!(None, v);
        assert!(
            cli.set_ex::<&str, &str, bool>("foo", "bar", 30)
                .await
                .unwrap()
        );
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
        assert!(
            cli.set_ex::<&str, String, bool>("user1", serde_json::to_string(&user1).unwrap(), 30,)
                .await
                .unwrap()
        );
        // get and deserialize user1 string
        match cli.get::<&str, Option<String>>("user1").await {
            Ok(st) => {
                assert_eq!(
                    Some(user1),
                    serde_json::from_str(st.unwrap().as_str()).unwrap()
                )
            }
            _ => {
                panic!("expected value not found for struct User");
            }
        }
        // for end
    }

    #[tokio::test]
    async fn pool_test() -> Result<()> {
        #[derive(Clone)]
        struct TestRedisPool {
            pool: RedisPool,
        }
        impl UseRedisPool for TestRedisPool {
            fn redis_pool(&self) -> &RedisPool {
                &self.pool
            }
        }
        let config = REDIS_CONFIG.clone();
        let p = new_redis_pool(config).await.unwrap();
        let client = TestRedisPool { pool: p };
        let th_p = client.clone();
        // blocking (pop)
        let pop_jh = tokio::spawn(async move {
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
        // use the pool like any other RedisClient with the Deref trait
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
            assert!(
                client
                    .connection()
                    .await
                    .unwrap()
                    .rpush::<&str, i64, bool>("foobl", idx)
                    .await
                    .is_ok()
            );
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
        pop_jh.await?;
        // pop_jh.abort();
        Ok(())
    }

    // lock and unlock test with UseRedisLock
    #[tokio::test]
    async fn lock_unlock_test() -> Result<()> {
        use crate::infra::test::REDIS_CONFIG;
        #[derive(Clone)]
        struct TestRedisPool {
            pool: RedisPool,
        }
        impl UseRedisPool for TestRedisPool {
            fn redis_pool(&self) -> &RedisPool {
                &self.pool
            }
        }
        impl UseRedisLock for TestRedisPool {}

        let config = REDIS_CONFIG.clone();
        let p = new_redis_pool(config).await.unwrap();
        let client = TestRedisPool { pool: p };
        // Use unique key to avoid interference between test runs with different features
        let key = format!("lock_test_{}", std::process::id());
        // Clean up any stale lock from previous failed runs
        let _ = client.unlock(&key).await;

        client.lock(&key, 10).await?;
        // try lock
        assert!(client.lock(&key, 10).await.is_err());
        // unlock
        client.unlock(&key).await?;
        // try lock again
        assert!(client.lock(&key, 10).await.is_ok());
        // Clean up: unlock at the end to avoid affecting subsequent test runs
        client.unlock(&key).await?;
        Ok(())
    }

    #[tokio::test]
    async fn use_redis_client_pubsub_test() -> Result<()> {
        use crate::infra::redis::UseRedisClient;
        #[derive(Clone)]
        struct TestRedisPool {
            pool: RedisPool,
            client: RedisClient,
        }
        impl UseRedisPool for TestRedisPool {
            fn redis_pool(&self) -> &RedisPool {
                &self.pool
            }
        }
        impl UseRedisClient for TestRedisPool {
            fn redis_client(&self) -> &RedisClient {
                &self.client
            }
        }
        let config = REDIS_CONFIG.clone();
        let p = new_redis_pool(config.clone()).await?;
        let client = TestRedisPool {
            pool: p,
            client: RedisClient::open(config.url)?,
        };
        // Use unique channel name to avoid test interference
        let ch = format!("test_pubsub_{}", std::process::id());
        let mut sub = client.subscribe(&ch).await?;
        let mut conn = client
            .redis_client()
            .get_multiplexed_async_connection()
            .await?;
        assert_eq!(client.numsub(&mut conn, &ch).await?, (ch.to_string(), 1));
        let pb = client.publish(&ch, &vec![1, 2, 3]).await?;
        assert!(pb == 1);
        let msg = sub.on_message().next().await.unwrap();
        assert_eq!(msg.get_payload::<Vec<u8>>().unwrap(), vec![1, 2, 3]);
        // for end
        Ok(())
    }
    #[tokio::test]
    async fn use_redis_client_pubsub_test2() -> Result<()> {
        use crate::infra::redis::UseRedisClient;
        #[derive(Clone)]
        struct TestRedisPool {
            pool: RedisPool,
            client: RedisClient,
        }
        impl UseRedisPool for TestRedisPool {
            fn redis_pool(&self) -> &RedisPool {
                &self.pool
            }
        }
        impl UseRedisClient for TestRedisPool {
            fn redis_client(&self) -> &RedisClient {
                &self.client
            }
        }
        let config = REDIS_CONFIG.clone();
        let p = new_redis_pool(config.clone()).await?;
        let client = TestRedisPool {
            pool: p,
            client: RedisClient::open(config.url)?,
        };
        // Use unique channel names to avoid test interference
        let pid = std::process::id();
        let ch = format!("test_pubsub2_ch1_{}", pid);
        let ch2 = format!("test_pubsub2_ch2_{}", pid);
        let pb = client.publish(ch.as_str(), &vec![0, 1]).await?;
        assert!(pb == 0);
        let pb = client
            .publish_multi_if_listen(&[ch.clone(), ch2.clone()], &vec![0, 1])
            .await?;
        assert!(!pb);
        let mut sub = client.subscribe(ch.as_str()).await?;
        let mut sub2 = client.subscribe(ch2.as_str()).await?;

        let mut conn = client
            .redis_client()
            .get_multiplexed_async_connection()
            .await?;
        assert_eq!(
            client.numsub(&mut conn, ch.as_str()).await?,
            (ch.to_string(), 1)
        );
        let pb = client
            .publish_multi_if_listen(&[ch, ch2], &vec![1, 2, 3])
            .await?;
        assert!(pb);
        let msg = sub.on_message().next().await.unwrap();
        assert_eq!(msg.get_payload::<Vec<u8>>().unwrap(), vec![1, 2, 3]);

        let msg2 = sub2.on_message().next().await.unwrap();
        assert_eq!(msg2.get_payload::<Vec<u8>>().unwrap(), vec![1, 2, 3]);

        // for end
        Ok(())
    }

    #[test]
    fn blocked_client_ids_selects_blocked_clients_with_the_name() {
        let list = "id=3 addr=127.0.0.1:50001 fd=8 name=pool-a age=1 idle=0 flags=b db=0 cmd=blpop\n\
                    id=4 addr=127.0.0.1:50002 fd=9 name=pool-a age=1 idle=0 flags=N db=0 cmd=ping\n\
                    id=5 addr=127.0.0.1:50003 fd=10 name=pool-b age=1 idle=0 flags=b db=0 cmd=blpop\n\
                    id=6 addr=127.0.0.1:50004 fd=11 name= age=1 idle=0 flags=b db=0 cmd=blpop\n\
                    id=7 addr=127.0.0.1:50005 fd=12 name=pool-a age=1 idle=0 flags=b db=0 cmd=brpop\n";
        assert_eq!(super::blocked_client_ids(list, "pool-a"), vec![3, 7]);
        assert!(super::blocked_client_ids(list, "pool-c").is_empty());
        assert!(super::blocked_client_ids("", "pool-a").is_empty());
        // A name that only prefixes another name does not match.
        assert!(super::blocked_client_ids(list, "pool").is_empty());
    }

    #[tokio::test]
    async fn kill_blocked_clients_closes_only_named_blocked_connections() -> Result<()> {
        use super::{kill_blocked_clients, new_named_redis_pool, new_redis_client};
        use std::time::Duration;

        let name = format!("infra-utils-kill-test-{}", std::process::id());
        let mut config = REDIS_CONFIG.clone();
        config.blocking = true;
        config.pool_size = 2;
        let pool = new_named_redis_pool(config, &name).await?;
        let client = new_redis_client(REDIS_CONFIG.clone())?;

        // A named pooled connection blocked in BLPOP, as a stopped worker leaves it.
        let named_queue = format!("{name}:named");
        let blocked_pool = pool.clone();
        let blocked_queue = named_queue.clone();
        let blocked = tokio::spawn(async move {
            let mut connection = blocked_pool.get().await?;
            let reply: redis::RedisResult<Vec<Vec<u8>>> =
                connection.blpop(&blocked_queue, 0.0).await;
            anyhow::Ok(reply)
        });
        // An unnamed connection blocked in BLPOP belongs to someone else.
        let other_queue = format!("{name}:other");
        let mut other = client
            .get_multiplexed_async_connection_with_config(
                &redis::AsyncConnectionConfig::new().set_response_timeout(None),
            )
            .await?;
        let other_wait = {
            let other_queue = other_queue.clone();
            tokio::spawn(async move {
                let reply: redis::RedisResult<Vec<Vec<u8>>> = other.blpop(&other_queue, 0.0).await;
                reply
            })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;

        assert_eq!(kill_blocked_clients(&client, &name).await?, 1);
        let reply = tokio::time::timeout(Duration::from_secs(5), blocked).await???;
        assert!(reply.is_err(), "the killed BLPOP must end with an error");

        // The other client's BLPOP is still waiting; release it by pushing.
        assert!(!other_wait.is_finished());
        let mut pusher = client.get_multiplexed_async_connection().await?;
        let _: () = pusher.rpush(&other_queue, b"done".to_vec()).await?;
        let reply = tokio::time::timeout(Duration::from_secs(5), other_wait).await??;
        assert_eq!(reply?.len(), 2);

        // The pool replaces the killed connection when it is checked out again.
        for _ in 0..2 {
            let mut connection = tokio::time::timeout(Duration::from_secs(5), pool.get()).await??;
            let pong: String = redis::cmd("PING").query_async(&mut *connection).await?;
            assert_eq!(pong, "PONG");
        }
        let _: () = pusher.del(&[&named_queue, &other_queue]).await?;
        Ok(())
    }
}
