pub mod broadcast;
pub mod mpmc;

use super::memory::{MemoryCacheConfig, MemoryCacheImpl, UseMemoryCache};
use anyhow::{anyhow, Result};
use debug_stub_derive::DebugStub;
use std::{collections::HashSet, marker::PhantomData, sync::Arc, time::Duration};
use tokio::sync::Mutex;

pub trait ChanTrait<T: Send + Sync + Clone>: Send + Sync + std::fmt::Debug {
    fn new(buf_size: Option<usize>) -> Self;
    /// send data to channel
    /// # Arguments
    /// * `data` - data to send
    fn send_to_chan(&self, data: T) -> impl std::future::Future<Output = Result<bool>> + Send;
    /// receive data from channel
    /// # Arguments
    /// * `recv_timeout` - timeout for receive
    fn receive_from_chan(
        &self,
        recv_timeout: Option<Duration>,
    ) -> impl std::future::Future<Output = Result<T>> + Send;

    fn try_receive_from_chan(&self) -> impl std::future::Future<Output = Result<T>> + Send;

    fn key_set(&self) -> Arc<Mutex<HashSet<String>>>; // prevent duplicate key
    fn count(&self) -> usize;
}

pub type ChanBufferItem<T> = (Option<String>, T);

#[derive(Clone, DebugStub)]
pub struct ChanBuffer<T: Send + Sync + Clone, C: ChanTrait<ChanBufferItem<T>> + 'static> {
    buf_size: Option<usize>,
    chan_buf: MemoryCacheImpl<String, Arc<C>>,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + Clone, C: ChanTrait<ChanBufferItem<T>>> ChanBuffer<T, C> {
    pub fn new(buf_size: Option<usize>, max_channels: usize) -> Self {
        Self {
            buf_size,
            chan_buf: super::memory::MemoryCacheImpl::new(
                &MemoryCacheConfig {
                    num_counters: max_channels,
                    max_cost: max_channels as i64,
                    use_metrics: false,
                },
                None,
            ),
            _phantom: PhantomData,
        }
    }

    pub async fn get_chan_if_exists(&self, name: impl Into<String>) -> Option<Arc<C>> {
        self.chan_buf.find_cache_locked(&name.into()).await
    }

    async fn get_or_create_chan(
        &self,
        name: impl Into<String>,
        ttl: Option<&Duration>,
    ) -> Result<Arc<C>> {
        let k = name.into();
        self.chan_buf
            .with_cache_locked(&k, ttl, || async {
                tracing::debug!("create new channel: {}", &k);
                let ch = Arc::new(C::new(self.buf_size));
                Ok(ch)
            })
            .await
    }

    pub async fn clear_chan_all(&self) -> Result<()> {
        self.chan_buf.clear().await
    }
    pub async fn send_to_chan(
        &self,
        name: impl Into<String> + Send,
        data: T,
        uniq_key: Option<String>,
        ttl: Option<&Duration>,
        only_if_exists: bool,
    ) -> Result<bool> {
        let nm = name.into();
        if let Some(ukey) = &uniq_key {
            let chan = self.get_or_create_chan(nm.clone(), ttl).await?;
            let key_set = chan.key_set();
            let mut ksl = key_set.lock().await;
            if !ksl.insert(ukey.clone()) {
                tracing::warn!("duplicate key: {}", ukey);
                return Err(anyhow!("duplicate uniq_key: {}", ukey));
            }
        }
        if only_if_exists {
            self.get_chan_if_exists(&nm)
                .await
                .ok_or(anyhow!("channel not found: {}", &nm))?
                .send_to_chan((uniq_key, data))
                .await
        } else {
            self.get_or_create_chan(nm, ttl)
                .await?
                .send_to_chan((uniq_key, data))
                .await
        }
    }
    /// receive data from channel
    /// # Arguments
    /// * `name` - channel name
    /// * `recv_timeout` - timeout for receive
    /// * `ttl` - ttl for channel (cannot change ttl of each named channel after created)
    pub async fn receive_from_chan(
        &self,
        name: impl Into<String> + Send,
        recv_timeout: Option<Duration>,
        ttl: Option<&Duration>,
    ) -> Result<T> {
        let nm = name.into();
        let chan = self.get_or_create_chan(nm.clone(), ttl).await?;
        let (res, key_set) = if let Some(dur) = recv_timeout {
            let key_set = chan.key_set();
            // XXX unique key may not be removed
            (
                tokio::time::timeout(dur, chan.receive_from_chan(recv_timeout))
                    .await
                    .map_err(|e| {
                        anyhow!("chan recv timeout error: timeout={:?}, err={:?}", dur, e)
                    })?
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        } else {
            let key_set = chan.key_set();
            (
                chan.receive_from_chan(None)
                    .await
                    .map_err(|e| anyhow!("chan recv error: {:?}", e))?,
                key_set,
            )
        };
        if let Some(ukey) = res.0 {
            let mut ksl = key_set.lock().await;
            ksl.remove(&ukey);
        }
        Ok(res.1)
    }

    pub async fn try_receive_from_chan(
        &self,
        name: impl Into<String> + Send,
        ttl: Option<&Duration>,
    ) -> Result<T> {
        let nm = name.into();
        let res: (Option<String>, T) = self
            .get_or_create_chan(nm.clone(), ttl)
            .await?
            .try_receive_from_chan()
            .await?;
        if let Some(ukey) = res.0 {
            let chan = self.get_or_create_chan(nm, ttl).await?;
            let key_set = chan.key_set();
            let mut ksl = key_set.lock().await;
            ksl.remove(&ukey);
        }
        Ok(res.1)
    }
    pub async fn count_chan_opt(&self, name: impl Into<String>) -> Option<usize> {
        self.get_chan_if_exists(name).await.map(|ch| ch.count())
    }
}

// // create test for UseChanPool
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use tokio::time::{sleep, Duration};

//     #[derive(Clone)]
//     struct Test {
//         pub chan_pool: ChanBuffer,
//     }

//     impl UseChanBuffer for Test {
//         fn chan_buf(&self) -> &ChanBuffer {
//             &self.chan_pool
//         }
//     }

//     #[tokio::test]
//     async fn test_use_chan_pool() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let key = "test_key";
//         let data = b"test".to_vec();
//         assert_eq!(test.count_chan(key).await, 0);
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         assert_eq!(test.count_chan(key).await, 1);
//         let recv_data = test.receive_from_chan(key, None, None).await.unwrap();
//         assert_eq!(data, recv_data);
//         assert_eq!(test.count_chan(key).await, 0);
//     }

//     #[tokio::test]
//     async fn test_duplicate_key() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let key = "test";
//         let data = b"test".to_vec();
//         test.send_to_chan(key, data.clone(), Some("test".to_string()), None)
//             .await
//             .unwrap();
//         let res = test
//             .send_to_chan(key, data.clone(), Some("test".to_string()), None)
//             .await;
//         assert!(res.is_err());
//     }

//     #[tokio::test]
//     async fn test_chan_pool_multi_thread() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let test_clone = test.clone();
//         let key = "test_key";
//         let data = b"test".to_vec();
//         let key_clone = key.to_string();
//         let data_clone = data.clone();
//         let handle = tokio::spawn(async move {
//             let recv_data = test_clone
//                 .receive_from_chan(&key_clone, None, None)
//                 .await
//                 .unwrap();
//             assert_eq!(data_clone, recv_data);
//         });
//         sleep(Duration::from_secs(1)).await;
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         handle.await.unwrap();
//     }
//     #[tokio::test]
//     async fn test_chan_pool_recv_timeout() {
//         let test = Test {
//             chan_pool: ChanBuffer::new(None, 10000),
//         };
//         let test_clone = test.clone();
//         let key = "test_key";
//         let data = b"test".to_vec();
//         let handle = tokio::spawn(async move {
//             let res = test_clone
//                 .receive_from_chan(key, Some(Duration::from_secs(1)), None)
//                 .await;
//             assert!(res.is_err());
//         });
//         sleep(Duration::from_secs(2)).await;
//         test.send_to_chan(key, data.clone(), None, None)
//             .await
//             .unwrap();
//         handle.await.unwrap();
//     }
// }
