use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, FixedOffset};
use command_utils::util::datetime;
use command_utils::util::option::FlatMap;
use command_utils::util::result::Flatten;
use deadpool::managed::{
    Manager, Object, Pool, PoolConfig, PoolError, RecycleError, RecycleResult, Timeouts,
};
use deadpool::Runtime;
use deadpool_redis::Metrics;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::DurationSeconds;
use std::borrow::Cow;
use std::time::Duration;
use strum::{self, IntoEnumIterator};
use strum_macros::{self, EnumIter};
use thirtyfour::prelude::*;
use thirtyfour::ChromeCapabilities;

#[serde_as]
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct WebDriverConfig {
    pub url: Option<String>,
    pub user_agent: Option<String>,
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub page_load_timeout_sec: Option<Duration>,
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub script_timeout_sec: Option<Duration>,
    pub pool_max_size: usize,
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub pool_timeout_wait_sec: Option<Duration>,
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub pool_timeout_create_sec: Option<Duration>,
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub pool_timeout_recycle_sec: Option<Duration>,
}

pub trait UsePoolConfig {
    fn pool_config(&self) -> PoolConfig;
}

impl UsePoolConfig for WebDriverConfig {
    fn pool_config(&self) -> PoolConfig {
        PoolConfig {
            max_size: self.pool_max_size,
            timeouts: Timeouts {
                wait: self.pool_timeout_wait_sec,
                create: self.pool_timeout_create_sec,
                recycle: self.pool_timeout_recycle_sec,
            },
            queue_mode: deadpool::managed::QueueMode::Fifo,
        }
    }
}
#[derive(Debug, Clone)]
pub struct ChromeDriverFactory {
    pub config: WebDriverConfig,
    pub capabilities: ChromeCapabilities,
}

pub trait UseWebDriver {
    fn driver(&self) -> &WebDriver;
}
pub struct WebDriverWrapper {
    driver: WebDriver,
}

impl UseWebDriver for WebDriverWrapper {
    fn driver(&self) -> &WebDriver {
        &self.driver
    }
}

impl ChromeDriverFactory {
    // default value (for test)
    // mac chrome
    const USER_AGENT: &'static str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36";
    const TIME_OUT: Duration = Duration::from_secs(5);
    const URL: &'static str = "http://localhost:9515";

    fn build_chrome_capabilities(
        user_agent: impl Into<Option<String>> + Send,
    ) -> Result<ChromeCapabilities, Box<WebDriverError>> {
        let mut caps = DesiredCapabilities::chrome();
        // https://stackoverflow.com/a/52340526
        // caps.add_extension(Path::new("./adblock.crx"))?;
        caps.add_arg("start-maximized")?; // open Browser in maximized mode
        caps.add_arg("enable-automation")?; // https://stackoverflow.com/a/43840128/1689770
        caps.set_headless()?;
        caps.add_arg("--no-sandbox")?; // Bypass OS security model // necessary in docker env
        caps.add_arg("--disable-dev-shm-usage")?; // if tab crash error occurred (add shm mem to container or this option turned on)
        caps.add_arg("--disable-browser-side-navigation")?; //https://stackoverflow.com/a/49123152/1689770"
        caps.add_arg("--disable-gpu")?;
        // caps.set_disable_local_storage()?;
        // https://stackoverflow.com/questions/48450594/selenium-timed-out-receiving-message-from-renderer
        // https://stackoverflow.com/questions/51959986/how-to-solve-selenium-chromedriver-timed-out-receiving-message-from-renderer-exc
        caps.add_arg("--disable-infobars")?; // disabling infobars
                                             // caps.add_arg("--disable-extensions")?; // disabling extensions
                                             //        caps.add_arg("--disk-cache=false")?;
                                             //        caps.add_arg("--load-images=false")?;
                                             //        caps.add_arg("--dns-prefetch-disable")?;

        caps.add_arg(
            format!(
                "--user-agent={}",
                user_agent
                    .into()
                    .unwrap_or_else(|| Self::USER_AGENT.to_string())
            )
            .as_str(),
        )?;
        caps.add_arg("--lang=ja-JP")?;
        Ok(caps)
    }

    pub async fn new(
        config: WebDriverConfig,
        // server_url: impl Into<String> + Sync,
        // page_load_timeout: Duration,
        // script_timeout: Duration,
        // user_agent: impl Into<String> + Sync,
    ) -> Result<Self, Box<WebDriverError>> {
        tracing::info!("setup chrome driver: {:?}", config);
        let caps = Self::build_chrome_capabilities(config.user_agent.clone())?;

        Ok(Self {
            config,
            capabilities: caps,
        })
    }

    pub async fn create(&self) -> Result<WebDriverWrapper, WebDriverError> {
        let driver = WebDriver::new(
            self.config.url.as_deref().unwrap_or(Self::URL),
            self.capabilities.clone(),
        )
        .await?;
        driver
            .set_page_load_timeout(self.config.page_load_timeout_sec.unwrap_or(Self::TIME_OUT))
            .await?;
        driver
            .set_implicit_wait_timeout(self.config.page_load_timeout_sec.unwrap_or(Self::TIME_OUT))
            .await?;
        driver
            .set_script_timeout(self.config.script_timeout_sec.unwrap_or(Self::TIME_OUT))
            .await?;
        Ok(WebDriverWrapper { driver })
    }
}

#[derive(Debug)]
pub struct WebDriverManagerImpl {
    web_driver_factory: ChromeDriverFactory,
    // connection_counter: AtomicUsize,
}

impl WebDriverManagerImpl {
    pub async fn new(config: WebDriverConfig) -> Result<Self, Box<WebDriverError>> {
        let driver = ChromeDriverFactory::new(config).await?;
        Ok(Self {
            web_driver_factory: driver,
            // connection_counter: AtomicUsize::new(0),
        })
    }
}

impl Manager for WebDriverManagerImpl {
    type Type = WebDriverWrapper;
    type Error = WebDriverError;

    async fn create(&self) -> Result<WebDriverWrapper, WebDriverError> {
        // let current = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let driver = self.web_driver_factory.create().await?;
        Ok(driver)
    }

    async fn recycle(
        &self,
        wrap: &mut WebDriverWrapper,
        _metrics: &Metrics,
    ) -> RecycleResult<WebDriverError> {
        // 一度ページをロードした場合のみ有効 (作成直後で放置した場合はこれでは死活(quitしてないかどうか)判断できない)
        //https://stackoverflow.com/a/46532764
        match wrap.driver.title().await {
            Ok(_title) => {
                // if session.is_empty() {
                //     //セッションがない(detachがasyncじゃないのでここで念のためquit()しておく)
                //     let quit = wrap.quit().await;
                //     Err(RecycleError::Message(format!(
                //         "session is empty, quit:{:?}",
                //         quit
                //     )))
                // } else {
                Ok(())
                // }
            }
            Err(e) => {
                // detachがasyncじゃないのでここで念のためquit()しておく
                let quit = wrap.quit().await;
                Err(RecycleError::Message(Cow::Owned(format!(
                    "error in getting session: {:?}, quit: {:?}",
                    e, quit
                ))))
            }
        }
    }

    fn detach(&self, obj: &mut WebDriverWrapper) {
        tracing::info!("webdriver detached");
        let driver = obj.driver.clone();
        tokio::spawn(async move {
            let _ = driver.quit().await;
        });
    }
}
pub trait UseWebDriverPool {
    fn web_driver_pool(&self) -> &WebDriverPool;
}

// pub type WebDriverPool = Pool<WebDriverManagerImpl, Object<WebDriverManagerImpl>>;
pub type WebDriverPoolError = PoolError<WebDriverError>;

pub struct WebDriverPool {
    pub pool: Pool<WebDriverManagerImpl, Object<WebDriverManagerImpl>>,
    pub max_size: usize,
    pub user_agent: Option<String>,
}
impl WebDriverPool {
    pub async fn new(wd_config: WebDriverConfig) -> Self {
        let manager = WebDriverManagerImpl::new(wd_config.clone()).await.unwrap();
        WebDriverPool {
            pool: Pool::builder(manager)
                .config(wd_config.pool_config())
                .runtime(Runtime::Tokio1)
                .build()
                .unwrap(),
            max_size: wd_config.pool_max_size,
            user_agent: wd_config.user_agent,
        }
    }
    pub async fn get(&self) -> Result<Object<WebDriverManagerImpl>, WebDriverPoolError> {
        self.pool.get().await
    }
}

#[async_trait]
pub trait WebScraper: UseWebDriver + Send + Sync {
    const MAX_PAGE_NUM: i32 = 50;
    // catch scraping error and capture screenshot
    // TODO Unsafe境界のエラーで入れられていない...
    // async fn try_scraping<T, F>(&self, process: F) -> Result<T, WebDriverError>
    // where
    //     Self: Sized + UnwindSafe,
    //     T: Send,
    //     F: Future<Output = Result<T, WebDriverError>> + Send + UnwindSafe,
    // {
    //     match process.catch_unwind().await {
    //         Ok(r) => Ok(r),
    //         Err(e) => {
    //             self.driver()
    //                 .screenshot(Path::new("./error_browser_screen.png"))
    //                 .await?;
    //             tracing::error!("caught panic: {:?}", e);
    //             Err(anyhow!("error in parsing datetime: {:?}", e))
    //         }
    //     }
    //     .flatten()
    // }
    #[allow(clippy::too_many_arguments)]
    async fn scraping(
        &self,
        url: impl Into<String> + Send,
        title_selector: By,
        content_selector: By,
        datetime_selector: Option<By>,
        datetime_attribute: &Option<String>,
        datetime_regex: &Option<String>,
        next_page_selector: Option<By>,
        tags_selector: Option<By>,
    ) -> Result<ScrapedData, WebDriverError> {
        let u: String = url.into();
        // ignore error for scraping (ignore not respond resource)
        if let Err(err) = self.driver().goto(&u).await {
            tracing::warn!("loading error: {:?}", err)
        }
        // self.driver().screenshot(path::Path::new("./browser_screen.png")).await?;
        //  impressはsourceを取るとscraping可能になったので念のためやっておく...
        let source = self.driver().source().await?;
        tracing::trace!("page source:{:?}", source);

        // wait title element
        let elem = self.driver().query(title_selector.clone()).first().await?;
        elem.wait_until().displayed().await?;

        let title_ele = self.driver().find(title_selector).await.map_err(|e| {
            tracing::warn!("error in scraping title: {:?}", &e);
            WebDriverError::ParseError(format!(
                "error in scraping page title: {:?}, err: {:?}",
                &u, e
            ))
        })?;
        let title = title_ele.text().await?;
        tracing::info!("title: {} ", &title);

        // parse contents
        let contents = self
            .scrape_content(&content_selector, next_page_selector.as_ref())
            .await
            .inspect_err(|e| tracing::warn!("error in scraping contents: {:?}", &e))?;
        if contents.is_empty() {
            Err(WebDriverError::ParseError(format!(
                "content not found: {}",
                u
            )))? // bail out
        }

        // parse datetime ()
        let datetime = if let Some(sel) = datetime_selector {
            self.parse_datetime(sel, datetime_attribute, datetime_regex)
                .await
                .map_err(|e| {
                    tracing::warn!("error in parse_datetime: {:?}", &e);
                    WebDriverError::ParseError(format!("parse_datetime error: {:?}", e))
                })?
        } else {
            None
        };
        tracing::info!("datetime res: {:?}", &datetime);

        let tags = if let Some(tsel) = tags_selector {
            let tag_elems = self
                .driver()
                .find_all(tsel)
                .await
                .inspect_err(|e| tracing::warn!("error in scraping tags: {:?}", &e))?;

            let mut tags = Vec::new();
            for elem in tag_elems.iter() {
                elem.wait_until().displayed().await?;
                let tag = elem.text().await?;
                tags.push(tag);
            }
            tags
        } else {
            vec![]
        };
        tracing::info!("content: len={} ", &contents.iter().len());
        Ok(ScrapedData {
            title,
            content: contents,
            datetime,
            tags,
        })
        // match process.catch_unwind().await {
        //     Ok(r) => Ok(r),
        //     Err(e) => {
        //         self.driver()
        //             .screenshot(Path::new("./error_browser_screen.png"))
        //             .await?;
        //         tracing::error!("caught panic: {:?}", e);
        //         Err(anyhow!("error in parsing datetime: {:?}", e))
        //     }
        // }
        // .flatten()
    }

    #[allow(unstable_name_collisions)] // for flatten()
    async fn parse_datetime(
        &self,
        datetime_selector: By,
        datetime_attribute: &Option<String>,
        datetime_regex: &Option<String>,
    ) -> Result<Option<DateTime<FixedOffset>>, anyhow::Error> {
        let datetime_element = self
            .driver()
            .find(datetime_selector)
            .await
            .inspect_err(|e| tracing::warn!("error in scraping datetime: {:?}", &e))?;
        let dt_value = match datetime_attribute {
            Some(dta) => datetime_element.attr(dta).await?.unwrap_or_default(),
            None => datetime_element.text().await?,
        };
        tracing::info!("scraped datetime: {:?}", &dt_value);
        std::panic::catch_unwind(move || {
            let dt = match datetime_regex {
                Some(fmt) if !fmt.is_empty() => {
                    let now = datetime::now();
                    Regex::new(fmt.as_str())
                        .map(|dt_re| {
                            dt_re.captures(dt_value.as_str()).flat_map(|c| {
                                // XXX 年月日、時分秒の順でマッチする前提
                                datetime::ymdhms(
                                    c.get(1).map_or(now.year(), |r| r.as_str().parse().unwrap()),
                                    c.get(2).map_or(0u32, |r| r.as_str().parse().unwrap()),
                                    c.get(3).map_or(0u32, |r| r.as_str().parse().unwrap()),
                                    c.get(4).map_or(0u32, |r| r.as_str().parse().unwrap()),
                                    c.get(5).map_or(0u32, |r| r.as_str().parse().unwrap()),
                                    c.get(6).map_or(0u32, |r| r.as_str().parse().unwrap()),
                                )
                            })
                        })
                        .context("on parse by datetime regex")
                }
                _ => DateTime::parse_from_rfc3339(dt_value.as_str())
                    .or_else(|_| DateTime::parse_from_str(dt_value.as_str(), "%+"))
                    .map(Some)
                    .context("on parse rf3339"),
            };
            dt
        })
        .map_err(|e| {
            tracing::error!("caught panic: {:?}", e);
            anyhow!("error in parsing datetime: {:?}", e)
        })
        .flatten()
    }

    async fn close(&self) -> Result<(), WebDriverError> {
        self.driver().clone().close_window().await
    }

    async fn quit(&self) -> Result<(), WebDriverError> {
        self.driver().clone().quit().await
    }

    async fn scrape_content(
        &self,
        content_selector: &By,
        next_page_selector: Option<&By>,
    ) -> Result<Vec<String>, WebDriverError> {
        // parse contents
        let content = self.driver().find(content_selector.clone()).await?;
        if let Ok(con) = content.text().await {
            if let Some(next) = next_page_selector {
                // ページ遷移して次のコンテンツを取得する
                let r = self
                    .goto_next_content(next, content_selector, 1)
                    .await
                    .unwrap_or_else(|err| {
                        tracing::warn!("next page content not parse: {}", err);
                        vec![]
                    });
                Ok([vec![con], r].concat())
            } else {
                Ok(vec![con])
            }
        } else {
            Ok(vec![])
        }
    }

    async fn goto_next_content(
        &self,
        next: &By,
        content_selector: &By,
        page_num: i32,
    ) -> Result<Vec<String>, WebDriverError> {
        let next_ele = self.driver().find(next.clone()).await?;
        // XXX 次のページへのリンクはhrefで取得できる前提がある
        if let Some(next_url) = next_ele.attr("href").await? {
            tracing::info!("found next content: {}", &next_url);
            // ignore error for scraping (ignore not respond resource)
            if let Err(err) = self.driver().goto(next_url).await {
                tracing::warn!("loading error: {:?}", err)
            }
            let content = self.driver().find(content_selector.clone()).await?;
            let con = content.text().await?;
            // ページで空しか取れない場合は何かがおかしい可能性があり無限ループになる可能性があるため無理しないで抜ける
            if con.is_empty() {
                tracing::info!("empty page?");
                Ok(vec![])
            } else if page_num <= Self::MAX_PAGE_NUM {
                // recursive call (next page while next href exists)
                let ncon = self
                    .goto_next_content(next, content_selector, page_num + 1)
                    .await;
                if let Ok(nc) = ncon {
                    Ok([vec![con], nc].concat())
                } else {
                    Ok(vec![con])
                }
            } else {
                tracing::warn!("over max page: {}", Self::MAX_PAGE_NUM);
                Ok(vec![con])
            }
        } else {
            tracing::info!("page end");
            Ok(vec![])
        }
    }
}

impl WebScraper for WebDriverWrapper {}

#[derive(Debug, Deserialize, Serialize)]
pub struct ScrapedData {
    pub title: String,
    pub content: Vec<String>,
    pub datetime: Option<DateTime<FixedOffset>>,
    pub tags: Vec<String>,
}

// Byのwrapper
#[derive(Debug, PartialEq, Eq, EnumIter)]
pub enum SelectorType {
    ById = 1,
    ByClassName = 2,
    ByCSS = 3,
    ByXPath = 4,
    ByName = 5,
    ByTag = 6,
    ByLinkText = 7,
}

impl SelectorType {
    // n: 0初まり、nth(): 0はじまり
    pub fn new(n: i32) -> Option<SelectorType> {
        if n == 0 {
            None
        } else {
            SelectorType::iter().nth((n - 1) as usize)
        }
    }
}

impl SelectorType {
    pub fn to_by(&self, s: &str) -> By {
        match *self {
            Self::ById => By::Id(s),
            Self::ByClassName => By::ClassName(s),
            Self::ByCSS => By::Css(s),
            Self::ByXPath => By::XPath(s),
            Self::ByName => By::Name(s),
            Self::ByTag => By::Tag(s),
            Self::ByLinkText => By::LinkText(s),
        }
    }
}
