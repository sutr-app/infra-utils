use anyhow::{anyhow, Result};
use command_utils::util::encoding;
use readability::extractor::Product;
use reqwest::{self, StatusCode};
use robotstxt::DefaultMatcher;
use std::{borrow::BorrowMut, io::Cursor, time::Duration};
use url::Url;

use super::{
    reqwest::ReqwestClient,
    webdriver::{UseWebDriver, WebDriverWrapper},
};

fn robots_txt_url(url_str: &str) -> Result<Url> {
    let mut url = Url::parse(url_str)?;
    url.set_path("/robots.txt");
    url.set_fragment(None);
    url.set_query(None);
    Ok(url)
}

pub async fn get_robots_txt(
    url_str: &str,
    user_agent: Option<&String>,
    timeout: Option<Duration>,
) -> Result<Option<String>> {
    let robots_url = robots_txt_url(url_str)?;
    let client = reqwest::Client::builder()
        .timeout(timeout.unwrap_or_else(|| Duration::new(30, 0)))
        .connect_timeout(timeout.unwrap_or_else(|| Duration::new(30, 0)));
    let client = if let Some(ua) = user_agent {
        client.user_agent(ua).build()?
    } else {
        client.build()?
    };

    let res = client.get(robots_url.as_str()).send().await?;
    if res.status().is_success() {
        let txt = res
            .text()
            .await
            .map_err(|e| anyhow!("content error: {:?}", e))?;
        Ok(Some(txt))
    } else if res.status() == StatusCode::NOT_FOUND {
        Ok(None)
    } else {
        Err(anyhow!("robot_txt request not success: {:?}", res))
    }
}

// TODO make struct, with caching
fn available_url_by_robots_txt(robots_txt: &str, url: &str, user_agent: &str) -> bool {
    let mut matcher = DefaultMatcher::default();
    matcher.one_agent_allowed_by_robots(robots_txt, user_agent, url)
}

pub async fn readable_by_robots_txt(
    url_str: &str,
    user_agent: Option<&String>,
) -> Result<Option<bool>> {
    let url = Url::parse(url_str)?;
    let robots_txt = get_robots_txt(url_str, user_agent, None).await?;
    Ok(robots_txt.map(|t| {
        available_url_by_robots_txt(
            t.as_str(),
            url.as_str(),
            user_agent.unwrap_or(&"robotstxt".to_string()),
        )
    }))
}

pub async fn request_to_utf8(
    url: &str,
    user_agent: Option<&String>,
    check_robotstxt: bool,
) -> Result<Product> {
    if check_robotstxt {
        let robotstxt = readable_by_robots_txt(url, user_agent).await?;
        if !robotstxt.unwrap_or(true) {
            Err(anyhow!("denied by robots.txt"))?
        }
    }
    let client = ReqwestClient::new(user_agent, Some(Duration::new(30, 0)), Some(2))?;
    let res = client.client().get(url).send().await?;
    if res.status().is_success() {
        let url = Url::parse(url)?;
        // add to encode to utf8 (all in buffer)
        let mut res = encoding::encode_to_utf8_raw(res.bytes().await?.borrow_mut())?;
        let mut c = unsafe { Cursor::new(res.as_bytes_mut()) };
        readability::extractor::extract(&mut c, &url)
            .map_err(|e| anyhow!("readable extract error: {:?}", e))
    } else {
        Err(anyhow!("request not success: {:?}", res))
    }
}

pub async fn request_by_webdriver(
    webdriver: &WebDriverWrapper,
    url: &str,
    user_agent: Option<&String>,
    check_robotstxt: bool,
) -> Result<Product> {
    if check_robotstxt {
        let robotstxt = readable_by_robots_txt(url, user_agent).await?;
        if !robotstxt.unwrap_or(true) {
            Err(anyhow!("denied by robots.txt"))?
        }
    }
    if let Err(err) = webdriver.driver().goto(url).await {
        tracing::warn!("loading error: {:?}", err)
    }
    let status = webdriver.driver().status().await?;
    // self.driver().screenshot(path::Path::new("./browser_screen.png")).await?;
    let source = webdriver.driver().source().await?;
    tracing::trace!("page source:{:?}", source);
    if status.ready {
        let url = Url::parse(url)?;
        // add to encode to utf8 (all in buffer)
        let mut res = encoding::encode_to_utf8_raw(source.as_bytes())?;
        let mut c = unsafe { Cursor::new(res.as_bytes_mut()) };
        readability::extractor::extract(&mut c, &url)
            .map_err(|e| anyhow!("readable extract error: {:?}", e))
    } else {
        Err(anyhow!("request not success: {:?}", status.message))
    }
}
