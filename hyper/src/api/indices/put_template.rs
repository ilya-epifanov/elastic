//! http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-templates.html

//Autogenerated

use hyper::client::Client;
use hyper::header::{Headers, ContentType};
use hyper::client::response::Response;
use hyper::error::Result;

pub fn post_name<'a>(client: &'a mut Client, base: String, name: String,
                 body: String) -> Result<Response>{
    let mut url_fmtd = String::with_capacity(base.len() + 11 + name.len());
    url_fmtd.push_str(&base);
    url_fmtd.push_str("/_template/");
    url_fmtd.push_str(&name);
    let mut headers = Headers::new();
    headers.set(ContentType::json());
    let res = client.post(&url_fmtd).headers(headers).body(&body);
    res.send()
}
pub fn put_name<'a>(client: &'a mut Client, base: String, name: String,
                body: String) -> Result<Response>{
    let mut url_fmtd = String::with_capacity(base.len() + 11 + name.len());
    url_fmtd.push_str(&base);
    url_fmtd.push_str("/_template/");
    url_fmtd.push_str(&name);
    let mut headers = Headers::new();
    headers.set(ContentType::json());
    let res = client.put(&url_fmtd).headers(headers).body(&body);
    res.send()
}