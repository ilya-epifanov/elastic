//! http://www.elastic.co/guide/en/elasticsearch/reference/master/cluster-reroute.html

//Autogenerated

use hyper::client::Client;
use hyper::header::{Headers, ContentType};
use hyper::client::response::Response;
use hyper::error::Result;

use RequestParams;

pub fn post<'a>(client: &'a mut Client, req: RequestParams, body: &'a str)
 -> Result<Response>{
    let url_qry = &req.get_url_qry();
    let mut url_fmtd =
        String::with_capacity(req.base_url.len() + 17 + url_qry.len());
    url_fmtd.push_str(req.base_url);
    url_fmtd.push_str("/_cluster/reroute");
    url_fmtd.push_str(url_qry);
    let mut headers = Headers::new();
    headers.set(ContentType::json());
    let res = client.post(&url_fmtd).headers(headers).body(body);
    res.send()
}
