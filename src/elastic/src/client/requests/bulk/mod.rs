#![allow(missing_docs)]

use std::time::Duration;
use std::marker::PhantomData;

use futures::{Future, Poll};
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use error::{self, Error};
use client::{AsyncSender, Client, Sender, SyncSender};
use client::requests::{RequestBuilder, SyncBody, AsyncBody};
use client::requests::params::{Index, Type};
use client::requests::endpoints::BulkRequest;
use client::requests::raw::RawRequestInner;
use client::responses::{BulkResponse, BulkErrorsResponse};
use client::responses::parse::IsOk;

pub type BulkRequestBuilder<TSender, TBody, TResponse> = RequestBuilder<TSender, BulkRequestInner<TBody, TResponse>>;

mod stream;
mod operation;

pub use self::stream::*;
pub use self::operation::*;

#[doc(hidden)]
pub struct BulkRequestInner<TBody, TResponse> {
    index: Option<Index<'static>>,
    ty: Option<Type<'static>>,
    body: TBody,
    _marker: PhantomData<TResponse>,
}

impl<TSender> Client<TSender>
where
    TSender: Sender,
{
    pub fn bulk(&self) -> BulkRequestBuilder<TSender, Vec<u8>, BulkResponse> {
        RequestBuilder::new(
            self.clone(),
            None,
            BulkRequestInner {
                index: None,
                ty: None,
                body: Vec::new(),
                _marker: PhantomData,
            },
        )
    }
}

pub struct Streamed<TDocument> {
    chunk_size: usize,
    timeout: Duration,
    _marker: PhantomData<TDocument>,
}

impl<TDocument> Streamed<TDocument> {
    fn new() -> Self {
        Streamed {
            chunk_size: 1024 * 1024,
            timeout: Duration::from_secs(30),
            _marker: PhantomData,
        }
    }
}

impl Client<AsyncSender> {
    pub fn bulk_stream<TDocument>(&self) -> BulkRequestBuilder<AsyncSender, Streamed<TDocument>, BulkResponse> {
        RequestBuilder::new(
            self.clone(),
            None,
            BulkRequestInner {
                index: None,
                ty: None,
                body: Streamed::new(),
                _marker: PhantomData,
            },
        )
    }
}

impl<TSender, TBody, TResponse> BulkRequestBuilder<TSender, TBody, TResponse>
where
    TSender: Sender,
{
    /** Set the default type for the bulk request. */
    pub fn ty<I>(mut self, ty: I) -> Self
    where
        I: Into<Type<'static>>,
    {
        self.inner.ty = Some(ty.into());
        self
    }

    /** Set the default index for the bulk request. */
    pub fn index<I>(mut self, index: I) -> Self
    where
        I: Into<Index<'static>>,
    {
        self.inner.index = Some(index.into());
        self
    }

    pub fn response_index<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::WithNewIndex>
    where
        TResponse: ChangeIndex<I>,
    {
        RequestBuilder::new(
            self.client,
            self.params,
            BulkRequestInner {
                index: self.inner.index,
                ty: self.inner.ty,
                body: self.inner.body,
                _marker: PhantomData,
            },
        )
    }

    pub fn response_ty<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::WithNewType>
    where
        TResponse: ChangeType<I>,
    {
        RequestBuilder::new(
            self.client,
            self.params,
            BulkRequestInner {
                index: self.inner.index,
                ty: self.inner.ty,
                body: self.inner.body,
                _marker: PhantomData,
            },
        )
    }

    pub fn response_id<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::WithNewId>
    where
        TResponse: ChangeId<I>,
    {
        RequestBuilder::new(
            self.client,
            self.params,
            BulkRequestInner {
                index: self.inner.index,
                ty: self.inner.ty,
                body: self.inner.body,
                _marker: PhantomData,
            },
        )
    }
}

impl<TSender, TBody, TIndex, TType, TId> BulkRequestBuilder<TSender, TBody, BulkResponse<TIndex, TType, TId>>
where
    TSender: Sender,
{
    /** Set the type for the update request. */
    pub fn errors_only(self) -> BulkRequestBuilder<TSender, TBody, BulkErrorsResponse<TIndex, TType, TId>> {
        RequestBuilder::new(
            self.client,
            self.params,
            BulkRequestInner {
                index: self.inner.index,
                ty: self.inner.ty,
                body: self.inner.body,
                _marker: PhantomData,
            },
        )
    }
}

impl<TSender, TBody, TResponse> BulkRequestBuilder<TSender, TBody, TResponse>
where
    TSender: Sender,
    TBody: BulkBody,
{
    fn push<TDocument, TOperation>(&mut self, op: TOperation)
    where
        TOperation: Into<BulkOperation<TDocument>>,
        TDocument: Serialize,
    {
        // TODO: Handle this error somehow?
        // Store until the request is sent?
        let _ = self.inner.body.push(op.into());
    }

    pub fn append<TDocument, TOperation>(mut self, op: TOperation) -> Self
    where
        TOperation: Into<BulkOperation<TDocument>>,
        TDocument: Serialize,
    {
        self.push(op);
        self
    }

    pub fn extend<TIter, TDocument>(mut self, iter: TIter) -> Self
    where
        TIter: IntoIterator<Item = BulkOperation<TDocument>>,
        TDocument: Serialize,
    {
        for op in iter.into_iter() {
            self.push(op);
        }
        self
    }
}

impl<TBody, TResponse> BulkRequestInner<TBody, TResponse>
where
    TBody: BulkBody,
{
    fn into_request(self) -> Result<BulkRequest<'static, TBody>, Error> {
        match (self.index, self.ty) {
            (Some(index), None) => Ok(BulkRequest::for_index(
                index,
                self.body,
            )),
            (Some(index), Some(ty)) => Ok(BulkRequest::for_index_ty(
                index,
                ty,
                self.body,
            )),
            (None, None) => Ok(BulkRequest::new(
                self.body,
            )),
            _ => unimplemented!()
        }
    }
}

impl<TBody, TResponse> BulkRequestBuilder<SyncSender, TBody, TResponse>
where
    TBody: Into<SyncBody> + BulkBody,
    TResponse: DeserializeOwned + IsOk + 'static,
{
    pub fn send(self) -> Result<TResponse, Error> {
        let req = self.inner.into_request()?;

        RequestBuilder::new(self.client, self.params, RawRequestInner::new(req))
            .send()?
            .into_response()
    }
}

impl<TSender, TBody, TDocument, TResponse> Extend<BulkOperation<TDocument>> for BulkRequestBuilder<TSender, TBody, TResponse>
where
    TSender: Sender,
    TBody: BulkBody,
    TDocument: Serialize,
{
    fn extend<T>(&mut self, iter: T) where
    T: IntoIterator<Item = BulkOperation<TDocument>>,
    {
        for op in iter.into_iter() {
            self.push(op);
        }
    }
}

impl<TBody, TResponse> BulkRequestBuilder<AsyncSender, TBody, TResponse>
where
    TBody: Into<AsyncBody> + BulkBody + Send + 'static,
    TResponse: DeserializeOwned + IsOk + Send + 'static,
{
    pub fn send(self) -> Pending<TResponse> {
        let (client, params, inner) = (self.client, self.params, self.inner);

        let req_future = client.sender.maybe_async(move || inner.into_request());

        let res_future = req_future.and_then(move |req| {
            RequestBuilder::new(client, params, RawRequestInner::new(req))
                .send()
                .and_then(|res| res.into_response())
        });

        Pending::new(res_future)
    }
}

impl<TDocument, TResponse> BulkRequestBuilder<AsyncSender, Streamed<TDocument>, TResponse> {
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.inner.body.timeout = timeout;
        self
    }

    pub fn chunk_size_bytes(mut self, chunk_size: usize) -> Self {
        self.inner.body.chunk_size = chunk_size;
        self
    }

    pub fn build(self) -> (BulkSender<TDocument, TResponse>, BulkReceiver<TResponse>) {
        let chunk_size = self.inner.body.chunk_size;
        let duration = self.inner.body.timeout;

        let body = SenderBody::new(chunk_size);
        let timeout = Timeout::new(duration);
        let req_template = SenderRequestTemplate::new(self.client, self.params, self.inner.index, self.inner.ty);

        BulkSender::new(req_template, timeout, body)
    }
}

pub trait BulkBody {
    fn push<TDocument>(&mut self, op: BulkOperation<TDocument>) -> Result<(), Error> where TDocument: Serialize;
}

impl BulkBody for Vec<u8> {
    fn push<TDocument>(&mut self, op: BulkOperation<TDocument>) -> Result<(), Error> where TDocument: Serialize {
        op.write(self).map_err(error::request)?;

        Ok(())
    }
}

pub struct Pending<TResponse> {
    inner: Box<Future<Item = TResponse, Error = Error>>,
}

impl<TResponse> Pending<TResponse> {
    fn new<F>(fut: F) -> Self
    where
        F: Future<Item = TResponse, Error = Error> + 'static,
    {
        Pending {
            inner: Box::new(fut),
        }
    }
}

impl<TResponse> Future for Pending<TResponse> {
    type Item = TResponse;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

#[doc(hidden)]
pub trait ChangeIndex<TIndex> { type WithNewIndex; }

impl<TIndex, TType, TId, TNewIndex> ChangeIndex<TNewIndex> for BulkResponse<TIndex, TType, TId> {
    type WithNewIndex = BulkResponse<TNewIndex, TType, TId>;
}

impl<TIndex, TType, TId, TNewIndex> ChangeIndex<TNewIndex> for BulkErrorsResponse<TIndex, TType, TId> {
    type WithNewIndex = BulkErrorsResponse<TNewIndex, TType, TId>;
}

#[doc(hidden)]
pub trait ChangeType<TType> { type WithNewType; }

impl<TIndex, TType, TId, TNewType> ChangeType<TNewType> for BulkResponse<TIndex, TType, TId> {
    type WithNewType = BulkResponse<TIndex, TNewType, TId>;
}

impl<TIndex, TType, TId, TNewType> ChangeType<TNewType> for BulkErrorsResponse<TIndex, TType, TId> {
    type WithNewType = BulkErrorsResponse<TIndex, TNewType, TId>;
}

#[doc(hidden)]
pub trait ChangeId<TId> { type WithNewId; }

impl<TIndex, TType, TId, TNewId> ChangeId<TNewId> for BulkResponse<TIndex, TType, TId> {
    type WithNewId = BulkResponse<TIndex, TType, TNewId>;
}

impl<TIndex, TType, TId, TNewId> ChangeId<TNewId> for BulkErrorsResponse<TIndex, TType, TId> {
    type WithNewId = BulkErrorsResponse<TIndex, TType, TNewId>;
}

#[cfg(test)]
mod tests {
    use serde_json::{self, Value};
    use prelude::*;

    
}
