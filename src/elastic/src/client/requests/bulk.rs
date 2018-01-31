#![allow(missing_docs, warnings)]

use std::{mem, fmt};
use std::time::Duration;
use std::error::Error as StdError;
use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};
use futures::{Future, Poll, Async, AsyncSink, Sink, Stream};
use channel::{self, TrySendError, TryRecvError};
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use error::{self, Error};
use client::{AsyncSender, Client, Sender, SyncSender};
use client::requests::{empty_body, RequestBuilder, DefaultBody, SyncBody, AsyncBody};
use client::requests::params::{Index, Type};
use client::requests::endpoints::BulkRequest;
use client::requests::raw::RawRequestInner;
use client::responses::{BulkResponse, BulkErrorsResponse};
use client::responses::parse::IsOk;

pub type BulkRequestBuilder<TSender, TBody, TResponse> = RequestBuilder<TSender, BulkRequestInner<TBody, TResponse>>;

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
    pub fn bulk(&self) -> BulkRequestBuilder<TSender, DefaultBody, BulkResponse> {
        RequestBuilder::new(
            self.clone(),
            None,
            BulkRequestInner {
                index: None,
                ty: None,
                body: empty_body(),
                _marker: PhantomData,
            },
        )
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

    pub fn response_index<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::Changed>
    where
        TResponse: ChangeIndex<I>,
    {
        unimplemented!()
    }

    pub fn response_ty<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::Changed>
    where
        TResponse: ChangeType<I>,
    {
        unimplemented!()
    }

    pub fn response_id<I>(self) -> BulkRequestBuilder<TSender, TBody, TResponse::Changed>
    where
        TResponse: ChangeId<I>,
    {
        unimplemented!()
    }
}

impl<TSender, TBody, TIndex, TType, TId> BulkRequestBuilder<TSender, TBody, BulkResponse<TIndex, TType, TId>>
where
    TSender: Sender,
{
    /** Set the type for the update request. */
    pub fn errors_only(self) -> BulkRequestBuilder<TSender, TBody, BulkErrorsResponse<TIndex, TType, TId>> {
        unimplemented!()
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
    {
        self.inner.body.push(op.into());
    }

    pub fn append<TDocument, TOperation>(mut self, op: TOperation) -> Self
    where
        TOperation: Into<BulkOperation<TDocument>>,
    {
        self.push(op);
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
    pub fn timeout(self, timeout: Duration) -> Self {
        unimplemented!()
    }

    pub fn chunk_size(self, chunk_size: usize) -> Self {
        unimplemented!()
    }

    pub fn stream(self) -> (BulkSender<TDocument, TResponse>, BulkReceiver<TResponse>) {
        unimplemented!()
    }
}

impl<TDocument> Sink for Streamed<TDocument>
where
    TDocument: Serialize + Send + 'static,
{
    type SinkItem = BulkOperation<TDocument>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        // TODO: if timer expired, call `poll_complete`
        // TODO: if over capacity, call `poll_complete`
        if self.has_capacity() {
            self.push(item);
            Ok(AsyncSink::Ready)
        }
        else {
            Ok(AsyncSink::NotReady(item))
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        if !self.is_empty() {
            unimplemented!()
        }
        else {
            Ok(Async::Ready(()))
        }
    }
}

pub trait BulkBody {
    fn push<TDocument>(&mut self, op: BulkOperation<TDocument>);
}

pub trait ChangeIndex<TIndex> { type Changed; }

impl<TIndex, TType, TId, TNewIndex> ChangeIndex<TNewIndex> for BulkResponse<TIndex, TType, TId> {
    type Changed = BulkResponse<TNewIndex, TType, TId>;
}

impl<TIndex, TType, TId, TNewIndex> ChangeIndex<TNewIndex> for BulkErrorsResponse<TIndex, TType, TId> {
    type Changed = BulkErrorsResponse<TNewIndex, TType, TId>;
}

pub trait ChangeType<TType> { type Changed; }

impl<TIndex, TType, TId, TNewType> ChangeType<TNewType> for BulkResponse<TIndex, TType, TId> {
    type Changed = BulkResponse<TIndex, TNewType, TId>;
}

impl<TIndex, TType, TId, TNewType> ChangeType<TNewType> for BulkErrorsResponse<TIndex, TType, TId> {
    type Changed = BulkErrorsResponse<TIndex, TNewType, TId>;
}

pub trait ChangeId<TId> { type Changed; }

impl<TIndex, TType, TId, TNewId> ChangeId<TNewId> for BulkResponse<TIndex, TType, TId> {
    type Changed = BulkResponse<TIndex, TType, TNewId>;
}

impl<TIndex, TType, TId, TNewId> ChangeId<TNewId> for BulkErrorsResponse<TIndex, TType, TId> {
    type Changed = BulkErrorsResponse<TIndex, TType, TNewId>;
}

pub struct Streamed<TDocument> {
    scratch: Vec<u8>,
    chunk_size: usize,
    body: BytesMut,
    _marker: PhantomData<TDocument>,
}

impl<TDocument> Streamed<TDocument> {
    fn new() -> Self {
        Streamed {
            scratch: Vec::new(),
            chunk_size: 1024 * 1024,
            body: BytesMut::new(),
            _marker: PhantomData,
        }
    }

    fn take(&mut self) -> Self {
        Streamed {
            scratch: Vec::new(),
            chunk_size: self.chunk_size,
            body: mem::replace(&mut self.body, BytesMut::with_capacity(self.chunk_size)),
            _marker: PhantomData,
        }
    }

    fn push(&mut self, _op: BulkOperation<TDocument>) {

    }

    fn has_capacity(&self) -> bool {
        self.body.remaining_mut() > 0
    }

    fn is_empty(&self) -> bool {
        self.body.len() == 0
    }
}

pub struct BulkOperation<TDocument> {
    doc: Option<TDocument>,
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

pub struct BulkSender<TDocument, TResponse> {
    tx: BulkSenderInner<TDocument>,
    req: BulkRequestBuilder<AsyncSender, Streamed<TDocument>, TResponse>,
}

struct BulkSenderInner<T>(channel::Sender<T>);
struct BulkReceiverInner<T>(channel::Receiver<T>);

pub struct BulkReceiver<TResponse> {
    rx: BulkReceiverInner<TResponse>
}

impl<TDocument, TResponse> Sink for BulkSender<TDocument, TResponse>
where
    TDocument: Serialize + Send + 'static,
    TResponse: DeserializeOwned + IsOk + Send + 'static,
{
    type SinkItem = BulkOperation<TDocument>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.req.inner.body.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        // TODO: Take body and send request if full or timeout expired
        // TODO: Send response on the `tx` stream
        self.req.inner.body.poll_complete()
    }
}

impl<T> Sink for BulkSenderInner<T>
where
    T: Send,
{
    type SinkItem = T;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        match self.0.try_send(item) {
            Ok(()) => Ok(AsyncSink::Ready),
            Err(TrySendError::Full(item)) => Ok(AsyncSink::NotReady(item)),
            Err(TrySendError::Disconnected(_)) => Err(error::request(Disconnected)),
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }
}

impl<TResponse> Stream for BulkReceiver<TResponse>
where
    TResponse: Send
{
    type Item = TResponse;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.rx.poll()
    }
}

impl<T> Stream for BulkReceiverInner<T>
where
    T: Send
{
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match self.0.try_recv() {
            Ok(item) => Ok(Async::Ready(Some(item))),
            Err(TryRecvError::Empty) => Ok(Async::NotReady),
            Err(TryRecvError::Disconnected) => Err(error::request(Disconnected)),
        }
    }
}

// Alternative disconnected error because `TrySendError` and `TryReceiveError`
// don't implement `Error`.
#[derive(Debug)]
struct Disconnected;

impl fmt::Display for Disconnected {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("disconnected")
    }
}

impl StdError for Disconnected {
    fn description(&self) -> &str {
        "disconnected"
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{self, Value};
    use super::ScriptBuilder;
    use prelude::*;

    
}
