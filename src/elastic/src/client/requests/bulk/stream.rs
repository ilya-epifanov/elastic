use std::{mem, fmt};
use std::io;
use std::time::Duration;
use std::error::Error as StdError;
use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};
use futures::{Future, Poll, Async, AsyncSink, Sink, Stream};
use tokio_timer::{Timer, Sleep};
use channel::{self, TrySendError, TryRecvError};
use serde::ser::Serialize;
use serde::de::DeserializeOwned;

use error::{self, Error};
use client::{AsyncSender, Client, RequestParams};
use client::requests::RequestBuilder;
use client::requests::params::{Index, Type};
use client::responses::parse::IsOk;
use super::{BulkRequestBuilder, BulkRequestInner, Pending, BulkOperation};

pub struct BulkSender<TDocument, TResponse> {
    tx: BulkSenderInner<TResponse>,
    req_template: SenderRequestTemplate<TResponse>,
    in_flight: BulkSenderInFlight<TResponse>,
    timeout: Timeout,
    body: SenderBody,
    _marker: PhantomData<TDocument>,
}

impl<TDocument, TResponse> BulkSender<TDocument, TResponse> {
    pub(super) fn new(req_template: SenderRequestTemplate<TResponse>, timeout: Timeout, body: SenderBody) -> (Self, BulkReceiver<TResponse>) {
        let (tx, rx) = channel::bounded(1);

        let sender = BulkSender {
            tx: BulkSenderInner(Some(tx)),
            req_template,
            timeout,
            body,
            in_flight: BulkSenderInFlight::ReadyToSend,
            _marker: PhantomData,
        };

        (sender, BulkReceiver { rx: BulkReceiverInner(rx) })
    }
}

pub(super) struct SenderRequestTemplate<TResponse> {
    client: Client<AsyncSender>,
    params: Option<RequestParams>,
    index: Option<Index<'static>>,
    ty: Option<Type<'static>>,
    _marker: PhantomData<TResponse>,
}

impl<TResponse> SenderRequestTemplate<TResponse> {
    pub(super) fn new(client: Client<AsyncSender>, params: Option<RequestParams>, index: Option<Index<'static>>, ty: Option<Type<'static>>) -> Self {
        SenderRequestTemplate {
            client,
            params,
            index,
            ty,
            _marker: PhantomData,
        }
    }

    fn to_request(&self, body: Vec<u8>) -> BulkRequestBuilder<AsyncSender, Vec<u8>, TResponse> {
        RequestBuilder::new(
            self.client.clone(),
            self.params.clone(),
            BulkRequestInner::<Vec<u8>, TResponse> {
                index: self.index.clone(),
                ty: self.ty.clone(),
                body: body,
                _marker: PhantomData,
            }
        )
    }
}

pub(super) struct Timeout {
    timer: Timer,
    duration: Duration,
    sleep: Sleep,
}

impl Timeout {
    pub(super) fn new(duration: Duration) -> Self {
        let timer = Timer::default();
        let sleep = timer.sleep(duration);

        Timeout {
            duration,
            timer,
            sleep
        }
    }

    fn restart(&mut self) {
        self.sleep = self.timer.sleep(self.duration);
    }
}

impl Future for Timeout {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.sleep.poll().map_err(error::request)
    }
}

enum BulkSenderInFlight<TResponse> {
    ReadyToSend,
    Pending(Pending<TResponse>),
    Transmitting(Option<TResponse>),
    Transmitted,
}

struct BulkSenderInner<T>(Option<channel::Sender<T>>);
struct BulkReceiverInner<T>(channel::Receiver<T>);

pub struct BulkReceiver<TResponse> {
    rx: BulkReceiverInner<TResponse>
}

enum SenderPush {
    Flushed,
    Full,
}

pub(super) struct SenderBody {
    scratch: Vec<u8>,
    body: BytesMut,
    chunk_size: usize,
}

impl SenderBody {
    pub(super) fn new(chunk_size: usize) -> Self {
        SenderBody {
            scratch: Vec::new(),
            chunk_size,
            body: BytesMut::with_capacity(chunk_size),
        }
    }

    fn take(&mut self) -> BytesMut {
        let size = usize::max(self.scratch.len(), self.chunk_size);
        let mut new_body = BytesMut::with_capacity(size);

        // Copy out any scratch into the new buffer
        if self.scratch.len() > 0 {
            new_body.put_slice(&self.scratch);
            self.scratch.clear();
        }

        mem::replace(&mut self.body, new_body)
    }

    fn has_capacity(&self) -> bool {
        self.scratch.len() == 0 && self.body.remaining_mut() > 0
    }

    fn is_empty(&self) -> bool {
        self.body.len() == 0
    }

    fn is_full(&self) -> bool {
        self.scratch.len() > 0
    }

    fn push<TDocument>(&mut self, op: BulkOperation<TDocument>) -> Result<SenderPush, io::Error>
    where
        TDocument: Serialize,
    {
        op.write(&mut self.scratch)?;

        // Copy the scratch buffer into the request buffer if it fits
        if self.scratch.len() < self.body.remaining_mut() {
            self.body.put_slice(&self.scratch);
            self.scratch.clear();

            Ok(SenderPush::Flushed)
        }
        else if self.body.len() == 0 {
            let scratch = mem::replace(&mut self.scratch, Vec::new());
            self.body = BytesMut::from(scratch);

            Ok(SenderPush::Flushed)
        }
        else {
            // If the buffer doesn't fit, then retain it for the next request?
            // That means returning `Async::Ready` for a request that isn't going to be sent immediately
            Ok(SenderPush::Full)
        }
    }
}

impl<TDocument, TResponse> Sink for BulkSender<TDocument, TResponse>
where
    TDocument: Serialize + Send + 'static,
    TResponse: DeserializeOwned + IsOk + Send + 'static,
{
    type SinkItem = BulkOperation<TDocument>;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        match self.timeout.poll() {
            Ok(Async::Ready(())) => {
                return match self.poll_complete() {
                    Ok(_) => Ok(AsyncSink::NotReady(item)),
                    Err(e) => Err(e)
                }
            },
            Err(e) => return Err(error::request(e)),
            _ => ()
        }

        if self.body.has_capacity() {
            self.body.push(item).map_err(error::request)?;
            Ok(AsyncSink::Ready)
        }
        else {
            match self.poll_complete() {
                Ok(_) => Ok(AsyncSink::NotReady(item)),
                Err(e) => Err(e)
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        let in_flight = match self.in_flight {
            // The `Sender` is ready to send another request
            BulkSenderInFlight::ReadyToSend => {
                // If the body is empty, then we have nothing to do
                if self.body.is_empty() {
                    return Ok(Async::Ready(()))
                }

                // If the timeout hasn't expired and the body isn't full then we're not ready
                match self.timeout.poll() {
                    Ok(Async::NotReady) if !self.body.is_full() => return Ok(Async::NotReady),
                    Err(e) => return Err(error::request(e)),
                    _ => ()
                }

                self.timeout.restart();
                let body = self.body.take();

                let req = self.req_template.to_request(body.to_vec());
                let pending = req.send();

                BulkSenderInFlight::Pending(pending)
            }
            // A request is pending
            BulkSenderInFlight::Pending(ref mut pending) => {
                let response = try_ready!(pending.poll());
                BulkSenderInFlight::Transmitting(Some(response))
            }
            // A response is transmitting
            BulkSenderInFlight::Transmitting(ref mut response) => {
                if let Some(item) = response.take() {
                    match self.tx.start_send(item) {
                        Ok(AsyncSink::Ready) => {
                            BulkSenderInFlight::Transmitted
                        },
                        Ok(AsyncSink::NotReady(item)) => {
                            *response = Some(item);
                            return Ok(Async::NotReady)
                        },
                        Err(e) => return Err(e)
                    }
                }
                else {
                    BulkSenderInFlight::Transmitted
                }
            }
            // The request has completed
            BulkSenderInFlight::Transmitted => {
                let _ = try_ready!(self.tx.poll_complete());
                BulkSenderInFlight::ReadyToSend
            }
        };

        self.in_flight = in_flight;
        self.poll_complete()
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        let _ = try_ready!(self.poll_complete());
        self.tx.close()
    }
}

impl<T> Sink for BulkSenderInner<T>
where
    T: Send,
{
    type SinkItem = T;
    type SinkError = Error;

    fn start_send(&mut self, item: Self::SinkItem) -> Result<AsyncSink<Self::SinkItem>, Self::SinkError> {
        self.0.as_ref().map(|tx| match tx.try_send(item) {
            Ok(()) => Ok(AsyncSink::Ready),
            Err(TrySendError::Full(item)) => Ok(AsyncSink::NotReady(item)),
            Err(TrySendError::Disconnected(_)) => Err(error::request(Disconnected)),
        })
        .unwrap_or(Err(error::request(Disconnected)))
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        Ok(Async::Ready(()))
    }

    fn close(&mut self) -> Poll<(), Self::SinkError> {
        self.0 = None;
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
            Err(TryRecvError::Disconnected) => Ok(Async::Ready(None)),
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
