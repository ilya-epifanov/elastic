use std::io::{self, Write};
use std::ops::Deref;

use serde::ser::{Serialize, Serializer, SerializeMap};
use serde_json;

use client::requests::params::{Index, Type, Id};

pub use client::responses::bulk::Action;

pub struct BulkOperation<TDocument> {
    action: Action,
    header: BulkHeader,
    doc: Option<TDocument>,
}

#[derive(Serialize)]
struct BulkHeader {
    #[serde(rename = "_index", serialize_with = "serialize_param", skip_serializing_if = "Option::is_none")]
    index: Option<Index<'static>>,
    #[serde(rename = "_type", serialize_with = "serialize_param", skip_serializing_if = "Option::is_none")]
    ty: Option<Type<'static>>,
    #[serde(rename = "_id", serialize_with = "serialize_param", skip_serializing_if = "Option::is_none")]
    id: Option<Id<'static>>,
}

fn serialize_param<S, T>(field: &Option<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Deref<Target = str>,
{
    serializer.serialize_str(&*field.as_ref().expect("serialize `None` value"))
}

impl BulkOperation<()> {
    pub fn new(action: Action) -> Self {
        BulkOperation {
            action,
            header: BulkHeader {
                index: None,
                ty: None,
                id: None,
            },
            doc: None,
        }
    }
}

impl<TDocument> BulkOperation<TDocument> {
    pub fn index<I>(mut self, index: I) -> Self
    where
        I: Into<Index<'static>>,
    {
        self.header.index = Some(index.into());
        self
    }

    pub fn ty<I>(mut self, ty: I) -> Self
    where
        I: Into<Type<'static>>,
    {
        self.header.ty = Some(ty.into());
        self
    }

    pub fn id<I>(mut self, id: I) -> Self
    where
        I: Into<Id<'static>>,
    {
        self.header.id = Some(id.into());
        self
    }

    pub fn doc<TNewDocument>(self, doc: TNewDocument) -> BulkOperation<TNewDocument> {
        BulkOperation {
            action: self.action,
            header: self.header,
            doc: Some(doc),
        }
    }
}

impl<TDocument> BulkOperation<TDocument>
where
    TDocument: Serialize
{
    pub fn write<W>(&self, mut writer: W) -> io::Result<()>
    where
        W: Write,
    {
        struct Header<'a> {
            action: Action,
            inner: &'a BulkHeader,
        }

        impl<'a> Serialize for Header<'a> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where S: Serializer
            {
                let mut map = serializer.serialize_map(Some(1))?;
                
                let k = match self.action {
                    Action::Create => "create",
                    Action::Delete => "delete",
                    Action::Index => "index",
                    Action::Update => "update",
                };

                map.serialize_entry(k, &self.inner)?;
                
                map.end()
            }
        }

        serde_json::to_writer(&mut writer, &Header { action: self.action, inner: &self.header })?;
        write!(&mut writer, "\n")?;
        serde_json::to_writer(&mut writer, &self.doc)?;
        write!(&mut writer, "\n")?;

        Ok(())
    }
}

pub fn bulk_index() -> BulkOperation<()> {
    BulkOperation::new(Action::Index)
}

pub fn bulk_update() -> BulkOperation<()> {
    BulkOperation::new(Action::Update)
}

pub fn bulk_create() -> BulkOperation<()> {
    BulkOperation::new(Action::Create)
}

pub fn bulk_delete() -> BulkOperation<()> {
    BulkOperation::new(Action::Delete)
}