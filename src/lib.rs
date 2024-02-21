//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use async_trait::async_trait;
use protobuf::{Enum, Message};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use up_rust::{
    rpc::{CallOptions, RpcClient, RpcClientResult, RpcMapperError, RpcServer},
    transport::{datamodel::UTransport, validator::Validators},
    uprotocol::{
        Data, UAttributes, UCode, UMessage, UMessageType, UPayload, UPayloadFormat, UPriority,
        UStatus, UUri, UUID,
    },
    uri::{
        serializer::{MicroUriSerializer, UriSerializer},
        validator::UriValidator,
    },
};
use zenoh::{
    config::Config,
    prelude::{r#async::*, Sample},
    queryable::{Query, Queryable},
    sample::{Attachment, AttachmentBuilder},
    subscriber::Subscriber,
};

// TODO: Mabye there's a nicer way transform encoding to UPayloadFormat
fn to_upayload_format(encoding: &Encoding) -> Option<UPayloadFormat> {
    let Ok(value) = encoding.suffix().parse::<i32>() else {
        return None;
    };
    match value {
        0 => Some(UPayloadFormat::UPAYLOAD_FORMAT_UNSPECIFIED),
        1 => Some(UPayloadFormat::UPAYLOAD_FORMAT_PROTOBUF_WRAPPED_IN_ANY),
        2 => Some(UPayloadFormat::UPAYLOAD_FORMAT_PROTOBUF),
        3 => Some(UPayloadFormat::UPAYLOAD_FORMAT_JSON),
        4 => Some(UPayloadFormat::UPAYLOAD_FORMAT_SOMEIP),
        5 => Some(UPayloadFormat::UPAYLOAD_FORMAT_SOMEIP_TLV),
        6 => Some(UPayloadFormat::UPAYLOAD_FORMAT_RAW),
        7 => Some(UPayloadFormat::UPAYLOAD_FORMAT_TEXT),
        _ => None,
    }
}

// TODO: Transformation between uAttributes and attachment
//       Transform all members first and then exclude some we don't need
fn uattributes_to_attachment(uattributes: &UAttributes) -> AttachmentBuilder {
    let mut attachment = AttachmentBuilder::new();
    attachment.insert("id", &uattributes.id.write_to_bytes().unwrap());
    attachment.insert("type_", &uattributes.type_.unwrap().to_type_string());
    attachment.insert("source", &uattributes.source.write_to_bytes().unwrap());
    attachment.insert("sink", &uattributes.sink.write_to_bytes().unwrap());
    // TODO: Check whether request & response need priority or not
    attachment.insert("priority", &uattributes.priority.value().to_string());
    if let Some(ttl) = uattributes.ttl {
        attachment.insert("ttl", &ttl.to_string());
    }
    if let Some(plevel) = uattributes.permission_level {
        attachment.insert("permission_level", &plevel.to_string());
    }
    if let Some(commstatus) = uattributes.commstatus {
        attachment.insert("commstatus", &commstatus.to_string());
    }
    attachment.insert("reqid", &uattributes.reqid.write_to_bytes().unwrap());
    if let Some(token) = uattributes.token.clone() {
        attachment.insert("token", &token);
    }
    if let Some(traceparent) = uattributes.traceparent.clone() {
        attachment.insert("traceparent", &traceparent);
    }
    attachment
}

// TODO: Same as uattributes_to_attachment
// TODO: Remove unwrap()
fn attachment_to_uattributes(attachment: &Attachment) -> Option<UAttributes> {
    let mut uattributes = UAttributes::new();
    if let Some(id) = attachment.get(&"id".as_bytes()) {
        let uuid = UUID::parse_from_bytes(&id).unwrap();
        uattributes.id = Some(uuid).into();
    } else {
        return None;
    }
    if let Some(type_) = attachment.get(&"type_".as_bytes()) {
        let uuid = UMessageType::from_type_string(type_.to_string());
        uattributes.type_ = uuid.into();
    } else {
        return None;
    }
    if let Some(source) = attachment.get(&"source".as_bytes()) {
        let source = UUri::parse_from_bytes(&source).unwrap();
        uattributes.source = Some(source).into();
    } else {
        return None;
    }
    if let Some(sink) = attachment.get(&"sink".as_bytes()) {
        let sink = UUri::parse_from_bytes(&sink).unwrap();
        uattributes.sink = Some(sink).into();
    } else {
        return None;
    }
    if let Some(priority) = attachment.get(&"priority".as_bytes()) {
        let priority = UPriority::from_i32(
            String::from_utf8(priority.to_vec())
                .unwrap()
                .parse()
                .unwrap(),
        )
        .unwrap();
        uattributes.priority = priority.into();
    } else {
        return None;
    }
    if let Some(ttl) = attachment.get(&"ttl".as_bytes()) {
        let ttl = String::from_utf8(ttl.to_vec())
            .unwrap()
            .parse::<i32>()
            .unwrap();
        uattributes.ttl = Some(ttl);
    }
    if let Some(permission_level) = attachment.get(&"permission_level".as_bytes()) {
        let permission_level = String::from_utf8(permission_level.to_vec())
            .unwrap()
            .parse::<i32>()
            .unwrap();
        uattributes.permission_level = Some(permission_level);
    }
    if let Some(commstatus) = attachment.get(&"commstatus".as_bytes()) {
        let commstatus = String::from_utf8(commstatus.to_vec())
            .unwrap()
            .parse::<i32>()
            .unwrap();
        uattributes.commstatus = Some(commstatus);
    }
    if let Some(reqid) = attachment.get(&"reqid".as_bytes()) {
        let reqid = UUID::parse_from_bytes(&reqid).unwrap();
        uattributes.reqid = Some(reqid).into();
    } else {
        return None;
    }
    if let Some(token) = attachment.get(&"token".as_bytes()) {
        let token = token.to_string();
        uattributes.token = Some(token);
    }
    if let Some(traceparent) = attachment.get(&"traceparent".as_bytes()) {
        let traceparent = traceparent.to_string();
        uattributes.traceparent = Some(traceparent);
    }
    Some(uattributes)
}

pub struct ZenohListener {}
pub struct UPClientZenoh {
    session: Arc<Session>,
    subscriber_map: Arc<Mutex<HashMap<String, Subscriber<'static, ()>>>>,
    queryable_map: Arc<Mutex<HashMap<String, Queryable<'static, ()>>>>,
    query_map: Arc<Mutex<HashMap<String, Query>>>,
    callback_counter: AtomicU64,
}

impl UPClientZenoh {
    /// # Errors
    /// Will return `Err` if unable to create Zenoh session
    pub async fn new(config: Config) -> Result<UPClientZenoh, UStatus> {
        let Ok(session) = zenoh::open(config).res().await else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to open Zenoh session",
            ));
        };
        Ok(UPClientZenoh {
            session: Arc::new(session),
            subscriber_map: Arc::new(Mutex::new(HashMap::new())),
            queryable_map: Arc::new(Mutex::new(HashMap::new())),
            query_map: Arc::new(Mutex::new(HashMap::new())),
            callback_counter: AtomicU64::new(0),
        })
    }

    fn to_zenoh_key_string(uri: &UUri) -> Result<String, UStatus> {
        let micro_uuri = MicroUriSerializer::serialize(uri).map_err(|_| {
            UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Unable to serialize into micro format",
            )
        })?;
        let micro_zenoh_key: String = micro_uuri
            .iter()
            .fold(String::new(), |s, c| s + &format!("{c:02x}"));
        Ok(micro_zenoh_key)
    }

    #[allow(clippy::match_same_arms)]
    fn map_zenoh_priority(upriority: UPriority) -> Priority {
        match upriority {
            UPriority::UPRIORITY_CS0 => Priority::Background,
            UPriority::UPRIORITY_CS1 => Priority::DataLow,
            UPriority::UPRIORITY_CS2 => Priority::Data,
            UPriority::UPRIORITY_CS3 => Priority::DataHigh,
            UPriority::UPRIORITY_CS4 => Priority::InteractiveLow,
            UPriority::UPRIORITY_CS5 => Priority::InteractiveHigh,
            UPriority::UPRIORITY_CS6 => Priority::RealTime,
            // If uProtocol prioritiy isn't specified, use CS1(DataLow) by default.
            // https://github.com/eclipse-uprotocol/uprotocol-spec/blob/main/basics/qos.adoc
            UPriority::UPRIORITY_UNSPECIFIED => Priority::DataLow,
        }
    }

    async fn send_publish(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid data",
            ));
        };

        // Serialized UAttributes into protobuf
        let attachment = uattributes_to_attachment(&attributes);
        // TODO: Check how to deal with unwrap()
        let priority = UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().unwrap());

        let putbuilder = self
            .session
            .put(zenoh_key, buf)
            .encoding(Encoding::WithSuffix(
                KnownEncoding::AppCustom,
                payload.format.value().to_string().into(),
            ))
            .priority(priority)
            .with_attachment(attachment.build());

        // Send data
        putbuilder
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to send with Zenoh"))?;

        Ok(())
    }

    async fn send_response(
        &self,
        zenoh_key: &str,
        payload: UPayload,
        attributes: UAttributes,
    ) -> Result<(), UStatus> {
        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid data",
            ));
        };

        // Serialized UAttributes into protobuf
        let attachment = uattributes_to_attachment(&attributes);
        // Get reqid
        let reqid = attributes.reqid.to_string();

        // Send back query
        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        let reply = Ok(Sample::new(
            KeyExpr::new(zenoh_key.to_string()).map_err(|_| {
                UStatus::fail_with_code(UCode::INTERNAL, "Unable to create Zenoh key")
            })?,
            value,
        ));
        let query = self
            .query_map
            .lock()
            .unwrap()
            .get(&reqid)
            .ok_or(UStatus::fail_with_code(
                UCode::INTERNAL,
                "query doesn't exist",
            ))?
            .clone();

        // Send data
        query
            .reply(reply)
            .with_attachment(attachment.build())
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to add attachment"))?
            .res()
            .await
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to reply with Zenoh"))?;

        Ok(())
    }
}

// TODO: Need to check how to use CallOptions
#[async_trait]
impl RpcClient for UPClientZenoh {
    async fn invoke_method(
        &self,
        topic: UUri,
        payload: UPayload,
        _options: CallOptions,
    ) -> RpcClientResult {
        // Validate UUri
        UriValidator::validate(&topic)
            .map_err(|_| RpcMapperError::UnexpectedError(String::from("Wrong UUri")))?;

        // TODO: Comment it and check whether uAttributes is not necessary.
        //// Validate UAttributes
        //{
        //    // TODO: Check why the validator doesn't have Send
        //    let validator = Validators::Request.validator();
        //    if let Err(e) = validator.validate(&attributes) {
        //        return Err(RpcMapperError::UnexpectedError(format!(
        //            "Wrong UAttributes {e:?}",
        //        )));
        //    }
        //}

        // Get Zenoh key
        let Ok(zenoh_key) = UPClientZenoh::to_zenoh_key_string(&topic) else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Unable to transform to Zenoh key",
            )));
        };

        // Get the data from UPayload
        let Some(Data::Value(buf)) = payload.data else {
            // TODO: Assume we only have Value here, no reference for shared memory
            return Err(RpcMapperError::InvalidPayload(String::from(
                "Wrong UPayload",
            )));
        };

        // TODO: Check how to generate uAttributes, ex: source, reqid...
        let mut uattributes = UAttributes::new();
        uattributes.source = Some(topic).into();
        uattributes.reqid = Some(UUID::default()).into();
        let attachment = uattributes_to_attachment(&uattributes);

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));
        // TODO: Query should support .encoding
        // TODO: Adjust the timeout
        let getbuilder = self
            .session
            .get(&zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(1000));

        // Send the query
        let Ok(replies) = getbuilder.res().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while sending Zenoh query",
            )));
        };

        let Ok(reply) = replies.recv_async().await else {
            return Err(RpcMapperError::UnexpectedError(String::from(
                "Error while receiving Zenoh reply",
            )));
        };
        match reply.sample {
            Ok(sample) => {
                let Some(encoding) = to_upayload_format(&sample.encoding) else {
                    return Err(RpcMapperError::UnexpectedError(String::from(
                        "Error while parsing Zenoh encoding",
                    )));
                };
                // TODO: Need to check attributes is correct or not
                Ok(UMessage {
                    attributes: Some(uattributes).into(),
                    payload: Some(UPayload {
                        length: Some(0),
                        format: encoding.into(),
                        data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                })
            }
            Err(_) => Err(RpcMapperError::UnexpectedError(String::from(
                "Error while parsing Zenoh reply",
            ))),
        }
    }
}

#[async_trait]
impl RpcServer for UPClientZenoh {
    async fn register_rpc_listener(
        &self,
        method: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        // Do the validation
        UriValidator::validate(&method)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(&method)?;
        // Generate listener string for users to delete
        let hashmap_key = format!(
            "{}_{:X}",
            zenoh_key,
            self.callback_counter.fetch_add(1, Ordering::SeqCst)
        );

        let query_map = self.query_map.clone();
        // Setup callback
        let callback = move |query: Query| {
            // Create UAttribute
            let Some(attachment) = query.attachment() else {
                listener(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                )));
                return;
            };
            let Some(u_attribute) = attachment_to_uattributes(attachment) else {
                listener(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to decode attribute",
                )));
                return;
            };
            // Create UPayload
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = to_upayload_format(&value.encoding) else {
                        listener(Err(UStatus::fail_with_code(
                            UCode::INTERNAL,
                            "Unable to get payload encoding",
                        )));
                        return;
                    };
                    UPayload {
                        length: Some(0),
                        format: encoding.into(),
                        data: Some(Data::Value(value.payload.contiguous().to_vec())),
                        ..Default::default()
                    }
                }
                None => UPayload {
                    length: Some(0),
                    format: UPayloadFormat::UPAYLOAD_FORMAT_UNSPECIFIED.into(),
                    data: None,
                    ..Default::default()
                },
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute.clone()).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            query_map
                .lock()
                .unwrap()
                .insert(u_attribute.reqid.to_string(), query);
            listener(Ok(msg));
        };
        if let Ok(queryable) = self
            .session
            .declare_queryable(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.queryable_map
                .lock()
                .unwrap()
                .insert(hashmap_key.clone(), queryable);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(hashmap_key)
    }
    async fn unregister_rpc_listener(&self, method: UUri, listener: &str) -> Result<(), UStatus> {
        // Do the validation
        UriValidator::validate(&method)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;
        // TODO: Check whether we still need method or not (Compare method with listener?)

        if self
            .queryable_map
            .lock()
            .unwrap()
            .remove(listener)
            .is_none()
        {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Listener doesn't exist",
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        // TODO: Should avoid unwrap()
        let payload = message.payload.unwrap();
        let attributes = message.attributes.unwrap();
        let topic = attributes.clone().sink;
        // Do the validation
        UriValidator::validate(&topic)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;

        // Check the type of UAttributes (Publish / Request / Response)
        // TODO: Need to void unwrap()
        match attributes.type_.unwrap() {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                Validators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|_| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            "Wrong Response UAttributes",
                        )
                    })?;
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                Validators::Response
                    .validator()
                    .validate(&attributes)
                    .map_err(|_| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            "Wrong Response UAttributes",
                        )
                    })?;
                self.send_response(&zenoh_key, payload, attributes).await
            }
            _ => Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Wrong Message type in UAttributes",
            )),
        }
    }

    async fn receive(&self, _topic: UUri) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "Not implemented",
        ))
    }

    async fn register_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        // Do the validation
        UriValidator::validate(&topic)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
        // Generate listener string for users to delete
        let hashmap_key = format!(
            "{}_{:X}",
            zenoh_key,
            self.callback_counter.fetch_add(1, Ordering::SeqCst)
        );

        // Setup callback
        let callback = move |sample: Sample| {
            // Create UAttribute
            let Some(attachment) = sample.attachment() else {
                listener(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get attachment",
                )));
                return;
            };
            let Some(u_attribute) = attachment_to_uattributes(attachment) else {
                listener(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to decode attribute",
                )));
                return;
            };
            // Create UPayload
            let Some(encoding) = to_upayload_format(&sample.encoding) else {
                listener(Err(UStatus::fail_with_code(
                    UCode::INTERNAL,
                    "Unable to get payload encoding",
                )));
                return;
            };
            let u_payload = UPayload {
                length: Some(0),
                format: encoding.into(),
                data: Some(Data::Value(sample.payload.contiguous().to_vec())),
                ..Default::default()
            };
            // Create UMessage
            let msg = UMessage {
                attributes: Some(u_attribute).into(),
                payload: Some(u_payload).into(),
                ..Default::default()
            };
            listener(Ok(msg));
        };
        if let Ok(subscriber) = self
            .session
            .declare_subscriber(&zenoh_key)
            .callback_mut(callback)
            .res()
            .await
        {
            self.subscriber_map
                .lock()
                .unwrap()
                .insert(hashmap_key.clone(), subscriber);
        } else {
            return Err(UStatus::fail_with_code(
                UCode::INTERNAL,
                "Unable to register callback with Zenoh",
            ));
        }

        Ok(hashmap_key)
    }

    async fn unregister_listener(&self, topic: UUri, listener: &str) -> Result<(), UStatus> {
        // Do the validation
        UriValidator::validate(&topic)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;
        // TODO: Check whether we still need topic or not (Compare topic with listener?)

        if !self.subscriber_map.lock().unwrap().contains_key(listener) {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Listener doesn't exist",
            ));
        }

        self.subscriber_map.lock().unwrap().remove(listener);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use up_rust::uprotocol::{UEntity, UResource, UUri};

    #[test]
    fn test_to_zenoh_key_string() {
        // create uuri for test
        let uuri = UUri {
            entity: Some(UEntity {
                name: "body.access".to_string(),
                version_major: Some(1),
                id: Some(1234),
                ..Default::default()
            })
            .into(),
            resource: Some(UResource {
                name: "door".to_string(),
                instance: Some("front_left".to_string()),
                message: Some("Door".to_string()),
                id: Some(5678),
                ..Default::default()
            })
            .into(),
            ..Default::default()
        };
        assert_eq!(
            UPClientZenoh::to_zenoh_key_string(&uuri).unwrap(),
            String::from("0100162e04d20100")
        );
    }
}
