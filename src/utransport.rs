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
use crate::UPClientZenoh;
use async_trait::async_trait;
use std::{sync::atomic::Ordering, time::Duration};
use up_rust::{
    transport::{datamodel::UTransport, validator::Validators},
    uprotocol::{
        Data, UAttributes, UCode, UMessage, UMessageType, UPayload, UPayloadFormat, UStatus, UUri,
    },
    uri::validator::UriValidator,
};
use zenoh::{
    prelude::{r#async::*, Sample},
    queryable::Query,
};

impl UPClientZenoh {
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
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };

        let priority =
            UPClientZenoh::map_zenoh_priority(attributes.priority.enum_value().map_err(|_| {
                UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid priority")
            })?);

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

    async fn send_request(
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
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };

        let value = Value::new(buf.into()).encoding(Encoding::WithSuffix(
            KnownEncoding::AppCustom,
            payload.format.value().to_string().into(),
        ));

        // TODO: Adjust the timeout
        let _getbuilder = self
            .session
            .get(zenoh_key)
            .with_value(value)
            .with_attachment(attachment.build())
            .target(QueryTarget::BestMatching)
            .timeout(Duration::from_millis(1000));

        // TODO: Retrieve the callback
        // TODO: Send the get with callback

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
        let Ok(attachment) = UPClientZenoh::uattributes_to_attachment(&attributes) else {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Invalid uAttributes",
            ));
        };
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
    async fn register_publish_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
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
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener(Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    )));
                    return;
                }
            };
            // Create UPayload
            let Some(encoding) = UPClientZenoh::to_upayload_format(&sample.encoding) else {
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

    async fn register_request_listener(
        &self,
        topic: UUri,
        listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;
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
            let u_attribute = match UPClientZenoh::attachment_to_uattributes(attachment) {
                Ok(uattributes) => uattributes,
                Err(e) => {
                    log::error!("attachment_to_uattributes error: {:?}", e);
                    listener(Err(UStatus::fail_with_code(
                        UCode::INTERNAL,
                        "Unable to decode attribute",
                    )));
                    return;
                }
            };
            // Create UPayload
            let u_payload = match query.value() {
                Some(value) => {
                    let Some(encoding) = UPClientZenoh::to_upayload_format(&value.encoding) else {
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
    async fn register_response_listener(
        &self,
        _topic: UUri,
        _listener: Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>,
    ) -> Result<String, UStatus> {
        Ok(String::from("Not implemented"))
    }
}

#[async_trait]
impl UTransport for UPClientZenoh {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let payload = *message.payload.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uPayload",
        ))?;
        let attributes = *message.attributes.0.ok_or(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Invalid uAttributes",
        ))?;
        let topic = attributes.clone().sink;
        // Do the validation
        UriValidator::validate(&topic)
            .map_err(|_| UStatus::fail_with_code(UCode::INVALID_ARGUMENT, "Invalid topic"))?;

        // Get Zenoh key
        let zenoh_key = UPClientZenoh::to_zenoh_key_string(&topic)?;

        // Check the type of UAttributes (Publish / Request / Response)
        match attributes
            .type_
            .enum_value()
            .map_err(|_| UStatus::fail_with_code(UCode::INTERNAL, "Unable to parse type"))?
        {
            UMessageType::UMESSAGE_TYPE_PUBLISH => {
                Validators::Publish
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Publish UAttributes {e:?}"),
                        )
                    })?;
                self.send_publish(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_REQUEST => {
                Validators::Request
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Request UAttributes {e:?}"),
                        )
                    })?;
                self.send_request(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_RESPONSE => {
                Validators::Response
                    .validator()
                    .validate(&attributes)
                    .map_err(|e| {
                        UStatus::fail_with_code(
                            UCode::INVALID_ARGUMENT,
                            format!("Wrong Response UAttributes {e:?}"),
                        )
                    })?;
                self.send_response(&zenoh_key, payload, attributes).await
            }
            UMessageType::UMESSAGE_TYPE_UNSPECIFIED => Err(UStatus::fail_with_code(
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

        if UriValidator::is_rpc_response(&topic) {
            // RPC response
            self.register_response_listener(topic, listener).await
        } else if UriValidator::is_rpc_method(&topic) {
            // RPC request
            self.register_request_listener(topic, listener).await
        } else {
            // Normal publish
            self.register_publish_listener(topic, listener).await
        }
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
