use std::sync::Arc;

#[cfg(test)]
use rustls::ClientConfig;

use crate::connection::VoiceConnection;
use crate::error::{Error, ErrorKind, Operation, RetryDisposition};
use crate::model::VoiceConnectInfo;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceLimits {
    gateway_text_bytes: usize,
    gateway_binary_bytes: usize,
    dave_binary_body_bytes: usize,
    gateway_command_capacity: usize,
    event_capacity: usize,
    event_subscriber_capacity: usize,
    encoded_opus_frame_bytes: usize,
    udp_datagram_bytes: usize,
    sender_command_capacity: usize,
    staged_frame_capacity: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            gateway_text_bytes: 1_280_000,
            gateway_binary_bytes: 1_280_000,
            dave_binary_body_bytes: 1_048_576,
            gateway_command_capacity: 32,
            event_capacity: 64,
            event_subscriber_capacity: 8,
            encoded_opus_frame_bytes: 1_275,
            udp_datagram_bytes: 2_048,
            sender_command_capacity: 8,
            staged_frame_capacity: 1,
        }
    }
}

macro_rules! limit_accessors {
    ($(($get:ident, $with:ident, $field:ident)),+ $(,)?) => {$ (
        #[must_use]
        pub fn $get(&self) -> usize { self.$field }
        #[must_use]
        pub fn $with(mut self, value: usize) -> Self { self.$field = value; self }
    )+ };
}

impl ResourceLimits {
    limit_accessors!(
        (
            gateway_text_bytes,
            with_gateway_text_bytes,
            gateway_text_bytes
        ),
        (
            gateway_binary_bytes,
            with_gateway_binary_bytes,
            gateway_binary_bytes
        ),
        (
            dave_binary_body_bytes,
            with_dave_binary_body_bytes,
            dave_binary_body_bytes
        ),
        (
            gateway_command_capacity,
            with_gateway_command_capacity,
            gateway_command_capacity
        ),
        (event_capacity, with_event_capacity, event_capacity),
        (
            event_subscriber_capacity,
            with_event_subscriber_capacity,
            event_subscriber_capacity
        ),
        (
            encoded_opus_frame_bytes,
            with_encoded_opus_frame_bytes,
            encoded_opus_frame_bytes
        ),
        (
            udp_datagram_bytes,
            with_udp_datagram_bytes,
            udp_datagram_bytes
        ),
        (
            sender_command_capacity,
            with_sender_command_capacity,
            sender_command_capacity
        ),
        (
            staged_frame_capacity,
            with_staged_frame_capacity,
            staged_frame_capacity
        ),
    );

    fn validate(&self) -> Result<(), Error> {
        let all_nonzero = self.gateway_text_bytes > 0
            && self.gateway_binary_bytes > 0
            && self.dave_binary_body_bytes > 0
            && self.gateway_command_capacity > 0
            && self.event_capacity > 0
            && self.event_subscriber_capacity > 0
            && self.encoded_opus_frame_bytes > 0
            && self.udp_datagram_bytes > 0
            && self.sender_command_capacity > 0
            && self.staged_frame_capacity > 0;
        if !all_nonzero
            || self.dave_binary_body_bytes > self.gateway_binary_bytes
            || self.encoded_opus_frame_bytes > self.udp_datagram_bytes
            || self.staged_frame_capacity != 1
        {
            return Err(Error::new(
                ErrorKind::InvalidConfiguration,
                Operation::Build,
                None,
                RetryDisposition::Fatal,
                None,
                "invalid resource limits",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct Oto {
    pub(crate) config: Arc<Config>,
}

impl std::fmt::Debug for Oto {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Oto")
            .field("limits", &self.config.limits)
            .finish()
    }
}

pub struct OtoBuilder {
    limits: ResourceLimits,
    #[cfg(test)]
    tls_config: Option<Arc<ClientConfig>>,
}

impl std::fmt::Debug for OtoBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtoBuilder")
            .field("limits", &self.limits)
            .field("custom_tls", &cfg!(test))
            .finish()
    }
}

pub(crate) struct Config {
    pub(crate) limits: ResourceLimits,
    #[cfg(test)]
    pub(crate) tls_config: Option<Arc<ClientConfig>>,
}

impl Oto {
    #[must_use]
    pub fn builder() -> OtoBuilder {
        OtoBuilder {
            limits: ResourceLimits::default(),
            #[cfg(test)]
            tls_config: None,
        }
    }

    pub async fn connect(&self, info: VoiceConnectInfo) -> Result<VoiceConnection, Error> {
        VoiceConnection::connect(self.config.clone(), info).await
    }
}

impl OtoBuilder {
    #[must_use]
    pub fn resource_limits(mut self, limits: ResourceLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn build(self) -> Result<Oto, Error> {
        self.limits.validate()?;
        Ok(Oto {
            config: Arc::new(Config {
                limits: self.limits,
                #[cfg(test)]
                tls_config: self.tls_config,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn test_tls_config(mut self, config: Arc<ClientConfig>) -> Self {
        self.tls_config = Some(config);
        self
    }
}
