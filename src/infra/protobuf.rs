use anyhow::{Context, Result};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage};
use serde_json::de::Deserializer;
use std::path::Path;
use std::{fs, path::PathBuf};
use tempfile::{self, TempDir};

pub trait ProtobufDescriptorLoader {
    fn build_protobuf_descriptor(proto_string: &String) -> Result<DescriptorPool> {
        let (tempdir, tempfile) =
            Self::_store_temp_proto_file(&"temp.proto".to_string(), proto_string)
                .context("on storing temp proto file")?;
        let descriptor_file = tempdir.path().join("descriptor.bin");
        tonic_build::configure()
            // only output message descriptor
            .build_server(false)
            .build_client(false)
            .build_transport(false)
            .out_dir(&tempdir)
            .protoc_arg("--experimental_allow_proto3_optional")
            .file_descriptor_set_path(&descriptor_file) // for reflection
            .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
            .compile(&[&tempfile], &[&tempdir])
            .context(format!("Failed to compile protos {:?}", &tempfile))?;

        let descriptor = Self::_load_protobuf_descriptor(&descriptor_file)?;
        Ok(descriptor)
    }

    fn _load_protobuf_descriptor(descriptor_file: &Path) -> Result<DescriptorPool> {
        let descriptor_bytes = fs::read(descriptor_file).context(format!(
            "on reading descriptor file: {:?}",
            descriptor_file.to_str()
        ))?;
        let descriptor_pool = DescriptorPool::decode(descriptor_bytes.as_ref())
            .context("on decoding descriptor bytes")?;
        Ok(descriptor_pool)
    }

    fn _store_temp_proto_file(
        proto_name: &String,
        proto_string: &String,
    ) -> Result<(TempDir, PathBuf)> {
        let temp_dir = tempfile::tempdir()
            .context(format!("on creating tempfile for proto: {}", proto_name))?;
        let tempfile = temp_dir.path().join(proto_name);
        // For now we need to write files to the disk.
        fs::write(&tempfile, proto_string).context(format!(
            "on saving tempfile for proto: {:?}",
            &tempfile.to_str()
        ))?;
        Ok((temp_dir, tempfile))
    }
}

#[derive(Debug, Clone)]
pub struct ProtobufDescriptor {
    pool: DescriptorPool,
}

impl ProtobufDescriptorLoader for ProtobufDescriptor {}
impl ProtobufDescriptor {
    pub fn new(proto_string: &String) -> Result<Self> {
        let pool = ProtobufDescriptor::build_protobuf_descriptor(proto_string)?;
        Ok(ProtobufDescriptor { pool })
    }

    pub fn get_message_by_name(&self, message_name: &str) -> Option<MessageDescriptor> {
        self.pool.get_message_by_name(message_name)
    }
    pub fn get_message_from_json(&self, message_name: &str, json: &str) -> Result<DynamicMessage> {
        let mut deserializer = Deserializer::from_str(json);
        let message_descriptor = self.get_message_by_name(message_name).unwrap();
        let dynamic_message = DynamicMessage::deserialize(message_descriptor, &mut deserializer)?;
        deserializer.end()?;
        Ok(dynamic_message)
    }
    pub fn get_message_from_bytes(
        &self,
        message_name: &str,
        bytes: &[u8],
    ) -> Result<DynamicMessage> {
        let message_descriptor = self.get_message_by_name(message_name).unwrap();
        let cursor = std::io::Cursor::new(bytes);
        let dynamic_message = DynamicMessage::decode(message_descriptor, cursor)?;
        Ok(dynamic_message)
    }
    #[allow(dead_code)]
    fn decode_from_json<T: ReflectMessage + Default>(json: impl AsRef<str>) -> Result<T> {
        let descriptor = T::default().descriptor();
        let mut deserializer = serde_json::Deserializer::from_str(json.as_ref());
        let decoded = DynamicMessage::deserialize(descriptor, &mut deserializer)?;
        deserializer.end()?;
        decoded.transcode_to::<T>().context(format!(
            "decode_from_json: on transcoding dynamic message to {}",
            std::any::type_name::<T>()
        ))
    }
}

// create test
#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use itertools::Itertools;
    use prost::Message;
    use prost_reflect::ReflectMessage;
    use std::io::Cursor;

    struct ProtobufDescriptorImpl {}
    impl ProtobufDescriptorLoader for ProtobufDescriptorImpl {}

    #[test]
    fn test_load_protobuf_descriptor() -> Result<()> {
        let proto_string = r#"
        syntax = "proto3";

        package jobworkerp.data;

        message Job {
            string id = 1;
            string name = 2;
            string description = 3;
        }
        "#;
        let descriptor_pool =
            ProtobufDescriptorImpl::build_protobuf_descriptor(&proto_string.to_string())?;
        println!(
            "messages:{:?}",
            descriptor_pool.all_messages().collect_vec()
        );
        assert!(!descriptor_pool.all_messages().collect_vec().is_empty());
        let job_descriptor = descriptor_pool
            .get_message_by_name("jobworkerp.data.Job")
            .unwrap();
        job_descriptor
            .fields()
            .for_each(|field| println!("field:{:?}", field));
        assert_eq!(job_descriptor.full_name(), "jobworkerp.data.Job");
        assert_eq!(job_descriptor.package_name(), "jobworkerp.data");
        assert_eq!(job_descriptor.name(), "Job");
        Ok(())
    }

    #[test]
    fn test_get_message_from_json() -> Result<()> {
        let proto_string = r#"
        syntax = "proto3";

        package jobworkerp.data;

        message Job {
            int64 id = 1;
            string name = 2;
            string description = 3;
        }
        "#;
        let descriptor = ProtobufDescriptor::new(&proto_string.to_string())?;
        let json = r#"
        {
            "id": 1,
            "name": "test name",
            "description": "test desc"
        }
        "#;
        let message = descriptor.get_message_from_json("jobworkerp.data.Job", json)?;

        assert_eq!(message.descriptor().name(), "Job");
        assert_eq!(
            message.get_field_by_name("id").unwrap().as_i64().unwrap(),
            1
        );
        assert_eq!(
            message.get_field_by_name("name").unwrap().as_str().unwrap(),
            "test name"
        );
        assert_eq!(
            message
                .get_field_by_name("description")
                .unwrap()
                .as_str()
                .unwrap(),
            "test desc"
        );
        let bytes = message.encode_to_vec();
        let cursor = Cursor::new(bytes);
        let mes = DynamicMessage::decode(
            descriptor
                .get_message_by_name("jobworkerp.data.Job")
                .unwrap(),
            cursor,
        )?;
        assert_eq!(message, mes);
        Ok(())
    }
}
