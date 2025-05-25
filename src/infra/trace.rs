use std::fmt::Debug;

use opentelemetry::global;
use opentelemetry::{
    propagation::Extractor,
    trace::{Span, Tracer},
    KeyValue,
};
use tonic::Request;

pub mod otel_span;

struct MetadataMap<'a>(&'a tonic::metadata::MetadataMap);

impl Extractor for MetadataMap<'_> {
    /// Get a value for a key from the MetadataMap.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collect all the keys from the MetadataMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

//https://opentelemetry.io/docs/specs/semconv/general/trace/
pub trait Tracing {
    fn trace_request<T: Debug>(
        name: &'static str,
        span_name: &'static str,
        request: &Request<T>,
    ) -> global::BoxedSpan {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        let mut span = global::tracer(name).start_with_context(span_name, &parent_cx);

        span.set_attribute(KeyValue::new("service.name", name));
        span.set_attribute(KeyValue::new("service.method", span_name));
        span.set_attribute(KeyValue::new("request", format!("{:?}", request)));

        if let Some(req_path) = request.metadata().get("path") {
            if let Ok(path_str) = req_path.to_str() {
                // Clone the string to own it, avoiding reference lifetime issues
                span.set_attribute(KeyValue::new("request.path", path_str.to_string()));
            }
        }

        span
    }
}
