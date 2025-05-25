use opentelemetry::{
    global::{self, BoxedTracer},
    trace::{self, SpanBuilder, SpanKind, Tracer},
    Context, KeyValue,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

/// Carrier extractor for extracting context from HTTP headers or similar key-value maps
struct CarrierExtractor<'a>(&'a HashMap<String, String>);

impl<'a> opentelemetry::propagation::Extractor for CarrierExtractor<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|s| s.as_str()).collect()
    }
}

/// Carrier injector for injecting context into HTTP headers or similar key-value maps
struct CarrierInjector<'a>(&'a mut HashMap<String, String>);

impl<'a> opentelemetry::propagation::Injector for CarrierInjector<'a> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

/// Configuration for Langfuse specific client  
#[derive(Debug, Clone)]
pub struct LangfuseConfig {
    pub public_key: String,
    pub secret_key: String,
}

/// Span types based on OpenTelemetry standards
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelSpanType {
    Span,
    Generation,
    Event,
}

/// Input/Output data for spans
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelSpanData {
    pub input: Option<serde_json::Value>,
    pub output: Option<serde_json::Value>,
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

/// Generic span attributes for OpenTelemetry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelSpanAttributes {
    pub span_type: OtelSpanType,
    pub name: String,
    pub span_id: Option<String>, // Span ID for referencing in parent/child relationships
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub version: Option<String>,
    pub release: Option<String>,
    pub tags: Vec<String>,
    pub data: OtelSpanData,
    pub model: Option<String>,
    pub model_parameters: Option<HashMap<String, serde_json::Value>>,
    pub usage: Option<HashMap<String, i64>>,
    pub level: Option<String>,
    pub status_message: Option<String>,
    pub parent_observation_id: Option<String>,
    pub trace_id: Option<String>,
    pub trace_name: Option<String>,
    pub trace_input: Option<serde_json::Value>,
    pub trace_output: Option<serde_json::Value>,
    pub trace_tags: Vec<String>,
    pub trace_public: Option<bool>,
    pub trace_metadata: Option<HashMap<String, serde_json::Value>>,
    pub prompt_name: Option<String>,
    pub prompt_version: Option<i32>,
    pub cost_details: Option<HashMap<String, f64>>,
    pub completion_start_time: Option<String>,
    // Additional OpenTelemetry standard fields
    pub system: Option<String>,
    pub operation_name: Option<String>,
    pub response_id: Option<String>,
    pub finish_reasons: Vec<String>,
    pub is_stream: Option<bool>,
    pub openinference_span_kind: Option<String>,
}

impl Default for OtelSpanAttributes {
    fn default() -> Self {
        Self {
            span_type: OtelSpanType::Span,
            name: String::new(),
            span_id: None,
            user_id: None,
            session_id: None,
            version: None,
            release: None,
            tags: Vec::new(),
            data: OtelSpanData {
                input: None,
                output: None,
                metadata: None,
            },
            model: None,
            model_parameters: None,
            usage: None,
            level: None,
            status_message: None,
            parent_observation_id: None,
            trace_id: None,
            trace_name: None,
            trace_input: None,
            trace_output: None,
            trace_tags: Vec::new(),
            trace_public: None,
            trace_metadata: None,
            prompt_name: None,
            prompt_version: None,
            cost_details: None,
            completion_start_time: None,
            system: None,
            operation_name: None,
            response_id: None,
            finish_reasons: Vec::new(),
            is_stream: None,
            openinference_span_kind: None,
        }
    }
}

/// Trait for GenAI OpenTelemetry clients
pub trait GenAIOtelClient: Send + Sync {
    /// Create a span builder with given attributes
    fn create_span_builder(&self, attributes: OtelSpanAttributes) -> SpanBuilder;

    /// Create and start a span with given attributes
    fn start_span(&self, attributes: OtelSpanAttributes) -> opentelemetry::global::BoxedSpan;

    /// Create a trace for an LLM generation
    fn create_generation_span(
        &self,
        name: impl Into<String>,
        model: impl Into<String>,
        input: serde_json::Value,
    ) -> OtelSpanAttributes {
        OtelSpanBuilder::new(name)
            .span_type(OtelSpanType::Generation)
            .model(model)
            .input(input)
            .build()
    }

    /// Create a trace for an event
    fn create_event_span(
        &self,
        name: impl Into<String>,
        level: Option<String>,
    ) -> OtelSpanAttributes {
        let mut builder = OtelSpanBuilder::new(name).span_type(OtelSpanType::Event);

        if let Some(level) = level {
            builder = builder.level(level);
        }

        builder.build()
    }

    /// Create a custom span
    fn create_custom_span(&self, name: impl Into<String>) -> OtelSpanAttributes {
        OtelSpanBuilder::new(name)
            .span_type(OtelSpanType::Span)
            .build()
    }

    /// Start and automatically finish a span with a closure
    // #[allow(async_fn_in_trait)]
    fn with_span<F, T>(
        &self,
        attributes: OtelSpanAttributes,
        f: F,
    ) -> impl std::future::Future<Output = T> + Send + '_
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        async move {
            use opentelemetry::trace::Span;

            let mut span = self.start_span(attributes);

            // Execute the future
            let result = f.await;

            // End the span manually
            span.end();
            result
        }
    }

    /// Start and automatically finish a span with error handling
    fn with_span_result<F, T, E>(
        &self,
        attributes: OtelSpanAttributes,
        f: F,
    ) -> impl std::future::Future<Output = Result<T, E>> + Send + '_
    where
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        async move {
            use opentelemetry::trace::{Span, Status};

            let mut span = self.start_span(attributes);

            // Execute the future and handle errors
            let result = f.await;

            match &result {
                Ok(_) => {
                    // Set success status
                    span.set_status(Status::Ok);
                }
                Err(error) => {
                    // Use the record_error method for consistent error handling
                    self.record_error(&mut span, &error);
                    
                    // Log the error for additional visibility
                    tracing::error!(
                        error = %error,
                        span_name = span.span_context().span_id().to_string(),
                        "Error occurred in traced span"
                    );
                }
            }

            // End the span manually
            span.end();
            result
        }
    }

    /// Record an error on an existing span without ending it
    fn record_error(
        &self,
        span: &mut opentelemetry::global::BoxedSpan,
        error: &(dyn std::error::Error + Send + Sync),
    ) {
        use opentelemetry::trace::{Span, Status};

        // Set error status
        span.set_status(Status::error(error.to_string()));

        // Record error information as span attributes
        span.set_attribute(opentelemetry::KeyValue::new(
            "error.type",
            error
                .source()
                .map_or_else(|| "unknown".to_string(), |e| format!("{:?}", e)),
        ));
        span.set_attribute(opentelemetry::KeyValue::new(
            "error.message",
            error.to_string(),
        ));
        span.set_attribute(opentelemetry::KeyValue::new("error.occurred", true));

        // Add error chain if available
        let mut error_chain = Vec::new();
        let mut current_error: &dyn std::error::Error = error;
        while let Some(source) = current_error.source() {
            error_chain.push(source.to_string());
            current_error = source;
        }

        if !error_chain.is_empty() {
            span.set_attribute(opentelemetry::KeyValue::new(
                "error.chain",
                error_chain.join(" -> "),
            ));
        }

        // Record error as an event with stack trace if available
        let mut event_attributes = vec![
            opentelemetry::KeyValue::new("error.occurred", true),
            opentelemetry::KeyValue::new("error.message", error.to_string()),
            opentelemetry::KeyValue::new("error.timestamp", chrono::Utc::now().to_rfc3339()),
        ];

        // Add stack trace if available (simplified for this example)
        if let Some(backtrace) = std::backtrace::Backtrace::capture()
            .to_string()
            .lines()
            .take(10)
            .collect::<Vec<_>>()
            .get(0)
        {
            event_attributes.push(opentelemetry::KeyValue::new(
                "error.stack_trace",
                backtrace.to_string(),
            ));
        }

        span.add_event("error".to_string(), event_attributes);

        // Log the error for additional visibility
        tracing::error!(
            error = %error,
            error_chain = ?error_chain,
            span_id = span.span_context().span_id().to_string(),
            "Error recorded in span"
        );
    }

    /// Create an error span for standalone error logging
    fn create_error_span(
        &self,
        error: &(dyn std::error::Error + Send + Sync),
        context: Option<&str>,
    ) -> OtelSpanAttributes {
        let span_name = context.map_or_else(
            || format!("error: {}", error),
            |ctx| format!("error in {}: {}", ctx, error),
        );

        let mut metadata = HashMap::new();
        metadata.insert(
            "error_type".to_string(),
            serde_json::json!(format!("{:?}", error)),
        );
        metadata.insert(
            "error_message".to_string(),
            serde_json::json!(error.to_string()),
        );
        metadata.insert(
            "error_timestamp".to_string(),
            serde_json::json!(chrono::Utc::now().to_rfc3339()),
        );

        // Add error chain
        let mut error_chain = Vec::new();
        let mut current_error: &dyn std::error::Error = error;
        while let Some(source) = current_error.source() {
            error_chain.push(source.to_string());
            current_error = source;
        }
        if !error_chain.is_empty() {
            metadata.insert("error_chain".to_string(), serde_json::json!(error_chain));
        }

        OtelSpanBuilder::new(span_name)
            .span_type(OtelSpanType::Event)
            .level("ERROR")
            .status_message(error.to_string())
            .metadata(metadata)
            .tags(vec!["error".to_string(), "exception".to_string()])
            .build()
    }

    /// Log an error with automatic span creation
    fn log_error(
        &self,
        error: &(dyn std::error::Error + Send + Sync),
        context: Option<&str>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        let error_string = error.to_string();
        let context_string = context.map(|s| s.to_string());
        let error_attributes = self.create_error_span(error, context);

        async move {
            self.with_span(error_attributes, async move {
                tracing::error!(
                    error = %error_string,
                    context = ?context_string,
                    "Error logged with automatic span creation"
                );
            })
            .await;
        }
    }

    /// Enhanced span with comprehensive error handling and timeout
    fn with_span_timeout<F, T, E>(
        &self,
        attributes: OtelSpanAttributes,
        timeout: std::time::Duration,
        f: F,
    ) -> impl std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + '_
    where
        F: std::future::Future<Output = Result<T, E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
        T: Send + 'static,
    {
        async move {
            use opentelemetry::trace::{Span, Status};
            use tokio::time::timeout as tokio_timeout;

            let mut span = self.start_span(attributes);

            // Add timeout information to span
            span.set_attribute(opentelemetry::KeyValue::new(
                "timeout.duration_ms",
                timeout.as_millis() as i64,
            ));
            span.set_attribute(opentelemetry::KeyValue::new("timeout.enabled", true));

            let start_time = std::time::Instant::now();

            // Execute with timeout
            let result = tokio_timeout(timeout, f).await;

            let execution_time = start_time.elapsed();
            span.set_attribute(opentelemetry::KeyValue::new(
                "execution.duration_ms",
                execution_time.as_millis() as i64,
            ));

            match result {
                Ok(Ok(value)) => {
                    span.set_status(Status::Ok);
                    span.set_attribute(opentelemetry::KeyValue::new("execution.status", "success"));
                    span.end();
                    Ok(value)
                }
                Ok(Err(error)) => {
                    // Handle the actual error
                    span.set_status(Status::error(error.to_string()));
                    span.set_attribute(opentelemetry::KeyValue::new("execution.status", "error"));

                    self.record_error(&mut span, &error);
                    span.end();

                    Err(Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
                }
                Err(_timeout_error) => {
                    // Handle timeout
                    let timeout_msg = format!("Operation timed out after {:?}", timeout);
                    span.set_status(Status::error(timeout_msg.clone()));
                    span.set_attribute(opentelemetry::KeyValue::new("execution.status", "timeout"));
                    span.set_attribute(opentelemetry::KeyValue::new("error.type", "timeout"));
                    span.set_attribute(opentelemetry::KeyValue::new(
                        "error.message",
                        timeout_msg.clone(),
                    ));

                    span.add_event(
                        "timeout".to_string(),
                        vec![
                            opentelemetry::KeyValue::new("timeout.occurred", true),
                            opentelemetry::KeyValue::new(
                                "timeout.duration_ms",
                                timeout.as_millis() as i64,
                            ),
                            opentelemetry::KeyValue::new(
                                "timeout.timestamp",
                                chrono::Utc::now().to_rfc3339(),
                            ),
                        ],
                    );

                    tracing::error!(
                        timeout_duration = ?timeout,
                        execution_time = ?execution_time,
                        span_id = span.span_context().span_id().to_string(),
                        "Operation timed out in traced span"
                    );

                    span.end();
                    Err(timeout_msg.into())
                }
            }
        }
    }
}

/// Generic OpenTelemetry client implementation
/// Uses the global tracer provider initialized by command_utils::util::tracing
#[derive(Debug, Clone)]
pub struct GenericOtelClient {
    tracer_name: String,
}

impl GenericOtelClient {
    /// Create a new client that uses the global tracer provider
    pub fn new(tracer_name: impl Into<String>) -> Self {
        Self {
            tracer_name: tracer_name.into(),
        }
    }

    /// Get the global tracer for this client
    fn get_tracer(&self) -> BoxedTracer {
        global::tracer(self.tracer_name.clone())
    }

    /// Get the tracer name - exposed for compatibility with context-aware client
    pub fn get_tracer_name(&self) -> String {
        self.tracer_name.clone()
    }
}

impl GenAIOtelClient for GenericOtelClient {
    /// Create a span builder with OpenTelemetry attributes
    fn create_span_builder(&self, attributes: OtelSpanAttributes) -> SpanBuilder {
        let tracer = self.get_tracer();
        let mut builder = tracer.span_builder(attributes.name.clone());

        // Set span kind based on type
        builder = builder.with_kind(match attributes.span_type {
            OtelSpanType::Generation => SpanKind::Producer,
            OtelSpanType::Event => SpanKind::Internal,
            OtelSpanType::Span => SpanKind::Internal,
        });

        let mut key_values = vec![];

        // Core Langfuse observation attributes
        key_values.push(KeyValue::new(
            "langfuse.observation.type",
            match attributes.span_type {
                OtelSpanType::Span => "span",
                OtelSpanType::Generation => "generation",
                OtelSpanType::Event => "event",
            },
        ));

        // User and session identification
        if let Some(user_id) = attributes.user_id {
            key_values.push(KeyValue::new("user.id", user_id));
        }

        if let Some(session_id) = attributes.session_id {
            key_values.push(KeyValue::new("session.id", session_id));
        }

        // Version and release information
        if let Some(version) = attributes.version {
            key_values.push(KeyValue::new("langfuse.version", version));
        }

        if let Some(release) = attributes.release {
            key_values.push(KeyValue::new("langfuse.release", release));
        }

        // Tags as array values or comma-separated strings
        if !attributes.tags.is_empty() {
            // For compatibility with Langfuse server, use array format
            let tags_str = serde_json::to_string(&attributes.tags).unwrap_or_default();
            key_values.push(KeyValue::new("langfuse.trace.tags", tags_str));
        }

        // Model information (OpenTelemetry gen_ai.* standard attributes)
        if let Some(model) = attributes.model {
            key_values.push(KeyValue::new(
                "langfuse.observation.model.name",
                model.clone(),
            ));
            // Also add gen_ai standard attributes for compatibility
            key_values.push(KeyValue::new("gen_ai.request.model", model));
        }

        // OpenTelemetry gen_ai.* standard attributes for model parameters
        if let Some(model_parameters) = &attributes.model_parameters {
            // Add as JSON string for Langfuse
            if let Ok(params_str) = serde_json::to_string(model_parameters) {
                key_values.push(KeyValue::new(
                    "langfuse.observation.model_parameters",
                    params_str,
                ));
            }

            // Also add individual gen_ai.request.* attributes for OpenTelemetry compatibility
            for (key, value) in model_parameters {
                match value {
                    serde_json::Value::Number(n) if n.is_f64() => {
                        key_values.push(KeyValue::new(
                            format!("gen_ai.request.{}", key),
                            n.as_f64().unwrap(),
                        ));
                    }
                    serde_json::Value::Number(n) if n.is_i64() => {
                        key_values.push(KeyValue::new(
                            format!("gen_ai.request.{}", key),
                            n.as_i64().unwrap(),
                        ));
                    }
                    serde_json::Value::Bool(b) => {
                        key_values.push(KeyValue::new(format!("gen_ai.request.{}", key), *b));
                    }
                    serde_json::Value::String(s) => {
                        key_values
                            .push(KeyValue::new(format!("gen_ai.request.{}", key), s.clone()));
                    }
                    _ => {
                        if let Ok(value_str) = serde_json::to_string(value) {
                            key_values
                                .push(KeyValue::new(format!("gen_ai.request.{}", key), value_str));
                        }
                    }
                }
            }
        }

        // Observation level and status
        if let Some(level) = attributes.level {
            key_values.push(KeyValue::new("langfuse.observation.level", level));
        }

        if let Some(status_message) = attributes.status_message {
            key_values.push(KeyValue::new(
                "langfuse.observation.status_message",
                status_message,
            ));
        }

        if let Some(parent_observation_id) = attributes.parent_observation_id {
            key_values.push(KeyValue::new("parent_id", parent_observation_id.clone()));
            key_values.push(KeyValue::new("parentId", parent_observation_id.clone()));
            key_values.push(KeyValue::new("parent.id", parent_observation_id.clone()));
            key_values.push(KeyValue::new(
                "langfuse.observation.parent_observation_id",
                parent_observation_id,
            ));
        }

        // Input/output data as JSON strings and gen_ai.prompt/completion attributes
        if let Some(ref input) = attributes.data.input {
            if let Ok(input_str) = serde_json::to_string(input) {
                key_values.push(KeyValue::new(
                    "langfuse.observation.input",
                    input_str.clone(),
                ));
                // Add gen_ai.prompt for OpenTelemetry compatibility
                key_values.push(KeyValue::new("gen_ai.prompt", input_str));
            }
        }

        if let Some(ref output) = attributes.data.output {
            if let Ok(output_str) = serde_json::to_string(output) {
                key_values.push(KeyValue::new(
                    "langfuse.observation.output",
                    output_str.clone(),
                ));
                // Add gen_ai.completion for OpenTelemetry compatibility
                key_values.push(KeyValue::new("gen_ai.completion", output_str));
            }
        }

        // Metadata with proper Langfuse naming
        if let Some(metadata) = attributes.data.metadata {
            for (key, value) in metadata {
                if let Ok(value_str) = serde_json::to_string(&value) {
                    key_values.push(KeyValue::new(
                        format!("langfuse.observation.metadata.{}", key),
                        value_str,
                    ));
                }
            }
        }

        // Model parameters as JSON string
        if let Some(model_parameters) = attributes.model_parameters {
            if let Ok(params_str) = serde_json::to_string(&model_parameters) {
                key_values.push(KeyValue::new(
                    "langfuse.observation.model_parameters",
                    params_str,
                ));
            }
        }

        // Usage details as JSON string and individual gen_ai.usage.* attributes
        if let Some(usage) = &attributes.usage {
            if let Ok(usage_str) = serde_json::to_string(usage) {
                key_values.push(KeyValue::new(
                    "langfuse.observation.usage_details",
                    usage_str,
                ));
            }

            // Add individual gen_ai.usage.* attributes for OpenTelemetry compatibility
            for (key, value) in usage {
                match key.as_str() {
                    "input_tokens" | "promptTokens" | "prompt" => {
                        key_values.push(KeyValue::new("gen_ai.usage.input_tokens", *value));
                        key_values.push(KeyValue::new("llm.token_count.prompt", *value));
                    }
                    "output_tokens" | "completionTokens" | "completion" => {
                        key_values.push(KeyValue::new("gen_ai.usage.completion_tokens", *value));
                        key_values.push(KeyValue::new("llm.token_count.completion", *value));
                    }
                    "total_tokens" | "totalTokens" | "total" => {
                        key_values.push(KeyValue::new("gen_ai.usage.total_tokens", *value));
                        key_values.push(KeyValue::new("llm.token_count.total", *value));
                    }
                    _ => {
                        // Custom usage metrics
                        key_values.push(KeyValue::new(format!("gen_ai.usage.{}", key), *value));
                    }
                }
            }
        }

        // Cost details as JSON string and gen_ai.usage.cost
        if let Some(cost_details) = &attributes.cost_details {
            if let Ok(cost_str) = serde_json::to_string(cost_details) {
                key_values.push(KeyValue::new("langfuse.observation.cost_details", cost_str));
            }

            // Add total cost as gen_ai.usage.cost for OpenTelemetry compatibility
            if let Some(total_cost) = cost_details.get("total") {
                key_values.push(KeyValue::new("gen_ai.usage.cost", *total_cost));
            }
        }

        // Prompt information
        if let Some(prompt_name) = attributes.prompt_name {
            key_values.push(KeyValue::new("langfuse.prompt.name", prompt_name));
        }

        if let Some(prompt_version) = attributes.prompt_version {
            key_values.push(KeyValue::new(
                "langfuse.prompt.version",
                prompt_version.to_string(),
            ));
        }

        // Completion start time
        if let Some(completion_start_time) = attributes.completion_start_time {
            key_values.push(KeyValue::new(
                "langfuse.observation.completion_start_time",
                completion_start_time,
            ));
        }

        // Trace-level attributes
        if let Some(trace_id) = &attributes.trace_id {
            key_values.push(KeyValue::new("trace.id", trace_id.clone()));
            // For compatibility with OpenTelemetry standard
            key_values.push(KeyValue::new("trace_id", trace_id.clone()));
        }

        // Add explicit span_id if provided - important for hierarchical spans
        if let Some(span_id) = &attributes.span_id {
            key_values.push(KeyValue::new("span.id", span_id.clone()));
            // Also add for other systems' compatibility
            key_values.push(KeyValue::new("span_id", span_id.clone()));
            key_values.push(KeyValue::new("spanId", span_id.clone()));
            key_values.push(KeyValue::new("langfuse.observation.id", span_id.clone()));
        }

        if let Some(trace_name) = attributes.trace_name {
            key_values.push(KeyValue::new("langfuse.trace.name", trace_name));
        }

        if let Some(trace_input) = attributes.trace_input {
            if let Ok(input_str) = serde_json::to_string(&trace_input) {
                key_values.push(KeyValue::new("langfuse.trace.input", input_str));
            }
        }

        if let Some(trace_output) = attributes.trace_output {
            if let Ok(output_str) = serde_json::to_string(&trace_output) {
                key_values.push(KeyValue::new("langfuse.trace.output", output_str));
            }
        }

        if !attributes.trace_tags.is_empty() {
            let trace_tags_str = serde_json::to_string(&attributes.trace_tags).unwrap_or_default();
            key_values.push(KeyValue::new("langfuse.trace.tags", trace_tags_str));
        }

        if let Some(trace_public) = attributes.trace_public {
            key_values.push(KeyValue::new("langfuse.trace.public", trace_public));
        }

        if let Some(trace_metadata) = attributes.trace_metadata {
            for (key, value) in trace_metadata {
                if let Ok(value_str) = serde_json::to_string(&value) {
                    key_values.push(KeyValue::new(
                        format!("langfuse.trace.metadata.{}", key),
                        value_str,
                    ));
                }
            }
        }

        // Add OpenTelemetry gen_ai.* standard attributes for broader compatibility
        if let Some(system) = attributes.system {
            key_values.push(KeyValue::new("gen_ai.system", system));
        }

        if let Some(operation_name) = attributes.operation_name {
            key_values.push(KeyValue::new("gen_ai.operation.name", operation_name));
        }

        if let Some(response_id) = attributes.response_id {
            key_values.push(KeyValue::new("gen_ai.response.id", response_id));
        }

        if !attributes.finish_reasons.is_empty() {
            for (i, reason) in attributes.finish_reasons.iter().enumerate() {
                key_values.push(KeyValue::new(
                    format!("gen_ai.completion.{}.finish_reason", i),
                    reason.clone(),
                ));
            }
        }

        if let Some(is_stream) = attributes.is_stream {
            key_values.push(KeyValue::new("gen_ai.request.is_stream", is_stream));
        }

        // OpenInference span kind for compatibility with Phoenix/Arize
        if let Some(openinference_span_kind) = attributes.openinference_span_kind {
            key_values.push(KeyValue::new(
                "openinference.span.kind",
                openinference_span_kind,
            ));
        }

        // TraceLoop compatibility attributes
        if let Some(ref input) = attributes.data.input {
            if let Ok(input_str) = serde_json::to_string(input) {
                key_values.push(KeyValue::new("traceloop.entity.input", input_str));
            }
        }

        if let Some(ref output) = attributes.data.output {
            if let Ok(output_str) = serde_json::to_string(output) {
                key_values.push(KeyValue::new("traceloop.entity.output", output_str));
            }
        }

        // MLFlow compatibility attributes
        if let Some(ref input) = attributes.data.input {
            if let Ok(input_str) = serde_json::to_string(input) {
                key_values.push(KeyValue::new("mlflow.spanInputs", input_str));
            }
        }

        if let Some(ref output) = attributes.data.output {
            if let Ok(output_str) = serde_json::to_string(output) {
                key_values.push(KeyValue::new("mlflow.spanOutputs", output_str));
            }
        }

        // SmolAgents compatibility
        if let Some(ref input) = attributes.data.input {
            if let Ok(input_str) = serde_json::to_string(input) {
                key_values.push(KeyValue::new("input.value", input_str));
            }
        }

        if let Some(ref output) = attributes.data.output {
            if let Ok(output_str) = serde_json::to_string(output) {
                key_values.push(KeyValue::new("output.value", output_str));
            }
        }

        // Pydantic/Pipecat compatibility
        if let Some(ref input) = attributes.data.input {
            if let Ok(input_str) = serde_json::to_string(input) {
                key_values.push(KeyValue::new("input", input_str));
            }
        }

        if let Some(ref output) = attributes.data.output {
            if let Ok(output_str) = serde_json::to_string(output) {
                key_values.push(KeyValue::new("output", output_str));
            }
        }

        builder.with_attributes(key_values)
    }

    /// Create and start a span with attributes
    fn start_span(&self, attributes: OtelSpanAttributes) -> opentelemetry::global::BoxedSpan {
        let builder = self.create_span_builder(attributes);
        let tracer = self.get_tracer();
        tracer.build(builder)
    }
}

/// Langfuse specific OpenTelemetry client implementation
#[derive(Debug, Clone)]
pub struct LangfuseOtelClient {
    client: GenericOtelClient,
}

impl LangfuseOtelClient {
    /// Initialize Langfuse OpenTelemetry client using global tracer provider
    /// Note: The actual endpoint configuration should be done during tracing initialization
    /// This client just provides Langfuse-specific span creation methods
    pub fn new(tracer_name: impl Into<String>) -> Self {
        let client = GenericOtelClient::new(tracer_name);

        Self { client }
    }

    /// Initialize Langfuse OpenTelemetry client with a specific tracer name
    /// Timeout parameter is maintained for compatibility but not used since we rely on global configuration
    pub fn with_timeout(
        tracer_name: impl Into<String>,
        _config: LangfuseConfig,
        _timeout: Duration,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::new(tracer_name))
    }
}

impl GenAIOtelClient for LangfuseOtelClient {
    fn create_span_builder(&self, attributes: OtelSpanAttributes) -> SpanBuilder {
        self.client.create_span_builder(attributes)
    }

    fn start_span(&self, attributes: OtelSpanAttributes) -> opentelemetry::global::BoxedSpan {
        self.client.start_span(attributes)
    }
}

/// Builder for creating spans with fluent API
#[derive(Debug, Clone)]
pub struct OtelSpanBuilder {
    attributes: OtelSpanAttributes,
}

impl OtelSpanBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            attributes: OtelSpanAttributes {
                name: name.into(),
                ..Default::default()
            },
        }
    }

    pub fn span_type(mut self, span_type: OtelSpanType) -> Self {
        self.attributes.span_type = span_type;
        self
    }

    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.attributes.user_id = Some(user_id.into());
        self
    }

    pub fn session_id(mut self, session_id: impl Into<String>) -> Self {
        self.attributes.session_id = Some(session_id.into());
        self
    }

    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.attributes.version = Some(version.into());
        self
    }

    pub fn release(mut self, release: impl Into<String>) -> Self {
        self.attributes.release = Some(release.into());
        self
    }

    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.attributes.tags = tags;
        self
    }

    pub fn model(mut self, model: impl Into<String>) -> Self {
        self.attributes.model = Some(model.into());
        self
    }

    pub fn input(mut self, input: serde_json::Value) -> Self {
        self.attributes.data.input = Some(input);
        self
    }

    pub fn output(mut self, output: serde_json::Value) -> Self {
        self.attributes.data.output = Some(output);
        self
    }

    pub fn metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.attributes.data.metadata = Some(metadata);
        self
    }

    pub fn model_parameters(mut self, params: HashMap<String, serde_json::Value>) -> Self {
        self.attributes.model_parameters = Some(params);
        self
    }

    pub fn usage(mut self, usage: HashMap<String, i64>) -> Self {
        self.attributes.usage = Some(usage);
        self
    }

    pub fn level(mut self, level: impl Into<String>) -> Self {
        self.attributes.level = Some(level.into());
        self
    }

    pub fn status_message(mut self, message: impl Into<String>) -> Self {
        self.attributes.status_message = Some(message.into());
        self
    }

    pub fn parent_observation_id(mut self, id: impl Into<String>) -> Self {
        self.attributes.parent_observation_id = Some(id.into());
        self
    }

    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.attributes.trace_id = Some(id.into());
        self
    }

    pub fn trace_name(mut self, name: impl Into<String>) -> Self {
        self.attributes.trace_name = Some(name.into());
        self
    }

    pub fn trace_input(mut self, input: serde_json::Value) -> Self {
        self.attributes.trace_input = Some(input);
        self
    }

    pub fn trace_output(mut self, output: serde_json::Value) -> Self {
        self.attributes.trace_output = Some(output);
        self
    }

    pub fn trace_tags(mut self, tags: Vec<String>) -> Self {
        self.attributes.trace_tags = tags;
        self
    }

    pub fn trace_public(mut self, public: bool) -> Self {
        self.attributes.trace_public = Some(public);
        self
    }

    pub fn trace_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.attributes.trace_metadata = Some(metadata);
        self
    }

    pub fn prompt_name(mut self, name: impl Into<String>) -> Self {
        self.attributes.prompt_name = Some(name.into());
        self
    }

    pub fn prompt_version(mut self, version: i32) -> Self {
        self.attributes.prompt_version = Some(version);
        self
    }

    pub fn cost_details(mut self, cost_details: HashMap<String, f64>) -> Self {
        self.attributes.cost_details = Some(cost_details);
        self
    }

    pub fn completion_start_time(mut self, time: impl Into<String>) -> Self {
        self.attributes.completion_start_time = Some(time.into());
        self
    }

    pub fn system(mut self, system: impl Into<String>) -> Self {
        self.attributes.system = Some(system.into());
        self
    }

    pub fn operation_name(mut self, operation_name: impl Into<String>) -> Self {
        self.attributes.operation_name = Some(operation_name.into());
        self
    }

    pub fn response_id(mut self, response_id: impl Into<String>) -> Self {
        self.attributes.response_id = Some(response_id.into());
        self
    }

    pub fn finish_reasons(mut self, finish_reasons: Vec<String>) -> Self {
        self.attributes.finish_reasons = finish_reasons;
        self
    }

    pub fn is_stream(mut self, is_stream: bool) -> Self {
        self.attributes.is_stream = Some(is_stream);
        self
    }

    pub fn openinference_span_kind(mut self, span_kind: impl Into<String>) -> Self {
        self.attributes.openinference_span_kind = Some(span_kind.into());
        self
    }

    pub fn span_id(mut self, span_id: impl Into<String>) -> Self {
        self.attributes.span_id = Some(span_id.into());
        self
    }

    pub fn build(self) -> OtelSpanAttributes {
        self.attributes
    }

    // Create builder from existing attributes
    pub fn from_attributes(attributes: OtelSpanAttributes) -> Self {
        Self { attributes }
    }
}

/// Tracer client with support for hierarchical spans
pub trait HierarchicalSpanClient: GenAIOtelClient {
    /// Create a parent span and execute a function that may create child spans
    #[allow(async_fn_in_trait)]
    async fn with_parent_span<F, T>(&self, attributes: OtelSpanAttributes, f: F) -> T
    where
        F: std::future::Future<Output = T> + Send,
    {
        use opentelemetry::trace::{TraceContextExt, Tracer};

        let tracer = self.get_tracer();
        let builder = self.create_span_builder(attributes);
        let span = tracer.build(builder);

        // Create context with this span as the current span
        let context = Context::current().with_span(span);

        // Execute the function within the span context
        let result = {
            let _guard = context.attach();
            f.await
        };

        result
    }

    /// Create a child span that automatically inherits from the current context
    fn with_child_span<F, T>(
        &self,
        attributes: OtelSpanAttributes,
        f: F,
    ) -> impl std::future::Future<Output = T> + Send + '_
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        async move {
            use opentelemetry::trace::Tracer;

            let tracer = self.get_tracer();
            let builder = self.create_span_builder(attributes);

            // Build span with current context as parent (automatic parent-child relationship)
            let span = tracer.build(builder);
            let result = {
                let _ = Arc::new(trace::mark_span_as_active(span));
                f.await
            };
            // drop(_guard);

            result
        }
    }

    /// Create a child span from a specific parent context
    #[allow(async_fn_in_trait)]
    async fn with_child_span_from_context<F, T>(
        &self,
        attributes: OtelSpanAttributes,
        parent_context: &Context,
        f: F,
    ) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        use opentelemetry::trace::{TraceContextExt, Tracer};

        let tracer = self.get_tracer();
        let builder = self.create_span_builder(attributes);

        // Build span with specified parent context
        let span = tracer.build_with_context(builder, parent_context);

        // Create new context with this child span
        let new_context = parent_context.with_span(span);

        // Execute the function with the child span context
        let result = {
            let _guard = new_context.attach();
            f.await
        };

        result
    }

    /// Create a remote child span from propagated trace context (for distributed tracing)
    #[allow(async_fn_in_trait)]
    async fn with_remote_child_span<F, T>(
        &self,
        attributes: OtelSpanAttributes,
        carrier: &HashMap<String, String>,
        f: F,
    ) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        use opentelemetry::{
            global,
            trace::{TraceContextExt, Tracer},
        };

        // Extract context from carrier using the global propagator
        let parent_context = global::get_text_map_propagator(|propagator| {
            propagator.extract(&CarrierExtractor(carrier))
        });

        let tracer = self.get_tracer();
        let builder = self.create_span_builder(attributes);

        // Build span with extracted parent context
        let span = tracer.build_with_context(builder, &parent_context);

        // Create new context with this child span
        let new_context = parent_context.with_span(span);

        // Execute the function with the child span context
        let _guard = new_context.attach();
        f.await
    }

    /// Inject current span context into a carrier for distributed tracing
    fn inject_span_context(&self, carrier: &mut HashMap<String, String>) {
        use opentelemetry::global;

        global::get_text_map_propagator(|propagator| {
            let context = Context::current();
            propagator.inject_context(&context, &mut CarrierInjector(carrier));
        });
    }

    /// Get the tracer for span operations - should be implemented by the concrete client
    fn get_tracer(&self) -> BoxedTracer;

    /// Create a trace containing multiple related spans
    #[allow(async_fn_in_trait)]
    async fn with_trace<F, T>(
        &self,
        trace_name: impl Into<String>,
        trace_id: Option<String>,
        tags: Vec<String>,
        f: F,
    ) -> T
    where
        F: std::future::Future<Output = T> + Send,
    {
        // Create trace span attributes
        let mut trace_attributes = OtelSpanBuilder::new(trace_name)
            .span_type(OtelSpanType::Span)
            .trace_tags(tags)
            .build();

        // Set trace_id if provided
        if let Some(id) = trace_id {
            trace_attributes.trace_id = Some(id);
        }

        // Execute with trace context
        self.with_parent_span(trace_attributes, f).await
    }
}
// Implement the trait for both client types
impl HierarchicalSpanClient for GenericOtelClient {
    fn get_tracer(&self) -> BoxedTracer {
        self.get_tracer()
    }
}

impl HierarchicalSpanClient for LangfuseOtelClient {
    fn get_tracer(&self) -> BoxedTracer {
        self.client.get_tracer()
    }
}
