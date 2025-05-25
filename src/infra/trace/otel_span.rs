use opentelemetry::{
    global::{self, BoxedTracer}, trace::{SpanBuilder, SpanKind, Tracer}, KeyValue
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

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
    fn with_span<F, T>(&self, attributes: OtelSpanAttributes, f: F) -> impl std::future::Future<Output = T> + Send + '_
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {async move {
        use opentelemetry::trace::Span;
        
        let mut span = self.start_span(attributes);
        
        // Execute the future
        let result = f.await;
        
        // End the span manually
        span.end();
        result
    } }
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
            key_values.push(KeyValue::new(
                "langfuse.observation.prompt.name",
                prompt_name,
            ));
        }

        if let Some(prompt_version) = attributes.prompt_version {
            key_values.push(KeyValue::new(
                "langfuse.observation.prompt.version",
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
            key_values.push(KeyValue::new(
                "langfuse.trace.public",
                trace_public.to_string(),
            ));
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
        use opentelemetry::trace::Span;
        
        // Start parent span
        let mut parent_span = self.start_span(attributes);
        
        // Execute the function
        let result = f.await;

        // End parent span
        parent_span.end();

        result
    }

    /// Create a child span that explicitly references a parent span
    #[allow(async_fn_in_trait)]
    async fn with_child_span<F, T>(
        &self,
        attributes: OtelSpanAttributes,
        parent_span_id: Option<String>,
        f: F,
    ) -> T
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        // If parent ID is provided, add it to the attributes
        let attributes = if let Some(parent_id) = parent_span_id {
            OtelSpanBuilder::from_attributes(attributes)
                .parent_observation_id(parent_id)
                .build()
        } else {
            attributes
        };

        // Create and use the span
        self.with_span(attributes, f).await
    }

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
        use rand::Rng;

        // Generate trace ID if not provided
        let trace_id = trace_id.unwrap_or_else(|| {
            let mut rng = rand::rng();
            format!("trace-{:x}", rng.random::<u128>())
        });

        // Create trace span attributes
        let trace_attributes = OtelSpanBuilder::new(trace_name)
            .span_type(OtelSpanType::Span)
            .trace_id(trace_id.clone())
            .trace_tags(tags)
            .build();

        // Execute with trace context
        self.with_parent_span(trace_attributes, f).await
    }
}

// Implement the trait for both client types
impl HierarchicalSpanClient for GenericOtelClient {}
impl HierarchicalSpanClient for LangfuseOtelClient {}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rand::rand_core::le;
    use serde_json::json;

    /// Common setup for integration tests that require OTLP endpoints
    async fn setup_integration_test() -> Result<GenericOtelClient, Box<dyn std::error::Error>> {
        // Set up environment variables for testing
        // We're explicitly setting the OTLP endpoint for the test
        let otlp_addr = "http://otel-collector.default.svc.cluster.local:4317".to_string();
        // let otlp_http_addr = "http://otel-collector.default.svc.cluster.local:4318".to_string();

        // Force override any existing configuration
        std::env::set_var("OTLP_ADDR", &otlp_addr);
        // std::env::set_var("OTLP_HTTP_ADDR", &otlp_http_addr);
        // Ensure OTLP export is enabled
        // std::env::set_var("ENABLE_OTLP_EXPORT", "true");
        // // Set a shorter OTLP timeout for testing
        // std::env::set_var("OTLP_TIMEOUT_MS", "500");
        // // Set batch settings to force more frequent exports
        // std::env::set_var("OTLP_BATCH_MAX_SIZE", "10");
        // std::env::set_var("OTLP_BATCH_TIMEOUT_MS", "500");

        println!("Running integration test with OTLP endpoint: {}", otlp_addr);

        // Initialize tracing with OTLP exporter using command_utils
        let logging_config = command_utils::util::tracing::LoggingConfig {
            app_name: Some("otel-span-integration-test".to_string()),
            level: Some("DEBUG".to_string()), // Use DEBUG to see more OTEL logs
            file_name: None,
            file_dir: None,
            use_json: false,
            use_stdout: true,
        };

        // Force cleanup of any previous tracer provider
        command_utils::util::tracing::shutdown_tracer_provider();

        // Initialize tracer with our settings
        command_utils::util::tracing::tracing_init(logging_config).await.unwrap();

        println!(
            "OpenTelemetry tracer initialized for OTLP endpoint: {}",
            otlp_addr
        );

        // Create and return client
        // Ok(LangfuseOtelClient::new("otel-span-integration-test"))
        Ok(GenericOtelClient::new("otel-span-integration-test"))
    }

    /// Common cleanup for integration tests
    async fn cleanup_integration_test() {
        println!("Waiting for spans to be exported...");

        // Allow more time for OTLP export - increase the wait time
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Shutdown tracer providers to ensure data is flushed
        // This is critical to ensure all data is sent
        println!("Shutting down tracer provider...");
        command_utils::util::tracing::shutdown_tracer_provider();

        // Additional wait to ensure export completes after shutdown
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        println!("Integration test completed successfully");
        println!("Check your OTLP endpoint/Langfuse dashboard for the test spans");
    }

    #[test]
    fn test_span_builder() {
        let attributes = OtelSpanBuilder::new("test-span")
            .span_type(OtelSpanType::Generation)
            .user_id("user-123")
            .session_id("session-456")
            .model("gpt-4")
            .input(json!({"prompt": "Hello world"}))
            .output(json!({"response": "Hello!"}))
            .tags(vec!["test".to_string(), "example".to_string()])
            .build();

        assert_eq!(attributes.name, "test-span");
        assert!(matches!(attributes.span_type, OtelSpanType::Generation));
        assert_eq!(attributes.user_id, Some("user-123".to_string()));
        assert_eq!(attributes.session_id, Some("session-456".to_string()));
        assert_eq!(attributes.model, Some("gpt-4".to_string()));
        assert_eq!(attributes.tags, vec!["test", "example"]);
    }

    #[test]
    fn test_langfuse_server_compatibility() {
        // Create a span that matches the Langfuse server test case structure
        let attributes = OtelSpanBuilder::new("my-generation")
            .span_type(OtelSpanType::Generation)
            .user_id("my-user")
            .session_id("my-session")
            .version("trace-0.0.1")
            .model("gpt-4o")
            .level("WARNING")
            .status_message("nothing to report")
            .input(json!([{"role": "user", "content": "hello"}]))
            .output(json!({"role": "assistant", "content": "what's up?"}))
            .prompt_name("my-prompt")
            .prompt_version(1)
            .usage({
                let mut usage = HashMap::new();
                usage.insert("input_tokens".to_string(), 123);
                usage.insert("output_tokens".to_string(), 456);
                usage
            })
            .cost_details({
                let mut cost = HashMap::new();
                cost.insert("input_tokens".to_string(), 0.0001);
                cost.insert("output_tokens".to_string(), 0.002);
                cost
            })
            .metadata({
                let mut metadata = HashMap::new();
                metadata.insert("key1".to_string(), json!("value1"));
                metadata.insert("key2".to_string(), json!("value2"));
                metadata
            })
            .trace_name("test-trace")
            .trace_input(json!([{"role": "user", "content": "hello"}]))
            .trace_output(json!({"role": "assistant", "content": "what's up?"}))
            .trace_tags(vec!["tag2".to_string()])
            .trace_public(true)
            .trace_metadata({
                let mut trace_metadata = HashMap::new();
                trace_metadata.insert("trace-key1".to_string(), json!("value1"));
                trace_metadata.insert("trace-key2".to_string(), json!("value2"));
                trace_metadata
            })
            .completion_start_time("2025-04-30T15:28:50.686390Z".to_string())
            .build();

        // Verify core attributes
        assert_eq!(attributes.name, "my-generation");
        assert!(matches!(attributes.span_type, OtelSpanType::Generation));
        assert_eq!(attributes.user_id, Some("my-user".to_string()));
        assert_eq!(attributes.session_id, Some("my-session".to_string()));
        assert_eq!(attributes.version, Some("trace-0.0.1".to_string()));
        assert_eq!(attributes.model, Some("gpt-4o".to_string()));
        assert_eq!(attributes.level, Some("WARNING".to_string()));
        assert_eq!(
            attributes.status_message,
            Some("nothing to report".to_string())
        );

        // Verify prompt attributes
        assert_eq!(attributes.prompt_name, Some("my-prompt".to_string()));
        assert_eq!(attributes.prompt_version, Some(1));

        // Verify usage and cost details
        assert!(attributes.usage.is_some());
        assert!(attributes.cost_details.is_some());

        // Verify trace-level attributes
        assert_eq!(attributes.trace_name, Some("test-trace".to_string()));
        assert_eq!(attributes.trace_public, Some(true));
        assert_eq!(attributes.trace_tags, vec!["tag2"]);
        assert!(attributes.trace_input.is_some());
        assert!(attributes.trace_output.is_some());
        assert!(attributes.trace_metadata.is_some());

        // Verify completion start time
        assert_eq!(
            attributes.completion_start_time,
            Some("2025-04-30T15:28:50.686390Z".to_string())
        );
    }

    #[test]
    fn test_genai_standard_attributes_mapping() {
        // Test that gen_ai.* attributes are properly mapped following server expectation
        let mut usage = HashMap::new();
        usage.insert("input_tokens".to_string(), 14);
        usage.insert("output_tokens".to_string(), 96);
        usage.insert("total_tokens".to_string(), 110);

        let mut cost_details = HashMap::new();
        cost_details.insert("total".to_string(), 0.000151);

        let attributes = OtelSpanBuilder::new("openai.chat.completions")
            .span_type(OtelSpanType::Generation)
            .system("openai")
            .operation_name("chat")
            .model("gpt-3.5-turbo")
            .input(json!({
                "messages": [{"role": "user", "content": "What is LLM Observability?"}]
            }))
            .output(json!({
                "role": "assistant",
                "content": "LLM Observability stands for logs, metrics, and traces observability."
            }))
            .usage(usage)
            .cost_details(cost_details)
            .response_id("chatcmpl-AugxBIoQzz2zFMWFoiyS3Vmm1OuQI")
            .finish_reasons(vec!["stop".to_string()])
            .is_stream(false)
            .openinference_span_kind("LLM")
            .build();

        // Verify that the attributes would map to the correct gen_ai.* keys
        assert_eq!(attributes.system, Some("openai".to_string()));
        assert_eq!(attributes.operation_name, Some("chat".to_string()));
        assert_eq!(attributes.model, Some("gpt-3.5-turbo".to_string()));
        assert_eq!(
            attributes.response_id,
            Some("chatcmpl-AugxBIoQzz2zFMWFoiyS3Vmm1OuQI".to_string())
        );
        assert_eq!(attributes.finish_reasons, vec!["stop"]);
        assert_eq!(attributes.is_stream, Some(false));
        assert_eq!(attributes.openinference_span_kind, Some("LLM".to_string()));

        // Verify usage mapping for gen_ai.usage.* attributes
        let usage = attributes.usage.unwrap();
        assert_eq!(usage.get("input_tokens"), Some(&14));
        assert_eq!(usage.get("output_tokens"), Some(&96));
        assert_eq!(usage.get("total_tokens"), Some(&110));

        // Verify cost mapping for gen_ai.usage.cost
        let cost = attributes.cost_details.unwrap();
        assert_eq!(cost.get("total"), Some(&0.000151));
    }

    #[test]
    fn test_nested_span_structure() {
        // Create a parent span
        let parent_attributes = OtelSpanBuilder::new("llm-conversation")
            .span_type(OtelSpanType::Span)
            .user_id("user_nested_test")
            .session_id("session_nested_123")
            .trace_id("trace_abc123")
            .tags(vec!["conversation".to_string(), "multi-turn".to_string()])
            .build();

        // Create a child generation span
        let mut child_usage = HashMap::new();
        child_usage.insert("promptTokens".to_string(), 89);
        child_usage.insert("completionTokens".to_string(), 156);
        child_usage.insert("totalTokens".to_string(), 245);

        let child_attributes = OtelSpanBuilder::new("llm-response-generation")
            .span_type(OtelSpanType::Generation)
            .user_id("user_nested_test")
            .session_id("session_nested_123")
            .trace_id("trace_abc123")
            .parent_observation_id("parent_span_id_456")
            .model("gpt-4-turbo")
            .input(json!({
                "messages": [
                    {"role": "user", "content": "What are the benefits of renewable energy?"}
                ]
            }))
            .output(json!({
                "content": "Renewable energy offers several key benefits including environmental protection, economic advantages, and energy security improvements."
            }))
            .usage(child_usage)
            .tags(vec!["generation".to_string(), "renewable-energy".to_string()])
            .build();

        // Verify parent span
        assert_eq!(parent_attributes.name, "llm-conversation");
        assert!(matches!(parent_attributes.span_type, OtelSpanType::Span));
        assert_eq!(parent_attributes.trace_id, Some("trace_abc123".to_string()));

        // Verify child span
        assert_eq!(child_attributes.name, "llm-response-generation");
        assert!(matches!(
            child_attributes.span_type,
            OtelSpanType::Generation
        ));
        assert_eq!(
            child_attributes.parent_observation_id,
            Some("parent_span_id_456".to_string())
        );
        assert_eq!(child_attributes.trace_id, Some("trace_abc123".to_string()));
        assert!(child_attributes.usage.is_some());
    }

    #[test]
    fn test_span_builder_attributes_serialization() {
        // Test that complex data structures serialize correctly
        let complex_input = json!({
            "prompt": {
                "text": "Generate a detailed analysis",
                "context": {
                    "domain": "finance",
                    "audience": "professional",
                    "constraints": ["accuracy", "brevity", "actionable"]
                },
                "examples": [
                    {"input": "example1", "output": "result1"},
                    {"input": "example2", "output": "result2"}
                ]
            },
            "parameters": {
                "style": "analytical",
                "depth": "comprehensive",
                "format": "structured"
            }
        });

        let attributes = OtelSpanBuilder::new("complex-analysis-generation")
            .span_type(OtelSpanType::Generation)
            .input(complex_input)
            .build();

        assert!(attributes.data.input.is_some());

        // Verify the input can be serialized back to JSON
        let input_data = attributes.data.input.unwrap();
        assert!(input_data.get("prompt").is_some());
        assert!(input_data["prompt"].get("context").is_some());
        assert!(input_data["prompt"]["context"].get("constraints").is_some());
        assert!(
            input_data["prompt"]["context"]["constraints"]
                .as_array()
                .unwrap()
                .len()
                == 3
        );
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_langfuse_integration() -> Result<(), Box<dyn std::error::Error>> {
        let client = setup_integration_test().await?;

        {
            // Test 1: Create and send a generation span
            let generation_attributes = OtelSpanBuilder::new("integration-test-generation")
                .span_type(OtelSpanType::Generation)
                .user_id("integration-test-user")
                .session_id("integration-test-session")
                .version("1.0.0")
                .model("gpt-4o")
                .input(json!({
                    "messages": [
                        {"role": "user", "content": "Hello, this is an integration test"}
                    ]
                }))
                .output(json!({
                    "content": "Hello! This is a response from the integration test."
                }))
                .usage({
                    let mut usage = HashMap::new();
                    usage.insert("input_tokens".to_string(), 12);
                    usage.insert("output_tokens".to_string(), 15);
                    usage.insert("total_tokens".to_string(), 27);
                    usage
                })
                .cost_details({
                    let mut cost = HashMap::new();
                    cost.insert("total".to_string(), 0.0001);
                    cost.insert("input".to_string(), 0.00004);
                    cost.insert("output".to_string(), 0.00006);
                    cost
                })
                .level("INFO")
                .tags(vec!["integration".to_string(), "test".to_string()])
                .build();

            // Start span and perform some work
            let _span = client
                .with_span(generation_attributes, async {
                    tracing::info!("Processing generation request in integration test");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    tracing::info!("Generation completed in integration test");
                })
                .await;

            // Test 2: Create and send an event span
            let event_attributes = OtelSpanBuilder::new("integration-test-event")
                .span_type(OtelSpanType::Event)
                .user_id("integration-test-user")
                .session_id("integration-test-session")
                .level("WARNING")
                .input(json!({
                    "event_type": "test_execution",
                    "test_name": "langfuse_integration"
                }))
                .metadata({
                    let mut metadata = HashMap::new();
                    metadata.insert("test_framework".to_string(), json!("tokio"));
                    metadata.insert("environment".to_string(), json!("integration"));
                    metadata
                })
                .tags(vec!["event".to_string(), "integration".to_string()])
                .build();

            let _event_span = client
                .with_span(event_attributes, async {
                    tracing::warn!("Test event executed in integration test");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                })
                .await;

            // Test 3: Create nested spans for trace hierarchy
            let parent_attributes = OtelSpanBuilder::new("integration-test-parent")
                .span_type(OtelSpanType::Span)
                .user_id("integration-test-user")
                .session_id("integration-test-session")
                .trace_name("integration-test-trace")
                .trace_input(json!({"operation": "nested_span_test"}))
                .trace_tags(vec!["parent".to_string(), "nested".to_string()])
                .build();

            let _parent_span = client
                .with_parent_span(parent_attributes, async {
                    tracing::info!("Starting parent span in integration test");

                    // Child span
                    let child_attributes = OtelSpanBuilder::new("integration-test-child")
                        .span_type(OtelSpanType::Generation)
                        .user_id("integration-test-user")
                        .session_id("integration-test-session")
                        .model("gpt-3.5-turbo")
                        .input(json!({"prompt": "Child span test"}))
                        .output(json!({"response": "Child span response"}))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 5);
                            usage.insert("output_tokens".to_string(), 8);
                            usage.insert("total_tokens".to_string(), 13);
                            usage
                        })
                        .build();

                    let _child_span = client
                        .with_child_span(
                            child_attributes,
                            Some("parent_span_id_456".to_string()),
                            async {
                                tracing::info!("Processing child span in integration test");
                                tokio::time::sleep(std::time::Duration::from_millis(75)).await;
                            },
                        )
                        .await;

                    tracing::info!("Parent span completed in integration test");
                })
                .await;
        }
        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_comprehensive_generation_span_integration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = setup_integration_test().await?;

        // Create a comprehensive generation span similar to Langfuse server tests
        let mut model_parameters = HashMap::new();
        model_parameters.insert("temperature".to_string(), json!(0.7));
        model_parameters.insert("max_tokens".to_string(), json!(1000));
        model_parameters.insert("top_p".to_string(), json!(0.9));
        model_parameters.insert("frequency_penalty".to_string(), json!(0.0));
        model_parameters.insert("presence_penalty".to_string(), json!(0.0));

        let mut usage = HashMap::new();
        usage.insert("promptTokens".to_string(), 45);
        usage.insert("completionTokens".to_string(), 123);
        usage.insert("totalTokens".to_string(), 168);

        let mut metadata = HashMap::new();
        metadata.insert("environment".to_string(), json!("production"));
        metadata.insert("model_version".to_string(), json!("gpt-4-0314"));
        metadata.insert("request_id".to_string(), json!("req_123456789"));
        metadata.insert("user_agent".to_string(), json!("rust-client/1.0.0"));

        let input = json!({
            "messages": [
                {
                    "role": "system",
                    "content": "You are a helpful assistant that provides accurate and concise answers."
                },
                {
                    "role": "user",
                    "content": "Explain the concept of machine learning in simple terms."
                }
            ],
            "max_tokens": 1000,
            "temperature": 0.7
        });

        let output = json!({
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Machine learning is a branch of artificial intelligence that enables computers to learn and make decisions from data without being explicitly programmed for every task. Think of it like teaching a computer to recognize patterns, similar to how humans learn from experience."
                    },
                    "finish_reason": "stop",
                    "index": 0
                }
            ],
            "usage": {
                "prompt_tokens": 45,
                "completion_tokens": 123,
                "total_tokens": 168
            }
        });

        let attributes = OtelSpanBuilder::new("openai-chat-completion")
            .span_type(OtelSpanType::Generation)
            .user_id("user_12345")
            .session_id("session_abcde")
            .version("1.2.3")
            .release("v2024.1")
            .model("gpt-4")
            .input(input)
            .output(output)
            .model_parameters(model_parameters)
            .usage(usage)
            .metadata(metadata)
            .tags(vec![
                "production".to_string(),
                "chat-completion".to_string(),
                "gpt-4".to_string(),
            ])
            .build();
        {
            // Send comprehensive span to OTLP endpoint
            let _span = client
                .with_span(attributes, async {
                    tracing::info!("Processing comprehensive generation with all attributes");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    tracing::info!("Comprehensive generation completed");
                })
                .await;
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_event_span_with_large_data_integration() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = setup_integration_test().await?;

        // Create an event span with substantial metadata and data
        let mut metadata = HashMap::new();
        metadata.insert("event_type".to_string(), json!("user_interaction"));
        metadata.insert("source".to_string(), json!("web_app"));
        metadata.insert(
            "user_agent".to_string(),
            json!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
        );
        metadata.insert("ip_address".to_string(), json!("192.168.1.100"));
        metadata.insert("timestamp".to_string(), json!("2024-01-15T10:30:00Z"));
        metadata.insert("request_duration_ms".to_string(), json!(234));

        let event_data = json!({
            "action": "button_click",
            "element_id": "submit_form",
            "page_url": "https://example.com/contact",
            "form_data": {
                "name": "John Doe",
                "email": "john.doe@example.com",
                "message": "I would like to know more about your services and pricing options."
            },
            "user_session": {
                "session_duration": 1234567,
                "pages_visited": 5,
                "previous_page": "https://example.com/about"
            },
            "device_info": {
                "screen_resolution": "1920x1080",
                "browser": "Chrome",
                "os": "Windows 10"
            }
        });

        let attributes = OtelSpanBuilder::new("user-form-submission")
            .span_type(OtelSpanType::Event)
            .user_id("user_67890")
            .session_id("session_xyz789")
            .level("INFO")
            .input(event_data)
            .metadata(metadata)
            .tags(vec![
                "user_interaction".to_string(),
                "form_submission".to_string(),
                "web_app".to_string(),
            ])
            .build();

        // Send event span to OTLP endpoint
        let _span = client
            .with_span(attributes, async {
                tracing::info!("Processing large event data submission");
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                tracing::info!("Event data processing completed");
            })
            .await;

        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_opentelemetry_genai_standard_attributes_integration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = setup_integration_test().await?;

        // Test OpenTelemetry gen_ai.* standard attributes compatibility
        let mut model_parameters = HashMap::new();
        model_parameters.insert("temperature".to_string(), json!(0.7));
        model_parameters.insert("max_tokens".to_string(), json!(2048));
        model_parameters.insert("top_p".to_string(), json!(0.9));
        model_parameters.insert("frequency_penalty".to_string(), json!(0.1));
        model_parameters.insert("presence_penalty".to_string(), json!(0.05));

        let mut usage = HashMap::new();
        usage.insert("input_tokens".to_string(), 156);
        usage.insert("output_tokens".to_string(), 234);
        usage.insert("total_tokens".to_string(), 390);

        let mut cost_details = HashMap::new();
        cost_details.insert("total".to_string(), 0.0078);
        cost_details.insert("input".to_string(), 0.00312);
        cost_details.insert("output".to_string(), 0.00468);

        let attributes = OtelSpanBuilder::new("openai.chat.completions")
            .span_type(OtelSpanType::Generation)
            .system("openai")
            .operation_name("chat")
            .model("gpt-4-turbo")
            .input(json!({
                "messages": [
                    {"role": "user", "content": "What is LLM Observability?"}
                ]
            }))
            .output(json!({
                "content": "LLM Observability is the practice of monitoring and analyzing the behavior and performance of large language models."
            }))
            .model_parameters(model_parameters)
            .usage(usage)
            .cost_details(cost_details)
            .response_id("chatcmpl-AugxBIoQzz2zFMWFoiyS3Vmm1OuQI")
            .finish_reasons(vec!["stop".to_string()])
            .is_stream(false)
            .openinference_span_kind("LLM")
            .build();

        // Send span with gen_ai.* standard attributes to OTLP endpoint
        let _span = client
            .with_span(attributes, async {
                tracing::info!("Processing OpenTelemetry gen_ai standard attributes");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                tracing::info!("OpenTelemetry gen_ai attributes span completed");
            })
            .await;

        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_multi_library_compatibility_integration() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = setup_integration_test().await?;

        // Test compatibility with multiple OTEL libraries
        let input_data = json!({
            "question": "What is the capital of France?",
            "context": "Geography quiz"
        });

        let output_data = json!({
            "answer": "Paris",
            "confidence": 0.95
        });

        let attributes = OtelSpanBuilder::new("llm-qa-generation")
            .span_type(OtelSpanType::Generation)
            .model("claude-3-opus")
            .input(input_data)
            .output(output_data)
            .system("anthropic")
            .operation_name("completion")
            .user_id("test-user-789")
            .session_id("session-qa-123")
            .build();

        // Send span that should be compatible with multiple libraries
        // - Langfuse: langfuse.observation.input, langfuse.observation.output
        // - Gen AI: gen_ai.prompt, gen_ai.completion
        // - TraceLoop: traceloop.entity.input, traceloop.entity.output
        // - MLFlow: mlflow.spanInputs, mlflow.spanOutputs
        // - SmolAgents: input.value, output.value
        // - Pydantic: input, output
        let _span = client
            .with_span(attributes, async {
                tracing::info!("Processing multi-library compatibility span");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                tracing::info!("Multi-library compatibility span completed");
            })
            .await;

        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_vendor_specific_otel_spans_integration() -> Result<(), Box<dyn std::error::Error>>
    {
        let client = setup_integration_test().await?;

        // Test OpenLit style span
        let openlit_attributes = OtelSpanBuilder::new("openai.chat.completions")
            .span_type(OtelSpanType::Generation)
            .system("openai")
            .operation_name("chat")
            .model("gpt-3.5-turbo")
            .usage({
                let mut usage = HashMap::new();
                usage.insert("input_tokens".to_string(), 14);
                usage.insert("output_tokens".to_string(), 96);
                usage.insert("total_tokens".to_string(), 110);
                usage
            })
            .cost_details({
                let mut cost = HashMap::new();
                cost.insert("total".to_string(), 0.000151);
                cost
            })
            .finish_reasons(vec!["stop".to_string()])
            .build();

        let _openlit_span = client
            .with_span(openlit_attributes, async {
                tracing::info!("Processing OpenLit style span");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            })
            .await;

        // Test TraceLoop style span
        let traceloop_attributes = OtelSpanBuilder::new("openai.chat")
            .span_type(OtelSpanType::Generation)
            .model("gpt-3.5-turbo")
            .input(json!({
                "messages": [{"role": "user", "content": "What is LLM Observability?"}]
            }))
            .output(json!({
                "content": "LLM Observability is monitoring and analyzing LLM behavior."
            }))
            .usage({
                let mut usage = HashMap::new();
                usage.insert("completion_tokens".to_string(), 173);
                usage.insert("prompt_tokens".to_string(), 14);
                usage.insert("total_tokens".to_string(), 187);
                usage
            })
            .build();

        let _traceloop_span = client
            .with_span(traceloop_attributes, async {
                tracing::info!("Processing TraceLoop style span");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            })
            .await;

        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_hierarchical_spans_integration() -> Result<(), Box<dyn std::error::Error>> {
        // Set up the client with OTLP exporter
        let client = setup_integration_test().await?;

        // Create a complete workflow with hierarchical spans
        // Root trace with a meaningful name
        client.with_trace(
            "customer-support-workflow",
            None,
            vec!["support".to_string(), "ai-workflow".to_string(), "integration-test".to_string()],
            async {
                tracing::info!("Starting customer support workflow trace");

                // Parent span: Customer inquiry handling
                let inquiry_attributes = OtelSpanBuilder::new("customer-inquiry")
                    .span_type(OtelSpanType::Span)
                    .user_id("customer-12345")
                    .session_id("session-67890")
                    .input(json!({
                        "channel": "email",
                        "subject": "Product feature inquiry",
                        "priority": "medium",
                        "category": "pre-sales",
                        "timestamp": "2025-05-25T10:30:00Z"
                    }))
                    .metadata({
                        let mut metadata = HashMap::new();
                        metadata.insert("customer_type".to_string(), json!("prospect"));
                        metadata.insert("region".to_string(), json!("APAC"));
                        metadata.insert("language".to_string(), json!("Japanese"));
                        metadata
                    })
                    .tags(vec!["email".to_string(), "pre-sales".to_string()])
                    .build();

                // Execute parent span with nested children
                client.with_parent_span(inquiry_attributes, async {
                    tracing::info!("Processing customer inquiry");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

                    // Child span 1: Analyze inquiry content with AI
                    let analysis_attributes = OtelSpanBuilder::new("analyze-inquiry-content")
                        .span_type(OtelSpanType::Generation)
                        .model("gpt-4o")
                        .input(json!({
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "You are a customer support assistant. Analyze the customer inquiry and identify key points."
                                },
                                {
                                    "role": "user", 
                                    "content": "I'm interested in your enterprise plan, but I need to know if it supports SSO and has an audit log feature."
                                }
                            ]
                        }))
                        .output(json!({
                            "analysis": {
                                "inquiry_type": "feature_information",
                                "features_mentioned": ["SSO", "audit_log"],
                                "product_interest": "enterprise_plan",
                                "sentiment": "neutral",
                                "priority": "medium"
                            }
                        }))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 78);
                            usage.insert("output_tokens".to_string(), 53);
                            usage.insert("total_tokens".to_string(), 131);
                            usage
                        })
                        .cost_details({
                            let mut cost = HashMap::new();
                            cost.insert("total".to_string(), 0.00262);
                            cost
                        })
                        .build();

                    client.with_child_span(analysis_attributes, Some("customer-inquiry".to_string()), async {
                        tracing::info!("Analyzing inquiry content with AI");
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }).await;

                    // Child span 2: Retrieve product information
                    let retrieval_attributes = OtelSpanBuilder::new("retrieve-product-info")
                        .span_type(OtelSpanType::Span)
                        .input(json!({
                            "product": "enterprise_plan",
                            "features_requested": ["SSO", "audit_log"]
                        }))
                        .output(json!({
                            "enterprise_plan": {
                                "SSO": {
                                    "supported": true,
                                    "providers": ["Okta", "Google Workspace", "Azure AD", "Custom SAML"]
                                },
                                "audit_log": {
                                    "supported": true,
                                    "retention_period": "2 years",
                                    "export_formats": ["CSV", "JSON"]
                                }
                            }
                        }))
                        .build();

                    client.with_child_span(retrieval_attributes, Some("customer-inquiry".to_string()), async {
                        tracing::info!("Retrieving product information from database");
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    }).await;

                    // Child span 3: Generate response using RAG
                    let generation_attributes = OtelSpanBuilder::new("generate-response")
                        .span_type(OtelSpanType::Generation)
                        .model("gpt-4o")
                        .input(json!({
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "You are a customer support assistant. Use the provided product information to answer the customer's inquiry professionally."
                                },
                                {
                                    "role": "user", 
                                    "content": "I'm interested in your enterprise plan, but I need to know if it supports SSO and has an audit log feature."
                                },
                                {
                                    "role": "assistant",
                                    "content": "I'll check our enterprise plan features for SSO and audit log support."
                                },
                                {
                                    "role": "system",
                                    "content": "Product information: Enterprise plan includes SSO support for Okta, Google Workspace, Azure AD, and custom SAML. It also includes audit logs with 2-year retention and export in CSV or JSON format."
                                }
                            ]
                        }))
                        .output(json!({
                            "content": "Thank you for your interest in our Enterprise plan! I'm happy to confirm that our Enterprise plan fully supports Single Sign-On (SSO) with multiple providers including Okta, Google Workspace, Azure AD, and custom SAML implementations.\n\nRegarding audit logs, the Enterprise plan includes comprehensive audit logging features with a 2-year retention period. You can export these logs in either CSV or JSON formats to integrate with your existing security monitoring systems.\n\nWould you like me to provide more specific details about either of these features, or do you have questions about other Enterprise plan capabilities?"
                        }))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 186);
                            usage.insert("output_tokens".to_string(), 142);
                            usage.insert("total_tokens".to_string(), 328);
                            usage
                        })
                        .cost_details({
                            let mut cost = HashMap::new();
                            cost.insert("total".to_string(), 0.00656);
                            cost
                        })
                        .build();

                    client.with_child_span(generation_attributes, Some("customer-inquiry".to_string()), async {
                        tracing::info!("Generating customer response using RAG");
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }).await;

                    // Child span 4: Log event for analytics
                    let event_attributes = OtelSpanBuilder::new("log-interaction")
                        .span_type(OtelSpanType::Event)
                        .level("INFO")
                        .input(json!({
                            "event_type": "inquiry_response",
                            "response_time_ms": 600,
                            "features_addressed": ["SSO", "audit_log"],
                            "product_discussed": "enterprise_plan"
                        }))
                        .build();

                    client.with_child_span(event_attributes, Some("customer-inquiry".to_string()), async {
                        tracing::info!("Logging interaction for analytics");
                    }).await;
                }).await;

                // Add a follow-up span as a sibling to the inquiry (same parent trace)
                let followup_attributes = OtelSpanBuilder::new("schedule-follow-up")
                    .span_type(OtelSpanType::Span)
                    .input(json!({
                        "customer_id": "customer-12345",
                        "follow_up_type": "sales_call",
                        "scheduled_for": "2025-09-17T14:00:00Z",
                        "assigned_to": "sales-rep-42"
                    }))
                    .build();

                client.with_parent_span(followup_attributes, async {
                    tracing::info!("Scheduling sales follow-up call");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }).await;

                tracing::info!("Customer support workflow completed");
            }
        ).await;

        // Ensure all spans are exported
        cleanup_integration_test().await;
        Ok(())
    }

    #[tokio::test]
    #[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
    async fn test_conversation_session_integration() -> Result<(), Box<dyn std::error::Error>> {
        // Set up the client with OTLP exporter
        let client = Arc::new(setup_integration_test().await?);
        
        // Generate a unique trace ID for this conversation
        use rand::Rng;
        let conversation_id = {
            let mut rng = rand::rng();
            format!("conversation-{:x}", rng.random::<u64>())
        };
       let client_clone = client.clone(); 
                let client_clone2 = client.clone();
                let client_clone3 = client.clone();
                let client_clone4 = client.clone();
                let client_clone5 = client.clone();
                let client_clone6 = client.clone();
                let client_clone7 = client.clone();

        // Use the with_trace method to create a proper trace context
        client.with_trace(
            "Travel Planning Assistant",
            Some(conversation_id.clone()),
            vec![
                "conversation".to_string(), 
                "travel-planning".to_string(), 
                "multi-turn".to_string()
            ],
            async move {
                tracing::info!("Starting multi-turn conversation trace");
                
                // Turn 1: Initial query from user
                let turn1_attributes = OtelSpanBuilder::new("conversation-turn-1")
                    .span_type(OtelSpanType::Span)
                    .user_id("user-integration-test")
                    .session_id("session-integration-test")
                    .trace_id(conversation_id.clone())
                    .input(json!({
                        "user_query": "I want to plan a two-week trip to Japan in cherry blossom season."
                    }))
                    .tags(vec!["turn-1".to_string()])
                    .build();
                    
                let conversation_id_turn1 = conversation_id.clone();
                client_clone.with_child_span(turn1_attributes, None, async move {
                    tracing::info!("Processing conversation turn 1");
                    
                    // LLM generation for turn 1
                    let gen1_attributes = OtelSpanBuilder::new("llm-generate-response-1")
                        .span_type(OtelSpanType::Generation)
                        .user_id("user-integration-test")
                        .session_id("session-integration-test")
                        .trace_id(conversation_id_turn1.clone())
                        .model("gpt-4o")
                        .input(json!({
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "You are a helpful travel assistant specializing in Japan trips."
                                },
                                {
                                    "role": "user",
                                    "content": "I want to plan a two-week trip to Japan in cherry blossom season."
                                }
                            ]
                        }))
                        .output(json!({
                            "content": "Great choice! Cherry blossom (sakura) season in Japan is magical. The best time to visit is typically late March to early April, though it varies by region.\n\nHere's a suggested 2-week itinerary:\n\n**Days 1-4: Tokyo**\n- Ueno Park and Shinjuku Gyoen for cherry blossoms\n- Explore neighborhoods like Shibuya, Harajuku, and Asakusa\n- Day trip to Kamakura\n\n**Days 5-6: Hakone**\n- Mount Fuji views\n- Hot springs (onsen) experience\n- Lake Ashi cruise\n\n**Days 7-10: Kyoto**\n- Philosopher's Path for cherry blossoms\n- Historic temples and shrines\n- Day trip to Nara\n\n**Days 11-12: Osaka**\n- Food adventures\n- Osaka Castle during sakura season\n\n**Days 13-14: Hiroshima and Miyajima**\n- Peace Memorial Park\n- Miyajima Island and the floating torii gate\n\nWould you like me to elaborate on any particular part of the itinerary or provide more specific cherry blossom viewing recommendations?"
                        }))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 52);
                            usage.insert("output_tokens".to_string(), 247);
                            usage.insert("total_tokens".to_string(), 299);
                            usage
                        })
                        .build();
                        
                    client_clone2.with_child_span(gen1_attributes, Some("conversation-turn-1".to_string()), async {
                        tracing::info!("Generating LLM response for turn 1");
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    }).await;
                }).await;
            
                // Turn 2: Follow-up question from user
                let turn2_attributes = OtelSpanBuilder::new("conversation-turn-2")
                    .span_type(OtelSpanType::Span)
                    .user_id("user-integration-test")
                    .session_id("session-integration-test")
                    .trace_id(conversation_id.clone())
                    .input(json!({
                        "user_query": "That sounds great! Can you recommend some good hotels in Tokyo and Kyoto that would have cherry blossom views?"
                    }))
                    .tags(vec!["turn-2".to_string()])
                    .build();
                    
                let conversation_id_turn2 = conversation_id.clone();
                client_clone3.with_child_span(turn2_attributes, None, async move {
                    tracing::info!("Processing conversation turn 2");
                    
                    // LLM generation for turn 2
                    let gen2_attributes = OtelSpanBuilder::new("llm-generate-response-2")
                        .span_type(OtelSpanType::Generation)
                        .user_id("user-integration-test")
                        .session_id("session-integration-test")
                        .trace_id(conversation_id_turn2.clone())
                        .model("gpt-4o")
                        .input(json!({
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "You are a helpful travel assistant specializing in Japan trips."
                                },
                                {
                                    "role": "user",
                                    "content": "I want to plan a two-week trip to Japan in cherry blossom season."
                                },
                                {
                                    "role": "assistant",
                                    "content": "Great choice! Cherry blossom (sakura) season in Japan is magical. The best time to visit is typically late March to early April, though it varies by region.\n\nHere's a suggested 2-week itinerary:\n\n**Days 1-4: Tokyo**\n- Ueno Park and Shinjuku Gyoen for cherry blossoms\n- Explore neighborhoods like Shibuya, Harajuku, and Asakusa\n- Day trip to Kamakura\n\n**Days 5-6: Hakone**\n- Mount Fuji views\n- Hot springs (onsen) experience\n- Lake Ashi cruise\n\n**Days 7-10: Kyoto**\n- Philosopher's Path for cherry blossoms\n- Historic temples and shrines\n- Day trip to Nara\n\n**Days 11-12: Osaka**\n- Food adventures\n- Osaka Castle during sakura season\n\n**Days 13-14: Hiroshima and Miyajima**\n- Peace Memorial Park\n- Miyajima Island and the floating torii gate\n\nWould you like me to elaborate on any particular part of the itinerary or provide more specific cherry blossom viewing recommendations?"
                                },
                                {
                                    "role": "user", 
                                    "content": "That sounds great! Can you recommend some good hotels in Tokyo and Kyoto that would have cherry blossom views?"
                                }
                            ]
                        }))
                        .output(json!({
                            "content": "Here are some hotels in Tokyo and Kyoto known for their cherry blossom views:\n\n**Tokyo Hotels with Cherry Blossom Views:**\n\n1. **Hotel Chinzanso Tokyo**\n   - Features a gorgeous Japanese garden with cherry trees\n   - Luxury property in a peaceful setting\n   - Bunkyo district\n\n2. **Cerulean Tower Tokyu Hotel**\n   - Higher floors offer views of Yoyogi Park's cherry blossoms\n   - Located in vibrant Shibuya\n   \n3. **The Ritz-Carlton, Tokyo**\n   - Upper floors offer panoramic views including Shinjuku Gyoen's cherry trees\n   - Located in Tokyo Midtown complex\n\n4. **Park Hotel Tokyo**\n   - Artist rooms with cherry blossom themes\n   - Views of Shiba Park's cherry trees\n\n**Kyoto Hotels with Cherry Blossom Views:**\n\n1. **The Westin Miyako Kyoto**\n   - Hillside location near the Philosopher's Path\n   - Garden with cherry trees\n   - Eastern Kyoto location\n\n2. **Kyoto Hotel Okura**\n   - Views of the Kamogawa River's cherry tree-lined banks\n   - Central location\n\n3. **Suiran, a Luxury Collection Hotel**\n   - Located along the Hozu River in Arashiyama\n   - Beautiful garden setting with cherry trees\n   - Western Kyoto\n\n4. **Hyatt Regency Kyoto**\n   - Near Maruyama Park (famous for cherry blossoms)\n   - Eastern Kyoto location\n\nTip: Hotels with cherry blossom views book up extremely quickly for sakura season - sometimes 6-12 months in advance. I'd recommend booking as soon as possible if you're set on these properties!"
                        }))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 354);
                            usage.insert("output_tokens".to_string(), 318);
                            usage.insert("total_tokens".to_string(), 672);
                            usage
                        })
                        .build();
                        
                    client_clone4.with_child_span(gen2_attributes, Some("conversation-turn-2".to_string()), async {
                        tracing::info!("Generating LLM response for turn 2");
                        tokio::time::sleep(std::time::Duration::from_millis(350)).await;
                    }).await;
                    }).await;
            
                // Turn 3: Specific question about transportation
                let turn3_attributes = OtelSpanBuilder::new("conversation-turn-3")
                    .span_type(OtelSpanType::Span)
                    .user_id("user-integration-test")
                    .session_id("session-integration-test")
                    .trace_id(conversation_id.clone())
                    .input(json!({
                        "user_query": "What's the best way to travel between these cities? Should I get a JR Pass?"
                    }))
                    .tags(vec!["turn-3".to_string()])
                    .build();
                    
                let conversation_id_turn3 = conversation_id.clone();
                client_clone5.with_child_span(turn3_attributes, None, async move {
                    tracing::info!("Processing conversation turn 3");
                    
                    // Add retrieval step to get transportation information
                    let retrieval_attributes = OtelSpanBuilder::new("retrieve-transportation-info")
                        .span_type(OtelSpanType::Span)
                        .user_id("user-integration-test")
                        .session_id("session-integration-test")
                        .trace_id(conversation_id_turn3.clone())
                        .input(json!({
                            "query": "Japan transportation between Tokyo, Hakone, Kyoto, Osaka, Hiroshima",
                            "retrieval_type": "vector_search"
                        }))
                        .output(json!({
                            "transportation_options": {
                                "jr_pass": {
                                    "cost": "¥50,000 for 14-day pass",
                                    "coverage": "Most JR trains including shinkansen (except Nozomi)",
                                    "recommended_for_itinerary": true
                                },
                                "route_info": [
                                    {"from": "Tokyo", "to": "Hakone", "best_option": "Odakyu Romance Car or JR to Odawara + bus"},
                                    {"from": "Hakone", "to": "Kyoto", "best_option": "JR Shinkansen from Odawara"},
                                    {"from": "Kyoto", "to": "Osaka", "best_option": "JR Special Rapid Service (30 min)"},
                                    {"from": "Osaka", "to": "Hiroshima", "best_option": "JR Shinkansen"}
                                ]
                            }
                        }))
                        .build();
                        
                    client_clone6.with_child_span(retrieval_attributes, Some("conversation-turn-3".to_string()), async {
                        tracing::info!("Retrieving transportation information");
                        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                    }).await;
                    
                    // LLM generation for turn 3
                    let gen3_attributes = OtelSpanBuilder::new("llm-generate-response-3")
                        .span_type(OtelSpanType::Generation)
                        .user_id("user-integration-test")
                        .session_id("session-integration-test")
                        .trace_id(conversation_id_turn3.clone())
                        .model("gpt-4o")
                        .input(json!({
                            "messages": [
                                {
                                    "role": "system",
                                    "content": "You are a helpful travel assistant specializing in Japan trips. Use the provided transportation information to give accurate advice."
                                },
                                {
                                    "role": "user",
                                    "content": "What's the best way to travel between these cities? Should I get a JR Pass?"
                                },
                                {
                                    "role": "system",
                                    "content": "Transportation information: A 14-day JR Pass costs about ¥50,000 and covers most JR trains including shinkansen (except Nozomi). For the Tokyo to Hakone route, use Odakyu Romance Car or JR to Odawara + bus. From Hakone to Kyoto, take the JR Shinkansen from Odawara. Between Kyoto and Osaka, the JR Special Rapid Service takes 30 minutes. From Osaka to Hiroshima, use the JR Shinkansen."
                                }
                            ]
                        }))
                        .output(json!({
                            "content": "For your two-week Japan itinerary covering Tokyo, Hakone, Kyoto, Osaka, and Hiroshima, a 14-day Japan Rail (JR) Pass would definitely be worth it!\n\n**JR Pass Details:**\n- Cost: Approximately ¥50,000 (around $350-380 USD) for a 14-day pass\n- This will cover most of your major intercity travel, particularly the expensive shinkansen (bullet train) journeys\n- The pass pays for itself just with the Tokyo-Kyoto-Hiroshima routes alone\n\n**Transportation between cities:**\n\n1. **Tokyo to Hakone:** \n   - The JR Pass doesn't fully cover this route\n   - Best option: Take JR train to Odawara station, then a bus into Hakone\n   - Alternative: The Odakyu Romance Car from Shinjuku (not covered by JR Pass)\n\n2. **Hakone to Kyoto:**\n   - JR Shinkansen from Odawara station (near Hakone)\n   - Fully covered by JR Pass\n   - About 2-2.5 hours\n\n3. **Kyoto to Osaka:**\n   - JR Special Rapid Service\n   - Fully covered by JR Pass\n   - Only 30 minutes\n\n4. **Osaka to Hiroshima:**\n   - JR Shinkansen\n   - Fully covered by JR Pass\n   - About 1.5-2 hours\n\nThe JR Pass also covers many local JR lines within cities, like the Yamanote Line in Tokyo, as well as the JR Nara Line for your day trip to Nara from Kyoto.\n\nOne important note: The JR Pass doesn't cover the fastest category of shinkansen (Nozomi trains), but it covers Hikari and Sakura trains which are only slightly slower and run on the same routes.\n\nI definitely recommend purchasing your JR Pass before arriving in Japan, as it's more expensive if purchased within the country."
                        }))
                        .usage({
                            let mut usage = HashMap::new();
                            usage.insert("input_tokens".to_string(), 219);
                            usage.insert("output_tokens".to_string(), 329);
                            usage.insert("total_tokens".to_string(), 548);
                            usage
                        })
                        .build();
                        
                    client_clone6.with_child_span(gen3_attributes, Some("conversation-turn-3".to_string()), async {
                        tracing::info!("Generating LLM response for turn 3");
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    }).await;
                    }).await;
            
                // Create an event for conversation completion
                let completion_event = OtelSpanBuilder::new("conversation-completed")
                    .span_type(OtelSpanType::Event)
                    .user_id("user-integration-test")
                    .session_id("session-integration-test")
                    .trace_id(conversation_id.clone())
                    .level("INFO")
                    .input(json!({
                        "conversation_turns": 3,
                        "total_tokens_used": 1519,
                        "session_duration_ms": 950
                    }))
                    .build();
                    
                client_clone7.with_child_span(completion_event, None, async {
                    tracing::info!("Logging conversation completion event");
                }).await;
                
                // Log final conversation summary
                let trace_output = json!({
                    "conversation_summary": {
                        "topic": "Japan travel planning during cherry blossom season",
                        "subtopics": ["Itinerary planning", "Hotel recommendations", "Transportation options"],
                        "user_satisfaction": "high",
                        "turns_completed": 3,
                        "recommendations_provided": {
                            "cities": ["Tokyo", "Hakone", "Kyoto", "Osaka", "Hiroshima"],
                            "hotels": {
                                "Tokyo": ["Hotel Chinzanso Tokyo", "Cerulean Tower Tokyu Hotel", "The Ritz-Carlton, Tokyo", "Park Hotel Tokyo"],
                                "Kyoto": ["The Westin Miyako Kyoto", "Kyoto Hotel Okura", "Suiran", "Hyatt Regency Kyoto"]
                            },
                            "transportation": "14-day JR Pass (¥50,000)"
                        }
                    }
                });
                
                tracing::info!(
                    trace_output = %serde_json::to_string(&trace_output).unwrap_or_default(),
                    "Completed travel planning conversation"
                );
            }
        ).await;
        
        cleanup_integration_test().await;
        Ok(())
    }
}
