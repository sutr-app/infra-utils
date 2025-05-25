//! Basic integration tests for OpenTelemetry tracing

use super::test_utils::*;
use crate::infra::trace::otel_span::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;

/// Test basic span creation and attributes
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_basic_span_creation() -> Result<(), Box<dyn std::error::Error>> {
    let client = setup_integration_test().await?;

    // Test a simple span
    let simple_attributes = OtelSpanBuilder::new("simple-test-span")
        .span_type(OtelSpanType::Span)
        .build();

    client
        .with_span(simple_attributes, async {
            tracing::info!("Inside a simple test span");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        })
        .await;

    // Test a span with additional attributes
    let detailed_attributes = OtelSpanBuilder::new("detailed-test-span")
        .span_type(OtelSpanType::Generation)
        .model("gpt-4")
        .user_id("test-user-123")
        .session_id("test-session-456")
        .tags(vec!["test".to_string(), "integration".to_string()])
        .input(json!({"prompt": "Test prompt for integration test"}))
        .build();

    client
        .with_span(detailed_attributes, async {
            tracing::info!("Inside a detailed test span");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        })
        .await;

    // Test span with custom metadata
    let mut metadata = HashMap::new();
    metadata.insert("custom_field".to_string(), json!("custom value"));
    metadata.insert("test_number".to_string(), json!(42));

    let metadata_attributes = OtelSpanBuilder::new("metadata-test-span")
        .span_type(OtelSpanType::Event)
        .level("INFO")
        .metadata(metadata)
        .build();

    client
        .with_span(metadata_attributes, async {
            tracing::info!("Inside a metadata test span");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        })
        .await;

    // Test span with usage metrics
    let mut usage = HashMap::new();
    usage.insert("input_tokens".to_string(), 10);
    usage.insert("output_tokens".to_string(), 20);
    usage.insert("total_tokens".to_string(), 30);

    let usage_attributes = OtelSpanBuilder::new("usage-test-span")
        .span_type(OtelSpanType::Generation)
        .model("gpt-3.5-turbo")
        .usage(usage)
        .build();

    client
        .with_span(usage_attributes, async {
            tracing::info!("Inside a usage metrics test span");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        })
        .await;

    cleanup_integration_test().await;
    Ok(())
}

/// Test nested span relationships
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_nested_spans() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Create parent span
    let parent_attributes = OtelSpanBuilder::new("parent-span")
        .span_type(OtelSpanType::Span)
        .tags(vec!["parent".to_string()])
        .build();

    let client_clone = client.clone();
    client
        .clone()
        .with_span(parent_attributes.clone(), async move {
            tracing::info!("Inside parent span");

            // Create first child span
            let child1_attributes = OtelSpanBuilder::new("child-span-1")
                .span_type(OtelSpanType::Event)
                .parent_observation_id("parent-span") // Link to parent
                .tags(vec!["child".to_string()])
                .build();

            client_clone
                .with_span(child1_attributes, async {
                    tracing::info!("Inside first child span");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                })
                .await;

            // Create second child span
            let child2_attributes = OtelSpanBuilder::new("child-span-2")
                .span_type(OtelSpanType::Generation)
                .parent_observation_id("parent-span") // Link to parent
                .model("gpt-4")
                .input(json!({"prompt": "Test prompt in child span"}))
                .build();

            let client_clone2 = client_clone.clone();
            client_clone
                .with_span(child2_attributes, async move {
                    tracing::info!("Inside second child span");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                    // Create grandchild span
                    let grandchild_attributes = OtelSpanBuilder::new("grandchild-span")
                        .span_type(OtelSpanType::Event)
                        .parent_observation_id("child-span-2") // Link to parent
                        .tags(vec!["grandchild".to_string()])
                        .build();

                    client_clone2
                        .with_span(grandchild_attributes, async {
                            tracing::info!("Inside grandchild span");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        })
                        .await;
                })
                .await;
        })
        .await;

    cleanup_integration_test().await;
    Ok(())
}

/// Test trace creation and span grouping
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_trace_with_spans() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Test with_trace which creates a trace containing multiple spans
    client
        .clone()
        .with_trace(
            "test-conversation-flow",
            Some("test-trace-id-123".to_string()), // Provide explicit trace ID
            vec!["test".to_string(), "conversation".to_string()],
            async move {
                tracing::info!("Starting conversation flow trace");

                // Step 1: User input processing
                let input_attributes = OtelSpanBuilder::new("process-user-input")
                    .span_type(OtelSpanType::Span)
                    .trace_id("test-trace-id-123") // Link to trace
                    .input(
                        json!({"user_input": "Hello, how can you help me with Rust programming?"}),
                    )
                    .build();

                client
                    .with_span(input_attributes, async {
                        tracing::info!("Processing user input");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    })
                    .await;

                // Step 2: LLM generation
                let generation_attributes = OtelSpanBuilder::new("generate-llm-response")
                    .span_type(OtelSpanType::Generation)
                    .trace_id("test-trace-id-123") // Link to trace
                    .model("gpt-4")
                    .input(json!({"prompt": "Help with Rust programming"}))
                    .output(
                        json!({"text": "I can help you with Rust programming in several ways..."}),
                    )
                    .build();

                client
                    .with_span(generation_attributes, async {
                        tracing::info!("Generating LLM response");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    })
                    .await;

                // Step 3: Response formatting
                let formatting_attributes = OtelSpanBuilder::new("format-response")
                    .span_type(OtelSpanType::Span)
                    .trace_id("test-trace-id-123") // Link to trace
                    .build();

                client
                    .with_span(formatting_attributes, async {
                        tracing::info!("Formatting response");
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    })
                    .await;

                tracing::info!("Completed conversation flow trace");
            },
        )
        .await;

    cleanup_integration_test().await;
    Ok(())
}
