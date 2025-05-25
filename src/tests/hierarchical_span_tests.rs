//! Integration tests for HierarchicalSpanClient functionality
use super::test_utils::*;
use crate::infra::trace::otel_span::*;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

/// Test hierarchical spans using with_parent_span and with_child_span
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_hierarchical_span_relationship() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Test unique ID for this test run to distinguish spans
    let test_id = format!("test-{}", chrono::Utc::now().timestamp());

    // Create shared context for this workflow
    let workflow_trace_id = format!("trace-workflow-{}", test_id);
    let workflow_user_id = format!("user-{}", test_id);
    let workflow_session_id = format!("session-{}", test_id);

    // Create parent span attributes with explicit span_id and shared context
    let parent_span_id = format!("parent-span-{}", test_id);
    let parent_attributes = OtelSpanBuilder::new(format!("workflow-parent-{}", test_id))
        .span_type(OtelSpanType::Span)
        .span_id(parent_span_id.clone()) // Set explicit span_id for parent reference
        .trace_id(workflow_trace_id.clone()) // Set explicit trace_id
        .user_id(workflow_user_id.clone()) // Set user context
        .session_id(workflow_session_id.clone()) // Set session context
        .tags(vec!["workflow".to_string(), "parent".to_string()])
        .input(json!({ "workflow": "data-processing-pipeline" }))
        .build();

    // Execute with parent span
    client
        .clone()
        .with_parent_span(parent_attributes, async move {
            // Clone values needed across multiple child spans
            let workflow_trace_id_clone = workflow_trace_id.clone();
            let workflow_user_id_clone = workflow_user_id.clone();
            let workflow_session_id_clone = workflow_session_id.clone();

            tracing::info!("Inside parent workflow span");
            tokio::time::sleep(Duration::from_millis(100)).await;

            // First child: Data extraction
            let child1_trace_id = workflow_trace_id_clone.clone();
            let child1_user_id = workflow_user_id_clone.clone();
            let child1_session_id = workflow_session_id_clone.clone();
            let child1_attributes = OtelSpanBuilder::new(format!("data-extraction-{}", test_id))
                .span_type(OtelSpanType::Span)
                .trace_id(child1_trace_id) // Inherit trace_id from parent
                .user_id(child1_user_id) // Inherit user_id from parent
                .session_id(child1_session_id) // Inherit session_id from parent
                .tags(vec!["extraction".to_string(), "child".to_string()])
                .input(json!({ "source": "database", "query": "SELECT * FROM events" }))
                .output(json!({ "records": 150, "status": "success" }))
                .build();

            // Use with_child_span to create child with explicit parent reference
            client
                .clone()
                .with_child_span(child1_attributes, async move {
                    tracing::info!("Extracting data from source");
                    tokio::time::sleep(Duration::from_millis(150)).await;
                })
                .await;

            // Second child: Data transformation
            let child2_trace_id = workflow_trace_id_clone.clone();
            let child2_user_id = workflow_user_id_clone.clone();
            let child2_session_id = workflow_session_id_clone.clone();
            let child2_attributes = OtelSpanBuilder::new(format!("data-transform-{}", test_id))
                .span_type(OtelSpanType::Span)
                .trace_id(child2_trace_id) // Inherit trace_id from parent
                .user_id(child2_user_id) // Inherit user_id from parent
                .session_id(child2_session_id) // Inherit session_id from parent
                .tags(vec!["transform".to_string(), "child".to_string()])
                .input(json!({ "records": 150 }))
                .output(json!({ "transformed_records": 150, "status": "success" }))
                .build();

            let child3_trace_id = workflow_trace_id_clone.clone();
            let child3_user_id = workflow_user_id_clone.clone();
            let child3_session_id = workflow_session_id_clone.clone();

            // Use with_child_span again
            let test_id_clone = test_id.clone();
            let client_clone = client.clone();

            // Extract necessary values before entering async block
            let workflow_trace_id_for_grandchild = workflow_trace_id_clone.clone();
            let workflow_user_id_for_grandchild = workflow_user_id_clone.clone();
            let workflow_session_id_for_grandchild = workflow_session_id_clone.clone();

            client
                .clone()
                .with_child_span(child2_attributes, async move {
                    tracing::info!("Transforming extracted data");
                    tokio::time::sleep(Duration::from_millis(200)).await;

                    // Create grandchild span (nested inside second child)
                    let grandchild_attributes =
                        OtelSpanBuilder::new(format!("data-validation-{}", test_id_clone))
                            .span_type(OtelSpanType::Event)
                            .trace_id(workflow_trace_id_for_grandchild) // Use pre-cloned value
                            .user_id(workflow_user_id_for_grandchild) // Use pre-cloned value
                            .session_id(workflow_session_id_for_grandchild) // Use pre-cloned value
                            .level("INFO")
                            .tags(vec!["validation".to_string(), "grandchild".to_string()])
                            .input(json!({ "records_to_validate": 150 }))
                            .output(json!({ "valid_records": 148, "invalid_records": 2 }))
                            .build();

                    // Reference the child as parent for this grandchild
                    client_clone
                        .with_child_span(grandchild_attributes, async move {
                            tracing::info!("Validating transformed data");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        })
                        .await;
                })
                .await;

            // Third child: LLM processing
            let child3_attributes = OtelSpanBuilder::new(format!("llm-processing-{}", test_id))
                .span_type(OtelSpanType::Generation)
                .model("gpt-4")
                .trace_id(child3_trace_id) // Inherit trace_id from parent
                .user_id(child3_user_id) // Inherit user_id from parent
                .session_id(child3_session_id) // Inherit session_id from parent
                .tags(vec!["llm".to_string(), "child".to_string()])
                .input(json!({ "prompt": "Analyze this data and provide insights" }))
                .output(json!({ "insights": "The data shows interesting patterns..." }))
                .build();

            client
                .clone()
                .with_child_span(child3_attributes, async move {
                    tracing::info!("Processing with LLM");
                    tokio::time::sleep(Duration::from_millis(250)).await;
                })
                .await;
        })
        .await;

    cleanup_integration_test().await;
    Ok(())
}

/// Test error handling in hierarchical spans
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_hierarchical_span_with_errors() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Test unique ID for this run
    let test_id = format!("error-test-{}", chrono::Utc::now().timestamp());

    // Create parent workflow with explicit span_id and trace context
    let parent_span_id = format!("error-parent-{}", test_id);
    let workflow_trace_id = format!("trace-error-{}", test_id);
    let workflow_user_id = format!("user-error-{}", test_id);
    let workflow_session_id = format!("session-error-{}", test_id);

    let parent_attributes = OtelSpanBuilder::new(format!("error-workflow-{}", test_id))
        .span_type(OtelSpanType::Span)
        .tags(vec!["workflow".to_string(), "error-test".to_string()])
        .span_id(parent_span_id.clone()) // Set explicit span_id
        .trace_id(workflow_trace_id.clone()) // Set trace context
        .user_id(workflow_user_id.clone()) // Set user context
        .session_id(workflow_session_id.clone()) // Set session context
        .build();

    // Create custom error for testing
    #[derive(Debug)]
    struct TestError(String);
    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Test error: {}", self.0)
        }
    }
    impl std::error::Error for TestError {}

    client
        .clone()
        .with_parent_span(parent_attributes, async move {
            tracing::info!("Starting workflow that will contain errors");

            // First child: will succeed
            let success_child_attributes =
                OtelSpanBuilder::new(format!("successful-step-{}", test_id))
                    .span_type(OtelSpanType::Span)
                    .trace_id(workflow_trace_id.clone()) // Inherit trace context
                    .user_id(workflow_user_id.clone()) // Inherit user context
                    .session_id(workflow_session_id.clone()) // Inherit session context
                    .tags(vec!["success".to_string()])
                    .build();

            client
                .clone()
                .with_child_span(success_child_attributes, async move {
                    tracing::info!("This step will succeed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok::<_, TestError>(())
                })
                .await
                .unwrap();

            // Second child: will fail
            let failing_child_attributes =
                OtelSpanBuilder::new(format!("failing-step-{}", test_id))
                    .span_type(OtelSpanType::Span)
                    .trace_id(workflow_trace_id.clone()) // Inherit trace context for error tracking
                    .user_id(workflow_user_id.clone()) // Inherit user context
                    .session_id(workflow_session_id.clone()) // Inherit session context
                    .tags(vec!["failure".to_string()])
                    .build();

            // Use with_span_result to handle the error properly
            let result = client
                .clone()
                .with_span_result(failing_child_attributes, async move {
                    tracing::info!("This step will fail");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Err::<(), _>(TestError("Something went wrong".to_string()))
                })
                .await;

            // Just log the error but continue
            if let Err(e) = result {
                tracing::error!("Child step failed as expected: {}", e);
            }

            // Third child: recovery step after error
            let recovery_child_attributes =
                OtelSpanBuilder::new(format!("recovery-step-{}", test_id))
                    .span_type(OtelSpanType::Span)
                    .trace_id(workflow_trace_id.clone()) // Inherit trace context for complete workflow tracking
                    .user_id(workflow_user_id.clone()) // Inherit user context
                    .session_id(workflow_session_id.clone()) // Inherit session context
                    .tags(vec!["recovery".to_string()])
                    .level("INFO")
                    .status_message("Recovered from previous error")
                    .build();

            client
                .clone()
                .with_child_span(recovery_child_attributes, async move {
                    tracing::info!("Recovering from previous error");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                })
                .await;
        })
        .await;

    cleanup_integration_test().await;
    Ok(())
}

/// Test explicit trace_id propagation through the span hierarchy
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_trace_id_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Create a unique explicit trace ID for this test
    let explicit_trace_id = format!("trace-explicit-{}", chrono::Utc::now().timestamp());

    // Create a parent span with the explicit trace ID
    let test_user_id = format!("user-{}", chrono::Utc::now().timestamp());
    let test_session_id = format!("session-{}", chrono::Utc::now().timestamp());

    let parent_attributes = OtelSpanBuilder::new("trace-parent-span")
        .span_type(OtelSpanType::Span)
        .tags(vec!["trace-test".to_string()])
        .trace_id(explicit_trace_id.clone())
        .user_id(test_user_id.clone())
        .session_id(test_session_id.clone())
        .build();

    // Store the parent span ID for child reference - using explicit span_id
    let parent_span_id = "trace-parent-span-id".to_string();
    // Update parent attributes to include span_id
    let parent_attributes = OtelSpanBuilder::from_attributes(parent_attributes)
        .span_id(parent_span_id.clone())
        .build();

    let client_clone = client.clone();
    client
        .clone()
        .with_parent_span(parent_attributes, async move {
            tracing::info!(
                "Inside parent span with explicit trace ID: {}",
                explicit_trace_id
            );
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Create first child - should inherit the trace ID
            let child1_attributes = OtelSpanBuilder::new("trace-child-1")
                .span_type(OtelSpanType::Span)
                // Explicitly NOT setting trace_id here to test inheritance
                .build();

            let client_clone2 = client_clone.clone();
            let client_clone3 = client_clone.clone();
            client_clone
                .clone()
                .with_child_span(child1_attributes, async move {
                    tracing::info!("Child span should inherit trace ID from parent");
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Create a grandchild to test multi-level inheritance
                    let grandchild_attributes = OtelSpanBuilder::new("trace-grandchild")
                        .span_type(OtelSpanType::Event)
                        // Again, NOT explicitly setting trace_id to test inheritance
                        .build();

                    client_clone2
                        .with_child_span(grandchild_attributes, async move {
                            tracing::info!("Grandchild span should inherit the same trace ID");
                            tokio::time::sleep(Duration::from_millis(30)).await;
                        })
                        .await;
                })
                .await;

            // Create second child with explicit override of trace ID
            // This should NOT be grouped with the same trace
            let different_trace_id = format!("trace-different-{}", chrono::Utc::now().timestamp());
            let child2_attributes = OtelSpanBuilder::new("trace-child-different")
                .span_type(OtelSpanType::Span)
                .trace_id(different_trace_id.clone()) // Override trace ID
                .build();

            client_clone3
                .with_child_span(child2_attributes, async move {
                    tracing::info!(
                        "This child has a different explicit trace ID: {}",
                        different_trace_id
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                })
                .await;
        })
        .await;

    // Create another traced workflow using the with_trace helper
    // This should generate a consistent trace_id for all spans in the workflow
    client
        .clone()
        .with_trace(
            "auto-traced-workflow",
            None, // Let the system generate a trace ID
            vec!["auto-trace".to_string()],
            async move {
                tracing::info!("Inside auto-generated trace workflow");

                // Child spans within this trace
                let child_attributes = OtelSpanBuilder::new("auto-trace-child")
                    .span_type(OtelSpanType::Span)
                    // Not setting the trace_id - should be inherited from the parent trace
                    .build();

                client
                    .clone()
                    .with_span(child_attributes, async move {
                        tracing::info!("This child should be part of the auto-generated trace");
                        tokio::time::sleep(Duration::from_millis(30)).await;
                    })
                    .await;
            },
        )
        .await;

    cleanup_integration_test().await;
    Ok(())
}

/// Test with explicit trace ID across all spans in a workflow
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_workflow_with_shared_trace_id() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Generate a unique workflow ID and trace ID
    let workflow_id = format!("workflow-{}", chrono::Utc::now().timestamp());
    let workflow_trace_id = format!("trace-{}", workflow_id);

    // Create parent span with explicit trace ID and span ID
    let parent_span_id = format!("workflow-parent-span-{}", workflow_id);
    let workflow_user_id = format!("user-{}", workflow_id);
    let workflow_session_id = format!("session-{}", workflow_id);

    let parent_attributes = OtelSpanBuilder::new(format!("parent-{}", workflow_id))
        .span_type(OtelSpanType::Span)
        .tags(vec!["parent".to_string(), "workflow".to_string()])
        // Set explicit trace ID that will be shared by all spans in this workflow
        .trace_id(workflow_trace_id.clone())
        .span_id(parent_span_id.clone()) // Set explicit span ID
        .user_id(workflow_user_id.clone()) // Set user context
        .session_id(workflow_session_id.clone()) // Set session context
        .build();

    client
        .clone()
        .with_parent_span(parent_attributes, async move {
            tracing::info!(
                "Starting workflow with shared trace ID: {}",
                workflow_trace_id
            );
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Execute multiple child spans all sharing the same trace ID

            // First child: explicitly inherits trace ID
            let child1_attributes = OtelSpanBuilder::new(format!("child1-{}", workflow_id))
                .span_type(OtelSpanType::Span)
                // Explicitly set same trace ID
                .trace_id(workflow_trace_id.clone())
                .user_id(workflow_user_id.clone()) // Inherit user context
                .session_id(workflow_session_id.clone()) // Inherit session context
                .build();

            client
                .clone()
                .with_child_span(child1_attributes, async move {
                    tracing::info!("First child with explicit trace ID");
                    tokio::time::sleep(Duration::from_millis(30)).await;
                })
                .await;

            // Second child: relies on automatic trace ID inheritance
            let child2_attributes = OtelSpanBuilder::new(format!("child2-{}", workflow_id))
                .span_type(OtelSpanType::Span)
                // No explicit trace ID - should be inherited from parent
                .user_id(workflow_user_id.clone()) // Inherit user context
                .session_id(workflow_session_id.clone()) // Inherit session context
                .build();

            client
                .clone()
                .with_child_span(child2_attributes, async move {
                    tracing::info!("Second child with inherited trace ID");
                    tokio::time::sleep(Duration::from_millis(30)).await;
                })
                .await;

            // Create parallel child spans
            let mut handles = Vec::new();

            for i in 1..=3 {
                let client_clone = client.clone();
                let workflow_trace_id_clone = workflow_trace_id.clone();
                let workflow_id_clone = workflow_id.clone();
                let workflow_user_id_clone = workflow_user_id.clone();
                let workflow_session_id_clone = workflow_session_id.clone();

                let handle = tokio::spawn(async move {
                    let parallel_attributes =
                        OtelSpanBuilder::new(format!("parallel-{}-{}", i, workflow_id_clone))
                            .span_type(OtelSpanType::Span)
                            .trace_id(workflow_trace_id_clone.clone())
                            .user_id(workflow_user_id_clone) // Inherit user context
                            .session_id(workflow_session_id_clone) // Inherit session context
                            .build();

                    client_clone
                        .with_child_span(parallel_attributes, async move {
                            tracing::info!("Parallel child span {}", i);
                            tokio::time::sleep(Duration::from_millis(20 * i as u64)).await;
                        })
                        .await;
                });

                handles.push(handle);
            }

            // Wait for all parallel spans to complete - handle errors without panicking
            for handle in handles {
                if let Err(e) = handle.await {
                    tracing::error!("Failed to join parallel span task: {}", e);
                    return Err(format!("Failed to join parallel span task: {}", e).into());
                }
            }

            tracing::info!("All child spans completed in workflow");
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await
        .unwrap();

    cleanup_integration_test().await;
    Ok(())
}

/// Enhanced test for trace_id propagation in parent-child relationships
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_parent_child_trace_propagation() -> Result<(), Box<dyn std::error::Error>> {
    let client = Arc::new(setup_integration_test().await?);

    // Generate a unique test ID and explicit trace ID
    let test_id = format!("trace-propagation-{}", chrono::Utc::now().timestamp());
    let explicit_trace_id = format!("trace-explicit-{}", test_id);
    let test_user_id = format!("user-{}", test_id);
    let test_session_id = format!("session-{}", test_id);

    // Create parent span with both explicit span_id and trace_id
    let parent_span_id = format!("parent-{}", test_id);
    let parent_attributes = OtelSpanBuilder::new(format!("parent-span-{}", test_id))
        .span_type(OtelSpanType::Span)
        .span_id(parent_span_id.clone())
        .trace_id(explicit_trace_id.clone()) // Explicitly set trace_id for parent
        .user_id(test_user_id.clone()) // Set shared user context
        .session_id(test_session_id.clone()) // Set shared session context
        .tags(vec!["parent".to_string(), "trace-test".to_string()])
        .build();

    client
        .clone()
        .with_parent_span(parent_attributes, async move {
            tracing::info!(
                "Inside parent span with explicit trace ID: {}",
                explicit_trace_id
            );
            tokio::time::sleep(Duration::from_millis(100)).await;

            // First child: explicitly use the same trace_id
            let child1_attributes = OtelSpanBuilder::new(format!("child1-{}", test_id))
                .span_type(OtelSpanType::Span)
                .trace_id(explicit_trace_id.clone()) // Explicitly set same trace_id
                .user_id(test_user_id.clone()) // Inherit user context
                .session_id(test_session_id.clone()) // Inherit session context
                .build();

            client
                .clone()
                .with_child_span(child1_attributes, async move {
                    tracing::info!("Child1 with explicit trace_id");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                })
                .await;

            // Second child: rely on inheritance of trace_id
            let child2_attributes = OtelSpanBuilder::new(format!("child2-{}", test_id))
                .span_type(OtelSpanType::Span)
                // Deliberately NOT setting trace_id to test inheritance
                .user_id(test_user_id.clone()) // Inherit user context
                .session_id(test_session_id.clone()) // Inherit session context
                .build();

            client
                .clone()
                .with_child_span(child2_attributes, async move {
                    tracing::info!("Child2 should inherit parent trace_id");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                })
                .await;

            // Create nested grandchild (3 levels deep)
            let child3_attributes = OtelSpanBuilder::new(format!("child3-{}", test_id))
                .span_type(OtelSpanType::Span)
                .span_id(format!("child3-{}", test_id)) // Set explicit span_id for reference by grandchild
                .user_id(test_user_id.clone()) // Inherit user context
                .session_id(test_session_id.clone()) // Inherit session context
                .build();

            client
                .clone()
                .with_child_span(child3_attributes, async move {
                    tracing::info!("Child3 should inherit parent trace_id");
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Grandchild should inherit trace_id through multiple levels
                    let grandchild_attributes =
                        OtelSpanBuilder::new(format!("grandchild-{}", test_id))
                            .span_type(OtelSpanType::Event)
                            .user_id(test_user_id.clone()) // Inherit user context through multiple levels
                            .session_id(test_session_id.clone()) // Inherit session context through multiple levels
                            .build();

                    client
                        .clone()
                        .with_child_span(grandchild_attributes, async move {
                            tracing::info!("Grandchild should inherit the same trace_id");
                            tokio::time::sleep(Duration::from_millis(30)).await;
                        })
                        .await;
                })
                .await;
        })
        .await;

    cleanup_integration_test().await;
    Ok(())
}
