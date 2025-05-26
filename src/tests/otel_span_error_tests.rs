//! Error handling integration tests for OpenTelemetry tracing
//! These tests demonstrate how to capture and report errors in spans
use super::test_utils::*;
use crate::infra::trace::otel_span::*;
use serde_json::json;
use uuid::Uuid;

/// Test error handling and reporting in spans
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_error_handling_integration() -> Result<(), Box<dyn std::error::Error>> {
    let client = setup_integration_test().await?;
    let session_id = Uuid::new_v4().to_string();
    let trace_id = Uuid::new_v4().to_string();

    // Test 1: Handle a simulated API error
    let error_attributes = OtelSpanBuilder::new("api-call-with-error")
        .session_id(session_id.clone())
        .span_type(OtelSpanType::Generation)
        .model("gpt-4")
        .input(json!({"prompt": "Test prompt that will fail"}))
        .build();

    #[derive(Debug)]
    struct ApiError {
        message: String,
        code: u16,
    }

    impl std::fmt::Display for ApiError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "API Error {}: {}", self.code, self.message)
        }
    }

    impl std::error::Error for ApiError {}

    let result: Result<String, ApiError> = client
        .with_span_result(error_attributes, async {
            // Simulate an API call that fails
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;

            let error = ApiError {
                message: "Rate limit exceeded".to_string(),
                code: 429,
            };

            Err(error)
        })
        .await;

    assert!(result.is_err());
    tracing::info!("Successfully captured API error in span");

    // Test 2: Handle a timeout error
    let timeout_attributes = OtelSpanBuilder::new("slow-operation-with-timeout")
        .session_id(session_id.clone())
        .span_type(OtelSpanType::Generation)
        .model("claude-3")
        .input(json!({"prompt": "This will take too long"}))
        .build();

    let timeout_result = client
        .with_span_timeout(
            timeout_attributes,
            std::time::Duration::from_millis(50), // Very short timeout
            async {
                // Simulate a slow operation
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                Ok::<String, ApiError>("Should not reach here".to_string())
            },
        )
        .await;

    assert!(timeout_result.is_err());
    tracing::info!("Successfully captured timeout error in span");

    // Test 3: Log a standalone error
    let standalone_error = ApiError {
        message: "Database connection failed".to_string(),
        code: 500,
    };

    client
        .log_error(&standalone_error, Some("database_connection"))
        .await;
    tracing::info!("Successfully logged standalone error");

    // Test 4: Multiple errors in sequence
    for i in 1..=3 {
        let error_attributes = OtelSpanBuilder::new(format!("batch-error-{}", i))
            .session_id(session_id.clone())
            .span_type(OtelSpanType::Event)
            .level("ERROR")
            .build();

        let _result: Result<String, ApiError> = client
            .with_span_result(error_attributes, async move {
                let error = ApiError {
                    message: format!("Batch operation {} failed", i),
                    code: 503,
                };
                Err(error)
            })
            .await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    tracing::info!("Successfully processed batch errors");

    cleanup_integration_test().await;
    Ok(())
}

/// Test error chain handling in spans
#[tokio::test]
#[ignore = "Integration test requiring OTLP endpoint - run with --ignored"]
async fn test_error_chain_handling_integration() -> Result<(), Box<dyn std::error::Error>> {
    let client = setup_integration_test().await?;

    // Create a nested error chain
    #[derive(Debug)]
    struct RootError {
        message: String,
    }

    impl std::fmt::Display for RootError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Root error: {}", self.message)
        }
    }

    impl std::error::Error for RootError {}

    #[derive(Debug)]
    struct MiddleError {
        message: String,
        source: RootError,
    }

    impl std::fmt::Display for MiddleError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Middle error: {}", self.message)
        }
    }

    impl std::error::Error for MiddleError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.source)
        }
    }

    #[derive(Debug)]
    struct TopError {
        message: String,
        source: MiddleError,
    }

    impl std::fmt::Display for TopError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Top error: {}", self.message)
        }
    }

    impl std::error::Error for TopError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.source)
        }
    }

    let error_chain_attributes = OtelSpanBuilder::new("complex-error-chain")
        .session_id("test-session-789")
        .span_type(OtelSpanType::Generation)
        .model("gpt-4")
        .input(json!({"operation": "complex_processing"}))
        .build();

    let result: Result<String, TopError> = client
        .with_span_result(error_chain_attributes, async {
            let root_error = RootError {
                message: "Network connection failed".to_string(),
            };

            let middle_error = MiddleError {
                message: "API request failed".to_string(),
                source: root_error,
            };

            let top_error = TopError {
                message: "LLM generation failed".to_string(),
                source: middle_error,
            };

            Err(top_error)
        })
        .await;

    assert!(result.is_err());
    tracing::info!("Successfully captured complex error chain in span");

    cleanup_integration_test().await;
    Ok(())
}
