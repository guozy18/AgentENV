use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::ApiImpl;
use crate::snapshot::{RepositoryError, SnapshotType};

const PLACEMENT_ROUTE: &str = "/internal/snapshots/placement";

#[derive(Debug, Deserialize)]
struct PlacementQuery {
    #[serde(rename = "snapshotID")]
    snapshot_id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlacementResponse {
    snapshot_type: SnapshotType,
    #[serde(rename = "ownerNodeID")]
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_node_id: Option<String>,
}

fn placement_error_status(error: &anyhow::Error) -> StatusCode {
    match error
        .chain()
        .find_map(|cause| cause.downcast_ref::<RepositoryError>())
    {
        Some(RepositoryError::InvalidRequest { .. }) => StatusCode::BAD_REQUEST,
        Some(
            RepositoryError::Unavailable { .. }
            | RepositoryError::Backend { .. }
            | RepositoryError::ConcurrentModification { .. },
        ) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

pub(crate) fn router<I>(api_impl: I) -> Router
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    Router::new()
        .route(PLACEMENT_ROUTE, get(get_placement::<I>))
        .with_state(api_impl)
}

async fn get_placement<I>(
    State(api_impl): State<I>,
    Query(query): Query<PlacementQuery>,
) -> Result<Json<PlacementResponse>, StatusCode>
where
    I: AsRef<ApiImpl> + Clone + Send + Sync + 'static,
{
    match api_impl
        .as_ref()
        .snapshot_manager()
        .get(&query.snapshot_id)
        .await
    {
        Ok(Some(record)) => match record.snapshot_type {
            SnapshotType::Distributed => Ok(Json(PlacementResponse {
                snapshot_type: record.snapshot_type,
                owner_node_id: None,
            })),
            SnapshotType::Local => match record.owner_node_id {
                Some(owner_node_id) if !owner_node_id.is_empty() => Ok(Json(PlacementResponse {
                    snapshot_type: record.snapshot_type,
                    owner_node_id: Some(owner_node_id),
                })),
                _ => Err(StatusCode::SERVICE_UNAVAILABLE),
            },
        },
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(error) => Err(placement_error_status(&error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placement_response_never_contains_artifact_paths() {
        let response = PlacementResponse {
            snapshot_type: SnapshotType::Local,
            owner_node_id: Some("node-a".to_string()),
        };

        assert_eq!(
            serde_json::to_value(response).expect("placement should serialize"),
            serde_json::json!({
                "snapshotType": "local",
                "ownerNodeID": "node-a"
            })
        );
    }

    #[test]
    fn invalid_snapshot_reference_is_a_bad_request() {
        let error = anyhow::Error::new(RepositoryError::InvalidRequest {
            reason: "invalid snapshot alias".to_string(),
        });

        assert_eq!(placement_error_status(&error), StatusCode::BAD_REQUEST);
    }
}
