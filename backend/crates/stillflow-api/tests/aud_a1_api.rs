use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde_json::json;
use tempfile::tempdir;
use uuid::Uuid;

use stillflow_api::{
    ApiRequest, ApiService, AuditLineageRequest, ListAuditEventsRequest, RequestMetadata,
    RequestPrincipal,
};
use stillflow_storage::{
    AuditActor, AuditActorKind, AuditEventDraft, AuditLineageEdge, AuditObjectRef, AuditQuery,
    AuditRetentionState, ControlPlaneStore, AUDIT_VERSION,
};

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(1_700_000_000 + seconds, 0)
        .single()
        .expect("valid timestamp")
}

fn draft(workspace_id: Uuid, event_id: Uuid, object_id: Uuid, sequence: i64) -> AuditEventDraft {
    AuditEventDraft {
        event_id,
        audit_version: AUDIT_VERSION,
        workspace_id,
        occurred_at: at(sequence),
        actor: AuditActor {
            kind: AuditActorKind::User,
            actor_ref: format!("user:{sequence}"),
        },
        action: "dataset.updated".to_owned(),
        reason_code: "user_request".to_owned(),
        request_id: format!("request-{sequence}"),
        correlation_id: Some(format!("correlation-{sequence}")),
        trace_id: Some(format!("trace-{sequence}")),
        object: AuditObjectRef {
            kind: "Dataset".to_owned(),
            id: object_id,
        },
        before: Some(json!({"state": "active"})),
        after: Some(json!({"state": "archived"})),
        lineage: vec![AuditLineageEdge {
            relation: "produces".to_owned(),
            from: AuditObjectRef {
                kind: "Dataset".to_owned(),
                id: object_id,
            },
            to: AuditObjectRef {
                kind: "PlanVersion".to_owned(),
                id: Uuid::from_u128(900 + sequence as u128),
            },
        }],
        source_event_id: None,
        payload: json!({"field": "state", "changed": true}),
        idempotency_key: Some(format!("audit-{sequence}")),
    }
}

#[test]
fn audit_append_is_idempotent_and_cursor_is_filter_bound() {
    let root = tempdir().expect("tempdir");
    let store = ControlPlaneStore::open(root.path()).expect("store");
    let workspace_id = Uuid::from_u128(1);
    let object_id = Uuid::from_u128(2);
    store
        .create_workspace(workspace_id, at(0))
        .expect("workspace");
    let first_draft = draft(workspace_id, Uuid::from_u128(3), object_id, 1);
    let first = store.audit().append(first_draft.clone()).expect("append");
    let replay = store.audit().append(first_draft).expect("replay");
    assert_eq!(first, replay);
    let second = store
        .audit()
        .append(draft(workspace_id, Uuid::from_u128(4), object_id, 2))
        .expect("second append");
    assert_eq!(first.sequence, 1);
    assert_eq!(second.sequence, 2);

    let first_page = store
        .audit()
        .query(AuditQuery {
            workspace_id,
            limit: 1,
            ..AuditQuery::default()
        })
        .expect("first page");
    assert_eq!(first_page.events.len(), 1);
    let cursor = first_page.next.expect("next cursor");
    let second_page = store
        .audit()
        .query(AuditQuery {
            workspace_id,
            limit: 1,
            cursor: Some(cursor.clone()),
            ..AuditQuery::default()
        })
        .expect("second page");
    assert_eq!(second_page.events[0].sequence, 2);

    let mismatch = store.audit().query(AuditQuery {
        workspace_id,
        actor_ref: Some("user:1".to_owned()),
        limit: 1,
        cursor: Some(cursor),
        ..AuditQuery::default()
    });
    assert!(mismatch.is_err(), "cursor must be bound to filters");
}

#[test]
fn audit_filters_are_workspace_scoped_and_expiry_is_explicit() {
    let root = tempdir().expect("tempdir");
    let store = ControlPlaneStore::open(root.path()).expect("store");
    let workspace_id = Uuid::from_u128(10);
    let other_workspace_id = Uuid::from_u128(11);
    let object_id = Uuid::from_u128(12);
    store
        .create_workspace(workspace_id, at(0))
        .expect("workspace");
    store
        .create_workspace(other_workspace_id, at(0))
        .expect("other workspace");
    let event = store
        .audit()
        .append(draft(workspace_id, Uuid::from_u128(13), object_id, 1))
        .expect("append");
    store
        .audit()
        .append(draft(other_workspace_id, Uuid::from_u128(14), object_id, 1))
        .expect("other append");
    let by_trace = store
        .audit()
        .query(AuditQuery {
            workspace_id,
            trace_id: Some("trace-1".to_owned()),
            object_kind: Some("Dataset".to_owned()),
            object_id: Some(object_id),
            limit: 10,
            ..AuditQuery::default()
        })
        .expect("trace filter");
    assert_eq!(by_trace.events.len(), 1);
    assert_eq!(by_trace.events[0].workspace_id, workspace_id);
    assert_eq!(by_trace.events[0].retention, AuditRetentionState::Active);

    store.audit().expire(event.event_id, at(5)).expect("expire");
    let hidden = store
        .audit()
        .query(AuditQuery {
            workspace_id,
            object_kind: Some("Dataset".to_owned()),
            object_id: Some(object_id),
            limit: 10,
            ..AuditQuery::default()
        })
        .expect("default retention view");
    assert!(hidden.events.is_empty());
    let visible = store
        .audit()
        .query(AuditQuery {
            workspace_id,
            object_kind: Some("Dataset".to_owned()),
            object_id: Some(object_id),
            include_expired: true,
            limit: 10,
            ..AuditQuery::default()
        })
        .expect("expired retention view");
    assert_eq!(visible.events[0].retention, AuditRetentionState::Expired);

    let rejected = store.audit().append(AuditEventDraft {
        payload: json!({"token": "must-not-persist"}),
        ..draft(workspace_id, Uuid::from_u128(15), object_id, 3)
    });
    assert!(
        rejected.is_err(),
        "secret-like audit payload must be rejected"
    );
}

#[test]
fn audit_api_enforces_capabilities_and_exposes_lineage_and_export() {
    let root = tempdir().expect("tempdir");
    let store = Arc::new(ControlPlaneStore::open(root.path()).expect("store"));
    let workspace_id = Uuid::from_u128(20);
    let member_id = Uuid::from_u128(21);
    let role_id = Uuid::from_u128(22);
    let object_id = Uuid::from_u128(23);
    let at = at(0);
    store.create_workspace(workspace_id, at).expect("workspace");
    store
        .identity()
        .create_member(workspace_id, member_id, "user:audit-api", at)
        .expect("member");
    store
        .identity()
        .create_role(workspace_id, role_id, "auditor", at)
        .expect("role");
    store
        .identity()
        .set_role_capabilities(
            workspace_id,
            role_id,
            &["workspace:read", "audit:read", "audit:export"],
        )
        .expect("capabilities");
    store
        .identity()
        .assign_role(workspace_id, member_id, role_id)
        .expect("role assignment");
    store
        .audit()
        .append(draft(workspace_id, Uuid::from_u128(24), object_id, 1))
        .expect("audit event");

    let server = ApiService::new(Arc::clone(&store)).with_server_authorization();
    let principal = RequestPrincipal::member(member_id);
    let meta = |id| RequestMetadata::new(id, workspace_id).with_principal(principal);
    let page = server
        .list_audit_events(ApiRequest {
            meta: meta(Uuid::from_u128(25)),
            body: ListAuditEventsRequest {
                object_kind: Some("Dataset".to_owned()),
                object_id: Some(object_id),
                limit: 10,
                ..ListAuditEventsRequest::default()
            },
        })
        .expect("authorized audit read");
    assert_eq!(page.body.events.len(), 1);
    assert_eq!(page.body.events[0].event_digest.len(), 64);

    let lineage = server
        .get_audit_lineage(ApiRequest {
            meta: meta(Uuid::from_u128(26)),
            body: AuditLineageRequest {
                object_kind: "Dataset".to_owned(),
                object_id,
                limit: 10,
                cursor: None,
            },
        })
        .expect("authorized lineage read");
    assert_eq!(lineage.body.events.len(), 1);
    assert_eq!(lineage.body.edges.len(), 1);

    let export = server
        .export_audit_events(ApiRequest {
            meta: meta(Uuid::from_u128(27)),
            body: ListAuditEventsRequest {
                limit: 10,
                ..ListAuditEventsRequest::default()
            },
        })
        .expect("authorized audit export");
    assert_eq!(export.body.event_count, 1);
    assert_eq!(export.body.export_digest.len(), 64);
}
