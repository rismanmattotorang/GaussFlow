use anyhow::Result;
use gaussflow_core::TypeSafeDag;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use serde_json::json;
use surrealdb::engine::remote::http::Http;
use surrealdb::opt::auth::Root;
use surrealdb::Surreal;
use warp::{Filter, Rejection, Reply};

/// Resolve SurrealDB connection settings from the environment, falling back to local-dev
/// defaults. No credentials are hardcoded; set `GAUSSFLOW_DB_PASS` for any non-local deployment.
/// Returns `(url, user, password, namespace, database)`.
fn surreal_settings() -> (String, String, String, String, String) {
    (
        std::env::var("GAUSSFLOW_SURREAL_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string()),
        std::env::var("GAUSSFLOW_DB_USER").unwrap_or_else(|_| "root".to_string()),
        std::env::var("GAUSSFLOW_DB_PASS").unwrap_or_else(|_| "root".to_string()),
        std::env::var("GAUSSFLOW_DB_NS").unwrap_or_else(|_| "gaussflow".to_string()),
        std::env::var("GAUSSFLOW_DB_NAME").unwrap_or_else(|_| "gaussflow".to_string()),
    )
}

const SCHEMA_SQL: &str = r#"
DEFINE NAMESPACE gaussflow;
DEFINE DATABASE gaussflow;
USE NS gaussflow DB gaussflow;

DEFINE TABLE workflow SCHEMAFULL;
DEFINE FIELD name            ON workflow TYPE string;
DEFINE FIELD created         ON workflow TYPE datetime DEFAULT time::now();

DEFINE TABLE run SCHEMAFULL;
DEFINE FIELD workflow_id     ON run TYPE record<workflow>;
DEFINE FIELD status          ON run TYPE string;
DEFINE FIELD started         ON run TYPE datetime DEFAULT time::now();
DEFINE FIELD finished        ON run TYPE datetime;
DEFINE FIELD input           ON run TYPE object;
DEFINE FIELD output          ON run TYPE object;

DEFINE TABLE artefact SCHEMAFULL;
DEFINE FIELD run_id          ON artefact TYPE record<run>;
DEFINE FIELD key             ON artefact TYPE string;
DEFINE FIELD path            ON artefact TYPE string;
"#;

pub async fn start_server() -> Result<()> {
    // Determine port from GF_PORT env or fallback
    let port: u16 = std::env::var("GF_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3030);
    // Connect or bootstrap SurrealDB using environment-sourced credentials.
    let (surreal_url, db_user, db_pass, ns, db_name) = surreal_settings();
    let db = Surreal::new::<Http>(surreal_url.as_str()).await?;
    db.signin(Root { username: db_user.as_str(), password: db_pass.as_str() }).await?;
    db.use_ns(ns.as_str()).use_db(db_name.as_str()).await?;

    // Try to run schema – if tables exist SurrealDB will ignore duplicates.
    let _ = db.query(SCHEMA_SQL).await?;

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        sub: String,
        exp: usize,
    }

    let auth = warp::header::optional::<String>("authorization").and_then(
        |h: Option<String>| async move {
            if let Some(header) = h {
                if let Some(token) = header.strip_prefix("Bearer ") {
                    let res = decode::<Claims>(
                        token,
                        &DecodingKey::from_secret("secret".as_ref()),
                        &Validation::new(Algorithm::HS256),
                    );
                    if res.is_ok() {
                        return Ok(());
                    }
                }
            }
            Err(warp::reject::custom(Unauthorized))
        },
    );

    #[derive(Debug)]
    struct Unauthorized;
    impl warp::reject::Reject for Unauthorized {}

    let health_route = warp::path("health").map(|| warp::reply::json(&json!({ "status": "ok" })));

    let run_route = warp::path("run")
        .and(warp::post())
        .and(auth.clone())
        .and(warp::body::json())
        .and_then(handle_run);

    let get_run_route = auth
        .clone()
        .and(warp::path!("runs" / String))
        .and_then(handle_get_run);

    let routes = health_route.or(run_route).or(get_run_route);

    println!("GaussFlow server running on http://0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
    Ok(())
}

async fn handle_run(_: (), body: serde_json::Value) -> Result<impl Reply, Rejection> {
    // expect {"workflow": <workflow spec json>, "input": <json optional> }
    let wf = body
        .get("workflow")
        .ok_or_else(warp::reject::reject)?;
    let input = body
        .get("input")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    let dag = TypeSafeDag::from_json(&wf.to_string()).map_err(|_| warp::reject())?;
    let res = gaussflow_runtime::execute(dag, input)
        .await
        .map_err(|_| warp::reject())?;

    Ok(warp::reply::json(&res))
}

async fn handle_get_run(_: (), id: String) -> Result<impl Reply, Rejection> {
    let (surreal_url, db_user, db_pass, ns, db_name) = surreal_settings();
    let db = Surreal::new::<Http>(surreal_url.as_str())
        .await
        .map_err(|_| warp::reject())?;
    db.signin(Root { username: db_user.as_str(), password: db_pass.as_str() })
        .await
        .map_err(|_| warp::reject())?;
    db.use_ns(ns.as_str()).use_db(db_name.as_str()).await.map_err(|_| warp::reject())?;
    let mut res = db
        .query("SELECT * FROM type::thing('run', $id)")
        .bind(("id", id.clone()))
        .await
        .map_err(|_| warp::reject())?;

    use surrealdb::sql::Value as SValue;
    let val: Option<SValue> = res.take(0).map_err(|_| warp::reject())?;
    let json_val = serde_json::to_value(val).map_err(|_| warp::reject())?;

    Ok(warp::reply::json(&json_val))
}
