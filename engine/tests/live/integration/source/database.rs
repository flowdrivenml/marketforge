use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use futures_util::FutureExt;
use sqlx::{Row, postgres::PgPoolOptions};
use marketforge_engine::live::stream::{persistence::*, registry::{StreamGraph,StreamRuntimeRegistry,StreamLifecycleState}, source::*};
use super::{profiles,harness,rest_support};

#[tokio::test]
#[ignore = "requires explicit scoped PostgreSQL write authorization and MARKETFORGE_DATABASE_URL"]
async fn authorized_persistence_restoration_and_cleanup() {
    let url = std::env::var("MARKETFORGE_DATABASE_URL").expect("authorized database environment required");
    let pool = PgPoolOptions::new().max_connections(4).connect(&url).await.unwrap_or_else(|_| panic!("database connection failed; details suppressed"));
    let repository = StreamDefinitionRepository::new(pool.clone());
    let prefix = format!("codex-source-{}-{}",std::process::id(),SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos());
    let requests = profiles::requests();
    let ids: Vec<String> = (0..16).map(|i| format!("{prefix}-{i}")).collect();
    let result = std::panic::AssertUnwindSafe(async {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("../../support/data/instruments/btc_sources.json")).unwrap();
        for (i,request) in requests.iter().enumerate() {
            let catalog = sqlx::query("SELECT i.*, s.quantity_type, s.contract_value, s.contract_value_asset, s.tick_size, s.qty_step FROM catalog.instruments i JOIN catalog.instrument_specs s ON s.instrument_id=i.id WHERE i.id=$1")
                .bind(request.instrument_id).fetch_one(&pool).await.unwrap();
            assert_eq!(catalog.get::<String,_>("symbol"),request.symbol);
            assert_eq!(catalog.get::<String,_>("market_category"),request.market_category.as_str());
            assert_eq!(catalog.get::<Option<rust_decimal::Decimal>,_>("contract_value"),request.contract_value.as_ref().map(|value|value.parse::<rust_decimal::Decimal>().unwrap()));
            assert_eq!(catalog.get::<Option<String>,_>("contract_value_asset"),request.contract_value_asset);
            assert_eq!(catalog.get::<String,_>("quantity_type"),fixture["instruments"][i]["quantity_type"].as_str().unwrap());
            for field in ["tick_size","qty_step"] {
                assert_eq!(catalog.get::<Option<rust_decimal::Decimal>,_>(field),fixture["instruments"][i][field].as_str().map(|v|v.parse().unwrap()));
            }
            let created = repository.register_source(&ids[i],request.clone(),DesiredStreamState::default()).await.unwrap();
            assert_eq!(repository.find(&ids[i]).await.unwrap().as_ref(),Some(&created));
            assert!(matches!(repository.register_source(&ids[i],request.clone(),DesiredStreamState::default()).await,Err(StreamDefinitionError::AlreadyExists(_))));
            let changed = repository.update_desired_state(&ids[i],1,DesiredStreamState { enabled:true,activation_policy:ActivationPolicy::AlwaysOn }).await.unwrap();
            assert_eq!(changed.revision,2);
            assert!(matches!(repository.update_desired_state(&ids[i],1,DesiredStreamState::default()).await,Err(StreamDefinitionError::RevisionConflict(_))));
            let profile = format!("{}_{}",request.exchange.as_str(),request.market_category.as_str());
            let builder = harness::builder(Arc::new(rest_support::load_profile(&profile,"http://127.0.0.1:1")),SourceInitializationLimits::default());
            let fresh = builder.load(&pool,&ids[i]).await.unwrap();
            assert_ne!(fresh.runner().readiness(),SourceReadiness::Ready);
            assert!(fresh.runner().active_epoch().is_none());
            let graph = StreamGraph::restore(vec![changed.clone()],vec![]).unwrap();
            let mut old = StreamRuntimeRegistry::new(&graph);
            let generation = old.begin_start(&ids[i]).unwrap();
            old.mark_running(&ids[i],generation).unwrap();
            // Simulated crash: discard runtime state, reload only durable definitions.
            drop(old);
            let persisted = repository.find(&ids[i]).await.unwrap().unwrap();
            let restored_graph = StreamGraph::restore(vec![persisted],vec![]).unwrap();
            let restored = StreamRuntimeRegistry::new(&restored_graph);
            assert_eq!(restored.status(&ids[i]).unwrap().lifecycle,StreamLifecycleState::Stopped);
            println!("[{profile}/postgres] create/retrieve/duplicate/desired revision/restart/fresh authority PASS");
        }
        let request = requests[0].clone();
        let (a,b) = tokio::join!(repository.register_source(&ids[14],request.clone(),DesiredStreamState::default()),repository.register_source(&ids[14],request,DesiredStreamState::default()));
        assert_eq!(usize::from(a.is_ok())+usize::from(b.is_ok()),1);
        assert!(matches!(a.as_ref().err().or(b.as_ref().err()),Some(StreamDefinitionError::AlreadyExists(_))));
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("INSERT INTO live.streams (stream_id,instrument_id,stream_kind) VALUES ($1,1,1)").bind(&ids[15]).execute(&mut *tx).await.unwrap();
        tx.rollback().await.unwrap();
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM live.streams WHERE stream_id=$1").bind(&ids[15]).fetch_one(&pool).await.unwrap(),0);
        // A failed FK insertion must leave neither a definition nor registration.
        let mut invalid = requests[0].clone(); invalid.instrument_id=i64::MAX;
        assert!(repository.register_source(&ids[15],invalid,DesiredStreamState::default()).await.is_err());
        assert!(repository.find(&ids[15]).await.unwrap().is_none());
        // Database failure is explicit; no in-memory success fallback.
        let failed_pool = PgPoolOptions::new().connect_lazy(&url).unwrap(); failed_pool.close().await;
        assert!(matches!(StreamDefinitionRepository::new(failed_pool).find(&ids[0]).await,Err(StreamDefinitionError::Database(_))));
    }).catch_unwind().await;
    // Always clean up exactly these test-owned identities, including on assertion failure.
    let mut cleanup = pool.begin().await.unwrap();
    let definitions_removed = sqlx::query("DELETE FROM live.stream_definitions WHERE stream_id=ANY($1)").bind(&ids).execute(&mut *cleanup).await.unwrap().rows_affected();
    let registrations_removed = sqlx::query("DELETE FROM live.streams WHERE stream_id=ANY($1)").bind(&ids).execute(&mut *cleanup).await.unwrap().rows_affected();
    cleanup.commit().await.unwrap();
    assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM live.streams WHERE stream_id=ANY($1)").bind(&ids).fetch_one(&pool).await.unwrap(),0);
    println!("[postgres] owner={prefix} registrations_removed={registrations_removed} definitions_removed={definitions_removed}; cleanup verified");
    pool.close().await;
    if let Err(error) = result { std::panic::resume_unwind(error); }
}
