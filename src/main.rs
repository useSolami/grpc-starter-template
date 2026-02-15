use std::collections::HashMap;
use std::env;
use futures::StreamExt;
use tracing::{error, info};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
};
#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    subscribe_transactions().await;
}

async fn subscribe_transactions() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let endpoint = env::var("GRPC_ENDPOINT").unwrap_or("https://grpc-ams.solami.fast".to_string());
    let token = env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN env var is required");

    info!(%endpoint, "connecting to geyser backend");

    let mut builder =
        GeyserGrpcClient::build_from_shared(endpoint.clone()).expect("invalid endpoint");

    if endpoint.starts_with("https") {
        builder = builder
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .expect("tls config failed");
    }

    let mut client = builder
        .x_token(Some(token))
        .expect("x_token failed")
        .connect_timeout(std::time::Duration::from_secs(10))
        .connect()
        .await
        .expect("failed to connect to geyser");

    info!("connected, subscribing to transactions for 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");

    let mut transactions = HashMap::new();
    transactions.insert(
        "pump_txs".to_owned(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include: vec!["6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P".to_owned()],
            account_exclude: vec![],
            account_required: vec![],
            signature: None,
        },
    );

    let request = SubscribeRequest {
        accounts: HashMap::default(),
        slots: HashMap::default(),
        transactions,
        transactions_status: HashMap::default(),
        blocks: HashMap::default(),
        blocks_meta: HashMap::default(),
        entry: HashMap::default(),
        commitment: Some(CommitmentLevel::Processed as i32),
        accounts_data_slice: vec![],
        ping: None,
        from_slot: None,
    };

    let (_sink, mut stream) = client
        .subscribe_with_request(Some(request))
        .await
        .expect("subscribe failed");

    info!("subscribed, waiting for transactions...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => {
                if let Some(ref inner) = update.update_oneof {
                    match inner {
                        yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Transaction(tx) => {
                            let sig = tx.transaction.as_ref()
                                .and_then(|t| t.signature.get(..64))
                                .map(|b| bs58::encode(b).into_string())
                                .unwrap_or_default();
                            let slot = tx.slot;
                            info!(%sig, slot, "transaction received");
                        }
                        yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Ping(_) => {
                            info!("ping");
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "stream error");
                break;
            }
        }
    }
}
