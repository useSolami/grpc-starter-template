use std::collections::HashMap;
use std::env;

use futures::StreamExt;
use tracing::{error, info, warn};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterTransactions,
    subscribe_update::UpdateOneof,
};

const PUMPFUN_PROGRAM_ID: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const CREATE_DISCRIMINATOR: [u8; 8] = [24, 30, 200, 40, 5, 28, 7, 119];
const CREATE_V2_DISCRIMINATOR: [u8; 8] = [214, 144, 76, 236, 95, 139, 49, 180];

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    subscribe_pumpfun_creates().await;
}

async fn subscribe_pumpfun_creates() {
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

    info!(
        program = PUMPFUN_PROGRAM_ID,
        "connected, subscribing to pump.fun transactions"
    );

    let mut transactions = HashMap::new();
    transactions.insert(
        "pump_txs".to_owned(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            account_include: vec![PUMPFUN_PROGRAM_ID.to_owned()],
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

    let pump_program = bs58::decode(PUMPFUN_PROGRAM_ID)
        .into_vec()
        .expect("bad pump.fun program id");

    info!("subscribed, watching for pump.fun create instructions...");
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(update) => match update.update_oneof {
                Some(UpdateOneof::Transaction(tx_update)) => {
                    if let Err(e) = handle_tx(&tx_update, &pump_program) {
                        warn!(error = %e, "failed to handle tx");
                    }
                }
                Some(UpdateOneof::Ping(_)) => {}
                _ => {}
            },
            Err(e) => {
                error!(error = %e, "stream error");
                break;
            }
        }
    }
}

fn handle_tx(
    tx_update: &yellowstone_grpc_proto::geyser::SubscribeUpdateTransaction,
    pump_program: &[u8],
) -> Result<(), String> {
    let info = tx_update.transaction.as_ref().ok_or("missing tx info")?;
    let tx = info.transaction.as_ref().ok_or("missing transaction")?;
    let message = tx.message.as_ref().ok_or("missing message")?;
    let meta = info.meta.as_ref();

    let mut keys: Vec<&[u8]> = message.account_keys.iter().map(|k| k.as_slice()).collect();
    if let Some(m) = meta {
        for k in &m.loaded_writable_addresses {
            keys.push(k.as_slice());
        }
        for k in &m.loaded_readonly_addresses {
            keys.push(k.as_slice());
        }
    }

    let sig = info
        .signature
        .get(..64)
        .map(|b| bs58::encode(b).into_string())
        .unwrap_or_default();
    let slot = tx_update.slot;

    for (idx, ix) in message.instructions.iter().enumerate() {
        try_decode_create(
            &keys,
            pump_program,
            ix.program_id_index as usize,
            &ix.accounts,
            &ix.data,
            &sig,
            slot,
            idx,
            None,
        );
    }

    if let Some(m) = meta {
        for inner in &m.inner_instructions {
            for (ii, inst) in inner.instructions.iter().enumerate() {
                try_decode_create(
                    &keys,
                    pump_program,
                    inst.program_id_index as usize,
                    &inst.accounts,
                    &inst.data,
                    &sig,
                    slot,
                    inner.index as usize,
                    Some(ii),
                );
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn try_decode_create(
    keys: &[&[u8]],
    pump_program: &[u8],
    program_idx: usize,
    accounts: &[u8],
    data: &[u8],
    sig: &str,
    slot: u64,
    outer_ix: usize,
    inner_ix: Option<usize>,
) {
    let Some(prog) = keys.get(program_idx) else {
        return;
    };
    if *prog != pump_program {
        return;
    }
    if data.len() < 8 {
        return;
    }
    let disc: [u8; 8] = data[..8].try_into().unwrap();

    let (variant, user_idx, parse_v2_tail) = match disc {
        CREATE_DISCRIMINATOR => ("create", 7, false),
        CREATE_V2_DISCRIMINATOR => ("create_v2", 5, true),
        _ => return,
    };

    let mut cursor = &data[8..];
    let Some(name) = read_borsh_string(&mut cursor) else {
        return;
    };
    let Some(symbol) = read_borsh_string(&mut cursor) else {
        return;
    };
    let Some(uri) = read_borsh_string(&mut cursor) else {
        return;
    };
    if cursor.len() < 32 {
        return;
    }
    let mut creator_bytes = [0u8; 32];
    creator_bytes.copy_from_slice(&cursor[..32]);
    let creator_arg = bs58::encode(creator_bytes).into_string();
    cursor = &cursor[32..];

    let (is_mayhem_mode, is_cashback_enabled) = if parse_v2_tail {
        let mayhem = cursor.first().copied().map(|b| b != 0);
        let cashback = cursor.get(1).copied().map(|b| b != 0);
        (mayhem, cashback)
    } else {
        (None, None)
    };

    let resolve = |pos: usize| -> Option<String> {
        accounts
            .get(pos)
            .and_then(|i| keys.get(*i as usize))
            .map(|b| bs58::encode(b).into_string())
    };
    let mint = resolve(0);
    let bonding_curve = resolve(2);
    let associated_bonding_curve = resolve(3);
    let user = resolve(user_idx);

    let location = match inner_ix {
        Some(ii) => format!("inner[{outer_ix}.{ii}]"),
        None => format!("ix[{outer_ix}]"),
    };

    info!(
        slot,
        sig = %sig,
        variant = %variant,
        location = %location,
        name = %name,
        symbol = %symbol,
        uri = %uri,
        mint = ?mint,
        bonding_curve = ?bonding_curve,
        associated_bonding_curve = ?associated_bonding_curve,
        user = ?user,
        creator = %creator_arg,
        is_mayhem_mode = ?is_mayhem_mode,
        is_cashback_enabled = ?is_cashback_enabled,
        "pump.fun create"
    );
}

fn read_borsh_string(cursor: &mut &[u8]) -> Option<String> {
    if cursor.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(cursor[..4].try_into().ok()?) as usize;
    *cursor = &cursor[4..];
    if cursor.len() < len {
        return None;
    }
    let s = String::from_utf8(cursor[..len].to_vec()).ok()?;
    *cursor = &cursor[len..];
    Some(s)
}
