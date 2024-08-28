use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use clap::Parser;
use futures::StreamExt;
use object_store::{
    gcp::GoogleCloudStorageBuilder, path::Path, BackoffConfig, ObjectStore, PutPayload,
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    initial_part_size: Option<u64>,

    #[arg(short, long)]
    total_size: Option<u64>,

    #[arg(short, long)]
    max_parallelism: Option<u64>,

    #[arg(short, long)]
    bucket: String,

    #[arg(short, long)]
    path: Option<String>,
}

const PART_SIZE_INCREMENT: u64 = 5 * 1024 * 1024;

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let path = args.path.unwrap_or("big_upload.data".to_string());
    let path = Path::from(path.as_str());

    let total_size = args.total_size.unwrap_or(2 * 1024 * 1024 * 1024 * 1024);
    let initial_part_size = args.initial_part_size.unwrap_or(PART_SIZE_INCREMENT);
    let max_parallelism = args.max_parallelism.unwrap_or(32);

    let make_store = move || {
        let builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name(args.bucket.clone())
            .with_retry(object_store::RetryConfig {
                max_retries: 1000,
                retry_timeout: Duration::from_secs(10000),
                backoff: BackoffConfig {
                    init_backoff: Duration::from_secs(5),
                    max_backoff: Duration::from_secs(30),
                    base: 2.,
                },
            });
        Arc::new(builder.build().unwrap())
    };

    let store = make_store();

    let mut bytes_written = 0;
    log::info!(
        "Uploading {} bytes of data starting with chunks of size {}",
        total_size,
        initial_part_size
    );

    // Just initialize whatever garbage.  Max that `part_size` can reach is 99 * initial_part_size
    let max_part_size = 99 * initial_part_size as usize;
    let mut data = bytes::BytesMut::with_capacity(max_part_size);
    unsafe { data.set_len(max_part_size) };
    let data = data.freeze();

    let multipart = Arc::new(Mutex::new(store.put_multipart(&path).await.unwrap()));
    let total_start = std::time::Instant::now();

    let mut tasks = Vec::with_capacity(10000);
    while bytes_written < total_size {
        let data = data.clone();
        let multipart = multipart.clone();
        let part_size =
            initial_part_size.max(((tasks.len() as u64 / 100) + 1) * PART_SIZE_INCREMENT);
        tasks.push(async move {
            let start = std::time::Instant::now();
            let part = data.slice(0..part_size as usize);
            log::info!("About to upload {} bytes of data", part.len());
            {
                let mut multipart = multipart.lock().unwrap();
                multipart.put_part(PutPayload::from_bytes(part))
            }
            .await
            .unwrap();
            log::info!(
                "Upload took {:?} seconds progress={}",
                start.elapsed().as_secs_f64(),
                bytes_written as f64 / total_size as f64
            );
        });
        bytes_written += part_size;
    }
    log::info!("Generated {} tasks to upload", tasks.len());
    futures::stream::iter(tasks)
        .buffered(max_parallelism as usize)
        .collect::<Vec<_>>()
        .await;

    let mut multipart = multipart.lock().unwrap();
    loop {
        match multipart.complete().await {
            Ok(_) => break,
            Err(e) => {
                log::error!("Error completing multipart upload: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
    log::info!(
        "Total upload took {:?} seconds",
        total_start.elapsed().as_secs_f64()
    );
}
