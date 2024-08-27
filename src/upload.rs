use std::sync::{Arc, Mutex};

use clap::Parser;
use futures::StreamExt;
use object_store::{gcp::GoogleCloudStorageBuilder, path::Path, ObjectStore, PutPayload};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    part_size: Option<u64>,

    #[arg(short, long)]
    total_size: Option<u64>,

    #[arg(short, long)]
    max_parallelism: Option<u64>,

    #[arg(short, long)]
    bucket: String,

    #[arg(short, long)]
    path: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();

    let path = args.path.unwrap_or("big_upload.data".to_string());
    let path = Path::from(path.as_str());

    let total_size = args.total_size.unwrap_or(2 * 1024 * 1024 * 1024 * 1024);
    let part_size = args.part_size.unwrap_or(10 * 1024 * 1024);
    let max_parallelism = args.max_parallelism.unwrap_or(32);

    let make_store = move || {
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(args.bucket.clone());
        Arc::new(builder.build().unwrap())
    };

    let store = make_store();

    let mut bytes_written = 0;
    log::info!(
        "Uploading {} bytes of data in chunks of {}",
        total_size,
        part_size
    );

    // Just initialize whatever garbage
    let mut data = bytes::BytesMut::with_capacity(part_size as usize);
    unsafe { data.set_len(part_size as usize) };
    let data = data.freeze();

    let multipart = Arc::new(Mutex::new(store.put_multipart(&path).await.unwrap()));
    let total_start = std::time::Instant::now();

    let mut tasks = Vec::with_capacity(total_size as usize / part_size as usize + 1);
    while bytes_written < total_size {
        let data = data.clone();
        let multipart = multipart.clone();
        tasks.push(async move {
            let start = std::time::Instant::now();
            log::info!("About to upload {} bytes of data", data.len());
            {
                let mut multipart = multipart.lock().unwrap();
                multipart.put_part(PutPayload::from_bytes(data.clone()))
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
    futures::stream::iter(tasks)
        .buffered(max_parallelism as usize)
        .collect::<Vec<_>>()
        .await;

    let mut multipart = multipart.lock().unwrap();
    multipart.complete().await.unwrap();
    log::info!(
        "Total upload took {:?} seconds",
        total_start.elapsed().as_secs_f64()
    );
}
