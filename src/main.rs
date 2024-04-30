use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use object_store::{aws::AmazonS3Builder, path::Path, ObjectStore, PutPayload};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    upload_size: Option<u64>,

    #[arg(short, long)]
    download_size: Option<u64>,

    #[arg(short, long)]
    total_size: Option<u64>,

    #[arg(long)]
    access_key: Option<String>,

    #[arg(long)]
    secret_key: Option<String>,

    #[arg(short, long)]
    skip_upload: bool,

    #[arg(short, long)]
    bucket: String,

    #[arg(short, long)]
    path: Option<String>,

    #[arg(short, long)]
    num_clients: Option<u32>,

    #[arg(short, long)]
    max_threads_per_client: Option<u32>,

    #[arg(short, long)]
    num_iterations: Option<u32>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let path = args.path.unwrap_or("some_file.data".to_string());
    let path = Path::from(path.as_str());

    let num_clients = args.num_clients.unwrap_or(8);
    let threads_per_client = args.max_threads_per_client.unwrap_or(8);

    let num_iterations = args.num_iterations.unwrap_or(5);

    let total_size = args.total_size.unwrap_or(1024 * 1024 * 1024);
    let upload_size = args.upload_size.unwrap_or(8 * 1024 * 1024);
    let download_size = args.download_size.unwrap_or(32 * 1024 * 1024);

    let make_store = move || {
        let mut store = AmazonS3Builder::new()
            .with_bucket_name(args.bucket.clone())
            .with_region("us-east-1");
        if let Some(access_key) = args.access_key.clone() {
            store = store.with_access_key_id(access_key);
        }
        if let Some(secret_key) = args.secret_key.clone() {
            store = store.with_secret_access_key(secret_key);
        }
        Arc::new(store.build().unwrap())
    };

    let store = make_store();

    let total_size = if !args.skip_upload {
        // Upload file
        let mut bytes_written = 0;
        println!(
            "Uploading {} bytes of data in chunks of {}",
            total_size, upload_size
        );
        // Just initialize whatever garbage
        let mut data = bytes::BytesMut::with_capacity(upload_size as usize);
        unsafe { data.set_len(upload_size as usize) };
        let data = data.freeze();
        let mut multipart = store.put_multipart(&path).await.unwrap();
        let total_start = std::time::Instant::now();
        while bytes_written < total_size {
            let start = std::time::Instant::now();
            println!("About to upload {} bytes of data", data.len());
            multipart
                .put_part(PutPayload::from_bytes(data.clone()))
                .await
                .unwrap();
            println!("Upload took {:?} seconds", start.elapsed().as_secs_f64());
            bytes_written += upload_size;
        }
        multipart.complete().await.unwrap();
        println!(
            "Total upload took {:?} seconds",
            total_start.elapsed().as_secs_f64()
        );

        bytes_written
    } else {
        let meta = store.head(&path).await.unwrap();
        meta.size as u64
    };

    for _ in 0..num_iterations {
        let mut task_idx = 0;
        let mut read_tasks = Vec::with_capacity(num_clients as usize);
        for _ in 0..num_clients {
            let store = make_store();
            read_tasks.push((store, Vec::new()));
        }
        while (task_idx * download_size) < total_size {
            let client_idx = (task_idx % num_clients as u64) as usize;
            let path = path.clone();
            let read_start = task_idx * download_size;
            let read_end = read_start + download_size;
            let store = read_tasks[client_idx].0.clone();
            read_tasks[client_idx].1.push(async move {
                let start = std::time::Instant::now();
                store
                    .get_range(&path, read_start as usize..read_end as usize)
                    .await
                    .unwrap();
                println!(
                    "Download on client {} took {:?} seconds",
                    client_idx,
                    start.elapsed().as_secs_f64()
                );
            });
            task_idx += 1;
        }

        let total_start = std::time::Instant::now();
        let read_tasks = read_tasks
            .into_iter()
            .map(|tasks| {
                futures::stream::iter(tasks.1)
                    .buffer_unordered(threads_per_client as usize)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        futures::stream::iter(read_tasks)
            .buffer_unordered(num_clients as usize)
            .collect::<Vec<_>>()
            .await;

        let total_elapsed = total_start.elapsed();
        println!(
            "Total download took {:?} seconds",
            total_elapsed.as_secs_f64()
        );
    }
}
