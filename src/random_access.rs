use std::sync::Arc;

use clap::Parser;
use futures::StreamExt;
use object_store::{
    aws::AmazonS3Builder, gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory,
    path::Path, ObjectStore, PutPayload,
};
use rand::prelude::SliceRandom;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    num_files: Option<u64>,

    #[arg(long)]
    num_rows: Option<u64>,

    #[arg(long)]
    bytes_per_row: Option<u64>,

    #[arg(long)]
    max_concurrent_reads: Option<u64>,

    #[arg(long)]
    access_key: Option<String>,

    #[arg(long)]
    secret_key: Option<String>,

    #[arg(long)]
    skip_upload: bool,

    #[arg(long)]
    base_uri: String,

    #[arg(long)]
    path: Option<String>,

    #[arg(long)]
    num_iterations: Option<u32>,

    #[arg(long)]
    takes_per_iter: Option<u32>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let path = args.path.unwrap_or("rab_files".to_string());
    let path = Path::from(path.as_str());

    let num_iterations = args.num_iterations.unwrap_or(5);

    let num_files = args.num_files.unwrap_or(10);
    let num_rows = args.num_rows.unwrap_or(1000000);
    let bytes_per_row = args.bytes_per_row.unwrap_or(8);
    let max_concurrent_reads = args.max_concurrent_reads.unwrap_or(10000);
    let takes_per_iter = args.takes_per_iter.unwrap_or(10000);

    let make_store = move || {
        if args.base_uri.starts_with("s3://") {
            let mut store = AmazonS3Builder::new()
                .with_bucket_name(args.base_uri[5..].to_string())
                .with_region("us-east-1");
            if let Some(access_key) = args.access_key.clone() {
                store = store.with_access_key_id(access_key);
            }
            if let Some(secret_key) = args.secret_key.clone() {
                store = store.with_secret_access_key(secret_key);
            }
            Arc::new(store.build().unwrap()) as Arc<dyn ObjectStore>
        } else if args.base_uri.starts_with("gs://") {
            let store = GoogleCloudStorageBuilder::new()
                .with_bucket_name(args.base_uri[4..].to_string())
                .build()
                .unwrap();
            Arc::new(store) as Arc<dyn ObjectStore>
        } else if args.base_uri == "memory" {
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>
        } else {
            Arc::new(LocalFileSystem::new_with_prefix(args.base_uri).unwrap())
        }
    };

    let store = make_store();
    let rows_per_file = num_rows / num_files;
    println!("Num rows: {}", num_rows);
    println!("Num files: {}", num_files);
    println!("Rows per file: {}", rows_per_file);

    if !args.skip_upload {
        for file_idx in 0..num_files {
            let offset = file_idx * rows_per_file;
            let capacity = num_rows - offset;
            let rows_this_file = rows_per_file.min(capacity);
            let upload_size = rows_this_file as usize * bytes_per_row as usize;
            let mut data = bytes::BytesMut::with_capacity(upload_size);
            unsafe { data.set_len(upload_size) };
            let data = data.freeze();
            let path = path.child(file_idx.to_string());
            let mut multipart = store.put_multipart(&path).await.unwrap();
            let start = std::time::Instant::now();
            println!("About to upload {} bytes of data", data.len());
            multipart
                .put_part(PutPayload::from_bytes(data.clone()))
                .await
                .unwrap();
            multipart.complete().await.unwrap();
            println!("Upload took {:?} seconds", start.elapsed().as_secs_f64());
        }
    }

    for _ in 0..num_iterations {
        let mut row_ids = (0..num_rows).collect::<Vec<_>>();
        row_ids.shuffle(&mut rand::thread_rng());
        let mut read_tasks = Vec::with_capacity(num_rows as usize);
        for task_id in 0..takes_per_iter {
            let path = path.clone();
            let addr = row_ids[task_id as usize];
            let file_id = addr / rows_per_file;
            if file_id > 10 {
                println!("{} {} {}", addr, file_id, rows_per_file);
                panic!();
            }
            let file_offset = (addr % rows_per_file) * bytes_per_row;
            let read_end = file_offset + bytes_per_row;
            let store = store.clone();
            let path = path.child(file_id.to_string());

            read_tasks.push(async move {
                // let start = std::time::Instant::now();
                let store = store.clone();
                store
                    .get_range(&path, file_offset as usize..read_end as usize)
                    .await
                    .unwrap();
                // println!("Download took {:?} seconds", start.elapsed().as_secs_f64());
            });
        }

        let total_start = std::time::Instant::now();
        futures::stream::iter(read_tasks)
            .buffer_unordered(max_concurrent_reads as usize)
            .collect::<Vec<_>>()
            .await;

        let total_elapsed = total_start.elapsed();
        let iops_per_second = (takes_per_iter as f64) / total_elapsed.as_secs_f64();
        let gibps = (takes_per_iter as f64 * bytes_per_row as f64)
            / total_elapsed.as_secs_f64()
            / (1024.0 * 1024.0 * 1024.0);
        println!(
            "Total download took {:?} seconds ({} iops/s {} GiB/s)",
            total_elapsed.as_secs_f64(),
            iops_per_second,
            gibps,
        );
    }
}
