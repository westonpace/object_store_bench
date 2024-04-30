use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use bytes::Buf;
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    download_size: Option<u64>,

    #[arg(short, long)]
    total_size: Option<u64>,

    #[arg(long)]
    access_key: Option<String>,

    #[arg(long)]
    secret_key: Option<String>,

    #[arg(short, long)]
    bucket: String,

    #[arg(short, long)]
    path: Option<String>,

    #[arg(short, long)]
    num_clients: Option<u32>,

    #[arg(short, long)]
    max_threads_per_client: Option<u32>,

    #[arg(long)]
    num_iterations: Option<u32>,
}

async fn make_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = aws_sdk_s3::Client::new(&config);
    client
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let path = args.path.unwrap_or("some_file.data".to_string());

    let num_clients = args.num_clients.unwrap_or(8);
    let threads_per_client = args.max_threads_per_client.unwrap_or(8);

    let num_iterations = args.num_iterations.unwrap_or(5);

    let download_size = args.download_size.unwrap_or(32 * 1024 * 1024);

    let bucket = args.bucket.clone();

    let client = make_client().await;

    let total_size = client
        .head_object()
        .bucket(&bucket)
        .key(path.clone())
        .send()
        .await
        .unwrap()
        .content_length
        .unwrap() as u64;

    for _ in 0..num_iterations {
        let mut task_idx = 0;
        let mut read_tasks = Vec::with_capacity(num_clients as usize);
        for _ in 0..num_clients {
            let client = make_client().await;
            read_tasks.push((client, Vec::new()));
        }
        while (task_idx * download_size) < total_size {
            let client_idx = (task_idx % num_clients as u64) as usize;
            let path = path.clone();
            let bucket = bucket.clone();
            let read_start = task_idx * download_size;
            let read_end = read_start + download_size;
            let store = read_tasks[client_idx].0.clone();
            read_tasks[client_idx].1.push(async move {
                let start = std::time::Instant::now();
                let range = format!("bytes={}-{}", read_start, read_end);
                let mut data = store
                    .get_object()
                    .bucket(bucket)
                    .range(range)
                    .key(path)
                    .send()
                    .await
                    .unwrap()
                    .body
                    .collect()
                    .await
                    .unwrap();
                let mut num_bytes = 0;
                let mut num_chunks = 0;
                while data.remaining() > 0 {
                    let bytes_in_chunk = data.chunk().len();
                    num_bytes += bytes_in_chunk;
                    num_chunks += 1;
                    data.advance(bytes_in_chunk);
                }
                println!(
                    "Download on client {} took {:?} seconds to get {} bytes across {} chunks",
                    client_idx,
                    start.elapsed().as_secs_f64(),
                    num_bytes,
                    num_chunks,
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
