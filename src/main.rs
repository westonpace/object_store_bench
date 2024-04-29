use clap::Parser;
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
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let mut store = AmazonS3Builder::new()
        .with_bucket_name(args.bucket)
        .with_region("us-east-1");
    if let Some(access_key) = args.access_key {
        store = store.with_access_key_id(access_key);
    }
    if let Some(secret_key) = args.secret_key {
        store = store.with_secret_access_key(secret_key);
    }
    let store = store.build().unwrap();

    let path = Path::from("some_file.data");

    let total_size = args.total_size.unwrap_or(1024 * 1024 * 1024);
    let upload_size = args.upload_size.unwrap_or(8 * 1024 * 1024);
    let download_size = args.download_size.unwrap_or(32 * 1024 * 1024);
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

    let total_size = bytes_written;

    let mut bytes_read = 0;
    let total_start = std::time::Instant::now();
    while bytes_read < total_size as usize {
        let start = std::time::Instant::now();
        store
            .get_range(&path, bytes_read..bytes_read + download_size as usize)
            .await
            .unwrap();
        let elapsed = start.elapsed();
        println!("Download took {:?} seconds", elapsed.as_secs_f64());
        bytes_read += download_size as usize;
    }
    let total_elapsed = total_start.elapsed();
    println!(
        "Total download took {:?} seconds",
        total_elapsed.as_secs_f64()
    );
}
