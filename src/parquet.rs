use futures::{stream, StreamExt};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use tokio::fs::File;

#[tokio::main]
async fn main() {
    let path = format!("/home/pace/dev/data/nyctaxi/year=2013/month=4/part-0.parquet");
    let start = std::time::Instant::now();

    let row_group_indices = (0..1200)
        .step_by(150)
        .map(|off| off..off + 150)
        .collect::<Vec<_>>();

    let streams = row_group_indices
        .into_iter()
        .map(|rgi| {
            let path = path.clone();
            async move {
                let file = File::open(path).await.unwrap();

                let builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

                let file_metadata = builder.metadata().file_metadata();
                let mask = ProjectionMask::roots(
                    file_metadata.schema_descr(),
                    [0, 2, 4, 6, 8, 10, 12, 14, 16, 18],
                );

                builder
                    .with_projection(mask)
                    .with_row_groups(rgi.into_iter().collect::<Vec<_>>())
                    .build()
                    .unwrap()
            }
        })
        .collect::<Vec<_>>();

    let mut stream = stream::iter(streams).buffered(8).flatten();

    while let Some(res) = stream.next().await {
        res.unwrap();
    }
    print!("Time taken: {:?}", start.elapsed().as_secs_f64());
}
