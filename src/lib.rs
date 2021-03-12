#![allow(non_snake_case, unused)]

include!(concat!(env!("OUT_DIR"), "/glue.rs"));

#[cfg(test)]
mod test {
    #[test]
    fn why() {
        use fluvio;
        use fluvio_future::task::run_block_on;
        run_block_on(async || {
            let consumer = fluvio::consumer("my-topic-key-value-iterator", 0).await.unwrap();
            let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;
            use fluvio_future::io::StreamExt;
            loop {
                let next = stream.next().await.unwrap().unwrap();
                println!("{:?} {:?}", next.key(), next.value());
            }
        }())
    }
}
