use mini_redis::{client, Result};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tracing_subscriber::prelude::*;

#[tokio::main]
pub async fn main() -> Result<()> {
    // console_subscriber::init();
    console_subscriber::build()
        .with(tracing_subscriber::fmt::layer())
        .init();

    let name = std::env::args().next();
    if std::env::args().skip(1).len() == 0 {
        panic!("{} needs >= 1 input path where it can find ASCII text.", name.unwrap());
    }

    loop {
        for arg in std::env::args().skip(1) {
            let total_read = read_file(&arg).await?;
            println!("read {:?} from {}", total_read, arg);
        }
    }

    // Ok(())
}

#[derive(Debug)]
struct TotalRead {
    bytes: usize,
    words: usize,
}

struct ChunkReader {
    file_name: String,
    offset: i64,
    max_chunk_size: usize,
    bytes: Arc<AtomicUsize>,
    words: Arc<AtomicUsize>,
}

async fn read_file(arg: &str) -> Result<TotalRead> {
    let bytes = Arc::new(AtomicUsize::new(0));
    let words = Arc::new(AtomicUsize::new(0));

    let chunk_reader = Arc::new(ChunkReader {
        offset: 0,
        max_chunk_size: usize::MAX,
        file_name: arg.to_string(),
        bytes: bytes.clone(),
        words: words.clone(),
    });

    let task = tokio::task::Builder::new()
        .name(&format!("read({})", arg))
        .spawn(read_chunks(chunk_reader));

    let (tr,) = tokio::try_join! { task }?;
    tr?;

    let read = TotalRead {
        bytes: bytes.load(Ordering::SeqCst),
        words: words.load(Ordering::SeqCst),
    };

    Ok(read)
}

async fn read_chunks(chunk_reader: Arc<ChunkReader>) -> Result<()> {
    let mut client = client::connect("127.0.0.1:6379").await?;

    #[derive(PartialEq, Eq)]
    enum State {
        Unsync,
        Word,
        Nonword,
    }

    let cr = chunk_reader.clone();

    let mut file = File::open(&cr.file_name)?;
    file.seek(SeekFrom::Current(cr.offset))?;
    let mut state = State::Unsync;
    let mut seen_bytes: usize = 0;
    let mut seen_words: usize = 0;
    let mut word_buf = String::new();
    for (j, byte) in file.bytes().enumerate() {
        seen_bytes += 1;
        let c = byte? as char;
        state = match (c.is_alphabetic(), state) {
            (true, State::Unsync) =>
                if cr.offset == 0 && seen_bytes == 1 {
                    // Alphabetic first byte starts a word
                    word_buf.push(c);
                    State::Word
                } else {
                    // Read until we see non-alphabetic, to synchronize with predecessor task
                    State::Unsync
                },

            // Non-alphabetic: synchronize and start looking for words.
            (false, State::Unsync) => State::Nonword,

            // Start of word, or another letter on a word; keep going
            (true, State::Word | State::Nonword) => {
                word_buf.push(c);
                State::Word
            }

            // End of word; record it and shift state.
            (false, State::Word) => {
                seen_words += 1;
                println!("avant client_get {}", word_buf); 
                let client_get = client.get(&word_buf).await?;
                println!("apres client_get {}", word_buf); 
                let new_count = if let Some(s) = client_get {
                    let s: &str = std::str::from_utf8(&*s).unwrap();
                    let count = s.parse::<u64>().unwrap();
                    count + 1
                } else {
                    1
                };
                println!("byte {}, saw \"{}\": word: {} total: {}", j, word_buf, new_count, seen_words);
                println!("avant client_set {} {}", word_buf, new_count); 
                let _client_set = client.set(&word_buf, format!("{}", new_count).into()).await?;
                println!("apres client_set {} {}", word_buf, new_count); 
                word_buf.clear();
                State::Nonword
            }

            // Continue reading until we hit new word (or end of chunk size)
            (false, State::Nonword) => State::Nonword,
        };

        // If we aren't reading a word and we've passed our chunking threshold,
        // then we can stop reading.
        if seen_bytes > cr.max_chunk_size && state != State::Word {
            break;
        }
    }

    println!("about to update chunk_reader bytes");

    chunk_reader.bytes.fetch_add(seen_bytes, Ordering::SeqCst);

    println!("about to update chunk_reader words");

    chunk_reader.words.fetch_add(seen_words, Ordering::SeqCst);

    Ok(())
}
