//! Benchmark client for kvstored's binary wire protocol, with pipelining.

use anyhow::Result;
use bytes::BytesMut;
use falcon_wire::{encode_request, OP_GET, OP_SET};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;

pub struct WireClient {
    reader: BufReader<OwnedReadHalf>,
    writer: BufWriter<OwnedWriteHalf>,
    scratch: BytesMut,
}

impl WireClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (r, w) = stream.into_split();
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, r),
            writer: BufWriter::with_capacity(64 * 1024, w),
            scratch: BytesMut::with_capacity(64 * 1024),
        })
    }

    /// Pipeline a batch of SETs: send all requests, then read all replies.
    pub async fn pipeline_set(&mut self, keys: &[String], value: &[u8]) -> Result<()> {
        self.scratch.clear();
        for k in keys {
            encode_request(&mut self.scratch, OP_SET, b"", k.as_bytes(), value);
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in keys {
            self.read_reply().await?;
        }
        Ok(())
    }

    /// Pipeline a batch of GETs: send all requests, then read all replies.
    pub async fn pipeline_get(&mut self, keys: &[String]) -> Result<()> {
        self.scratch.clear();
        for k in keys {
            encode_request(&mut self.scratch, OP_GET, b"", k.as_bytes(), b"");
        }
        self.writer.write_all(&self.scratch).await?;
        self.writer.flush().await?;
        for _ in keys {
            self.read_reply().await?;
        }
        Ok(())
    }

    /// Read and discard one reply. The benchmark measures throughput and
    /// latency, so it only needs to consume the frame, not inspect it — but it
    /// must consume it exactly, or the next reply would decode misaligned.
    async fn read_reply(&mut self) -> Result<()> {
        let mut header = [0u8; 5];
        self.reader.read_exact(&mut header).await?;
        let val_len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if val_len > 0 {
            let mut payload = vec![0u8; val_len];
            self.reader.read_exact(&mut payload).await?;
        }
        Ok(())
    }
}
