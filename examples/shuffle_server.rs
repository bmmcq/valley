use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use bytes::{Buf, BufMut};
use structopt::StructOpt;
use valley::codec::{Decode, Encode};
use valley::name_service::{NameService, StaticNameService};

#[derive(Debug, StructOpt)]
#[structopt(name = "ShuffleServer")]
pub struct Configs {
    /// set id of server
    #[structopt(short = "i", default_value = "0")]
    server_id: u32,
    /// set hosts
    #[structopt(short = "h")]
    host_file: PathBuf,
}

struct TestMessage {
    create_time: u128,
    content: Vec<u8>,
}

impl TestMessage {
    pub fn new(size: usize) -> Self {
        let create_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        Self { create_time, content: vec![9; size] }
    }

    pub fn get_dur(&self) -> u128 {
        let current_time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        current_time - self.create_time
    }
}

impl Encode for TestMessage {
    fn write_to<W: BufMut>(&self, writer: &mut W) -> std::io::Result<()> {
        writer.put_u128(self.create_time);
        writer.put_u32(self.content.len() as u32);
        writer.put_slice(self.content.as_slice());
        Ok(())
    }
}

impl Decode for TestMessage {
    fn read_from<R: Buf>(reader: &mut R) -> std::io::Result<Self> {
        let create_time = reader.get_u128();
        let size = reader.get_u32() as usize;
        let mut content = vec![0u8; size];
        reader.copy_to_slice(&mut content[0..]);
        Ok(Self { create_time, content })
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let configs: Configs = Configs::from_args();
    let hosts = BufReader::new(File::open(&configs.host_file).unwrap());
    let ns = StaticNameService::new();
    let mut peers = vec![];
    let mut port = 0u16;
    for (i, host) in hosts.lines().enumerate() {
        let host_addr = host.unwrap().parse::<SocketAddr>().unwrap();
        if i as u32 != configs.server_id {
            peers.push(i as u32);
        } else {
            port = host_addr.port();
        }
        ns.register(i as u32, host_addr)
            .await
            .unwrap();
    }
    let addr = SocketAddr::new("0.0.0.0".parse().unwrap(), port);

    let mut server = valley::server::new_tcp_server(configs.server_id, addr, ns);
    server.start().await.unwrap();
    std::thread::sleep(Duration::from_secs(5));
    println!("try to get connections to {:? }...", peers);
    let (push, mut pull) = server.get_connections(1, &peers).await.unwrap();

    println!("connected;");
    tokio::spawn(async move {
        let mut i = 0;
        while let Some(mut next) = pull.recv().await {
            let msg = TestMessage::read_from(next.get_payload()).unwrap();
            println!("{} get message from {}, use {} micros", i, next.get_source(), msg.get_dur());
            i += 1;
        }
    });

    for _ in 0..1_000 {
        let mut send_all = vec![];
        for id in peers.iter() {
            send_all.push(push.send(*id, TestMessage::new(64)));
        }

        for x in futures::future::join_all(send_all).await {
            x.unwrap();
        }
    }
    push.flush().await.unwrap();
    push.close().await;
}
