mod message;
pub mod reader;
pub mod writer;

use std::net::SocketAddr;
use std::net::TcpStream;
use std::{cmp, fmt, io, mem, time};

pub use self::message::Message;
use self::reader::{RRes, Reader};
use self::writer::Writer;
use bencode;
use control::cio;
use rpc::{self, resource};
use socket::Socket;
use stat;
use throttle::Throttle;
use torrent::{Bitfield, TorrentInfo, Torrent};
use tracker;
use util;
use {CONFIG, DHT_EXT};

error_chain! {
    errors {
        ProtocolError(r: &'static str) {
            description("Peer did not conform to the bittorrent protocol")
                display("Peer protocol error: {:?}", r)
        }
    }
}

const INIT_MAX_QUEUE: u16 = 5;
const MAX_QUEUE_CAP: u16 = 600;

/// Peer connection and associated metadata.
pub struct Peer<T: cio::ControlIO> {
    id: usize,
    control_io: T,
    pieces: Bitfield,
    piece_count: usize,
    piece_cache: Vec<u32>,
    remote_status: Status,
    local_status: Status,
    /// Current number of queued requests
    queued: u16,
    /// Maximum number of requests that can be queued
    /// at a time.
    max_queue: u16,
    pieces_updated: bool,
    tid: usize,
    downloaded: u32,
    uploaded: u32,
    stat: stat::EMA,
    addr: SocketAddr,
    t_hash: [u8; 20],
    cid: Option<[u8; 20]>,
    rsv: Option<[u8; 8]>,
    extension_ids: ExtensionIDs,
    pub rank: usize,
}

pub struct ExtensionIDs {
    pub ut_meta: Option<u8>,
    pub ut_pex: Option<u8>,
}

#[derive(Debug)]
pub struct Status {
    pub choked: bool,
    pub interested: bool,
}

pub struct PeerConnection {
    last_action_time: time::Instant,
    sock: Socket,
    reader: Reader,
    writer: Writer,
}

impl PeerConnection {
    pub fn new(sock: Socket) -> PeerConnection {
        let writer = Writer::new();
        let reader = Reader::new();
        PeerConnection {
            sock,
            writer,
            reader,
            last_action_time: time::Instant::now(),
        }
    }

    #[cfg(test)]
    pub fn test() -> PeerConnection {
        let writer = Writer::new();
        let reader = Reader::new();
        PeerConnection {
            last_action_time: time::Instant::now(),
            sock: Socket::empty(),
            writer,
            reader,
        }
    }

    pub fn sock(&self) -> &Socket {
        &self.sock
    }

    pub fn sock_mut(&mut self) -> &mut Socket {
        &mut self.sock
    }

    pub fn last_action_time(&self) -> &time::Instant {
        &self.last_action_time
    }

    /// Creates a new "outgoing" peer, which acts as a client.
    /// Once created, set_torrent should be called.
    pub fn new_outgoing(ip: &SocketAddr) -> io::Result<PeerConnection> {
        Ok(PeerConnection::new(Socket::new(ip)?))
    }

    /// Creates a peer where we are acting as the server.
    /// Once the handshake is received, set_torrent should be called.
    pub fn new_incoming(sock: TcpStream, reader: Reader) -> io::Result<PeerConnection> {
        let mut peer = PeerConnection::new(Socket::from_stream(sock)?);
        peer.reader = reader;
        Ok(peer)
    }

    pub fn writable(&mut self) -> io::Result<()> {
        self.last_action_time = time::Instant::now();
        self.writer.writable(&mut self.sock)
    }

    pub fn readable(&mut self) -> RRes {
        self.last_action_time = time::Instant::now();
        self.reader.readable(&mut self.sock)
    }

    pub fn write_message(&mut self, msg: Message) -> io::Result<()> {
        self.writer.write_message(msg, &mut self.sock)
    }

    pub fn set_throttle(&mut self, throt: Throttle) {
        self.sock.throttle = Some(throt);
    }
}

impl Status {
    fn new() -> Status {
        Status {
            choked: true,
            interested: false,
        }
    }
}

#[cfg(test)]
impl Peer<cio::test::TestControlIO> {
    pub fn test(
        id: usize,
        uploaded: u32,
        downloaded: u32,
        queued: u16,
        pieces: Bitfield,
    ) -> Peer<cio::test::TestControlIO> {
        let piece_count = pieces.iter().count();
        Peer {
            id,
            remote_status: Status::new(),
            local_status: Status::new(),
            uploaded,
            downloaded,
            stat: stat::EMA::new(),
            addr: "127.0.0.1:0".parse().unwrap(),
            control_io: cio::test::TestControlIO::new(),
            queued,
            max_queue: queued,
            pieces,
            piece_cache: Vec::new(),
            piece_count,
            tid: 0,
            t_hash: [0u8; 20],
            rsv: None,
            cid: None,
            extension_ids: ExtensionIDs::new(),
            pieces_updated: false,
            rank: 0,
        }
    }

    pub fn test_from_pieces(id: usize, pieces: Bitfield) -> Peer<cio::test::TestControlIO> {
        Peer::test(id, 0, 0, 0, pieces)
    }

    pub fn test_from_stats(id: usize, ul: u32, dl: u32) -> Peer<cio::test::TestControlIO> {
        Peer::test(id, ul, dl, 0, Bitfield::new(4))
    }

    pub fn test_with_tcio(mut cio: cio::test::TestControlIO) -> Peer<cio::test::TestControlIO> {
        use control::cio::ControlIO;

        let conn = PeerConnection::test();
        let id = cio.add_peer(conn).unwrap();
        let mut peer = Peer::test(id, 0, 0, 0, Bitfield::new(4));
        peer.control_io = cio;
        peer
    }
}

impl<T: cio::ControlIO> Peer<T> {
    pub fn new(
        mut conn: PeerConnection,
        t: &mut Torrent<T>,
        cid: Option<[u8; 20]>,
        rsv: Option<[u8; 8]>,
    ) -> cio::Result<Peer<T>> {
        let addr = conn.sock().addr();
        conn.set_throttle(t.get_throttle(0));
        let id = t.cio.add_peer(conn)?;
        let mut p = Peer {
            id,
            addr,
            remote_status: Status::new(),
            local_status: Status::new(),
            uploaded: 0,
            downloaded: 0,
            stat: stat::EMA::new(),
            control_io: t.cio.new_handle(),
            queued: 0,
            max_queue: INIT_MAX_QUEUE,
            pieces: Bitfield::new(t.info.hashes.len() as u64),
            piece_cache: Vec::new(),
            piece_count: 0,
            tid: t.id,
            t_hash: t.info.hash,
            rsv,
            cid,
            extension_ids: ExtensionIDs::new(),
            pieces_updated: false,
            rank: t.num_peers(),
        };
        p.send_message(Message::handshake(&t.info));
        if t.info.complete() {
            p.send_message(Message::Bitfield(t.pieces.clone()));
        }
        p.send_rpc_info();
        Ok(p)
    }

    pub fn magnet_complete(&mut self, info: &TorrentInfo) {
        if self.pieces.len() == 0 {
            self.pieces = Bitfield::new(u64::from(info.pieces()));
        } else {
            self.pieces.cap(u64::from(info.pieces()));
        }
    }

    /// Returns whether or not the peer has received a handshake
    pub fn ready(&self) -> bool {
        self.cid.is_some()
    }

    pub fn exts(&self) -> &ExtensionIDs {
        &self.extension_ids
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn pieces(&self) -> &Bitfield {
        &self.pieces
    }

    pub fn piece_cache(&mut self) -> &mut Vec<u32> {
        &mut self.piece_cache
    }

    #[cfg(test)]
    pub fn pieces_mut(&mut self) -> &mut Bitfield {
        &mut self.pieces
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn flush(&mut self) -> (u32, u32) {
        (
            mem::replace(&mut self.uploaded, 0),
            mem::replace(&mut self.downloaded, 0),
        )
    }

    pub fn active(&self) -> bool {
        self.stat.active()
    }

    pub fn tick(&mut self) -> bool {
        self.stat.tick();
        if !self.stat.active() {
            return false;
        }
        let dl = self.stat.avg_download();
        let rate = (dl / 1024) as u16;
        // Taken from rtorrent's pipeline calculation
        let nmq = if rate < 20 { rate + 2 } else { rate / 5 + 18 };
        // Clamp between -15 / +50 for queue len changes
        self.max_queue = cmp::min(
            cmp::max(nmq, self.max_queue.saturating_sub(15)),
            self.max_queue + 50,
        );
        // Keep it under the max cap
        self.max_queue = cmp::min(self.max_queue, MAX_QUEUE_CAP);
        if self.pieces_updated {
            self.pieces_updated = false;
            self.send_rpc_update();
        }
        true
    }

    pub fn get_tx_rates(&self) -> (u64, u64) {
        (self.stat.avg_upload(), self.stat.avg_download())
    }

    pub fn queue_reqs(&mut self) -> Option<u16> {
        if self.remote_status.choked || self.queued > self.max_queue.saturating_sub(16) {
            None
        } else {
            let amnt = self.max_queue.saturating_sub(self.queued);
            Some(amnt)
        }
    }

    pub fn handle_msg(&mut self, msg: &mut Message) -> Result<()> {
        match *msg {
            Message::Handshake { rsv, id, .. } => {
                if (rsv[DHT_EXT.0] & DHT_EXT.1) != 0 {
                    self.send_message(Message::Port(CONFIG.dht.port));
                }
                self.rsv = Some(rsv);
                self.cid = Some(id);
                self.send_rpc_info();
            }
            Message::Piece { length, .. } | Message::SharedPiece { length, .. } => {
                self.stat.add_dl(u64::from(length));
                self.downloaded += 1;
                self.queued -= 1;
            }
            Message::Request { .. } => {
                if self.local_status.choked {
                    info!("Got request while choked!");
                    return Err(ErrorKind::ProtocolError("Peer requested while choked!").into());
                }
            }
            Message::Choke => {
                self.remote_status.choked = true;
            }
            Message::Unchoke => {
                self.remote_status.choked = false;
            }
            Message::Interested => {
                self.remote_status.interested = true;
            }
            Message::Uninterested => {
                self.remote_status.interested = false;
            }
            Message::Have(idx) => {
                if idx >= self.pieces.len() as u32 {
                    return Err(ErrorKind::ProtocolError("Invalid piece provided in HAVE!").into());
                }
                if self.pieces.has_bit(u64::from(idx)) {
                    return Err(
                        ErrorKind::ProtocolError("Duplicate piece provided in HAVE!").into(),
                    );
                }
                self.pieces.set_bit(u64::from(idx));
                self.piece_count += 1;
                self.pieces_updated = true;
            }
            Message::Bitfield(ref mut pieces) => {
                // Set the correct length, then swap the pieces
                // Don't do this with magnets though
                if self.pieces.len() > 0 {
                    pieces.cap(self.pieces.len());
                }
                mem::swap(pieces, &mut self.pieces);
                self.piece_count = self.pieces.iter().count();
                self.send_rpc_update();
            }
            Message::KeepAlive => {
                self.send_message(Message::KeepAlive);
            }
            Message::Cancel { index, begin, .. } => {
                self.control_io.get_peer(self.id, |conn| {
                    conn.writer.write_queue.retain(|m| {
                        if let Message::Piece {
                            index: i, begin: b, ..
                        } = *m
                        {
                            return !(i == index && b == begin);
                        }
                        true
                    });
                });
            }
            Message::Port(p) => {
                let mut s = self.addr();
                s.set_port(p);
                self.control_io.msg_trk(tracker::Request::AddNode(s));
            }
            Message::Extension { id, ref payload } => {
                if id == 0 {
                    let b = bencode::decode_buf(payload).map_err(|_| {
                        ErrorKind::ProtocolError("Invalid bencode in ext handshake")
                    })?;
                    let mut d = b.into_dict().ok_or_else(|| {
                        ErrorKind::ProtocolError("Invalid bencode type in ext handshake")
                    })?;
                    let mut m = d.remove("m").and_then(|v| v.into_dict()).ok_or_else(|| {
                        ErrorKind::ProtocolError("Invalid metadata in in ext handshake")
                    })?;
                    self.extension_ids.ut_meta = m
                        .remove("ut_metadata")
                        .and_then(|v| v.into_int())
                        .map(|v| v as u8);
                    self.extension_ids.ut_pex = m
                        .remove("ut_pex")
                        .and_then(|v| v.into_int())
                        .map(|v| v as u8);
                }
            }
        }
        Ok(())
    }

    pub fn request_piece(&mut self, idx: u32, offset: u32, len: u32) {
        let m = Message::request(idx, offset, len);
        self.queued += 1;
        self.send_message(m);
    }

    pub fn choke(&mut self) {
        if !self.local_status.choked {
            self.local_status.choked = true;
            self.send_message(Message::Choke);
        }
    }

    pub fn unchoke(&mut self) {
        if self.local_status.choked {
            self.local_status.choked = false;
            self.send_message(Message::Unchoke);
        }
    }

    pub fn interested(&mut self) {
        if !self.local_status.interested {
            self.local_status.interested = true;
            self.send_message(Message::Interested);
        }
    }

    pub fn send_message(&mut self, msg: Message) {
        match msg {
            Message::SharedPiece { length, .. } | Message::Piece { length, .. } => {
                self.uploaded += 1;
                self.stat.add_ul(u64::from(length));
            }
            _ => {}
        }
        self.control_io.msg_peer(self.id, msg);
    }

    fn send_rpc_info(&mut self) {
        if let Some(cid) = self.cid {
            let id = util::peer_rpc_id(&self.t_hash, self.id as u64);
            self.control_io
                .msg_rpc(rpc::CtlMessage::Extant(vec![resource::Resource::Peer(
                    resource::Peer {
                        id,
                        torrent_id: util::hash_to_id(&self.t_hash[..]),
                        client_id: util::hash_to_id(&cid[..]),
                        ip: self.addr.to_string(),
                        rate_up: 0,
                        rate_down: 0,
                        availability: self.piece_count as f32 / self.pieces.len() as f32,
                        ..Default::default()
                    },
                )]));
        }
    }

    fn send_rpc_update(&mut self) {
        if self.cid.is_some() {
            let id = util::peer_rpc_id(&self.t_hash, self.id as u64);
            self.control_io.msg_rpc(rpc::CtlMessage::Update(vec![
                resource::SResourceUpdate::PeerAvailability {
                    id,
                    kind: resource::ResourceKind::Peer,
                    availability: self.piece_count as f32 / self.pieces.len() as f32,
                },
            ]));
        }
    }

    pub fn send_rpc_removal(&mut self) {
        if self.ready() {
            self.control_io
                .msg_rpc(rpc::CtlMessage::Removed(vec![util::peer_rpc_id(
                    &self.t_hash,
                    self.id as u64,
                )]));
        }
    }
}

impl<T: cio::ControlIO> Drop for Peer<T> {
    fn drop(&mut self) {
        self.send_rpc_removal();
    }
}

impl<T: cio::ControlIO> fmt::Debug for Peer<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Peer {{ IP: {}, tid: {}, local_status: {:?}, remote_status: {:?} }}",
            self.addr, self.tid, self.local_status, self.remote_status
        )
    }
}

impl ExtensionIDs {
    fn new() -> ExtensionIDs {
        ExtensionIDs {
            ut_meta: None,
            ut_pex: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Peer;
    use buffers::Buffer;
    use control::cio::{test, ControlIO};
    use torrent::Message;

    #[test]
    fn test_cancel() {
        let mut tcio = test::TestControlIO::new();
        let mut peer = Peer::test_with_tcio(tcio.new_handle());
        let p1 = Message::Piece {
            index: 0,
            begin: 0,
            data: Buffer::get().unwrap(),
            length: 16_384,
        };
        let p2 = Message::Piece {
            index: 1,
            begin: 1,
            data: Buffer::get().unwrap(),
            length: 16_384,
        };
        let p3 = Message::Piece {
            index: 2,
            begin: 2,
            data: Buffer::get().unwrap(),
            length: 16_384,
        };
        peer.send_message(Message::KeepAlive);
        peer.send_message(p1.clone());
        peer.send_message(p2.clone());
        peer.send_message(p3.clone());

        let mut c = Message::Cancel {
            index: 1,
            begin: 1,
            length: 16_384,
        };
        peer.handle_msg(&mut c).unwrap();
        let wq = tcio
            .get_peer(peer.id, |p| p.writer.write_queue.clone())
            .unwrap();
        assert_eq!(wq.len(), 2);
        assert_eq!(wq[0], p1);
        assert_eq!(wq[1], p3);
    }
}
