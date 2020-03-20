use {disk, listener, rpc, torrent, tracker};

error_chain! {
    errors {
        IO {
            description("IO error")
                display("IO error")
        }

        Full {
            description("FD limit reached")
                display("Too many existing open fd's, socket rejected")
        }

        Request {
            description("removal requested")
                display("removal requested")
        }
    }
}

pub type PeerID = usize;
pub type TID = usize;

pub enum Event {
    Timer(TID),
    Peer {
        peer: PeerID,
        event: Result<torrent::Message>,
    },
    RPC(Result<rpc::Message>),
    Tracker(Result<tracker::Response>),
    Disk(Result<disk::DiskResponse>),
    Listener(Result<Box<listener::Message>>),
}

/// Control IO trait used as an abstraction boundary between
/// the actual logic of the torrent client and the IO that needs
/// to be done.
pub trait ControlIO {
    /// Returns events for peers, timers, channels, etc.
    fn poll(&mut self, events: &mut Vec<Event>) -> Result<()>;

    /// Self propagate an event. Used to achieve non standard control flow.
    fn propagate(&mut self, event: Event);

    /// Adds a peer to be polled on
    fn add_peer(&mut self, peer: torrent::PeerConnection) -> Result<PeerID>;

    /// Applies f to a peer if it exists
    fn get_peer<T, F: FnOnce(&mut torrent::PeerConnection) -> T>(
        &mut self,
        peer: PeerID,
        f: F,
    ) -> Option<T>;

    /// Removes a peer - This will trigger an error being
    /// reported at the next poll time, clients should wait
    /// for this to occur before internally removing the peer.
    fn remove_peer(&self, peer: PeerID);

    /// Flushes events on the given vec of peers
    fn flush_peers(&mut self, peers: Vec<PeerID>);

    /// Sends a message to a peer
    fn msg_peer(&mut self, peer: PeerID, msg: torrent::Message);

    /// Sends a message over RPC
    fn msg_rpc(&mut self, msg: rpc::CtlMessage);

    /// Sends a message to the tracker
    fn msg_trk(&mut self, msg: tracker::Request);

    /// Sends a message to the disk worker
    fn msg_disk(&mut self, msg: disk::DiskRequest);

    /// Sends a message to the listener worker
    fn msg_listener(&mut self, msg: listener::Request);

    /// Sets a timer in milliseconds
    fn set_timer(&mut self, interval: usize) -> Result<TID>;

    /// Creates a copy of the IO object, which has the same underlying data
    fn new_handle(&self) -> Self;
}

#[cfg(test)]
pub mod test {
    use super::{ControlIO, Event, PeerID, Result, TID};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use {disk, listener, rpc, torrent, tracker};

    pub struct TestControlIO {
        data: Arc<Mutex<TestControlIOD>>,
    }

    /// A reference CIO implementation which serves as a test mock
    pub struct TestControlIOD {
        pub peers: HashMap<PeerID, torrent::PeerConnection>,
        pub peer_msgs: Vec<(PeerID, torrent::Message)>,
        pub flushed_peers: Vec<PeerID>,
        pub rpc_msgs: Vec<rpc::CtlMessage>,
        pub trk_msgs: Vec<tracker::Request>,
        pub disk_msgs: Vec<disk::DiskRequest>,
        pub listener_msgs: Vec<listener::Request>,
        pub timers: usize,
        pub peer_cnt: usize,
    }

    impl TestControlIO {
        pub fn new() -> TestControlIO {
            let d = TestControlIOD {
                peers: HashMap::new(),
                peer_msgs: Vec::new(),
                flushed_peers: Vec::new(),
                rpc_msgs: Vec::new(),
                trk_msgs: Vec::new(),
                disk_msgs: Vec::new(),
                listener_msgs: Vec::new(),
                timers: 0,
                peer_cnt: 0,
            };
            TestControlIO {
                data: Arc::new(Mutex::new(d)),
            }
        }
    }

    impl ControlIO for TestControlIO {
        fn poll(&mut self, _: &mut Vec<Event>) -> Result<()> {
            return Ok(());
        }

        fn propagate(&mut self, _: Event) {}

        fn add_peer(&mut self, peer: torrent::PeerConnection) -> Result<PeerID> {
            let mut d = self.data.lock().unwrap();
            let id = d.peer_cnt;
            d.peers.insert(id, peer);
            d.peer_cnt += 1;
            Ok(id)
        }

        fn get_peer<T, F: FnOnce(&mut torrent::PeerConnection) -> T>(
            &mut self,
            peer_id: PeerID,
            f: F,
        ) -> Option<T> {
            let mut d = self.data.lock().unwrap();
            if let Some(peer_connection) = d.peers.get_mut(&peer_id) {
                Some(f(peer_connection))
            } else {
                None
            }
        }

        fn remove_peer(&self, peer: PeerID) {
            let mut d = self.data.lock().unwrap();
            d.peers.remove(&peer);
        }

        fn flush_peers(&mut self, mut peers: Vec<PeerID>) {
            let mut d = self.data.lock().unwrap();
            d.flushed_peers.extend(peers.drain(..));
        }

        fn msg_peer(&mut self, peer: PeerID, msg: torrent::Message) {
            let mut d = self.data.lock().unwrap();
            d.peer_msgs.push((peer, msg.clone()));
            if let Some(p) = d.peers.get_mut(&peer) {
                p.write_message(msg).ok();
            }
        }

        fn msg_rpc(&mut self, msg: rpc::CtlMessage) {
            let mut d = self.data.lock().unwrap();
            d.rpc_msgs.push(msg);
        }

        fn msg_trk(&mut self, msg: tracker::Request) {
            let mut d = self.data.lock().unwrap();
            d.trk_msgs.push(msg);
        }

        fn msg_disk(&mut self, msg: disk::DiskRequest) {
            let mut d = self.data.lock().unwrap();
            d.disk_msgs.push(msg);
        }

        fn msg_listener(&mut self, msg: listener::Request) {
            let mut d = self.data.lock().unwrap();
            d.listener_msgs.push(msg);
        }

        fn set_timer(&mut self, _: usize) -> Result<TID> {
            let mut d = self.data.lock().unwrap();
            let timer = d.timers;
            d.timers += 1;
            Ok(timer)
        }

        fn new_handle(&self) -> Self {
            TestControlIO {
                data: self.data.clone(),
            }
        }
    }
}
