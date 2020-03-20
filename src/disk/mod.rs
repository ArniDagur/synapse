mod cache;
mod job;

pub use self::job::Context;
pub use self::job::DiskRequest;
pub use self::job::DiskResponse;
pub use self::job::Location;

use std::collections::VecDeque;
use std::{fs, io, thread};

use amy;

use self::cache::{BufCache, FileCache};
use self::job::DiskJobResult;
use util::UHashMap;
use {handle, CONFIG};

const POLL_INT_MS: usize = 1000;
const JOB_TIME_SLICE: u64 = 150;

pub struct Disk {
    poll: amy::Poller,
    reg: amy::Registrar,
    ch: handle::Handle<DiskRequest, DiskResponse>,
    jobs: amy::Receiver<DiskRequest>,
    files: FileCache,
    active: VecDeque<DiskRequest>,
    sequential: VecDeque<DiskRequest>,
    blocked: UHashMap<DiskRequest>,
    bufs: BufCache,
}

impl Disk {
    pub fn new(
        poll: amy::Poller,
        reg: amy::Registrar,
        ch: handle::Handle<DiskRequest, DiskResponse>,
        jobs: amy::Receiver<DiskRequest>,
    ) -> Disk {
        Disk {
            poll,
            reg,
            ch,
            jobs,
            files: FileCache::new(),
            bufs: BufCache::new(),
            active: VecDeque::new(),
            sequential: VecDeque::new(),
            blocked: UHashMap::default(),
        }
    }

    pub fn run(&mut self) {
        let sd = &CONFIG.disk.session;
        fs::create_dir_all(sd).unwrap();

        loop {
            match self.poll.wait(POLL_INT_MS) {
                Ok(v) => {
                    if self.handle_events() {
                        break;
                    }
                    for ev in v {
                        if let Some(request) = self.blocked.remove(&ev.id) {
                            self.enqueue_request(request);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to poll for events: {}", e);
                }
            }
            if !self.active.is_empty() && self.handle_active() {
                break;
            }
        }

        // Try to finish up remaining jobs
        for job in self.active.drain(..) {
            if job.concurrent() {
                job.execute(&mut self.files, &mut self.bufs).ok();
            }
        }
    }

    fn enqueue_request(&mut self, request: DiskRequest) {
        if request.concurrent() || self.active.iter().find(|r| !r.concurrent()).is_none() {
            self.active.push_back(request);
        } else {
            self.sequential.push_back(request);
        }
    }

    fn handle_active(&mut self) -> bool {
        let mut rotate = 1;
        while let Some(j) = self.active.pop_front() {
            let tid = j.tid();
            let seq = !j.concurrent();
            let mut done = false;
            match j.execute(&mut self.files, &mut self.bufs) {
                Ok(DiskJobResult::Resp(r)) => {
                    done = true;
                    self.ch.send(r).ok();
                }
                Ok(DiskJobResult::Update(s, r)) => {
                    self.ch.send(r).ok();
                    if rotate % 3 == 0 {
                        self.active.push_back(s);
                    } else {
                        self.active.push_front(s);
                    }
                }
                Ok(DiskJobResult::Paused(s)) => {
                    if rotate % 3 == 0 {
                        self.active.push_back(s);
                    } else {
                        self.active.push_front(s);
                    }
                }
                Ok(DiskJobResult::Blocked((id, s))) => {
                    self.blocked.insert(id, s);
                }
                Ok(DiskJobResult::Done) => {
                    done = true;
                }
                Err(e) => {
                    done = true;
                    if let Some(t) = tid {
                        self.ch.send(DiskResponse::error(t, e)).ok();
                    } else {
                        error!("Disk job failed: {}", e);
                    }
                }
            }
            if done && seq {
                if let Some(r) = self.sequential.pop_front() {
                    self.active.push_back(r);
                }
            }
            match self.poll.wait(0) {
                Ok(v) => {
                    if self.handle_events() {
                        return true;
                    }
                    for ev in v {
                        if let Some(r) = self.blocked.remove(&ev.id) {
                            self.enqueue_request(r);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to poll for events: {:?}", e);
                }
            }
            rotate += 1;
        }
        false
    }

    pub fn handle_events(&mut self) -> bool {
        loop {
            match self.ch.recv() {
                Ok(DiskRequest::Shutdown) => {
                    return true;
                }
                Ok(mut r) => {
                    trace!("Handling disk job!");
                    let tid = r.tid();
                    if let Err(e) = r.register(&self.reg) {
                        if let Some(t) = tid {
                            self.ch.send(DiskResponse::error(t, e)).ok();
                        }
                    }
                    self.enqueue_request(r);
                }
                _ => break,
            }
        }
        while let Ok(mut r) = self.jobs.try_recv() {
            if r.register(&self.reg).is_err() {
                continue;
            }
            self.enqueue_request(r);
        }
        false
    }
}

pub fn start(
    creg: &mut amy::Registrar,
) -> io::Result<(
    handle::Handle<DiskResponse, DiskRequest>,
    amy::Sender<DiskRequest>,
    thread::JoinHandle<()>,
)> {
    let poll = amy::Poller::new()?;
    let mut reg = poll.get_registrar();
    let (ch, dh) = handle::Handle::new(creg, &mut reg)?;
    let (tx, rx) = reg.channel()?;
    let h = dh.run("disk", move |h| Disk::new(poll, reg, h, rx).run())?;
    Ok((ch, tx, h))
}
