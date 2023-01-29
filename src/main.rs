#[macro_use]
extern crate log;

use futures_lite::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    io::ImmutableFileBuilder, net::TcpListener, sync::Semaphore, timer::TimerActionRepeat, CpuSet,
    Latency, LocalExecutorPoolBuilder, PoolPlacement, Shares,
};
use speedy::Writable;
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    rc::Rc,
    time::{Duration, Instant},
};

type Key = u64;
type Db = Rc<RefCell<HashMap<u64, Vec<u8>>>>;
type Expiration = Instant;

const KEY_EXPIRE_SEC: u64 = 30;

async fn serve() {
    let id = glommio::executor().id();
    info!("[{id}] Starting executor {}", id);

    let db: Db = Rc::new(RefCell::new(HashMap::new()));
    let db2 = db.clone();
    let db3 = db.clone();

    let expirations: Rc<RefCell<BTreeMap<Expiration, Key>>> =
        Rc::new(RefCell::new(BTreeMap::new()));
    let expirations2 = expirations.clone();

    let last_snapshot = Rc::new(RefCell::new(None));

    let priority_queue = glommio::executor().create_task_queue(
        Shares::Static(1000),
        Latency::NotImportant,
        "priority_queue",
    );
    let backgorund_queue = glommio::executor().create_task_queue(
        Shares::Static(100),
        Latency::NotImportant,
        "background_queue",
    );

    glommio::spawn_local_into(
        async move {
            let action = TimerActionRepeat::repeat(move || {
                let db_local = db3.clone();
                let last_snapshot_local = last_snapshot.clone();
                async move {
                    let new_snap_filename = format!("snapshot-{id}-{}.db", fastrand::u64(..));
                    let snapshot = ImmutableFileBuilder::new(&new_snap_filename);
                    let mut sf = snapshot.build_sink().await.unwrap();

                    let mut db_g = db_local.borrow_mut();
                    db_g.shrink_to_fit();
                    let data = db_g.write_to_vec().unwrap();

                    drop(db_g);

                    glommio::yield_if_needed().await;

                    sf.write(&data).await.unwrap();
                    sf.sync().await.unwrap();
                    sf.seal().await.unwrap();
                    glommio::yield_if_needed().await;

                    let last_snap_fname = last_snapshot_local.borrow().clone();
                    if let Some(fname) = last_snap_fname {
                        glommio::io::remove(fname).await.unwrap();
                    }
                    *last_snapshot_local.borrow_mut() = Some(new_snap_filename);

                    Some(Duration::from_secs(1))
                }
            });
            action.join().await;
        },
        backgorund_queue,
    )
    .unwrap()
    .detach();

    glommio::spawn_local_into(
        async move {
            let action = TimerActionRepeat::repeat(move || {
                let expirations_local = expirations2.clone();
                let db_local = db2.clone();
                async move {
                    let now = Instant::now();
                    loop {
                        let (expiration, key) = match expirations_local.borrow().iter().next() {
                            Some((&expiration, &key)) => (expiration, key),
                            None => break,
                        };

                        if expiration > now {
                            return Some(Duration::from_millis(5));
                        }

                        db_local.borrow_mut().remove(&key);
                        expirations_local.borrow_mut().remove(&expiration);
                        glommio::yield_if_needed().await;
                    }

                    Some(Duration::from_millis(10))
                }
            });
            action.join().await;
        },
        backgorund_queue,
    )
    .unwrap()
    .detach();

    let listener = TcpListener::bind("0.0.0.0:6379").unwrap();
    info!("[{id}] Listening on {:?}", listener.local_addr().unwrap());

    let semaphore = Rc::new(Semaphore::new(2048));

    loop {
        let permit = semaphore.acquire_static_permit(1).await.unwrap();

        let mut stream = listener.accept().await.unwrap();
        let local_db = db.clone();
        let expirations2 = expirations.clone();

        glommio::spawn_local_into(
            async move {
                let mut buf = [0u8; 1070];
                loop {
                    let len = stream.read(&mut buf).await.unwrap();

                    if len == 0 {
                        break;
                    } else {
                        if buf.len() < 1069 {
                            continue;
                        }

                        let key = fastrand::u64(..);

                        let mut db_guard = local_db.borrow_mut();
                        db_guard.insert(key, Vec::from(buf));
                        drop(db_guard);
                        let expire = Instant::now() + Duration::from_secs(KEY_EXPIRE_SEC);
                        let mut queue_guard = expirations2.borrow_mut();
                        queue_guard.insert(expire, key);
                        drop(queue_guard);

                        stream.write(b"$-1\r\n").await.unwrap();
                    }

                    buf.fill(0);
                }
                drop(permit);
            },
            priority_queue,
        )
        .unwrap()
        .detach();
    }
}

fn main() {
    env_logger::init();

    LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
        num_cpus::get(),
        CpuSet::online().ok(),
    ))
    .on_all_shards(|| async move {
        serve().await;
    })
    .unwrap()
    .join_all();
}
