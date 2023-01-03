use std::sync::{atomic::AtomicUsize, Arc, RwLock};

use concurrent_list::Writer;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash};

fn main() {
    // for terms,
    // we only lock (not visible at this level) when the capacity changes
    // since they only hold handles to the lists, shouldn't be too costly
    // (though it's not so easy to predict capacity changing...)
    let cap_changed = Arc::new(AtomicUsize::new(0));
    // the w_map will contain the writer handle to the list per term
    let w_map = Arc::new(LockFreeCuckooHash::with_capacity(200));
    // the r_map will contain the reader handle to the list per term
    let r_map = Arc::new(LockFreeCuckooHash::with_capacity(200));
    let initial_r_map_size = r_map.size();
    let initial_r_map_capacity = r_map.capacity();

    // Create 4 threads to write the hash table.
    let mut handles = Vec::with_capacity(4);
    for i in 0..4 {
        // Transfer the reference to each thread, no need for a mutex.
        let wmap = w_map.clone();
        let rmap = r_map.clone();
        let cc = cap_changed.clone();
        let handle = std::thread::spawn(move || {
            let guard = pin();
            for j in 0..100 {
                let i_cap = wmap.capacity();
                let key = j;
                // (assume if wmap has key, all of initialization of this key happened)
                if wmap.contains_key(&key) {
                    let wlist: &Arc<RwLock<Writer<String>>> = wmap.get(&key, &guard).unwrap();
                    // minority of the time one already won
                    let v = format!("handle {} index {}", i, j);
                    wlist.write().unwrap().push(v);
                } else {
                    // for posting lists, tail is static, head has writer
                    // the trade-off is writing is append-only
                    // (this means tombstones must be used for update/delete)
                    let (w, r) = concurrent_list::new();
                    // so we put a lock around the single writer
                    let a_w = Arc::new(RwLock::new(w));
                    // but the reader is lock-free
                    let a_r = Arc::new(r);
                    rmap.insert_if_not_exists(key, a_r.clone());
                    let wlist = wmap.get_or_insert(key, a_w.clone(), &guard);
                    let v = format!("handle {} initialized index {}", i, j);
                    wlist.write().unwrap().push(v);
                }
                // i found this is actually not deterministic, prob doing it wrong
                if wmap.capacity() > i_cap {
                    cc.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let guard = pin();
    for j in 0..100 {
        let key = j;
        // get the read handle
        let r = r_map.get(&key, &guard).unwrap();
        // it's not debug so we just iter it to peek
        for i in r.iter() {
            println!("key {} plist entry {}", key, i);
        }
    }
    // to see a bit about what changed throughout
    dbg!(
        r_map.size(),
        r_map.capacity(),
        initial_r_map_size,
        initial_r_map_capacity,
        cap_changed
    );
}
