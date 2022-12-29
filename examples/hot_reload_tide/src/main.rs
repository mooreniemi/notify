use hot_reload_tide::messages::{load_config, Config};
use notify::{Error, Event, PollWatcher, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::fs::read_dir;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tide::{log, Body, Response};

const CONFIG_PATH: &str = "config.json";
const SEGMENTS_PATH: &str = "/tmp/segments/";

/// A pretend ii
type Segment = HashMap<String, Vec<usize>>;

// for initial loading of everything at boot (only)
pub fn load_segments(path: &str) -> Result<HashMap<String, Segment>, Box<dyn std::error::Error>> {
    let paths = read_dir(path)?;
    let mut segments = HashMap::new();
    for p in paths {
        let full_p = format!(
            "{}{}",
            path,
            p.expect("valid segment file name")
                .file_name()
                .as_os_str()
                .to_str()
                .expect("valid segment file name")
        );
        let file = std::fs::File::open(full_p.clone())?;
        let file_size = file.metadata()?.len();
        if file_size == 0 {
            // add more validation here
            log::info!("The segment file {} is empty. Skipping.", full_p);
        } else {
            let reader = std::io::BufReader::new(file);
            let segment: Segment = serde_json::from_reader(reader)?;
            segments.insert(full_p, segment);
        }
    }
    Ok(segments)
}

// Because we're running a web server we need a runtime,
// for more information on async runtimes, please check out [async-std](https://github.com/async-rs/async-std)
#[async_std::main]
async fn main() -> tide::Result<()> {
    log::start();

    let config = load_config(CONFIG_PATH).unwrap();
    let segments: HashMap<String, Segment> = load_segments(SEGMENTS_PATH).unwrap();

    let config = Arc::new(RwLock::new(config));
    let cloned_config = Arc::clone(&config);

    let a_segments = Arc::new(RwLock::new(segments));
    let c_a_segments = Arc::clone(&a_segments);

    let mut watcher = RecommendedWatcher::new(
        move |result: Result<Event, Error>| {
            let event = result.unwrap();

            if event.kind.is_modify() {
                match load_config(CONFIG_PATH) {
                    Ok(new_config) => {
                        *cloned_config.write().unwrap() = new_config;
                        log::info!("updated config");
                    }
                    Err(error) => log::info!("Error reloading config: {:?}", error),
                }
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(Path::new(CONFIG_PATH), RecursiveMode::Recursive)?;

    // for nfs need to poll
    let mut segments_watcher = PollWatcher::new(
        move |result: Result<Event, Error>| {
            let event = result.unwrap();
            log::info!("segments event kind: {:?}", event.kind);

            // we treat segments as immutable once they're created
            // so we only need to handle:
            // 1. segment already exists, skip
            // 2. segment doesn't already exist, insert (load)
            // 3. segment exists but was removed, delete (unload)
            match event.kind {
                notify::EventKind::Any => todo!(),
                notify::EventKind::Access(e) => {
                    log::info!("ACCESS: {:?}", e);
                }
                notify::EventKind::Create(_) => {
                    log::info!("CREATE: {:?}", event.paths);
                    for p in event.paths {
                        let file = std::fs::File::open(p.clone()).expect("segment");
                        let file_size = file.metadata().expect("metadata").len();
                        if file_size == 0 {
                            // add more validation here
                            log::warn!("The segment file {:?} is empty. Skipping.", p);
                        } else {
                            let reader = std::io::BufReader::new(file);
                            let segment: Segment =
                                serde_json::from_reader(reader).expect("segment");
                            log::info!("LOAD: {:?}", &p);
                            // we don't lock until AFTER we have already loaded the structure
                            let mut segments = c_a_segments.write().unwrap();
                            segments.insert(p.to_str().expect("valid path").to_string(), segment);
                        }
                    }
                }
                notify::EventKind::Modify(_) => {
                    log::warn!("MODIFY: (unused)");
                }
                notify::EventKind::Remove(_) => {
                    log::info!("REMOVE: {:?}", event.paths);
                    for p in event.paths {
                        log::info!("UNLOAD: {:?}", &p);
                        let mut segments = c_a_segments.write().unwrap();
                        segments.remove(&p.to_str().expect("valid path").to_string());
                    }
                }
                notify::EventKind::Other => todo!(),
            }
        },
        notify::Config::default().with_poll_interval(Duration::from_secs(1)),
    )?;
    segments_watcher.watch(Path::new(SEGMENTS_PATH), RecursiveMode::Recursive)?;

    // We set up a web server using [Tide](https://github.com/http-rs/tide)
    let mut app = tide::with_state((config, a_segments));

    // what the repo originally works with
    app.at("/messages").get(get_messages);
    app.at("/message/:name").get(get_message);
    // what i added
    app.at("/segments").get(get_segments);
    app.at("/segment/:name").get(get_segment);
    app.at("/search/:term").get(search);

    log::info!("starting app");
    app.listen("127.0.0.1:8080").await?;

    Ok(())
}

type Request = tide::Request<(Arc<RwLock<Config>>, Arc<RwLock<HashMap<String, Segment>>>)>;

async fn get_segments(req: Request) -> tide::Result {
    let mut res = Response::new(200);
    let segments = req.state().1.read().unwrap();
    let json = serde_json::to_value(&*segments)?;
    let body = Body::from_json(&json)?;
    res.set_body(body);
    Ok(res)
}

async fn get_segment(req: Request) -> tide::Result {
    let mut res = Response::new(200);

    let name: String = req.param("name")?.parse()?;
    let segments = &req.state().1.read().unwrap();
    let value = segments.get(&name);

    let body = Body::from_json(&value)?;
    res.set_body(body);
    Ok(res)
}

async fn search(req: Request) -> tide::Result {
    let mut res = Response::new(200);

    let term: String = req.param("term")?.parse()?;
    log::info!("searching for {}", term);
    let segments = &req.state().1.read().unwrap();
    let mut results: Vec<usize> = Vec::new();
    for (_segment_path, segment) in segments.iter() {
        if let Some(doc_ids) = segment.get(&term) {
            for doc_id in doc_ids {
                results.push(*doc_id);
            }
        }
    }

    let body = Body::from_json(&results)?;
    res.set_body(body);
    Ok(res)
}

async fn get_messages(req: Request) -> tide::Result {
    let mut res = Response::new(200);
    let config = &req.state().0.read().unwrap();
    let body = Body::from_json(&config.messages)?;
    res.set_body(body);
    Ok(res)
}

async fn get_message(req: Request) -> tide::Result {
    let mut res = Response::new(200);

    let name: String = req.param("name")?.parse()?;
    let config = &req.state().0.read().unwrap();
    let value = config.messages.get(&name);

    let body = Body::from_json(&value)?;
    res.set_body(body);
    Ok(res)
}
