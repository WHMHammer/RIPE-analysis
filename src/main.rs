use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::File,
    future::Future,
    path::Path,
};

#[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
struct AsGraph {
    paths: Vec<Vec<u32>>,
    neighbors: HashMap<u32, HashSet<u32>>,
    transits: HashMap<u32, HashSet<u32>>,
    providers: HashMap<u32, HashSet<u32>>,
    customers: HashMap<u32, HashSet<u32>>,
    peers: HashMap<u32, HashSet<u32>>,
    siblings: HashMap<u32, HashSet<u32>>,
    enterprise_customers: HashSet<u32>,
    small_transit_providers: HashSet<u32>,
    large_transit_providers: HashSet<u32>,
    content_access_hosting_providers: HashSet<u32>,
}

enum AsGraphSource {
    Parameters(Vec<Vec<u32>>, HashMap<u32, HashSet<u32>>, String),
    File(File),
}

impl AsGraph {
    async fn parse_all<P>(path: P) -> BTreeMap<usize, (AsGraph, AsGraph)>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let mut graphs = BTreeMap::new();
        let mut handles = VecDeque::new();
        for path in path.read_dir().unwrap() {
            handles.push_back(tokio::spawn(AsGraph::parse(path.unwrap().path())));
        }
        for handle in handles {
            let (year, v4_graph, v6_graph) = handle.await.unwrap();
            graphs.insert(year, (v4_graph.await, v6_graph.await));
        }
        graphs
    }

    async fn parse<P>(
        path: P,
    ) -> (
        usize,
        impl Future<Output = AsGraph>,
        impl Future<Output = AsGraph>,
    )
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        let year = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split('.')
            .nth(1)
            .unwrap()
            .parse::<usize>()
            .unwrap()
            / 10000;

        if let Ok(v4_file) = File::open(format!("AS-graph-serializations/{}-v4", year)) {
            if let Ok(v6_file) = File::open(format!("AS-graph-serializations/{}-v6", year)) {
                println!("Deserializing data from year {}", year,);
                return (
                    year,
                    Self::new(AsGraphSource::File(v4_file)),
                    Self::new(AsGraphSource::File(v6_file)),
                );
            }
        }
        println!("Parsing data from year {}", year,);

        let mut v4_neighbors: HashMap<u32, HashSet<u32>> = HashMap::new();
        let mut v6_neighbors = HashMap::new();
        let mut v4_paths = Vec::new();
        let mut v6_paths = Vec::new();
        for elem in bgpkit_parser::BgpkitParser::new(path.to_str().unwrap()).unwrap() {
            let neighbors;
            let paths;
            if let std::net::IpAddr::V4(_) = elem.peer_ip {
                neighbors = &mut v4_neighbors;
                paths = &mut v4_paths;
            } else {
                neighbors = &mut v6_neighbors;
                paths = &mut v6_paths;
            };
            if let Some(path) = elem.as_path {
                let mut p = Vec::new();
                let mut prev_asn = 0;
                for segment in path.segments().iter() {
                    use bgpkit_parser::AsPathSegment::*;
                    match segment {
                        AsSequence(v) | ConfedSequence(v) => {
                            for asn in v.iter() {
                                let asn = asn.asn;
                                p.push(asn);
                                if prev_asn == 0 {
                                    neighbors.entry(asn).or_default();
                                } else {
                                    neighbors.entry(asn).or_default().insert(prev_asn);
                                    neighbors.entry(prev_asn).or_default().insert(asn);
                                }
                                prev_asn = asn;
                            }
                        }
                        AsSet(v) | ConfedSet(v) => {
                            for asn in v.iter() {
                                p.push(asn.asn);
                                neighbors.entry(asn.asn).or_default();
                            }
                            prev_asn = 0;
                        }
                    }
                }
                if p.len() != 0 {
                    paths.push(p);
                }
            }
        }

        (
            year,
            if let Ok(v4_file) = File::open(format!("AS-graph-serializations/{}-v4", year)) {
                Self::new(AsGraphSource::File(v4_file))
            } else {
                Self::new(AsGraphSource::Parameters(
                    v4_paths,
                    v4_neighbors,
                    format!("AS-graph-serializations/{}-v4", year),
                ))
            },
            if let Ok(v6_file) = File::open(format!("AS-graph-serializations/{}-v6", year)) {
                Self::new(AsGraphSource::File(v6_file))
            } else {
                Self::new(AsGraphSource::Parameters(
                    v6_paths,
                    v6_neighbors,
                    format!("AS-graph-serializations/{}-v6", year),
                ))
            },
        )
    }

    async fn new(source: AsGraphSource) -> Self {
        let (paths, neighbors, _filename) = match source {
            AsGraphSource::Parameters(p, n, f) => (p, n, f),
            AsGraphSource::File(f) => {
                return bincode::deserialize_from(f).unwrap();
            }
        };

        let mut graph = AsGraph {
            paths,
            neighbors,
            ..Default::default()
        };

        for path in graph.paths.iter() {
            let mut j = 0;
            let mut degree = graph.neighbors[&path[0]].len();
            for i in 1..path.len() {
                if graph.neighbors[&path[i]].len() > degree {
                    j = i;
                    degree = graph.neighbors[&path[i]].len();
                }
            }
            for i in 0..j {
                graph
                    .transits
                    .entry(path[i])
                    .or_default()
                    .insert(path[i + 1]);
            }
            for i in (j + 1)..path.len() {
                graph
                    .transits
                    .entry(path[i])
                    .or_default()
                    .insert(path[i - 1]);
            }
        }

        for path in graph.paths.iter() {
            for i in 0..(path.len() - 1) {
                match (
                    graph
                        .transits
                        .get(&path[i])
                        .map_or_else(|| false, |t| t.contains(&path[i + 1])),
                    graph
                        .transits
                        .get(&path[i + 1])
                        .map_or_else(|| false, |t| t.contains(&path[i])),
                ) {
                    (true, true) => {
                        graph
                            .siblings
                            .entry(path[i])
                            .or_default()
                            .insert(path[i + 1]);
                        graph
                            .siblings
                            .entry(path[i + 1])
                            .or_default()
                            .insert(path[i]);
                    }
                    (false, true) => {
                        graph
                            .customers
                            .entry(path[i])
                            .or_default()
                            .insert(path[i + 1]);
                        graph
                            .providers
                            .entry(path[i + 1])
                            .or_default()
                            .insert(path[i]);
                    }
                    (true, false) => {
                        graph
                            .providers
                            .entry(path[i])
                            .or_default()
                            .insert(path[i + 1]);
                        graph
                            .customers
                            .entry(path[i + 1])
                            .or_default()
                            .insert(path[i]);
                    }
                    (false, false) => {
                        graph.peers.entry(path[i]).or_default().insert(path[i + 1]);
                        graph.peers.entry(path[i + 1]).or_default().insert(path[i]);
                    }
                }
            }
        }

        for asn in graph.neighbors.keys() {
            match graph
                .customers
                .get(asn)
                .map_or_else(|| 0, |customers| customers.len())
            {
                0..=2 => {
                    if graph.peers.get(asn).map_or_else(|| 0, |peers| peers.len()) <= 1 {
                        graph.enterprise_customers.insert(*asn);
                    } else {
                        graph.content_access_hosting_providers.insert(*asn);
                    }
                }
                3..=47 => {
                    if graph.peers.get(asn).map_or_else(|| 0, |peers| peers.len()) < 4 {
                        graph.small_transit_providers.insert(*asn);
                    } else {
                        graph.content_access_hosting_providers.insert(*asn);
                    }
                }
                48..=179 => {
                    graph.small_transit_providers.insert(*asn);
                }
                _ => {
                    graph.large_transit_providers.insert(*asn);
                }
            }
        }

        //let file = File::create(filename).unwrap();
        //bincode::serialize_into(file, &graph).unwrap();
        graph
    }

    fn count_vertices(&self) -> usize {
        self.neighbors.len()
    }

    fn count_edges(&self) -> usize {
        self.neighbors
            .values()
            .fold(0, |count, neighbors| count + neighbors.len())
    }
}

fn reproduce_figure1(graphs: &BTreeMap<usize, (AsGraph, AsGraph)>) {
    {
        let mut f = File::create("results/Figure 1.1.csv").unwrap();
        use std::io::Write;
        writeln!(f, "year,IPv4,IPv6").unwrap();
        for (&year, (v4_graph, v6_graph)) in graphs.iter() {
            println!("Counting ASes for year {}", year);
            writeln!(
                f,
                "{},{},{}",
                year,
                v4_graph.count_vertices(),
                v6_graph.count_vertices()
            )
            .unwrap();
        }
    }
    {
        let mut f = File::create("results/Figure 1.2.csv").unwrap();
        use std::io::Write;
        writeln!(f, "year,IPv4,IPv6").unwrap();
        for (&year, (v4_graph, v6_graph)) in graphs.iter() {
            println!("Counting ASes for year {}", year);
            writeln!(
                f,
                "{},{},{}",
                year,
                v4_graph.count_edges(),
                v6_graph.count_edges()
            )
            .unwrap();
        }
    }
}

fn reproduce_figure7(graphs: &BTreeMap<usize, (AsGraph, AsGraph)>) {
    let mut f = File::create("results/Figure 7.csv").unwrap();
    use std::io::Write;
    writeln!(f, "year,IPv4,IPv6").unwrap();
    for (&year, (v4_graph, v6_graph)) in graphs.iter() {
        println!("Calculating average path length for year {}", year);
        let mut v4_mean = v4_graph.paths.iter().fold(0, |sum, path| sum + path.len()) as f64
            / v4_graph.paths.len() as f64;
        if v4_mean.is_nan() {
            v4_mean = 0.0;
        }
        let mut v6_mean = v6_graph.paths.iter().fold(0, |sum, path| sum + path.len()) as f64
            / v6_graph.paths.len() as f64;
        if v6_mean.is_nan() {
            v6_mean = 0.0;
        }
        writeln!(f, "{},{},{}", year, v4_mean, v6_mean).unwrap();
    }
}

fn reproduce_figure8(graphs: &BTreeMap<usize, (AsGraph, AsGraph)>) {
    let mut f = File::create("results/Figure 8.csv").unwrap();
    use std::io::Write;
    writeln!(f, "year,EC,STP,LTP,CAHP,all").unwrap();
    for (&year, (v4_graph, v6_graph)) in graphs.iter() {
        println!(
            "Calculating fractions of all ASes present in an IPv6 graph for year {}",
            year
        );

        let mut ec_count = v6_graph.enterprise_customers.len();
        for asn in v4_graph.enterprise_customers.iter() {
            if !v6_graph.enterprise_customers.contains(asn) {
                ec_count += 1;
            }
        }
        let mut ec_fraction = v6_graph.enterprise_customers.len() as f64 / ec_count as f64;
        if ec_fraction.is_nan() {
            ec_fraction = 0.0;
        }

        let mut stp_count = v6_graph.small_transit_providers.len();
        for asn in v4_graph.small_transit_providers.iter() {
            if !v6_graph.small_transit_providers.contains(asn) {
                stp_count += 1;
            }
        }
        let mut stp_fraction = v6_graph.small_transit_providers.len() as f64 / stp_count as f64;
        if stp_fraction.is_nan() {
            stp_fraction = 0.0;
        }

        let mut ltp_count = v6_graph.large_transit_providers.len();
        for asn in v4_graph.large_transit_providers.iter() {
            if !v6_graph.large_transit_providers.contains(asn) {
                ltp_count += 1;
            }
        }
        let mut ltp_fraction = v6_graph.large_transit_providers.len() as f64 / ltp_count as f64;
        if ltp_fraction.is_nan() {
            ltp_fraction = 0.0;
        }

        let mut cahp_count = v6_graph.content_access_hosting_providers.len();
        for asn in v4_graph.content_access_hosting_providers.iter() {
            if !v6_graph.content_access_hosting_providers.contains(asn) {
                cahp_count += 1;
            }
        }
        let mut cahp_fraction =
            v6_graph.content_access_hosting_providers.len() as f64 / cahp_count as f64;
        if cahp_fraction.is_nan() {
            cahp_fraction = 0.0;
        }

        writeln!(
            f,
            "{},{},{},{},{},{}",
            year,
            ec_fraction,
            stp_fraction,
            ltp_fraction,
            cahp_fraction,
            (v6_graph.enterprise_customers.len()
                + v6_graph.small_transit_providers.len()
                + v6_graph.large_transit_providers.len()
                + v6_graph.content_access_hosting_providers.len()) as f64
                / (ec_count + stp_count + ltp_count + cahp_count) as f64
        )
        .unwrap();
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let graphs = AsGraph::parse_all("RIPE-data").await;
    println!("Finished parsing all data");
    reproduce_figure1(&graphs);
    reproduce_figure7(&graphs);
    reproduce_figure8(&graphs);
}
