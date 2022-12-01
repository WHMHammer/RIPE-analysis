use plotters::prelude::*;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

struct AsGraph {
    year: usize,
    v4: HashMap<u32, HashSet<u32>>,
    v6: HashMap<u32, HashSet<u32>>,
}

fn determine_relationships(graph: &HashMap<u32, HashSet<u32>>, name: &str) {
    let mut peer_count = 0;
    let mut provider_count = 0;
    let mut customer_count = 0;
    let mut sibling_count = 0;
    let mut f = std::fs::File::create(format!("results/relationships/{}.csv", name)).unwrap();
    use std::io::Write;
    writeln!(
        f,
        "asn,peer_count,provider_count,customer_count,sibling_count"
    )
    .unwrap();
    for asn in graph.keys() {
        for other in graph.keys() {
            if other == asn {
                continue;
            }
            match (graph[asn].contains(other), graph[other].contains(asn)) {
                (true, true) => {
                    sibling_count += 1;
                }
                (true, false) => {
                    customer_count += 1;
                }
                (false, true) => {
                    provider_count += 1;
                }
                (false, false) => {
                    peer_count += 1;
                }
            }
        }
        writeln!(
            f,
            "{},{},{},{},{}",
            asn, peer_count, provider_count, customer_count, sibling_count
        )
        .unwrap();
        peer_count = 0;
        provider_count = 0;
        customer_count = 0;
        sibling_count = 0;
    }
}

async fn process(path: std::path::PathBuf) -> AsGraph {
    let mut as_graph = AsGraph {
        year: path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split('.')
            .nth(1)
            .unwrap()
            .parse::<usize>()
            .unwrap()
            / 10000,
        v4: HashMap::new(),
        v6: HashMap::new(),
    };
    for elem in bgpkit_parser::BgpkitParser::new(path.to_str().unwrap()).unwrap() {
        let graph = if let std::net::IpAddr::V4(_) = elem.peer_ip {
            &mut as_graph.v4
        } else {
            &mut as_graph.v6
        };
        if let Some(path) = elem.as_path {
            let mut prev_asn = 0;
            for segment in path.segments().iter() {
                use bgpkit_parser::AsPathSegment::*;
                match segment {
                    AsSequence(v) | ConfedSequence(v) => {
                        for asn in v.iter() {
                            if prev_asn == 0 {
                                graph.entry(asn.asn).or_default();
                            } else {
                                graph.entry(asn.asn).or_default().insert(prev_asn);
                            }
                            prev_asn = asn.asn;
                        }
                    }
                    AsSet(v) | ConfedSet(v) => {
                        for asn in v.iter() {
                            graph.entry(asn.asn).or_default();
                        }
                        prev_asn = 0;
                    }
                }
            }
        }
    }

    println!(
        "Finished counting ASes from year {} ({}:{}).",
        as_graph.year,
        as_graph.v4.len(),
        as_graph.v6.len()
    );

    determine_relationships(&as_graph.v4, format!("{}-v4", as_graph.year).as_str());
    determine_relationships(&as_graph.v6, format!("{}-v6", as_graph.year).as_str());
    println!(
        "Finished determining relationships from year {}.",
        as_graph.year
    );

    as_graph
}

async fn process_all<P>(path: P) -> BTreeMap<usize, AsGraph>
where
    P: AsRef<std::path::Path>,
{
    let mut as_graphs = BTreeMap::new();
    let mut handles = VecDeque::new();
    let path = path.as_ref();
    for path in path.read_dir().unwrap() {
        handles.push_back(tokio::spawn(process(path.unwrap().path())));
    }
    for handle in handles {
        let as_graph = handle.await.unwrap();
        as_graphs.insert(as_graph.year, as_graph);
    }
    as_graphs
}

fn plot_counts(as_graphs: BTreeMap<usize, AsGraph>) {
    let mut min_x = usize::MAX;
    let mut max_x = usize::MIN;
    let mut min_y = usize::MAX;
    let mut max_y = usize::MIN;
    let mut v4_counts = Vec::new();
    let mut v6_counts = Vec::new();
    let mut prev_year = 0;
    let mut f = std::fs::File::create("results/Figure 1.1.csv").unwrap();
    use std::io::Write;
    writeln!(f, "year,v4,v6").unwrap();
    for as_graph in as_graphs.values() {
        if prev_year == 0 {
            min_x = as_graph.year
        } else {
            for year in (prev_year + 1)..as_graph.year {
                min_y = 0;
                v4_counts.push((year, 0));
                v6_counts.push((year, 0));
            }
        }
        max_x = as_graph.year;
        if as_graph.v4.len() < min_y {
            min_y = as_graph.v4.len()
        }
        if as_graph.v6.len() < min_y {
            min_y = as_graph.v6.len()
        }
        if as_graph.v4.len() > max_y {
            max_y = as_graph.v4.len()
        }
        if as_graph.v6.len() > max_y {
            max_y = as_graph.v6.len()
        }
        v4_counts.push((as_graph.year, as_graph.v4.len()));
        v6_counts.push((as_graph.year, as_graph.v6.len()));
        prev_year = as_graph.year;
        writeln!(
            f,
            "{},{},{}",
            as_graph.year,
            as_graph.v4.len(),
            as_graph.v6.len()
        )
        .unwrap();
    }

    let root_area = BitMapBackend::new("results/Figure 1.1.png", (768, 512)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 64)
        .margin_right(64)
        .set_label_area_size(LabelAreaPosition::Bottom, 64)
        .caption("IPv4/IPv6 AS Count vs. Year", ("Arial", 24))
        .build_cartesian_2d(min_x..max_x, min_y..max_y)
        .unwrap();
    ctx.configure_mesh().draw().unwrap();
    ctx.draw_series(LineSeries::new(v4_counts.into_iter(), &RED))
        .unwrap()
        .label("IPv4")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 16, y)], &RED));
    ctx.draw_series(LineSeries::new(v6_counts.into_iter(), &BLUE))
        .unwrap()
        .label("IPv6")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 16, y)], &BLUE));
    ctx.configure_series_labels().draw().unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let as_graphs = process_all("RIPE-data").await;
    plot_counts(as_graphs);
}
