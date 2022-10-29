use plotters::prelude::*;
use std::collections::*;

async fn count(path: std::path::PathBuf) -> (usize, usize, usize) {
    let year: usize = path
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
    let mut ipv4_ases = HashSet::new();
    let mut ipv6_ases = HashSet::new();
    for elem in bgpkit_parser::BgpkitParser::new(path.to_str().unwrap()).unwrap() {
        let ases = if let std::net::IpAddr::V4(_) = elem.peer_ip {
            &mut ipv4_ases
        } else {
            &mut ipv6_ases
        };
        if let Some(path) = elem.as_path {
            for segment in path.segments().iter() {
                use bgpkit_parser::AsPathSegment::*;
                match segment {
                    AsSequence(v) | AsSet(v) | ConfedSequence(v) | ConfedSet(v) => {
                        for asn in v.iter() {
                            ases.insert(asn.asn);
                        }
                    }
                }
            }
        }
    }
    println!(
        "Finished counting data from year {} ({}:{}).",
        year,
        ipv4_ases.len(),
        ipv6_ases.len()
    );
    (year, ipv4_ases.len(), ipv6_ases.len())
}

async fn count_all<P>(path: P) -> BTreeMap<usize, (usize, usize)>
where
    P: AsRef<std::path::Path>,
{
    let mut counts = BTreeMap::new();
    let mut handles = VecDeque::new();
    let path = path.as_ref();
    for path in path.read_dir().unwrap() {
        handles.push_back(tokio::spawn(count(path.unwrap().path())));
    }
    for handle in handles {
        let (date, ipv4_count, ipv6_count) = handle.await.unwrap();
        counts.insert(date, (ipv4_count, ipv6_count));
    }
    counts
}

fn plot(counts: BTreeMap<usize, (usize, usize)>) {
    let mut min_x = usize::MAX;
    let mut max_x = usize::MIN;
    let mut min_y = usize::MAX;
    let mut max_y = usize::MIN;
    let mut ipv4_counts = Vec::new();
    let mut ipv6_counts = Vec::new();
    let mut prev_year = 0;
    let mut log_file = std::fs::File::create("Figure 1.1.csv").unwrap();
    use std::io::Write;
    writeln!(log_file, "Year,IPv4,IPv6").unwrap();
    for (&year, &(ipv4_count, ipv6_count)) in counts.iter() {
        if prev_year == 0 {
            min_x = year
        } else {
            for year in (prev_year + 1)..year {
                min_y = 0;
                ipv4_counts.push((year, 0));
                ipv6_counts.push((year, 0));
            }
        }
        max_x = year;
        if ipv4_count < min_y {
            min_y = ipv4_count
        }
        if ipv6_count < min_y {
            min_y = ipv6_count
        }
        if ipv4_count > max_y {
            max_y = ipv4_count
        }
        if ipv6_count > max_y {
            max_y = ipv6_count
        }
        ipv4_counts.push((year, ipv4_count));
        ipv6_counts.push((year, ipv6_count));
        prev_year = year;
        writeln!(log_file, "{},{},{}", year, ipv4_count, ipv6_count).unwrap();
    }

    let root_area = BitMapBackend::new("Figure 1.1.png", (768, 512)).into_drawing_area();
    root_area.fill(&WHITE).unwrap();
    let mut ctx = ChartBuilder::on(&root_area)
        .set_label_area_size(LabelAreaPosition::Left, 64)
        .margin_right(64)
        .set_label_area_size(LabelAreaPosition::Bottom, 64)
        .caption("IPv4/IPv6 AS Count vs. Year", ("Arial", 24))
        .build_cartesian_2d(min_x..max_x, min_y..max_y)
        .unwrap();
    ctx.configure_mesh().draw().unwrap();
    ctx.draw_series(LineSeries::new(ipv4_counts.into_iter(), &RED))
        .unwrap()
        .label("IPv4")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 16, y)], &RED));
    ctx.draw_series(LineSeries::new(ipv6_counts.into_iter(), &BLUE))
        .unwrap()
        .label("IPv6")
        .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 16, y)], &BLUE));
    ctx.configure_series_labels().draw().unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let counts = count_all("RIPE-data").await;
    plot(counts);
}
