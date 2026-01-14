use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use crossref_citation_extraction::extract::extract_arxiv_matches_from_text;

fn bench_arxiv_extraction(c: &mut Criterion) {
    let sample_texts = vec![
        "arXiv:2403.12345",
        "arXiv:hep-ph/9901234 and arXiv:cs.DM/9910013",
        "https://arxiv.org/abs/2403.12345",
        "10.48550/arXiv.2403.12345",
        "No arXiv references here",
    ];

    let mut group = c.benchmark_group("arxiv_extraction");
    group.throughput(Throughput::Elements(sample_texts.len() as u64));

    group.bench_function("extract_arxiv_matches", |b| {
        b.iter(|| {
            for text in &sample_texts {
                black_box(extract_arxiv_matches_from_text(text));
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_arxiv_extraction);
criterion_main!(benches);
