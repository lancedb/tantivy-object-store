// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::HashSet, sync::Arc, time::Instant};

use log::info;
use object_store::local::LocalFileSystem;
use tantivy::{
    doc,
    schema::{STORED, TEXT},
    IndexSettings,
};
use tantivy_object_store::new_object_store_directory;
use wikidump::{config, Parser};

fn main() {
    env_logger::init();
    info!("Straring indexing Wikipedia dump");
    let parser = Parser::new().use_config(config::wikipedia::english());
    let site = parser
        .parse_file("./simplewiki-20230720-pages-articles-multistream.xml")
        .expect("Could not parse wikipedia dump file.");

    // use linux words file for query
    let words = std::fs::read_to_string("./1000-most-common-words.txt").unwrap();
    let mut stop_words = stop_words::get(stop_words::LANGUAGE::English)
        .into_iter()
        .map(|s| s.replace("\"", ""))
        .collect::<HashSet<_>>();

    for w in vec!["I", "you", "me", "it", "he", "she", "her", "his", "its"] {
        stop_words.insert(w.to_string());
    }
    let words = words.split("\n").collect::<Vec<_>>();
    let words = words
        .into_iter()
        .filter(|w| !stop_words.contains(&w.to_string()) && w.len() >= 5)
        .collect::<Vec<_>>();

    let dir = tempfile::tempdir().unwrap();
    let mut schema_builder = tantivy::schema::Schema::builder();
    let id_field = schema_builder.add_u64_field("id", STORED);
    let title_field = schema_builder.add_text_field("title", TEXT | STORED);
    let text_field = schema_builder.add_text_field("text", TEXT | STORED);
    let schema = schema_builder.build();

    let index = tantivy::Index::create_in_dir(dir.path(), schema.clone()).unwrap();

    // 1 GB
    let mut writer = index.writer(1024 * 1024 * 1024).unwrap();

    let start = Instant::now();
    info!("starting indexing using native tantivy diretory");
    for (idx, page) in site.pages.iter().enumerate() {
        let doc = doc! {
            id_field => idx as u64,
            title_field => page.title.clone(),
            text_field => page.revisions[page.revisions.len() - 1].text.to_string(),
        };

        writer.add_document(doc).unwrap();
    }

    info!("waiting for index to fully commit");
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    info!("indexing took {:?}", start.elapsed());

    let searcher = index.reader().unwrap().searcher();
    let query_parser = tantivy::query::QueryParser::for_index(&index, vec![title_field]);

    info!("starting search using native tantivy diretory");
    let start = Instant::now();
    for (idx, word) in words.iter().enumerate() {
        if idx % 1000 == 0 {
            info!("searched {} words in {:?}", idx, start.elapsed());
        }
        searcher
            .search(
                &query_parser.parse_query(word).unwrap(),
                &tantivy::collector::TopDocs::with_limit(10),
            )
            .unwrap();
    }
    info!("search took {:?}", start.elapsed());

    let dir = tempfile::tempdir().unwrap();

    let dir = new_object_store_directory(
        Arc::new(LocalFileSystem::new()),
        dir.path().to_str().unwrap(),
        None,
        0,
        None,
        None,
    )
    .unwrap();

    let index_using_object_store =
        tantivy::Index::create(dir, schema.clone(), IndexSettings::default()).unwrap();

    let mut writer = index_using_object_store.writer(1024 * 1024 * 1024).unwrap();

    let start = Instant::now();
    info!("starting indexing using object store tantivy diretory");
    for (idx, page) in site.pages.iter().enumerate() {
        let doc = doc! {
            id_field => idx as u64,
            title_field => page.title.clone(),
            text_field => page.revisions[page.revisions.len() - 1].text.to_string(),
        };

        writer.add_document(doc).unwrap();
    }

    info!("waiting for index to fully commit");
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    info!("indexing took {:?}", start.elapsed());

    let searcherusing_object_store = index_using_object_store.reader().unwrap().searcher();
    // only search title as text is too noisy
    let query_parser = tantivy::query::QueryParser::for_index(&index, vec![text_field]);

    info!("starting search using object store tantivy diretory");
    let start = Instant::now();
    for (idx, word) in words.iter().enumerate() {
        if idx % 1000 == 0 {
            info!("searched {} words in {:?}", idx, start.elapsed());
        }
        searcherusing_object_store
            .search(
                &query_parser.parse_query(word).unwrap(),
                &tantivy::collector::TopDocs::with_limit(10),
            )
            .unwrap();
    }
    info!("search took {:?}", start.elapsed());

    info!("checking if results are the same");
    let start = Instant::now();
    let mut total_overlap = 0;
    let mut total = 0;
    for (idx, word) in words.iter().enumerate() {
        if idx % 1000 == 0 {
            info!("searched {} words in {:?}", idx, start.elapsed());
        }
        let mut top_docs = searcher
            .search(
                &query_parser.parse_query(word).unwrap(),
                &tantivy::collector::TopDocs::with_limit(100),
            )
            .unwrap();
        top_docs.sort_by(|(score_a, _), (score_b, _)| score_b.partial_cmp(score_a).unwrap());
        let top_docs = top_docs
            .into_iter()
            .take(10)
            .map(|(_, doc)| {
                searcher
                    .doc(doc)
                    .unwrap()
                    .get_first(id_field)
                    .cloned()
                    .unwrap()
                    .as_u64()
                    .unwrap()
            })
            .collect::<HashSet<_>>();

        let mut top_docs_using_object_store = searcherusing_object_store
            .search(
                &query_parser.parse_query(word).unwrap(),
                &tantivy::collector::TopDocs::with_limit(100),
            )
            .unwrap();
        top_docs_using_object_store
            .sort_by(|(score_a, _), (score_b, _)| score_b.partial_cmp(score_a).unwrap());
        let top_docs_using_object_store = top_docs_using_object_store
            .into_iter()
            .take(10)
            .map(|(_, doc)| {
                searcherusing_object_store
                    .doc(doc)
                    .unwrap()
                    .get_first(id_field)
                    .cloned()
                    .unwrap()
                    .as_u64()
                    .unwrap()
            })
            .collect::<HashSet<_>>();

        let min_size = std::cmp::min(top_docs_using_object_store.len(), top_docs.len());
        total += min_size;

        let overlap = top_docs.intersection(&top_docs_using_object_store).count();
        total_overlap += overlap;

        // At least 1 overlap
        assert!(
            top_docs.intersection(&top_docs_using_object_store).count() > 0
                || (top_docs_using_object_store.is_empty() && top_docs.is_empty()),
            "results are not the same for word {}, {}, {}, {}",
            word,
            overlap,
            top_docs.len(),
            top_docs_using_object_store.len()
        );
    }

    // Tantivy index build is non-deterministic, so we can't expect the results to be exactly the same
    // check that overall the results are the very similar
    assert!(
        total_overlap > (total * 99 / 100),
        "results are not the same"
    );
}
