use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::*;
use tantivy::{Index, TantivyDocument, Term};

lazy_static::lazy_static! {
    static ref INDEXES: Mutex<HashMap<String, Arc<Index>>> = Mutex::new(HashMap::new());
}

/// Converts a Rust String into a C-compatible string
fn to_c_string(s: String) -> *mut c_char {
    CString::new(s).unwrap().into_raw()
}

/// Creates an index (either in-memory or persistent)
#[unsafe(no_mangle)]
pub extern "C" fn create_index(path: *const c_char) -> *mut c_char {
    let schema = {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("title", TEXT | STORED);
        schema_builder.add_facet_field("category", FacetOptions::default().set_stored()); // ✅ STORE FACETS
        schema_builder.build()
    };

    let index = if path.is_null() {
        // Create an in-memory index
        Index::create_in_ram(schema)
    } else {
        // Create a persistent index at the given path
        let path_str = unsafe { CStr::from_ptr(path).to_string_lossy().into_owned() };
        let directory = MmapDirectory::open(Path::new(&path_str)).unwrap();
        Index::open_or_create(directory, schema).unwrap()
    };

    let index_arc = Arc::new(index);
    let mut indexes = INDEXES.lock().unwrap();
    let id = format!("{}", indexes.len());
    indexes.insert(id.clone(), index_arc);

    to_c_string(id)
}

/// Adds a document to the index
#[unsafe(no_mangle)]
pub extern "C" fn add_document(index_id: *const c_char, title: *const c_char, category: *const c_char) -> *mut c_char {
    let index_id_str = unsafe { CStr::from_ptr(index_id).to_string_lossy().into_owned() };
    let indexes = INDEXES.lock().unwrap();
    let index = match indexes.get(&index_id_str) {
        Some(idx) => idx.clone(),
        None => return to_c_string("Error: Index not found".to_string()),
    };

    let title_str = unsafe { CStr::from_ptr(title).to_string_lossy().into_owned() };
    let category_str = unsafe { CStr::from_ptr(category).to_string_lossy().into_owned() };

    let schema = index.schema();
    let title_field = schema.get_field("title").unwrap();
    let category_field = schema.get_field("category").unwrap();

    let mut index_writer = index.writer(50_000_000).unwrap();
    let mut doc = TantivyDocument::default();
    doc.add_text(title_field, &title_str);
    doc.add_facet(category_field, Facet::from(category_str.as_str()));

    match index_writer.add_document(doc) {
        Ok(_) => {
            index_writer.commit().unwrap();
            to_c_string("Document added successfully".to_string())
        }
        Err(e) => to_c_string(format!("Error: {}", e)),
    }
}

/// Performs a search with an optional query and/or facet filter
#[unsafe(no_mangle)]
pub extern "C" fn search(index_id: *const c_char, query: *const c_char, facet: *const c_char) -> *mut c_char {
    let index_id_str = unsafe { CStr::from_ptr(index_id).to_string_lossy().into_owned() };
    let indexes = INDEXES.lock().unwrap();
    let index = match indexes.get(&index_id_str) {
        Some(idx) => idx.clone(),
        None => return to_c_string("Error: Index not found".to_string()),
    };

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let schema = index.schema();

    let mut queries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    // Add text search if query is provided
    if !query.is_null() {
        let query_str = unsafe { CStr::from_ptr(query).to_string_lossy().into_owned() };
        let title_field = schema.get_field("title").unwrap();
        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![title_field]);
        let text_query = query_parser.parse_query(&query_str).unwrap();
        queries.push((Occur::Must, text_query));
    }

    // Add facet filtering if facet is provided
    if !facet.is_null() {
        let facet_str = unsafe { CStr::from_ptr(facet).to_string_lossy().into_owned() };
        let category_field = schema.get_field("category").unwrap();
        let facet_query = TermQuery::new(
            Term::from_facet(category_field, &Facet::from(facet_str.as_str())),
            IndexRecordOption::Basic
        );
        queries.push((Occur::Must, Box::new(facet_query)));
    }

    // Combine queries using BooleanQuery
    let combined_query: Box<dyn Query> = if queries.is_empty() {
        Box::new(AllQuery) // ✅ Use AllQuery if no filters are present
    } else if queries.len() == 1 {
        queries.remove(0).1
    } else {
        Box::new(BooleanQuery::new(queries))
    };

    // Perform search
    let top_docs = searcher.search(&combined_query, &TopDocs::with_limit(10)).unwrap();
    let mut results = Vec::new();

    for (_score, doc_address) in top_docs {
        let retrieved_doc: TantivyDocument = searcher.doc(doc_address).unwrap();
        println!("DEBUG: Retrieved Doc: {:?}", retrieved_doc); // ✅ Print full document
        results.push(format!("{:?}", retrieved_doc));
    }

    let result_string = results.join("\n");
    to_c_string(result_string)
}

/// Frees a previously allocated C string
#[unsafe(no_mangle)]
pub extern "C" fn free_string(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    unsafe { drop(CString::from_raw(s)) };
}