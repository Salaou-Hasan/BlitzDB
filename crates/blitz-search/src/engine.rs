use std::collections::HashMap;
use std::sync::RwLock;

use blitz_types::id::RowId;

/// A document to be indexed.
#[derive(Debug, Clone)]
pub struct Document {
    pub id: RowId,
    pub fields: HashMap<String, String>,
}

impl Document {
    pub fn new(id: RowId) -> Self {
        Self {
            id,
            fields: HashMap::new(),
        }
    }

    pub fn with_field(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    pub fn get_text(&self, field: &str) -> Option<&str> {
        self.fields.get(field).map(|s| s.as_str())
    }

    pub fn all_text(&self) -> String {
        self.fields.values().cloned().collect::<Vec<_>>().join(" ")
    }
}

/// A search hit with relevance score.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: RowId,
    pub score: f64,
    pub matched_fields: Vec<String>,
}

/// Inverted index entry: term -> set of (document_id, field, count).
#[derive(Debug, Clone)]
struct IndexEntry {
    /// doc_id -> (field, term_count in that field)
    postings: HashMap<RowId, Vec<(String, usize)>>,
    /// Total occurrences across all documents.
    total_count: usize,
}

/// Full-text search engine using an inverted index with TF scoring.
pub struct SearchEngine {
    /// term -> IndexEntry
    index: RwLock<HashMap<String, IndexEntry>>,
    /// doc_id -> Document (for retrieval)
    documents: RwLock<HashMap<RowId, Document>>,
    /// doc_id -> total term count
    doc_lengths: RwLock<HashMap<RowId, usize>>,
}

impl SearchEngine {
    pub fn new() -> Self {
        Self {
            index: RwLock::new(HashMap::new()),
            documents: RwLock::new(HashMap::new()),
            doc_lengths: RwLock::new(HashMap::new()),
        }
    }

    /// Index a document on specific fields.
    pub fn index_document(&self, doc: Document, fields: &[&str]) {
        let mut terms_per_field: HashMap<String, Vec<(String, usize)>> = HashMap::new();
        let mut total_terms = 0usize;

        for field in fields {
            if let Some(text) = doc.fields.get(*field) {
                let tokens = tokenize(text);
                let mut term_counts: HashMap<String, usize> = HashMap::new();
                for token in &tokens {
                    *term_counts.entry(token.clone()).or_insert(0) += 1;
                }
                for (term, count) in term_counts {
                    total_terms += count;
                    terms_per_field
                        .entry(term)
                        .or_default()
                        .push((field.to_string(), count));
                }
            }
        }

        let mut index = self.index.write().unwrap();
        for (term, field_entries) in terms_per_field {
            let entry = index.entry(term).or_insert_with(|| IndexEntry {
                postings: HashMap::new(),
                total_count: 0,
            });
            entry.total_count += field_entries.iter().map(|(_, c)| c).sum::<usize>();
            entry.postings.insert(doc.id, field_entries);
        }

        self.documents.write().unwrap().insert(doc.id, doc.clone());
        self.doc_lengths.write().unwrap().insert(doc.id, total_terms);
    }

    /// Remove a document from the index.
    pub fn remove_document(&self, doc_id: RowId) {
        if let Some(doc) = self.documents.write().unwrap().remove(&doc_id) {
            let mut index = self.index.write().unwrap();
            for field in doc.fields.keys() {
                if let Some(text) = doc.fields.get(field.as_str()) {
                    let tokens = tokenize(text);
                    let mut term_counts: HashMap<String, usize> = HashMap::new();
                    for token in &tokens {
                        *term_counts.entry(token.clone()).or_insert(0) += 1;
                    }
                    for (term, count) in term_counts {
                        if let Some(entry) = index.get_mut(&term) {
                            entry.total_count = entry.total_count.saturating_sub(count);
                            entry.postings.remove(&doc_id);
                            if entry.postings.is_empty() {
                                index.remove(&term);
                            }
                        }
                    }
                }
            }
        }
        self.doc_lengths.write().unwrap().remove(&doc_id);
    }

    /// Search for documents matching the query.
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchHit> {
        let query_tokens = tokenize(query);
        if query_tokens.is_empty() {
            return Vec::new();
        }

        let index = self.index.read().unwrap();
        let doc_lengths = self.doc_lengths.read().unwrap();
        let total_docs = doc_lengths.len() as f64;

        if total_docs == 0.0 {
            return Vec::new();
        }

        // Calculate average document length for BM25-style normalization
        let avg_doc_len: f64 = if total_docs > 0.0 {
            doc_lengths.values().sum::<usize>() as f64 / total_docs
        } else {
            1.0
        };

        // Score each document
        let mut scores: HashMap<RowId, (f64, Vec<String>)> = HashMap::new();

        for token in &query_tokens {
            if let Some(entry) = index.get(token) {
                let idf = ((total_docs - entry.postings.len() as f64 + 0.5)
                    / (entry.postings.len() as f64 + 0.5)
                    + 1.0)
                    .ln();

                for (doc_id, field_entries) in &entry.postings {
                    let doc_len = doc_lengths.get(doc_id).copied().unwrap_or(0) as f64;
                    let k1 = 1.5;
                    let b = 0.75;

                    let mut tf_sum = 0.0;
                    let mut matched_fields = Vec::new();
                    for (field, count) in field_entries {
                        tf_sum += *count as f64;
                        if !matched_fields.contains(field) {
                            matched_fields.push(field.clone());
                        }
                    }

                    let tf_normalized = (tf_sum * (k1 + 1.0))
                        / (tf_sum + k1 * (1.0 - b + b * doc_len / avg_doc_len));

                    let score = idf * tf_normalized;

                    let entry = scores.entry(*doc_id).or_insert((0.0, Vec::new()));
                    entry.0 += score;
                    for f in matched_fields {
                        if !entry.1.contains(&f) {
                            entry.1.push(f);
                        }
                    }
                }
            }
        }

        let mut results: Vec<SearchHit> = scores
            .into_iter()
            .map(|(id, (score, matched_fields))| SearchHit {
                id,
                score,
                matched_fields,
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    /// Get the number of indexed documents.
    pub fn document_count(&self) -> usize {
        self.documents.read().unwrap().len()
    }

    /// Get the number of unique terms.
    pub fn term_count(&self) -> usize {
        self.index.read().unwrap().len()
    }

    /// Check if a document exists in the index.
    pub fn has_document(&self, doc_id: RowId) -> bool {
        self.documents.read().unwrap().contains_key(&doc_id)
    }

    /// Get a document by ID.
    pub fn get_document(&self, doc_id: RowId) -> Option<Document> {
        self.documents.read().unwrap().get(&doc_id).cloned()
    }
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Tokenize text into lowercase terms.
fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 1)
        .map(|s| s.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_docs() -> SearchEngine {
        let engine = SearchEngine::new();
        engine.index_document(
            Document::new(RowId::new(1))
                .with_field("title", "The Great Gatsby")
                .with_field("body", "A novel about the American dream"),
            &["title", "body"],
        );
        engine.index_document(
            Document::new(RowId::new(2))
                .with_field("title", "Great Expectations")
                .with_field("body", "A novel by Charles Dickens"),
            &["title", "body"],
        );
        engine.index_document(
            Document::new(RowId::new(3))
                .with_field("title", "To Kill a Mockingbird")
                .with_field("body", "A novel about justice in the American south"),
            &["title", "body"],
        );
        engine
    }

    #[test]
    fn test_tokenize() {
        assert_eq!(tokenize("Hello World"), vec!["hello", "world"]);
        assert_eq!(tokenize("foo-bar baz"), vec!["foo", "bar", "baz"]);
        assert!(tokenize("a").is_empty()); // single chars filtered
    }

    #[test]
    fn test_index_and_search() {
        let engine = test_docs();
        assert_eq!(engine.document_count(), 3);

        let results = engine.search("great", 10);
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.id == RowId::new(1)));
        assert!(results.iter().any(|r| r.id == RowId::new(2)));
    }

    #[test]
    fn test_search_no_match() {
        let engine = test_docs();
        let results = engine.search("quantum physics", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_ranking() {
        let engine = test_docs();
        let results = engine.search("novel american", 10);
        // Doc 1 has "novel" and "american" in body
        // Doc 3 has "novel" and "american" in body
        assert!(!results.is_empty());
    }

    #[test]
    fn test_remove_document() {
        let engine = test_docs();
        engine.remove_document(RowId::new(1));
        assert_eq!(engine.document_count(), 2);
        let results = engine.search("gatsby", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_has_document() {
        let engine = test_docs();
        assert!(engine.has_document(RowId::new(1)));
        assert!(!engine.has_document(RowId::new(99)));
    }

    #[test]
    fn test_get_document() {
        let engine = test_docs();
        let doc = engine.get_document(RowId::new(1)).unwrap();
        assert_eq!(doc.get_text("title"), Some("The Great Gatsby"));
    }

    #[test]
    fn test_empty_search() {
        let engine = SearchEngine::new();
        let results = engine.search("test", 10);
        assert!(results.is_empty());
    }

    #[test]
    fn test_limit() {
        let engine = test_docs();
        let results = engine.search("novel", 1);
        assert_eq!(results.len(), 1);
    }
}
