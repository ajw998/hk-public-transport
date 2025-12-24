-- FTS5 search across EN/TC/SC names.
-- Input:
--   :fts_q (TEXT) required, preprocessed for app search rules:
--     - English: you may append '*' for prefix matching (e.g. 'central*')
--     - TC/SC: segment into space-separated characters (e.g. '將軍澳' -> '將 軍 澳')
SELECT
  d.kind,
  d.ref_id,
  d.mode_id,
  d.operator_id,
  bm25(search_fts, 8.0, 4.0, 2.0, 2.0) AS rank
FROM search_fts
JOIN search_docs d ON d.doc_id = search_fts.rowid
WHERE search_fts MATCH :fts_q
ORDER BY rank DESC 
LIMIT 100;
