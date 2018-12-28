package srchx

import "github.com/blevesearch/bleve/search/query"

// Query - represents a query request
type Query struct {
	Query  query.Query `json:"-"`
	Offset int         `json:"offset"`
	Size   int         `json:"size"`
	Sort   []string    `json:"sort"`
	Join   []*Join     `json:"join"`
}

// Join - a relation request to load children/parents of document(s)
type Join struct {
	Src   *Index `json:"-"`
	On    string `json:"on"`
	As    string `json:"as"`
	Where *Query `json:"where"`
}
