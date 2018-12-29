package srchx

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/blevesearch/bleve/search/query"

	"github.com/blevesearch/bleve"
	"github.com/imdario/mergo"
	"github.com/satori/go.uuid"
)

// Index - the index wrapper
type Index struct {
	bleve bleve.Index
}

// NewIndex - initialize a new index wrapper
func NewIndex(ndx bleve.Index) *Index {
	i := new(Index)
	i.bleve = ndx

	return i
}

// Delete - delete a document from the index
func (i *Index) Delete(id string) {
	i.bleve.Delete(id)
}

// Get - loads a document from the index
func (i *Index) Get(id string) (map[string]interface{}, error) {
	res, err := i.Search(&Query{
		Query: query.Query(bleve.NewDocIDQuery([]string{id})),
	})

	if err != nil {
		return nil, err
	}

	if res.Totals < 1 {
		return nil, errors.New("no data found")
	}

	return res.Docs[0], nil
}

// Put - set/update a document
func (i *Index) Put(data map[string]interface{}) (document map[string]interface{}, err error) {
	if _, found := data["id"]; !found || data["id"] == "" {
		uid, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		data["id"] = uid.String()
		data["created_at"] = time.Now().UnixNano()
	}

	id, ok := data["id"].(string)
	if !ok {
		return nil, errors.New("invalid id specified, it must be string")
	}

	if document, err = i.Get(id); err == nil && document != nil {
		document["id"] = id
	}

	if err = mergo.Map(&document, data, mergo.WithOverride); err != nil {
		return nil, err
	}

	document["updated_at"] = time.Now().UnixNano()

	if err = i.bleve.Index(id, document); err != nil {
		return nil, err
	}

	return document, nil
}

// Aggregate - aggregates (count, sum and avg) using the specified query and field
func (i *Index) Aggregate(q *Query, field, fn string) (result float64) {
	scroll := func(from, size int) *bleve.SearchResult {
		searchRequest := bleve.NewSearchRequest(q.Query)
		searchRequest.Fields = []string{field}
		searchRequest.From = from
		searchRequest.Size = size

		res, _ := i.bleve.Search(searchRequest)
		return res
	}

	frst := scroll(0, 1)
	if nil == frst || frst.Total < 1 {
		return 0
	}

	switch strings.ToLower(fn) {
	case "count":
		result = float64(frst.Total)
	case "sum":
		all := scroll(0, math.MaxInt32)
		if nil != all && all.Total > 0 {
			for _, doc := range all.Hits {
				val, ok := doc.Fields[field].(float64)
				if !ok {
					continue
				}
				result += val
			}
		}
	case "avg":
		all := scroll(0, math.MaxInt32)
		sum := float64(0)
		if nil != all && all.Total > 0 {
			for _, doc := range all.Hits {
				val, ok := doc.Fields[field].(float64)
				if !ok {
					continue
				}
				sum += val
			}
		}
		result = sum / float64(frst.Total)
	}

	return result
}

// Search - search in the index for the specified query
func (i *Index) Search(q *Query) (*SearchResult, error) {
	if q.Size < 1 {
		q.Size = 10
	}

	searchRequest := bleve.NewSearchRequest(q.Query)
	searchRequest.Fields = []string{"*"}
	searchRequest.IncludeLocations = true
	searchRequest.From = q.Offset
	searchRequest.Size = q.Size

	searchRequest.SortBy(q.Sort)

	res, err := i.bleve.Search(searchRequest)
	if err != nil {
		return nil, err
	}

	docs := []map[string]interface{}{}

	for _, doc := range res.Hits {
		doc.Fields["_score"] = doc.Score
		doc.Fields["_size"] = doc.Size()
		doc.Fields["_offset"] = doc.HitNumber

		docs = append(docs, doc.Fields)
	}

	ret := &SearchResult{
		Totals: res.Total,
		Docs:   docs,
		Time:   int64(res.Took),
	}

	i.applyJOIN(ret, q)

	return ret, nil
}

// ApplyJOIN - apply joins on the specified search result
func (i *Index) applyJOIN(res *SearchResult, q *Query) {
	if len(q.Join) < 1 {
		return
	}
	for x, doc := range res.Docs {
		for _, join := range q.Join {

			if join.Where == nil {
				join.Where = &Query{}
			}
			if join.On == "" || join.As == "" {
				continue
			}
			if doc[join.On] == nil {
				continue
			}

			join.Where.Query = bleve.NewDocIDQuery([]string{doc[join.On].(string)})
			join.Where.Join = q.Join

			if join.Where.Query != nil {
				join.Where.Query = bleve.NewConjunctionQuery(join.Where.Query, bleve.NewDocIDQuery([]string{doc[join.On].(string)}))
			} else {
				join.Where.Query = bleve.NewDocIDQuery([]string{doc[join.On].(string)})
			}

			sub, _ := i.Search(join.Where)

			delete(doc, join.On)

			doc[join.As] = sub.Docs
			res.Docs[x] = doc
		}
	}
}
