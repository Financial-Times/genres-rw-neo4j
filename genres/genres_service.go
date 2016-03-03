package genres

import (
	"encoding/json"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
)

type service struct {
	cypherRunner neoutils.CypherRunner
	indexManager neoutils.IndexManager
}

// NewCypherGenresService provides functions for create, update, delete operations on genres in Neo4j,
// plus other utility functions needed for a service
func NewCypherGenresService(cypherRunner neoutils.CypherRunner, indexManager neoutils.IndexManager) service {
	return service{cypherRunner, indexManager}
}

func (s service) Initialise() error {
	return neoutils.EnsureConstraints(s.indexManager, map[string]string{
		"Thing":   "uuid",
		"Concept": "uuid",
		"Classification": "uuid",
		"Genre": "uuid"})
}

func (s service) Read(uuid string) (interface{}, bool, error) {
	results := []Genre{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Genre {uuid:{uuid}}) return n.uuid
		as uuid, n.canonicalName as canonicalName,
		n.tmeIdentifier as tmeIdentifier`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
		Result: &results,
	}

	err := s.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return Genre{}, false, err
	}

	if len(results) == 0 {
		return Genre{}, false, nil
	}

	return results[0], true, nil
}

func (s service) Write(thing interface{}) error {

	sub := thing.(Genre)

	params := map[string]interface{}{
		"uuid": sub.UUID,
	}

	if sub.CanonicalName != "" {
		params["canonicalName"] = sub.CanonicalName
		params["prefLabel"] = sub.CanonicalName
	}

	if sub.TmeIdentifier != "" {
		params["tmeIdentifier"] = sub.TmeIdentifier
	}

	query := &neoism.CypherQuery{
		Statement: `MERGE (n:Thing {uuid: {uuid}})
					set n={allprops}
					set n :Concept
					set n :Classification
					set n :Genre
		`,
		Parameters: map[string]interface{}{
			"uuid":     sub.UUID,
			"allprops": params,
		},
	}

	return s.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

}

func (s service) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (s:Thing {uuid: {uuid}})
			REMOVE s:Concept
			REMOVE s:Classification
			REMOVE s:Genre
			SET s={props}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
			"props": map[string]interface{}{
				"uuid": uuid,
			},
		},
		IncludeStats: true,
	}

	removeNodeIfUnused := &neoism.CypherQuery{
		Statement: `
			MATCH (s:Thing {uuid: {uuid}})
			OPTIONAL MATCH (s)-[a]-(x)
			WITH s, count(a) AS relCount
			WHERE relCount = 0
			DELETE s
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
		},
	}

	err := s.cypherRunner.CypherBatch([]*neoism.CypherQuery{clearNode, removeNodeIfUnused})

	s1, err := clearNode.Stats()
	if err != nil {
		return false, err
	}

	var deleted bool
	if s1.ContainsUpdates && s1.LabelsRemoved > 0 {
		deleted = true
	}

	return deleted, err
}

func (s service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	sub := Genre{}
	err := dec.Decode(&sub)
	return sub, sub.UUID, err
}

func (s service) Check() error {
	return neoutils.Check(s.cypherRunner)
}

func (s service) Count() (int, error) {

	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Genre) return count(n) as c`,
		Result:    &results,
	}

	err := s.cypherRunner.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}
