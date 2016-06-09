package genres

import (
	"encoding/json"
	"fmt"
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
		"Genre": "uuid",
		"FactsetIdentifier": "value",
		"TMEIdentifier":     "value",
		"UPPIdentifier":     "value"})
}

func (s service) Read(uuid string) (interface{}, bool, error) {
	results := []Genre{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (n:Genre {uuid:{uuid}})
OPTIONAL MATCH (upp:UPPIdentifier)-[:IDENTIFIES]->(n)
OPTIONAL MATCH (fs:FactsetIdentifier)-[:IDENTIFIES]->(n)
OPTIONAL MATCH (tme:TMEIdentifier)-[:IDENTIFIES]->(n)
OPTIONAL MATCH (lei:LegalEntityIdentifier)-[:IDENTIFIES]->(n)
return distinct n.uuid as uuid, n.prefLabel as prefLabel, labels(n) as types, {uuids:collect(distinct upp.value), TME:collect(distinct tme.value), factsetIdentifier:fs.value, leiCode:lei.value} as alternativeIdentifiers`,
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

	genre := thing.(Genre)

	//cleanUP all the previous IDENTIFIERS referring to that uuid
	deletePreviousIdentifiersQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}})
		OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i)
		DELETE iden, i`,
		Parameters: map[string]interface{}{
			"uuid": genre.UUID,
		},
	}

	//create-update node for TOPIC
	createTopicQuery := &neoism.CypherQuery{
		Statement: `MERGE (n:Thing {uuid: {uuid}})
					set n={allprops}
					set n :Concept
					set n :Classification
					set n :Genre
		`,
		Parameters: map[string]interface{}{
			"uuid": genre.UUID,
			"allprops": map[string]interface{}{
				"uuid":      genre.UUID,
				"prefLabel": genre.PrefLabel,
			},
		},
	}

	queryBatch := []*neoism.CypherQuery{deletePreviousIdentifiersQuery, createTopicQuery}

	//ADD all the IDENTIFIER nodes and IDENTIFIES relationships
	if genre.AlternativeIdentifiers.FactsetIdentifier != "" {
		factsetIdentifierQuery := createNewIdentifierQuery(genre.UUID, factsetIdentifierLabel, genre.AlternativeIdentifiers.FactsetIdentifier)
		queryBatch = append(queryBatch, factsetIdentifierQuery)
	}

	if genre.AlternativeIdentifiers.LeiCode != "" {
		leiCodeIdentifierQuery := createNewIdentifierQuery(genre.UUID, leiIdentifierLabel, genre.AlternativeIdentifiers.LeiCode)
		queryBatch = append(queryBatch, leiCodeIdentifierQuery)
	}

	for _, alternativeUUID := range genre.AlternativeIdentifiers.TME {
		alternativeIdentifierQuery := createNewIdentifierQuery(genre.UUID, tmeIdentifierLabel, alternativeUUID)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)
	}

	for _, alternativeUUID := range genre.AlternativeIdentifiers.UUIDS {
		alternativeIdentifierQuery := createNewIdentifierQuery(genre.UUID, uppIdentifierLabel, alternativeUUID)
		queryBatch = append(queryBatch, alternativeIdentifierQuery)
	}
	return s.cypherRunner.CypherBatch(queryBatch)

}

func createNewIdentifierQuery(uuid string, identifierLabel string, identifierValue string) *neoism.CypherQuery {
	statementTemplate := fmt.Sprintf(`MERGE (t:Thing {uuid:{uuid}})
					CREATE (i:Identifier {value:{value}})
					MERGE (t)<-[:IDENTIFIES]-(i)
					set i : %s `, identifierLabel)
	query := &neoism.CypherQuery{
		Statement: statementTemplate,
		Parameters: map[string]interface{}{
			"uuid":  uuid,
			"value": identifierValue,
		},
	}
	return query
}

func (s service) Delete(uuid string) (bool, error) {
	clearNode := &neoism.CypherQuery{
		Statement: `
			MATCH (s:Thing {uuid: {uuid}})
			OPTIONAL MATCH (t)<-[iden:IDENTIFIES]-(i:Identifier)
			REMOVE s:Concept
			REMOVE s:Classification
			REMOVE s:Genre
			DELETE iden, i
			SET s={uuid:{uuid}}
		`,
		Parameters: map[string]interface{}{
			"uuid": uuid,
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
