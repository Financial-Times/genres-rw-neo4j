package genres

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
	"github.com/Financial-Times/annotations-rw-neo4j/annotations"
	"os"
	"testing"
	"github.com/jmcvetta/neoism"
	"encoding/json"
	"fmt"
)

const (
	genreUUID            = "a7f1c62e-1cc9-469c-9477-ce2004a01d8c"
	contentUUID	     = "f1a3cf23-e86f-439a-8e30-6864106650fb"
	newGenreUUID         = "46d8dde1-a2c9-48db-b07d-4896767fd708"
	tmeID                = "TME_ID"
	newTmeID             = "NEW_TME_ID"
	prefLabel            = "Test"
	specialCharPrefLabel = "Test 'special chars"
)

var defaultTypes = []string{"Thing", "Concept", "Classification", "Genre"}

func TestConnectivityCheck(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)
	err := genresDriver.Check()
	assert.NoError(t, err, "Unexpected error on connectivity check")
}

func TestPrefLabelIsCorrectlyWritten(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	err := genresDriver.Write(genreToWrite)
	assert.NoError(t, err, "ERROR happened during write time")

	storedGenre, found, err := genresDriver.Read(genreUUID)
	assert.NoError(t, err, "ERROR happened during read time")
	assert.Equal(t, true, found)
	assert.NotEmpty(t, storedGenre)

	assert.Equal(t, prefLabel, storedGenre.(Genre).PrefLabel, "PrefLabel should be "+prefLabel)
	cleanUp(t, genreUUID, genresDriver)
}

func TestPrefLabelSpecialCharactersAreHandledByCreate(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, genresDriver.Write(genreToWrite), "Failed to write genre")

	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	//check if genreToWrite is the same with the one inside the DB
	readGenreForUUIDAndCheckFieldsMatch(t, genresDriver, genreUUID, genreToWrite)
	cleanUp(t, genreUUID, genresDriver)
}

func TestCreateCompleteGenreWithPropsAndIdentifiers(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, genresDriver.Write(genreToWrite), "Failed to write genre")

	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	//check if genreToWrite is the same with the one inside the DB
	readGenreForUUIDAndCheckFieldsMatch(t, genresDriver, genreUUID, genreToWrite)
	cleanUp(t, genreUUID, genresDriver)
}

func TestUpdateWillRemovePropertiesAndIdentifiersNoLongerPresent(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	allAlternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: allAlternativeIdentifiers}

	assert.NoError(t, genresDriver.Write(genreToWrite), "Failed to write genre")
	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	readGenreForUUIDAndCheckFieldsMatch(t, genresDriver, genreUUID, genreToWrite)

	tmeAlternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	updatedGenre := Genre{UUID: genreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: tmeAlternativeIdentifiers}

	assert.NoError(t, genresDriver.Write(updatedGenre), "Failed to write updated genre")
	//add default types that will be automatically added by the writer
	updatedGenre.Types = defaultTypes
	readGenreForUUIDAndCheckFieldsMatch(t, genresDriver, genreUUID, updatedGenre)

	cleanUp(t, genreUUID, genresDriver)
}

func TestDeleteWithNoExternalRelationships(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreToDelete := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}
	defer cleanThingsFromDB(genresDriver, assert)

	assert.Nil(genresDriver.Write(genreToDelete))

	found, err := genresDriver.Delete(genreUUID)
	assert.True(found, "Didn't manage to delete genre for uuid %", genreUUID)
	assert.NoError(err, "Error deleting genre for uuid %s", genreUUID)

	p, found, err := genresDriver.Read(genreUUID)

	assert.Equal(Genre{}, p, "Found genre %s who should have been deleted", p)
	assert.False(found, "Found genre for uuid %s who should have been deleted", genreUUID)
	assert.NoError(err, "Error trying to find genre for uuid %s", genreUUID)
	assert.Equal(false, doesThingExistAtAll(genreUUID, genresDriver, assert), "Found thing which should have been deleted with uuid %v", genreUUID)
}

func TestDeleteMaintainsNodeIdenfifiersAndExternalRelationshipsIfTheyExist(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreToDelete := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}
	defer cleanThingsFromDB(genresDriver, assert)

	assert.Nil(genresDriver.Write(genreToDelete))
	annotationsRW := annotations.NewCypherAnnotationsService(genresDriver.conn, "v2")
	writeJSONToService(annotationsRW, "./test-resources/singleAnnotationForGenre.json", contentUUID, assert)

	found, err := genresDriver.Delete(genreUUID)
	assert.True(found, "Didn't manage to delete genre for uuid %", genreUUID)
	assert.NoError(err, "Error deleting genre for uuid %s", genreUUID)

	p, found, err := genresDriver.Read(genreUUID)

	assert.Equal(Genre{}, p, "Found genre %s who should have been deleted", p)
	assert.False(found, "Found genre for uuid %s who should have been turned into a thing", genreUUID)
	assert.NoError(err, "Error trying to find genre for uuid %s", genreUUID)
	assert.Equal(true, doesThingExistWithIdentifiers(genreUUID, genresDriver, assert), "Found thing which should have been deleted with uuid %v", genreUUID)
}

func TestCount(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	alternativeIds := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreOneToCount := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIds}

	assert.NoError(t, genresDriver.Write(genreOneToCount), "Failed to write genre")

	nr, err := genresDriver.Count()
	assert.Equal(t, 1, nr, "Should be 1 genres in DB - count differs")
	assert.NoError(t, err, "An unexpected error occurred during count")

	newAlternativeIds := alternativeIdentifiers{TME: []string{newTmeID}, UUIDS: []string{newGenreUUID}}
	genreTwoToCount := Genre{UUID: newGenreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: newAlternativeIds}

	assert.NoError(t, genresDriver.Write(genreTwoToCount), "Failed to write genre")

	nr, err = genresDriver.Count()
	assert.Equal(t, 2, nr, "Should be 2 genres in DB - count differs")
	assert.NoError(t, err, "An unexpected error occurred during count")

	cleanUp(t, genreUUID, genresDriver)
	cleanUp(t, newGenreUUID, genresDriver)
}

func readGenreForUUIDAndCheckFieldsMatch(t *testing.T, genresDriver service, uuid string, expectedGenre Genre) {

	storedGenre, found, err := genresDriver.Read(uuid)

	assert.NoError(t, err, "Error finding genre for uuid %s", uuid)
	assert.True(t, found, "Didn't find genre for uuid %s", uuid)
	assert.Equal(t, expectedGenre, storedGenre, "genres should be the same")
}

func getGenresCypherDriver(t *testing.T) service {
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	conf := neoutils.DefaultConnectionConfig()
	conf.Transactional = false
	db, err := neoutils.Connect(url, conf)
	assert.NoError(t, err, "Failed to connect to Neo4j")
	service := NewCypherGenresService(db)
	service.Initialise()
	return service
}

func cleanUp(t *testing.T, uuid string, genresDriver service) {
	found, err := genresDriver.Delete(uuid)
	assert.True(t, found, "Didn't manage to delete genre for uuid %", uuid)
	assert.NoError(t, err, "Error deleting genre for uuid %s", uuid)
}

func writeJSONToService(service annotations.Service, pathToJSONFile string, contentUUID string, assert *assert.Assertions) {
	f, err := os.Open(pathToJSONFile)
	assert.NoError(err)
	dec := json.NewDecoder(f)
	annotation, errr := service.DecodeJSON(dec)
	assert.NoError(errr)
	errrr := service.Write(contentUUID, annotation)
	assert.NoError(errrr)
}

func cleanThingsFromDB(service service, assert *assert.Assertions) {
	qs := []*neoism.CypherQuery{}

	qs = append(qs, &neoism.CypherQuery{Statement: fmt.Sprintf("MATCH (content:Thing {uuid: '%v'}) DETACH DELETE content", contentUUID)})
	qs = append(qs, &neoism.CypherQuery{Statement: fmt.Sprintf("MATCH (t:Thing {uuid: '%v'})<-[:IDENTIFIES*0..]-(i:Identifier) DETACH DELETE t, i", genreUUID)})

	err := service.conn.CypherBatch(qs)
	assert.NoError(err)
}

func doesThingExistAtAll(uuid string, service service, assert *assert.Assertions) bool {
	result := []struct {
		Uuid string `json:"thing.uuid"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: "%v"}) return a.uuid
		`,
		Parameters: neoism.Props{
			"uuid": uuid,
		},
		Result: &result,
	}

	err := service.conn.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.Empty(result)

	if len(result) == 0 {
		return false
	}

	return true
}

func doesThingExistWithIdentifiers(uuid string, service service, assert *assert.Assertions) bool {

	result := []struct {
		UUID string `json:"UUID"`
	}{}

	checkGraph := neoism.CypherQuery{
		Statement: `
			MATCH (a:Thing {uuid: {Uuid}})-[:IDENTIFIES]-(:Identifier)
			RETURN distinct a.uuid as UUID
		`,
		Parameters: neoism.Props{
			"Uuid": uuid,
		},
		Result: &result,
	}

	err := service.conn.CypherBatch([]*neoism.CypherQuery{&checkGraph})
	assert.NoError(err)
	assert.NotEmpty(result)

	if len(result) == 0 {
		return false
	}

	return true
}
