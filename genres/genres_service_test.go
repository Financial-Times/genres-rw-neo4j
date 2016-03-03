package genres

import (
	"os"
	"testing"

	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

var genresDriver baseftrwapp.Service

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"

	genresDriver = getGenresCypherDriver(t)

	genreToDelete := Genre{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(genreToDelete), "Failed to write genre")

	found, err := genresDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete genre for uuid %", uuid)
	assert.NoError(err, "Error deleting genre for uuid %s", uuid)

	p, found, err := genresDriver.Read(uuid)

	assert.Equal(Genre{}, p, "Found genre %s who should have been deleted", p)
	assert.False(found, "Found genre for uuid %s who should have been deleted", uuid)
	assert.NoError(err, "Error trying to find genre for uuid %s", uuid)
}

func TestCreateAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	genresDriver = getGenresCypherDriver(t)

	genreToWrite := Genre{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	readGenreForUUIDAndCheckFieldsMatch(t, uuid, genreToWrite)

	cleanUp(t, uuid)
}

func TestCreateHandlesSpecialCharacters(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	genresDriver = getGenresCypherDriver(t)

	genreToWrite := Genre{UUID: uuid, CanonicalName: "Test 'special chars", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	readGenreForUUIDAndCheckFieldsMatch(t, uuid, genreToWrite)

	cleanUp(t, uuid)
}

func TestCreateNotAllValuesPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	genresDriver = getGenresCypherDriver(t)

	genreToWrite := Genre{UUID: uuid, CanonicalName: "Test"}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	readGenreForUUIDAndCheckFieldsMatch(t, uuid, genreToWrite)

	cleanUp(t, uuid)
}

func TestUpdateWillRemovePropertiesNoLongerPresent(t *testing.T) {
	assert := assert.New(t)
	uuid := "12345"
	genresDriver = getGenresCypherDriver(t)

	genreToWrite := Genre{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")
	readGenreForUUIDAndCheckFieldsMatch(t, uuid, genreToWrite)

	updatedGenre := Genre{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(updatedGenre), "Failed to write updated genre")
	readGenreForUUIDAndCheckFieldsMatch(t, uuid, updatedGenre)

	cleanUp(t, uuid)
}

func TestConnectivityCheck(t *testing.T) {
	assert := assert.New(t)
	genresDriver = getGenresCypherDriver(t)
	err := genresDriver.Check()
	assert.NoError(err, "Unexpected error on connectivity check")
}

func getGenresCypherDriver(t *testing.T) service {
	assert := assert.New(t)
	url := os.Getenv("NEO4J_TEST_URL")
	if url == "" {
		url = "http://localhost:7474/db/data"
	}

	db, err := neoism.Connect(url)
	assert.NoError(err, "Failed to connect to Neo4j")
	return NewCypherGenresService(neoutils.StringerDb{db}, db)
}

func readGenreForUUIDAndCheckFieldsMatch(t *testing.T, uuid string, expectedgenre Genre) {
	assert := assert.New(t)
	storedGenre, found, err := genresDriver.Read(uuid)

	assert.NoError(err, "Error finding genre for uuid %s", uuid)
	assert.True(found, "Didn't find genre for uuid %s", uuid)
	assert.Equal(expectedgenre, storedGenre, "genres should be the same")
}

func TestWritePrefLabelIsAlsoWrittenAndIsEqualToName(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)
	uuid := "12345"
	genreToWrite := Genre{UUID: uuid, CanonicalName: "Test", TmeIdentifier: "TME_ID"}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	result := []struct {
		PrefLabel string `json:"t.prefLabel"`
	}{}

	getPrefLabelQuery := &neoism.CypherQuery{
		Statement: `
				MATCH (t:Genre {uuid:"12345"}) RETURN t.prefLabel
				`,
		Result: &result,
	}

	err := genresDriver.cypherRunner.CypherBatch([]*neoism.CypherQuery{getPrefLabelQuery})
	assert.NoError(err)
	assert.Equal("Test", result[0].PrefLabel, "PrefLabel should be 'Test")
	cleanUp(t, uuid)
}

func cleanUp(t *testing.T, uuid string) {
	assert := assert.New(t)
	found, err := genresDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete genre for uuid %", uuid)
	assert.NoError(err, "Error deleting genre for uuid %s", uuid)
}
