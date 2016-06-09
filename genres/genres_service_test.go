package genres

import (
	"os"
	"testing"

	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"github.com/stretchr/testify/assert"
)

const (
	genreUUID            = "12345"
	newGenreUUID         = "123456"
	tmeID                = "TME_ID"
	newTmeID             = "NEW_TME_ID"
	fsetID               = "fset_ID"
	leiCodeID            = "leiCode"
	prefLabel            = "Test"
	specialCharPrefLabel = "Test 'special chars"
)

var defaultTypes = []string{"Thing", "Concept", "Classification", "Genre"}

func TestConnectivityCheck(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)
	err := genresDriver.Check()
	assert.NoError(err, "Unexpected error on connectivity check")
}

func TestPrefLabelIsCorrectlyWritten(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	err := genresDriver.Write(genreToWrite)
	assert.NoError(err, "ERROR happened during write time")

	storedGenre, found, err := genresDriver.Read(genreUUID)
	assert.NoError(err, "ERROR happened during read time")
	assert.Equal(true, found)
	assert.NotEmpty(storedGenre)

	assert.Equal(prefLabel, storedGenre.(Genre).PrefLabel, "PrefLabel should be "+prefLabel)
	cleanUp(assert, genreUUID, genresDriver)
}

func TestPrefLabelSpecialCharactersAreHandledByCreate(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{genreUUID}}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	//check if genreToWrite is the same with the one inside the DB
	readGenreForUUIDAndCheckFieldsMatch(assert, genresDriver, genreUUID, genreToWrite)
	cleanUp(assert, genreUUID, genresDriver)
}

func TestCreateCompleteGenreWithPropsAndIdentifiers(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}, FactsetIdentifier: fsetID, LeiCode: leiCodeID}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")

	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	//check if genreToWrite is the same with the one inside the DB
	readGenreForUUIDAndCheckFieldsMatch(assert, genresDriver, genreUUID, genreToWrite)
	cleanUp(assert, genreUUID, genresDriver)
}

func TestUpdateWillRemovePropertiesAndIdentifiersNoLongerPresent(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	allAlternativeIdentifiers := alternativeIdentifiers{TME: []string{}, UUIDS: []string{genreUUID}, FactsetIdentifier: fsetID, LeiCode: leiCodeID}
	genreToWrite := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: allAlternativeIdentifiers}

	assert.NoError(genresDriver.Write(genreToWrite), "Failed to write genre")
	//add default types that will be automatically added by the writer
	genreToWrite.Types = defaultTypes
	readGenreForUUIDAndCheckFieldsMatch(assert, genresDriver, genreUUID, genreToWrite)

	tmeAlternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	updatedGenre := Genre{UUID: genreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: tmeAlternativeIdentifiers}

	assert.NoError(genresDriver.Write(updatedGenre), "Failed to write updated genre")
	//add default types that will be automatically added by the writer
	updatedGenre.Types = defaultTypes
	readGenreForUUIDAndCheckFieldsMatch(assert, genresDriver, genreUUID, updatedGenre)

	cleanUp(assert, genreUUID, genresDriver)
}

func TestDelete(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreToDelete := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(genresDriver.Write(genreToDelete), "Failed to write genre")

	found, err := genresDriver.Delete(genreUUID)
	assert.True(found, "Didn't manage to delete genre for uuid %", genreUUID)
	assert.NoError(err, "Error deleting genre for uuid %s", genreUUID)

	p, found, err := genresDriver.Read(genreUUID)

	assert.Equal(Genre{}, p, "Found genre %s who should have been deleted", p)
	assert.False(found, "Found genre for uuid %s who should have been deleted", genreUUID)
	assert.NoError(err, "Error trying to find genre for uuid %s", genreUUID)
}

func TestCount(t *testing.T) {
	assert := assert.New(t)
	genresDriver := getGenresCypherDriver(t)

	alternativeIds := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreOneToCount := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIds}

	assert.NoError(genresDriver.Write(genreOneToCount), "Failed to write genre")

	nr, err := genresDriver.Count()
	assert.Equal(1, nr, "Should be 1 genres in DB - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	newAlternativeIds := alternativeIdentifiers{TME: []string{newTmeID}, UUIDS: []string{newGenreUUID}}
	genreTwoToCount := Genre{UUID: newGenreUUID, PrefLabel: specialCharPrefLabel, AlternativeIdentifiers: newAlternativeIds}

	assert.NoError(genresDriver.Write(genreTwoToCount), "Failed to write genre")

	nr, err = genresDriver.Count()
	assert.Equal(2, nr, "Should be 2 genres in DB - count differs")
	assert.NoError(err, "An unexpected error occurred during count")

	cleanUp(assert, genreUUID, genresDriver)
	cleanUp(assert, newGenreUUID, genresDriver)
}

func readGenreForUUIDAndCheckFieldsMatch(assert *assert.Assertions, genresDriver service, uuid string, expectedGenre Genre) {

	storedGenre, found, err := genresDriver.Read(uuid)

	assert.NoError(err, "Error finding genre for uuid %s", uuid)
	assert.True(found, "Didn't find genre for uuid %s", uuid)
	assert.Equal(expectedGenre, storedGenre, "genres should be the same")
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

func cleanUp(assert *assert.Assertions, uuid string, genresDriver service) {
	found, err := genresDriver.Delete(uuid)
	assert.True(found, "Didn't manage to delete genre for uuid %", uuid)
	assert.NoError(err, "Error deleting genre for uuid %s", uuid)
}
