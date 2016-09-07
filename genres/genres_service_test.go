package genres

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	genreUUID            = "12345"
	newGenreUUID         = "123456"
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

func TestDelete(t *testing.T) {

	genresDriver := getGenresCypherDriver(t)

	alternativeIdentifiers := alternativeIdentifiers{TME: []string{tmeID}, UUIDS: []string{genreUUID}}
	genreToDelete := Genre{UUID: genreUUID, PrefLabel: prefLabel, AlternativeIdentifiers: alternativeIdentifiers}

	assert.NoError(t, genresDriver.Write(genreToDelete), "Failed to write genre")

	found, err := genresDriver.Delete(genreUUID)
	assert.True(t, found, "Didn't manage to delete genre for uuid %", genreUUID)
	assert.NoError(t, err, "Error deleting genre for uuid %s", genreUUID)

	p, found, err := genresDriver.Read(genreUUID)

	assert.Equal(t, Genre{}, p, "Found genre %s who should have been deleted", p)
	assert.False(t, found, "Found genre for uuid %s who should have been deleted", genreUUID)
	assert.NoError(t, err, "Error trying to find genre for uuid %s", genreUUID)
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
