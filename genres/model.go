package genres

type Genre struct {
	UUID                   string                 `json:"uuid"`
	PrefLabel              string                 `json:"prefLabel"`
	AlternativeIdentifiers alternativeIdentifiers `json:"alternativeIdentifiers"`
	Types                  []string               `json:"types,omitempty"`
}

type alternativeIdentifiers struct {
	TME               []string `json:"TME,omitempty"`
	FactsetIdentifier string   `json:"factsetIdentifier,omitempty"`
	LeiCode           string   `json:"leiCode,omitempty"`
	UUIDS             []string `json:"uuids"`
}

const (
	factsetIdentifierLabel = "FactsetIdentifier"
	leiIdentifierLabel     = "LegalEntityIdentifier"
	tmeIdentifierLabel     = "TMEIdentifier"
	uppIdentifierLabel     = "UPPIdentifier"
)

type GenreLink struct {
	ApiUrl string `json:"apiUrl"`
}
