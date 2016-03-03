package genres

type Genre struct {
	UUID          string `json:"uuid"`
	CanonicalName string `json:"canonicalName"`
	TmeIdentifier string `json:"tmeIdentifier,omitempty"`
	Type          string `json:"type,omitempty"`
}

type GenreLink struct {
	ApiUrl string `json:"apiUrl"`
}
