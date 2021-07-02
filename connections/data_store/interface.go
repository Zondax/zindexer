package data_store

type IDataStoreClient interface {
	GetFile(name string, location string) (*[]byte, error)
	UploadFile(name string, dest string) error
	List(dir string, prefix string) *[]string
}

type DataStoreClient struct {
	Client IDataStoreClient
}

type DataStoreConfig struct {
	Url      string
	User     string
	Password string
	Service  string
}
