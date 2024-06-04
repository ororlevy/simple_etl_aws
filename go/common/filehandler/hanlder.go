package filehandler

type Handler interface {
	Write(data []byte, fileName string) error
	Read(fileName string) ([]byte, error)
	List() ([]string, error)
}
