package load

type DBHandler interface {
	Insert(fileName string, table string) error
}
