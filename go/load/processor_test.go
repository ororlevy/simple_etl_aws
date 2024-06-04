package load

import (
	"fmt"
	"github.com/magiconair/properties/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
	"os"
	"path/filepath"
	"simple_etl_aws/common/filehandler"
	"strconv"
	"testing"
)

func TestProcessor(t *testing.T) {
	users := []User{
		{
			Name: "one",
			Id:   1,
		},
		{
			Name: "two",
			Id:   2,
		},
	}

	path, filesCleanup := setupTestFiles(t, users)
	defer filesCleanup()

	dbHandler := sqlSimpleHandler{
		db:   make(map[string]User),
		path: path,
	}

	fileHandler := filehandler.NewFileSystemHandler(path)

	tableConfig := TableConfig{
		FilePattern: "([a-zA-Z_]+)_\\d+\\.parquet",
		PrefixTableMap: map[string]string{
			"users_data":   "users_table",
			"company_data": "company_table",
		},
	}

	tableMapper, err := NewTableMapper(tableConfig)
	if err != nil {
		t.Fatalf("could not create table mapper: %v", err)
	}

	processor := NewProcessor(fileHandler, &dbHandler, tableMapper)

	err = processor.processFiles()
	if err != nil {
		t.Fatalf("could not process files: %v", err)
	}

	count := dbHandler.countUsers()

	assert.Equal(t, count, 2)
}

func setupTestFiles(t *testing.T, users []User) (string, func()) {
	tempDir, err := os.MkdirTemp("", "testfiles_")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	userFileName := "users_data_300250.parquet"

	fw, err := local.NewLocalFileWriter(filepath.Join(tempDir, userFileName))
	if err != nil {
		t.Fatalf("Can't create local file %v", err)
	}

	defer fw.Close()

	pw, err := writer.NewParquetWriter(fw, new(User), 4)
	if err != nil {
		t.Fatalf("Can't create parquet writer: %v", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.PageSize = 8 * 1024              //8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, user := range users {
		if err = pw.Write(user); err != nil {
			t.Fatalf("Write error: %v", err)
		}
	}

	pw.WriteStop()

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return tempDir, cleanup
}

type User struct {
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Id   int64  `parquet:"name=id, type=INT64"`
}

type sqlSimpleHandler struct {
	db   map[string]User
	path string
}

func (sh *sqlSimpleHandler) countUsers() int {
	return len(sh.db)
}

func (sh *sqlSimpleHandler) Insert(fileName string, _ string) error {
	fullPath := filepath.Join(sh.path, fileName)

	// Check if the file exists and is accessible
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s %w", fullPath, err)
	} else if err != nil {
		return fmt.Errorf("error accessing file: %s, error: %v", fullPath, err)
	}

	fr, err := local.NewLocalFileReader(fullPath)
	if err != nil {
		return fmt.Errorf("could not read file, %w", err)
	}

	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(User), 4)
	if err != nil {
		return fmt.Errorf("could not read parquet file: %w", err)
	}

	users := make([]User, pr.GetNumRows())

	err = pr.Read(&users)
	if err != nil {
		return fmt.Errorf("could read parquet data:%w", err)
	}

	pr.ReadStop()

	for _, user := range users {
		sh.db[strconv.FormatInt(user.Id, 10)] = user
	}

	return nil
}
