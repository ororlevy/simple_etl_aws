package extract

import (
	"os"
	"path/filepath"
)

type FileWriter interface {
	Write(data []byte, fileName string) error
}

type FileSystemWriter struct {
	filePath string
}

func NewFileSystemWriter(filePath string) *FileSystemWriter {
	return &FileSystemWriter{filePath: filePath}
}

func (fsw FileSystemWriter) Write(data []byte, fileName string) error {
	file, err := os.Create(filepath.Join(fsw.filePath, fileName))
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	return nil
}
