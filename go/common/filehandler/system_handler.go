package filehandler

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FileSystemHandler struct {
	filePath string
}

var _ Handler = FileSystemHandler{}

func NewFileSystemHandler(filePath string) *FileSystemHandler {
	return &FileSystemHandler{filePath: filePath}
}

func (fsh FileSystemHandler) Write(data []byte, fileName string) error {
	file, err := os.Create(filepath.Join(fsh.filePath, fileName))
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

func (fsh FileSystemHandler) Read(fileName string) ([]byte, error) {
	file, err := os.Open(filepath.Join(fsh.filePath, fileName))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return io.ReadAll(file)
}

func (fsh FileSystemHandler) List() ([]string, error) {
	entries, err := os.ReadDir(fsh.filePath)
	if err != nil {
		return nil, fmt.Errorf("can't list files: %w", err)
	}

	files := make([]string, 0)
	for _, entry := range entries {
		files = append(files, entry.Name())
	}

	return files, nil
}

func (fsh FileSystemHandler) Delete(fileName string) error {
	err := os.Remove(filepath.Join(fsh.filePath, fileName))
	if err != nil {
		return fmt.Errorf("could not remove file: %w", err)
	}

	return nil
}
