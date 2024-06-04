package load

import (
	"errors"
	"fmt"
	"regexp"
)

type TableMapper struct {
	tableConfig TableConfig
	fileRegex   *regexp.Regexp
}

var NoMatchingFilePattern = errors.New("file does not match pattern")
var NoMatchingTable = errors.New("no table mapping found for file prefix")

func NewTableMapper(config TableConfig) (*TableMapper, error) {
	fileRegex, err := regexp.Compile(config.FilePattern)
	if err != nil {
		return nil, fmt.Errorf("could not create regex from pattern:%w", err)
	}

	return &TableMapper{
		tableConfig: config,
		fileRegex:   fileRegex,
	}, nil
}

func (tm *TableMapper) GetTable(fileName string) (string, error) {
	matches := tm.fileRegex.FindStringSubmatch(fileName)
	if len(matches) < 2 {
		return "", NoMatchingFilePattern
	}

	prefix := matches[1]
	table, exists := tm.tableConfig.PrefixTableMap[prefix]
	if !exists {
		return "", NoMatchingTable
	}

	return table, nil
}

type TableConfig struct {
	FilePattern    string
	PrefixTableMap map[string]string
}
