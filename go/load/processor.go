package load

import (
	"fmt"
	"simple_etl_aws/common/filehandler"
)

type Processor struct {
	filesHandler filehandler.Handler
	dbHandler    DBHandler
	tableMapper  *TableMapper
}

type ProcessorConfig struct {
}

func NewProcessor(fileHandler filehandler.Handler, dbHandler DBHandler, mapper *TableMapper) *Processor {
	return &Processor{
		filesHandler: fileHandler,
		dbHandler:    dbHandler,
		tableMapper:  mapper,
	}
}

func (p *Processor) processFiles() error {
	files, err := p.filesHandler.List()
	if err != nil {
		return fmt.Errorf("could list files for processing: %w", err)
	}

	for _, fileName := range files {
		table, err := p.tableMapper.GetTable(fileName)
		if err != nil {
			return fmt.Errorf("could not map file name: %s, into table: %w", fileName, err)
		}

		err = p.dbHandler.Insert(fileName, table)
		if err != nil {
			return fmt.Errorf("could not insert data into database: %w", err)
		}
	}

	return nil
}
