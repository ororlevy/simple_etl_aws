package load

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestTableMapper(t *testing.T) {
	config := TableConfig{
		FilePattern: "([a-zA-Z_]+)_\\d+\\.parquet",
		PrefixTableMap: map[string]string{
			"users_data":   "users_table",
			"company_data": "company_table",
		},
	}
	mapper, err := NewTableMapper(config)
	assert.Equal(t, err, nil)

	testCases := []struct {
		name          string
		fileName      string
		expectedError error
		expectedTable string
	}{
		{
			name:          "should fail for file that not follow the pattern",
			fileName:      "users.data",
			expectedError: NoMatchingFilePattern,
			expectedTable: "",
		},
		{
			name:          "should fail for file that not exist in the map",
			fileName:      "product_data_300250.parquet",
			expectedError: NoMatchingTable,
			expectedTable: "",
		},
		{
			name:          "should work for correct file name that exist in the map",
			fileName:      "users_data_300250.parquet",
			expectedError: nil,
			expectedTable: "users_table",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			table, err := mapper.GetTable(testCase.fileName)
			if testCase.expectedError != nil {
				assert.Equal(t, err, testCase.expectedError)
			}

			assert.Equal(t, table, testCase.expectedTable)
		})
	}
}
