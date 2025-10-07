package database

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/Miha-s/it_db_lab1/internal/database/attributes"
)

type AttributeValue struct {
	Name  string
	Value string
}

type AcceptRow func([]string) bool

type Table struct {
	storage_path string
	name         string
	attributes   []attributes.Attribute
	rows         [][]string
	last_id      uint
}

func NewTable(storage_path string, name string, attrs []attributes.Attribute) *Table {
	id_attr, _ := attributes.CreateAttribute("integer", "id")
	attrs = append([]attributes.Attribute{id_attr}, attrs...)
	table := &Table{
		storage_path: storage_path,
		name:         name,
		attributes:   attrs,
		last_id:      1,
	}

	table.saveTable()
	return table
}

func LoadFromFile(storage_path string, name string) *Table {
	filePath := storage_path + "/" + name + ".csv"
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}

	if len(records) < 2 {
		panic("CSV file does not contain enough rows to load table")
	}

	dataTypes := records[0]
	columnNames := records[1]

	attrs := make([]attributes.Attribute, len(columnNames))
	for i, name := range columnNames {
		attrs[i], err = attributes.CreateAttribute(dataTypes[i], name)
		if err != nil {
			return nil
		}
	}

	t := &Table{
		storage_path: storage_path,
		name:         name,
		attributes:   attrs,
	}
	t.rows = records[2:]

	return t
}

func (t *Table) Name() string {
	return t.name
}

func (t *Table) Attributes() []attributes.Attribute {
	return t.attributes
}

func (t *Table) GetAllData() [][]string {
	return t.rows
}

func (t *Table) GetRow(id string) ([]string, error) {
	for _, row := range t.rows {
		if row[0] == id {
			return row, nil
		}
	}

	return nil, fmt.Errorf("failed to find row with id %v", id)
}

func (t *Table) DeleteRow(id string) error {
	for index, row := range t.rows {
		if id == row[0] {
			t.rows = append(t.rows[:index], t.rows[index+1:]...)
			t.Sync()
			return nil
		}
	}
	return fmt.Errorf("failed to fin drow with id %v", id)
}

func (t *Table) AddRow(row []string) error {
	row = append([]string{strconv.Itoa(int(t.last_id))}, row...)
	if len(row) != len(t.attributes) {
		return errors.New("invalid number of arguments")
	}

	if err := t.validateRow(row); err != nil {
		return err
	}

	t.rows = append(t.rows, row)
	t.last_id++
	t.Sync()

	return nil
}

func (t *Table) UpdateRowWithAttributes(row []string, attrs []AttributeValue) error {
	var attrs_indexes []int
	for _, attr := range attrs {
		found := false
		for index, attr_type := range t.attributes {
			if attr_type.Name() == attr.Name {
				err := attr_type.Validate(attr.Value)
				if err != nil {
					return fmt.Errorf("incorrect value for attribute %v", attr)
				}
				attrs_indexes = append(attrs_indexes, index)
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("failed to find attribute %v", attr.Name)
		}
	}

	return t.UpdateRow(row, func(row []string) bool {
		for index, value := range attrs {
			if row[attrs_indexes[index]] != value.Value {
				return false
			}
		}
		return true
	})
}

func (t *Table) UpdateRow(row []string, row_to_update AcceptRow) error {
	if len(row) != len(t.attributes) {
		return errors.New("invalid number of arguments")
	}

	if err := t.validateRow(row); err != nil {
		return err
	}

	for index, value := range t.rows {
		if row_to_update(value) {
			t.rows[index] = row
		}
	}

	t.Sync()

	return nil
}

func (t *Table) Sync() {
	t.saveTable()
}

func (t *Table) Delete() error {
	t.rows = [][]string{}

	filePath := t.storage_path + "/" + t.name + ".csv"
	err := os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}

func (t *Table) RemoveDuplicates() {
	rowMap := make(map[string]bool)
	var uniqueRows [][]string

	for _, row := range t.rows {
		rowStr := fmt.Sprintf("%v", row[1:])

		if !rowMap[rowStr] {
			uniqueRows = append(uniqueRows, row)
			rowMap[rowStr] = true
		}
	}

	t.rows = uniqueRows
	t.Sync()
}

func (t *Table) validateRow(row []string) error {
	for index, value := range row {
		err := t.attributes[index].Validate(value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *Table) saveTable() {
	filePath := t.storage_path + "/" + t.name + ".csv"
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	dataTypes := make([]string, len(t.attributes))
	for i, attr := range t.attributes {
		dataTypes[i] = attr.Type()
	}
	if err := writer.Write(dataTypes); err != nil {
		panic(err)
	}

	columnNames := make([]string, len(t.attributes))
	for i, attr := range t.attributes {
		columnNames[i] = attr.Name()
	}
	if err := writer.Write(columnNames); err != nil {
		panic(err)
	}

	for _, row := range t.rows {
		if err := writer.Write(row); err != nil {
			panic(err)
		}
	}
}
