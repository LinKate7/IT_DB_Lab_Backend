package database

import (
	"os"
	"testing"

	"github.com/LinKate7/it_db_lab1/internal/database/attributes"
)

func TestTable_AddRow(t *testing.T) {
	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Column1"),
		attributes.NewCharAttribute("Column2"),
		attributes.NewIntegerAttribute("Column3"),
	}

	table := NewTable(".", "test_table", attributes)

	validRow := []string{"A", "B", "123"}

	err := table.AddRow(validRow)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(table.rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(table.rows))
	}

	if table.rows[0][0] != validRow[0] || table.rows[0][1] != validRow[1] || table.rows[0][2] != validRow[2] {
		t.Fatalf("expected row %v, got %v", validRow, table.rows[0])
	}

	invalidRow := []string{"A", "B"}
	err = table.AddRow(invalidRow)
	if err == nil {
		t.Fatal("expected an error for invalid row, got none")
	}

	table.Delete()
}

func TestTable_SaveTable_Serialization(t *testing.T) {
	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Column1"),
		attributes.NewIntegerAttribute("Column2"),
	}

	table := NewTable("./", "test_serialization_table", attributes)

	rows := [][]string{
		{"A", "1"},
		{"B", "2"},
		{"C", "3"},
	}

	for _, row := range rows {
		err := table.AddRow(row)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	}

	table.Sync()

	loadedTable := LoadFromFile("./", "test_serialization_table")

	if len(loadedTable.rows) != len(rows) {
		t.Fatalf("expected %d rows, got %d", len(rows), len(loadedTable.rows))
	}

	for i, row := range loadedTable.rows {
		for j, value := range row {
			if value != rows[i][j] {
				t.Fatalf("row %d column %d mismatch: expected %s, got %s", i, j, rows[i][j], value)
			}
		}
	}

	filePath := "./test_serialization_table.csv"
	err := os.Remove(filePath)
	if err != nil {
		t.Fatalf("failed to remove file: %v", err)
	}
}

func TestTable_RemoveDuplicates(t *testing.T) {
	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Name"),
		attributes.NewIntegerAttribute("Age"),
	}

	table := NewTable("./", "test_remove_duplicates", attributes)

	row1 := []string{"Alice", "20"}
	row2 := []string{"Bob", "30"}

	duplicateRow1 := []string{"Alice", "20"}

	_ = table.AddRow(row1)
	_ = table.AddRow(row2)
	_ = table.AddRow(duplicateRow1)

	if len(table.rows) != 3 {
		t.Fatalf("expected 3 rows before removing duplicates, got %d", len(table.rows))
	}

	table.RemoveDuplicates()

	if len(table.rows) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(table.rows))
	}

	names := []string{table.rows[0][1], table.rows[1][1]}

	if !(names[0] == "Alice" && names[1] == "Bob") {
		t.Fatalf("unexpected remaining rows: %v", table.rows)
	}
	
	_ = os.Remove("./test_remove_duplicates.csv")
}

