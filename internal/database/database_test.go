package database

import (
	"os"
	"testing"

	"github.com/Miha-s/it_db_lab1/internal/database/attributes"
)

func TestDatabase_CreateTable(t *testing.T) {
	db, err := NewDatabase(".", "test_db")
	if err != nil {
		t.Fatalf("expected no error creating database, got %v", err)
	}

	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Column1"),
		attributes.NewIntegerAttribute("Column2"),
	}

	err = db.CreateTable("test_table", attributes)
	if err != nil {
		t.Fatalf("expected no error creating table, got %v", err)
	}

	filePath := "./test_db/test_table.csv"
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected file %s to exist, but it does not", filePath)
	}

	defer os.RemoveAll("./test_db")
}

func TestDatabase_RemoveTable_StorageRemoval(t *testing.T) {
	db, err := NewDatabase(".", "test_db")
	if err != nil {
		t.Fatalf("expected no error creating database, got %v", err)
	}

	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Column1"),
		attributes.NewIntegerAttribute("Column2"),
	}

	err = db.CreateTable("test_table", attributes)
	if err != nil {
		t.Fatalf("expected no error creating table, got %v", err)
	}

	filePath := "./test_db/test_table.csv"
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("expected file %s to exist, but it does not", filePath)
	}

	err = db.RemoveTable("test_table")
	if err != nil {
		t.Fatalf("expected no error removing table, got %v", err)
	}

	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("expected file %s to be removed, but it still exists", filePath)
	}

	defer os.RemoveAll("./test_db")
}

func TestDatabase_RemoveTable_NotFound(t *testing.T) {
	db, err := NewDatabase(".", "test_db")
	if err != nil {
		t.Fatalf("expected no error creating database, got %v", err)
	}

	err = db.RemoveTable("non_existing_table")
	if err == nil {
		t.Fatal("expected an error removing a non-existing table, got nil")
	}
}

func TestDatabase_GetTable(t *testing.T) {
	db, err := NewDatabase(".", "test_db")
	if err != nil {
		t.Fatalf("expected no error creating database, got %v", err)
	}

	attributes := []attributes.Attribute{
		attributes.NewCharAttribute("Column1"),
		attributes.NewIntegerAttribute("Column2"),
	}

	err = db.CreateTable("test_table", attributes)
	if err != nil {
		t.Fatalf("expected no error creating table, got %v", err)
	}

	_, err = db.GetTable("test_table")
	if err != nil {
		t.Fatal("expected to retrieve existing table, got nil")
	}

	defer os.RemoveAll("./test_db")
}
