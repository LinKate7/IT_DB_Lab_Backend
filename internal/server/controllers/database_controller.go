package controllers

import (
	"fmt"
	"log"

	"github.com/Miha-s/it_db_lab1/internal/database"
)

type DatabaseController struct {
	storage_path string
	databases    map[string]*database.Database
}

func NewDatabaseController(storage_path string) (*DatabaseController, error) {
	db_controller := &DatabaseController{
		storage_path: storage_path,
		databases:    make(map[string]*database.Database),
	}

	databases, err := database.LoadDatabases(storage_path)
	if err != nil {
		return nil, err
	}

	for _, db := range databases {
		db_controller.databases[db.Name()] = db
	}

	return db_controller, nil
}

func (dbc *DatabaseController) CreateDatabase(name string) error {
	_, exists := dbc.databases[name]
	if exists {
		return fmt.Errorf("database with name %v already exists", name)
	}

	var err error
	dbc.databases[name], err = database.NewDatabase(dbc.storage_path, name)
	if err != nil {
		log.Print(err)
		return err
	}

	return nil
}

func (dbc *DatabaseController) DeleteDatabase(name string) error {
	db, exists := dbc.databases[name]
	if !exists {
		return fmt.Errorf("database does not exist %v", name)
	}

	db.Delete()
	delete(dbc.databases, name)

	return nil
}

func (dbc *DatabaseController) GetDatabase(name string) (*database.Database, error) {
	db, exists := dbc.databases[name]
	if !exists {
		return nil, fmt.Errorf("database does not exist %v", name)
	}

	return db, nil
}

func (dbc *DatabaseController) GetAllDatabasesNames() []string {
	keys := make([]string, 0, len(dbc.databases))

	for key := range dbc.databases {
		keys = append(keys, key)
	}

	return keys
}
