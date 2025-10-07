package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/Miha-s/it_db_lab1/internal/database"
	"github.com/Miha-s/it_db_lab1/internal/database/attributes"
	"github.com/go-chi/chi/v5"
)

func (f *HandlersFactory) GetTableAttributes() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		attributes := table.Attributes()
		var attributes_array []map[string]string
		for _, attr := range attributes {
			attr_map := map[string]string{
				"name": attr.Name(),
				"type": attr.Type(),
			}
			attributes_array = append(attributes_array, attr_map)
		}

		data := map[string]interface{}{
			"attributes": attributes_array,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	})
}

func (f *HandlersFactory) GetTableData() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		data := map[string]interface{}{
			"rows": table.GetAllData(),
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	})
}

type RowRequestData struct {
	NewRow []string `json:"new_row"`
}

func (f *HandlersFactory) AddRow() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		var data RowRequestData
		err = json.Unmarshal(body, &data)
		if err != nil {
			log.Print(err)
			http.Error(w, "Failed to get body", http.StatusBadRequest)
			return
		}

		err = table.AddRow(data.NewRow)
		if err != nil {
			log.Print(err)
			http.Error(w, "Failed to add new row", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) GetRow() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")
		id := chi.URLParam(r, "id")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		row, err := table.GetRow(id)
		if err != nil {
			http.Error(w, "Failed to find row", http.StatusNotFound)
			return
		}

		data := map[string]interface{}{
			"row": row,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)

	})
}

func (f *HandlersFactory) DeleteRow() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")
		id := chi.URLParam(r, "id")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		err = table.DeleteRow(id)
		if err != nil {
			http.Error(w, "Failed to find row", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) CreateTable() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		var attrs []attributes.Attribute
		query := r.URL.Query()
		for param, value := range query {
			if len(value) != 1 {
				http.Error(w, fmt.Sprintf("Invalid field type %v", param), http.StatusBadRequest)
				return
			}
			new_attr, err := attributes.CreateAttribute(value[0], param)
			if err != nil {
				http.Error(w, "Failed to create attribute", http.StatusInternalServerError)
				return
			}

			attrs = append(attrs, new_attr)
		}

		err = db.CreateTable(table_name, attrs)
		if err != nil {
			http.Error(w, "Failed to create table", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) DeleteTable() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		err = db.RemoveTable(table_name)
		if err != nil {
			http.Error(w, "Failed to remove table", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) UpdateTable() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		var attrs []database.AttributeValue
		query := r.URL.Query()
		for param, value := range query {
			if len(value) != 1 {
				http.Error(w, fmt.Sprintf("Invalid field type %v", param), http.StatusBadRequest)
				return
			}

			new_attr := database.AttributeValue{
				Name:  param,
				Value: value[0],
			}

			attrs = append(attrs, new_attr)
		}

		defer r.Body.Close()
		body, _ := io.ReadAll(r.Body)

		var data RowRequestData
		err = json.Unmarshal(body, &data)
		if err != nil {
			log.Print(err)
			http.Error(w, "Failed to get body", http.StatusBadRequest)
			return
		}

		err = table.UpdateRowWithAttributes(data.NewRow, attrs)
		if err != nil {
			http.Error(w, "Failed to update row", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) RemoveDuplicates() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")
		table_name := chi.URLParam(r, "table_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		table, err := db.GetTable(table_name)
		if err != nil {
			http.Error(w, "Failed to find table", http.StatusNotFound)
			return
		}

		table.RemoveDuplicates()

		w.WriteHeader(http.StatusOK)
	})
}
