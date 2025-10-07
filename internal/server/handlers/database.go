package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func (f *HandlersFactory) GetAllDb() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dbs := f.dbc.GetAllDatabasesNames()

		result := map[string]interface{}{
			"databases": dbs,
		}

		jsonData, err := json.Marshal(result)
		if err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err = w.Write(jsonData)
		if err != nil {
			http.Error(w, "Failed to write response", http.StatusInternalServerError)
			return
		}
	})
}

func (f *HandlersFactory) CreateDb() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")

		err := f.dbc.CreateDatabase(db_name)
		if err != nil {
			http.Error(w, "Database already exists", http.StatusConflict)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) DeleteDb() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")

		err := f.dbc.DeleteDatabase(db_name)
		if err != nil {
			http.Error(w, "Databaes does not exists", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

func (f *HandlersFactory) GetDb() http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		db_name := chi.URLParam(r, "db_name")

		db, err := f.dbc.GetDatabase(db_name)
		if err != nil {
			http.Error(w, "Database not found", http.StatusNotFound)
			return
		}

		tables := db.GetAllTablesNames()

		result := map[string]interface{}{
			"tables": tables,
		}

		jsonData, err := json.Marshal(result)
		if err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err = w.Write(jsonData)
		if err != nil {
			http.Error(w, "Failed to write response", http.StatusInternalServerError)
			return
		}
	})
}
