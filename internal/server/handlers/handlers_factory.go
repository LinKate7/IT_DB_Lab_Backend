package handlers

import "github.com/Miha-s/it_db_lab1/internal/server/controllers"

type HandlersFactory struct {
	dbc *controllers.DatabaseController
}

func NewHandlersFactory(dbc *controllers.DatabaseController) *HandlersFactory {
	return &HandlersFactory{
		dbc: dbc,
	}
}
