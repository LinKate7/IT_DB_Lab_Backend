package main

import (
	"github.com/Miha-s/it_db_lab1/internal/server/controllers"
	"github.com/Miha-s/it_db_lab1/internal/server/handlers"
	"github.com/Miha-s/it_db_lab1/internal/server/server"
)

func main() {
	db_controller, err := controllers.NewDatabaseController("./storage")
	if err != nil {
		panic(err)
	}
	handlers_factory := handlers.NewHandlersFactory(db_controller)
	router := server.NewServer(7777, *handlers_factory)

	router.Run()
}
