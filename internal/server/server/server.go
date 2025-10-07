package server

import (
	"fmt"
	"net/http"
	"time"

	"github.com/Miha-s/it_db_lab1/internal/server/handlers"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

type Server struct {
	router chi.Router
	port   uint
}

func NewServer(port uint, handlers handlers.HandlersFactory) *Server {
	serv := &Server{
		port: port,
	}

	serv.router = chi.NewRouter()
	serv.router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}))
	serv.router.Use(middleware.RequestID)
	serv.router.Use(middleware.RealIP)
	serv.router.Use(middleware.Logger)
	serv.router.Use(middleware.Recoverer)

	serv.router.Use(middleware.Timeout(60 * time.Second))

	serv.router.Get("/database", handlers.GetAllDb())
	serv.router.Route("/database/{db_name}", func(r chi.Router) {
		r.Get("/", handlers.GetDb())
		r.Post("/", handlers.CreateDb())
		r.Delete("/", handlers.DeleteDb())

		r.Route("/{table_name}", func(r chi.Router) {
			r.Get("/", handlers.GetTableAttributes())
			r.Post("/", handlers.CreateTable())
			r.Delete("/", handlers.DeleteTable())
			r.Patch("/rows", handlers.UpdateTable())
			r.Get("/rows", handlers.GetTableData())
			r.Post("/rows", handlers.AddRow())
			r.Get("/rows/{id}", handlers.GetRow())
			r.Delete("/rows/{id}", handlers.DeleteRow())
			r.Patch("/rows/remove_duplicates", handlers.RemoveDuplicates())
		})
	})

	return serv
}

func (serv *Server) Run() error {
	err := http.ListenAndServe(fmt.Sprintf(":%v", serv.port), serv.router)
	return err
}
