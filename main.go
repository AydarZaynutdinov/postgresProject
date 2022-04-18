package main

import (
	"fmt"
	mux "github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"postgresProject/db"
	"postgresProject/db/repository"
	"postgresProject/service"
	"strconv"
)

const (
	defaultHost = "localhost"
	defaultPort = 8080

	hostKey = "HOST"
	portKey = "PORT"
)

func main() {
	pool, err := db.InitDb()
	if err != nil {
		log.Printf("Error during init db: %s", err)
		os.Exit(-1)
	}
	defer pool.Close()

	userService := &service.UserService{
		Repository: &repository.UserRepository{
			Pool: pool,
		},
	}

	r := mux.NewRouter()
	r.HandleFunc("/users/{id:[0-9]+}", userService.GetUser).Methods(http.MethodGet)
	r.HandleFunc("/users", userService.GetUsers).Methods(http.MethodGet)
	r.HandleFunc("/users", userService.AddUser).Methods(http.MethodPost)
	r.HandleFunc("/users/{id:[0-9]+}", userService.UpdateUser).Methods(http.MethodPut)
	r.HandleFunc("/users/{id:[0-9]+}", userService.DeleteUser).Methods(http.MethodDelete)
	r.HandleFunc("/users", userService.DeleteUsers).Methods(http.MethodDelete)

	addr := getAddr()
	if err = http.ListenAndServe(addr, r); err != nil {
		log.Printf("Error during server working: %s", err)
		os.Exit(-1)
	}
}

func getAddr() string {
	port := defaultPort
	if v, exists := os.LookupEnv(portKey); exists {
		intV, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("Environment contains incorrect port format: %s\n", v)
			log.Printf("Using default port: %v\n", port)
		} else {
			port = intV
		}
	}
	host := defaultHost
	if v, exists := os.LookupEnv(hostKey); exists {
		host = v
	}
	return fmt.Sprintf("%s:%v", host, port)
}
