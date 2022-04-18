package db

import (
	"github.com/jackc/pgx"
	_ "github.com/jackc/pgx"
	"log"
	"os"
	"strconv"
)

const (
	defaultDbHost = "localhost"
	defaultDbPort = 5432
	defaultDbUser = "postgres"
	defaultDbName = "test_db"

	dbHostKey = "DB_HOST"
	dbPortKey = "DB_PORT"
	dbUserKey = "DB_USER"
	dbNameKey = "DB_NAME"
)

func InitDb() (*pgx.ConnPool, error) {
	pool, err := pgx.NewConnPool(getConnPoolConfig())
	if err != nil {
		log.Printf("Error during connect to the db: %s", err)
		return nil, err
	}
	return pool, nil
}

func getConnPoolConfig() pgx.ConnPoolConfig {
	connConfig := getConnConfig()

	return pgx.ConnPoolConfig{
		ConnConfig:     connConfig,
		MaxConnections: 5,
		AfterConnect:   prepareStatements,
	}
}

func getConnConfig() pgx.ConnConfig {
	dbHost := defaultDbHost
	dbPort := defaultDbPort
	dbUser := defaultDbUser
	dbName := defaultDbName

	if host, exists := os.LookupEnv(dbHostKey); exists {
		dbHost = host
	}
	if port, exists := os.LookupEnv(dbPortKey); exists {
		intPort, err := strconv.Atoi(port)
		if err != nil {
			log.Printf("Environment contains incorrect port format: %s\n", port)
			log.Printf("Using default port: %v\n", dbPort)
		} else {
			dbPort = intPort
		}
	}
	if user, exists := os.LookupEnv(dbUserKey); exists {
		dbUser = user
	}
	if name, exists := os.LookupEnv(dbNameKey); exists {
		dbName = name
	}

	return pgx.ConnConfig{
		Host:     dbHost,
		Port:     uint16(dbPort),
		User:     dbUser,
		Database: dbName,
	}
}

func prepareStatements(conn *pgx.Conn) error {
	_, err := conn.Prepare("getUser", "SELECT * FROM users WHERE id = $1")
	if err != nil {
		log.Printf("Error during preparing statement for the 'getUser' request: %s\n", err)
		return err
	}

	_, err = conn.Prepare("addUser", "INSERT INTO users (name, age) VALUES ($1, $2)")
	if err != nil {
		log.Printf("Error during preparing statement for the 'addUser' request: %s\n", err)
		return err
	}

	_, err = conn.Prepare("deleteUser", "DELETE FROM users WHERE id = $1")
	if err != nil {
		log.Printf("Error during preparing statement for the 'deleteUser' request: %s\n", err)
		return err
	}

	_, err = conn.Prepare("updateUser", "UPDATE users SET name = $1, age = $2 WHERE id = $3")
	if err != nil {
		log.Printf("Error during preparing statement for the 'updateUser' request: %s\n", err)
		return err
	}
	return nil
}
