package db

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"postgresProject/dao"
)

const dbName = "test_db"

func GetConn() (*sql.DB, error) {
	connStr := fmt.Sprintf("dbname=%s sslmode=disable", dbName)
	return sql.Open("postgres", connStr)
}

func GetUsers(db *sql.DB) ([]dao.User, error) {
	rows, err := db.Query("select * from users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := make([]dao.User, 0)
	for rows.Next() {
		user := dao.User{}
		err := rows.Scan(&user.Id, &user.Name, &user.Age)
		if err != nil {
			fmt.Println(err)
			continue
		}
		users = append(users, user)
	}
	return users, nil
}

func AddUser(db *sql.DB, user dao.User) error {
	_, err := db.Exec("insert into users (name, age) values ($1, $2)", user.Name, user.Age)
	return err
}

func DeleteUser(db *sql.DB, userId int) error {
	_, err := db.Exec("delete from users where id = $1", userId)
	return err
}
