package main

import (
	"fmt"
	"postgresProject/dao"
	"postgresProject/db"
)

func main() {
	conn, err := db.GetConn()
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Users before:")
	// get users
	users, err := db.GetUsers(conn)
	if err != nil {
		panic(err)
	}
	fmt.Println(users)

	// add user
	user := dao.User{
		Name: "TEST",
		Age:  14,
	}
	err = db.AddUser(conn, user)
	if err != nil {
		panic(err)
	}

	fmt.Println("Users after ADD:")
	// get users
	users, err = db.GetUsers(conn)
	if err != nil {
		panic(err)
	}
	fmt.Println(users)

	// delete user
	err = db.DeleteUser(conn, users[len(users)-1].Id)
	if err != nil {
		panic(err)
	}

	fmt.Println("Users after DELETE:")
	// get users
	users, err = db.GetUsers(conn)
	if err != nil {
		panic(err)
	}
	fmt.Println(users)
}
