package repository

import (
	"github.com/jackc/pgx"
	"log"
	"postgresProject/dao"
)

type UserRepository struct {
	Pool *pgx.ConnPool
}

func (ur *UserRepository) GetUser(id int) (dao.User, bool, error) {
	user := dao.User{}
	rows, err := ur.Pool.Query("getUser", id)
	if err != nil {
		log.Printf("Error during getting user by id '%v': %s\n", id, err)
		return user, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		log.Printf("There is no user with id = %v\n", id)
		return user, false, nil
	}

	err = rows.Scan(&user.Id, &user.Name, &user.Age)
	if err != nil {
		values, _ := rows.Values()
		log.Printf("Error during scaning row: %s\n", values)
		return user, false, err
	}

	return user, true, nil
}

func (ur *UserRepository) GetUsers() ([]dao.User, error) {
	rows, err := ur.Pool.Query("SELECT id, name, age FROM users")
	if err != nil {
		log.Printf("Error dirung getting all users: %s\n", err)
		return nil, err
	}
	defer rows.Close()

	users := make([]dao.User, 0)
	for rows.Next() {
		user := &dao.User{}
		err = rows.Scan(&user.Id, &user.Name, &user.Age)
		if err != nil {
			values, _ := rows.Values()
			log.Printf("Erorr during scaning row: %s\n", values)
			continue
		}
		users = append(users, *user)
	}
	return users, nil
}

func (ur *UserRepository) AddUser(user dao.User) error {
	_, err := ur.Pool.Exec("addUser", user.Name, user.Age)
	if err != nil {
		log.Printf("Error during adding new user '%s': %s\n", user, err)
	}
	return err
}

func (ur *UserRepository) UpdateUser(user dao.User) error {
	_, err := ur.Pool.Exec("updateUser", user.Name, user.Age, user.Id)
	if err != nil {
		log.Printf("Error during updating user '%s': %s\n", user, err)
	}
	return err
}

func (ur *UserRepository) DeleteUser(id int) error {
	_, err := ur.Pool.Exec("deleteUser", id)
	if err != nil {
		log.Printf("Error dirung deleting user by id = '%v': %s\n", id, err)
	}
	return err
}

func (ur *UserRepository) DeleteUsers() error {
	_, err := ur.Pool.Exec("DELETE FROM users")
	if err != nil {
		log.Printf("Error dirung deleting all users: %s\n", err)
	}
	return err
}
