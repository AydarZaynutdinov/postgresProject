package dao

import "fmt"

type User struct {
	Id   int
	Name string
	Age  int
}

func (u User) String() string {
	return fmt.Sprintf("User: id:%v, name:%v, age:%v;", u.Id, u.Name, u.Age)
}
