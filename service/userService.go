package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
	"postgresProject/dao"
	"postgresProject/db/repository"
	"postgresProject/queue"
	"strconv"
)

type UserService struct {
	Repository *repository.UserRepository
}

func (us *UserService) GetUser(writer http.ResponseWriter, request *http.Request) {
	idString, exists := mux.Vars(request)["id"]
	if !exists {
		log.Printf("Request doesn't contain 'id'. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request doesn't contain 'id'"), http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idString)
	if err != nil {
		log.Printf("Request must contains 'id' as 'int' variable. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request must contains 'id' as 'int' variable"), http.StatusBadRequest)
		return
	}

	user, found, err := us.Repository.GetUser(id)
	if err != nil {
		log.Printf("Error during getting user by id '%v': %s\n", id, err)
		http.Error(writer, fmt.Sprintf("Error during getting user by id '%v': %s", id, err), http.StatusInternalServerError)
		return
	}

	if !found {
		http.Error(writer, fmt.Sprintf("There is no user with id = '%v'", id), http.StatusNotFound)
		return
	}

	resp, err := json.Marshal(user)
	if err != nil {
		log.Printf("Error during marshaling response to JSON: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during marshaling response to JSON: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write(resp)
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}

func (us *UserService) GetUsers(writer http.ResponseWriter, _ *http.Request) {
	users, err := us.Repository.GetUsers()
	if err != nil {
		log.Printf("Error during getting all users: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during getting all users: %s", err), http.StatusInternalServerError)
		return
	}

	resp, err := json.Marshal(users)
	if err != nil {
		log.Printf("Error during marshaling response to JSON: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during marshaling response to JSON: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write(resp)
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}

func (us *UserService) AddUser(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		log.Printf("Error during reading body: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during reading body: %s", err), http.StatusInternalServerError)
		return
	}

	user := &dao.User{}
	err = json.Unmarshal(body, user)
	if err != nil {
		log.Printf("Error during unmarshaling body: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during unmarshaling body: %s", err), http.StatusInternalServerError)
		return
	}

	err = us.Repository.AddUser(*user)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Error during adding new user: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusCreated)
	_, err = writer.Write([]byte("User was added"))
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}

func (us *UserService) UpdateUser(writer http.ResponseWriter, request *http.Request) {
	idString, exists := mux.Vars(request)["id"]
	if !exists {
		log.Printf("Request doesn't contain 'id'. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request doesn't contain 'id'"), http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idString)
	if err != nil {
		log.Printf("Request must contains 'id' as 'int' variable. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request must contains 'id' as 'int' variable"), http.StatusBadRequest)
		return
	}

	_, found, err := us.Repository.GetUser(id)
	if err != nil {
		log.Printf("Error during getting user by id '%v': %s\n", id, err)
		http.Error(writer, fmt.Sprintf("Error during getting user by id '%v': %s", id, err), http.StatusInternalServerError)
		return
	}

	if !found {
		http.Error(writer, fmt.Sprintf("There is no user with id = '%v'", id), http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		log.Printf("Error during reading body: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during reading body: %s", err), http.StatusInternalServerError)
		return
	}

	user := &dao.User{}
	err = json.Unmarshal(body, user)
	if err != nil {
		log.Printf("Error during unmarshaling body: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during unmarshaling body: %s", err), http.StatusInternalServerError)
		return
	}

	user.Id = id

	err = us.Repository.UpdateUser(*user)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Error during updating user: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write([]byte(fmt.Sprintf("Users with id = '%v' was deleted", id)))
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}

func (us *UserService) DeleteUser(writer http.ResponseWriter, request *http.Request) {
	idString, exists := mux.Vars(request)["id"]
	if !exists {
		log.Printf("Request doesn't contain 'id'. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request doesn't contain 'id'"), http.StatusBadRequest)
		return
	}
	id, err := strconv.Atoi(idString)
	if err != nil {
		log.Printf("Request must contains 'id' as 'int' variable. Request: %v\n", request)
		http.Error(writer, fmt.Sprintf("Request must contains 'id' as 'int' variable"), http.StatusBadRequest)
		return
	}

	err = us.Repository.DeleteUser(id)
	if err != nil {
		http.Error(writer, fmt.Sprintf("Error during deleting user by id = '%v': %s", id, err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (us *UserService) DeleteUsers(writer http.ResponseWriter, _ *http.Request) {
	err := us.Repository.DeleteUsers()
	if err != nil {
		http.Error(writer, fmt.Sprintf("Error during deleting all users: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write([]byte("All users were deleted"))
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}

func (us *UserService) AddUserViaKafka(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		log.Printf("Error during reading body: %s\n", err)
		http.Error(writer, fmt.Sprintf("Error during reading body: %s", err), http.StatusInternalServerError)
		return
	}

	if err = queue.SendMessage(context.Background(), queue.UserTopic, body); err != nil {
		http.Error(writer, fmt.Sprintf("Error during sending user via queue: %s", err), http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusOK)
	_, err = writer.Write([]byte("User was sent via queue"))
	if err != nil {
		log.Printf("Error during senting response to thre client: %s\n", err)
	}
}
