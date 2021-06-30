package database

import "gorm.io/gorm"

type DBConnection interface {
	GetDB() *gorm.DB
}

type IDBQueryClient interface {
	Connect() error
	GetDB() interface{}
}

type DBQueryClient struct {
	Client IDBQueryClient
}
