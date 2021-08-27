package database

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoConnection struct {
	db *mongo.Client
}

func NewMongoConnection(params *DBConnectionParams) (*MongoConnection, error) {
	uri := params.URI
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Printf("Failed to connect to db: %v \n", err)
		return nil, err
	}

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}
	fmt.Println("Successfully connected and pinged.")

	return &MongoConnection{db: client}, nil
}

func (c *MongoConnection) GetDB() *mongo.Client {
	return c.db
}

func (c *DBQueryClient) Connect(params *DBConnectionParams) (*mongo.Client, error) {
	conn, err := NewMongoConnection(params)
	if err != nil {
		return nil, err
	}

	return conn.GetDB(), nil
}
