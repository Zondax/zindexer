package database

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func (c *DBQueryClient) Connect() error {
	uri := os.Getenv("MONGO_URI")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		fmt.Println("Failed to connect to db")
	}

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		fmt.Println("Failed to ping after connecting to the db")
	}
	fmt.Println("Successfully connected and pinged.")

	c.Client = client
	return err
}

func (c *DBQueryClient) GetDB() *mongo.Client {
	err := c.Connect()
	if err != nil {
		panic(err)
	}

	return c.Client
}
