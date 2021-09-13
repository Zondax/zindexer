package database

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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
		return nil, err
	}

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

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

func (c *DBQueryClient) GetMongoDoc(collection *mongo.Collection, docId string) (bson.M, error) {
	fmt.Printf("document with id:%v \n", docId)
	opts := options.FindOne()
	var result bson.M
	readErr := collection.FindOne(
		context.TODO(),
		bson.D{{Key: "_id", Value: docId}},
		opts,
	).Decode(&result)

	if readErr != nil {
		// ErrNoDocuments means that the filter did not match any documents in
		// the collection.
		if readErr == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("No document found")
		}
		return nil, readErr
	}

	return result, nil
}
