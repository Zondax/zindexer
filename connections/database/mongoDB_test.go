package database

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func TestBuffer_InsertRead(t *testing.T) {
	var c DBQueryClient
	err := c.Connect()
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	coll := c.Client.Database("test").Collection("sample")
	res, err := coll.InsertOne(context.TODO(), bson.D{{Key: "name",Value: "Alice"}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Printf("inserted document with ID %v\n", res.InsertedID)

	// Find the document for which the _id field matches id.
	// Specify the Sort option to sort the documents by age.
	// The first document in the sorted order will be returned.
	opts := options.FindOne().SetSort(bson.D{{Key: "age",Value: 1}})
	var result bson.M
	readErr := coll.FindOne(
		context.TODO(),
		bson.D{{Key: "_id",Value: res.InsertedID}},
		opts,
	).Decode(&result)
	if readErr != nil {
		// ErrNoDocuments means that the filter did not match any documents in
		// the collection.
		if err == mongo.ErrNoDocuments {
			fmt.Println("No document found")
		}
		fmt.Println(err)
		t.Fail()
	}

	fmt.Printf("found document %v", result)
}
