package mongodb

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/Zondax/zindexer/connections/database"
	"go.mongodb.org/mongo-driver/bson"
	"os"
	"testing"
)

func TestBuffer_InsertRead(t *testing.T) {
	params := &database.DBConnectionParams{URI: os.Getenv("MONGO_URI")}
	client, err := NewMongoConnection(params)
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}

	coll := client.GetDB().Database("test").Collection("sample")
	key, _ := rand.Prime(rand.Reader, 32)
	res, err := coll.InsertOne(context.TODO(), bson.D{{Key: "name", Value: "Alice"}, {Key: "_id", Value: key.String()}})
	if err != nil {
		fmt.Println(err)
		t.Fail()
	}
	fmt.Printf("inserted document with ID %v\n", res.InsertedID)

	// Get the inserted file
	result, err := client.GetMongoDoc(coll, key.String())
	if err != nil {
		fmt.Printf("Failed to get with error %v", err)
		t.Fail()
		return
	}

	fmt.Printf("found document %v\n", result)
}
