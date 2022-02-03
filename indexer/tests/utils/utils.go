package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database"
	"github.com/Zondax/zindexer/components/connections/database/postgres"
	"github.com/Zondax/zindexer/components/tracker"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/big"
)

func InitdbConn() *gorm.DB {
	connParams := database.DBConnectionParams{
		User:     "postgres",
		Password: "postgrespassword",
		Name:     "postgres",
		Host:     "localhost",
		Port:     "5432",
	}

	// Connect to database
	dbConn, err := postgres.Connect(
		connParams,
		postgres.DBConnectionConfig{
			Gorm: &gorm.Config{
				Logger: logger.Default.LogMode(logger.Error),
			},
		},
	)
	if err != nil {
		zap.S().Fatalf(err.Error())
		panic(err)
	}

	// Try creating schema, fails if already exists
	tx := dbConn.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", "testing"))
	if tx.Error != nil {
		panic(tx.Error)
	}

	if err != nil {
		panic(err)
	}

	err = dbConn.AutoMigrate(tracker.DbSection{})
	if err != nil {
		panic(err)
	}

	return dbConn
}

func NewSHA256Hash(n ...int) string {
	noRandomCharacters := 32

	if len(n) > 0 {
		noRandomCharacters = n[0]
	}

	randString := RandomString(noRandomCharacters)

	hash := sha256.New()
	hash.Write([]byte(randString))
	bs := hash.Sum(nil)

	return fmt.Sprintf("%x", bs)
}

var characterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandomString(n int) string {
	b := make([]rune, n)
	r, _ := rand.Int(rand.Reader, big.NewInt(int64(len(characterRunes))))
	for i := range b {
		b[i] = characterRunes[r.Int64()]
	}
	return string(b)
}

func RandomInt64(max int64) int64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(max))
	return n.Int64()
}
