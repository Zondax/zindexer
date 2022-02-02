package utils

import (
	"crypto/sha1"
	"fmt"
	"github.com/Zondax/zindexer/components/connections/database"
	"github.com/Zondax/zindexer/components/connections/database/postgres"
	"github.com/Zondax/zindexer/components/tracker"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"math/rand"
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

func SetGenesisAndTipInTracker(genesis, tip uint64, indexerId string, dbConn *gorm.DB) {
	err := tracker.UpdateTrackedHeights(&[]uint64{genesis, tip}, indexerId, dbConn)
	if err != nil {
		panic(err)
	}
}

func NewSHA1Hash(n ...int) string {
	noRandomCharacters := 32

	if len(n) > 0 {
		noRandomCharacters = n[0]
	}

	randString := RandomString(noRandomCharacters)

	hash := sha1.New()
	hash.Write([]byte(randString))
	bs := hash.Sum(nil)

	return fmt.Sprintf("%x", bs)
}

var characterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func RandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = characterRunes[rand.Intn(len(characterRunes))]
	}
	return string(b)
}
