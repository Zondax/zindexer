package zindexer

import (
	"fmt"
	"os"
)

const (
	// ENV_DB_USER is the name of the environment variable that specifies
	// the user of required db we have to connect to.
	ENV_DB_USER = "ZINDEXER_DB_USER"

	// ENV_DB_PASSWORD is the name of the environment variable that specifies
	// the password of required db we have to connect to.
	ENV_DB_PASSWORD = "ZINDEXER_DB_PASSWORD" // nolint

	// ENV_DB_NAME is the name of the environment variable that specifies
	// the schema of required db we have to connect to.
	ENV_DB_NAME = "ZINDEXER_DB_NAME"

	// ENV_DB_HOST is the name of the environment variable that specifies
	// the host IP of required db we have to connect to.
	ENV_DB_HOST = "ZINDEXER_DB_HOST"

	// ENV_DB_PORT is the name of the environment variable that specifies
	// the port of required db we have to connect to.
	ENV_DB_PORT = "ZINDEXER_DB_PORT"
)

type ConnectionParams struct {
	User     string
	Password string
	Name     string
	Host     string
	Port     string
}

// GetConnectionParamsFromEnv gets required config options from environmental variables
func GetConnectionParamsFromEnv() (*ConnectionParams, error) {
	user := os.Getenv(ENV_DB_USER)
	if user == "" {
		return nil, fmt.Errorf("%s environment variable not specified", ENV_DB_USER)
	}

	pass := os.Getenv(ENV_DB_PASSWORD)
	if pass == "" {
		return nil, fmt.Errorf("%s environment variable not specified", ENV_DB_PASSWORD)
	}

	name := os.Getenv(ENV_DB_NAME)
	if name == "" {
		return nil, fmt.Errorf("%s environment variable not specified", ENV_DB_NAME)
	}

	host := os.Getenv(ENV_DB_HOST)
	if host == "" {
		return nil, fmt.Errorf("%s environment variable not specified", ENV_DB_HOST)
	}

	port := os.Getenv(ENV_DB_PORT)
	if port == "" {
		return nil, fmt.Errorf("%s environment variable not specified", ENV_DB_PORT)
	}

	return &ConnectionParams{
		User:     user,
		Password: pass,
		Name:     name,
		Host:     host,
		Port:     port,
	}, nil
}

func (p *ConnectionParams) GetDSN() (string, error) {
	return fmt.Sprintf(
		"user=%s password=%s dbname=%s host=%s port=%s sslmode=disable",
		p.User, p.Password, p.Name, p.Host, p.Port), nil
}
