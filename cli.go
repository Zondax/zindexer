package zindexer

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"syscall"
)

type CleanUpHandler func()
type DefaultConfigHandler func()

var defaultConfigHandler DefaultConfigHandler

func SetupCloseHandler(handler CleanUpHandler) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		zap.S().Warn("\r- Ctrl+C pressed in Terminal")

		if handler != nil {
			handler()
		}

		_ = zap.S().Sync() // Sync logger
		os.Exit(0)
	}()
}

func SetupDefaultConfiguration(handler DefaultConfigHandler) {
	defaultConfigHandler = handler
}

func SetupConfiguration(c *cobra.Command, args []string) {
	var configFileFlag string
	c.PersistentFlags().StringVarP(&configFileFlag, "config", "c", "./config.yaml", "The path to the config file to use.")
	err := viper.BindPFlag("config", c.PersistentFlags().Lookup("config"))
	if err != nil {
		zap.S().Fatalf("unable to bind config flag: %+v", err)
	}
	viper.SetConfigFile(configFileFlag)

	viper.SetConfigName("config") // config file name without extension
	viper.AddConfigPath(".")      // search path

	if defaultConfigHandler != nil {
		defaultConfigHandler()
	}

	viper.AutomaticEnv() // read value ENV variables
	err = viper.ReadInConfig()
	if err != nil {
		zap.S().Fatalf("unable to read in config due to error: %+v", err)
	}
}

func RunCLI(rootCommand *cobra.Command) {
	if err := rootCommand.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
