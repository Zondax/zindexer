package main

import (
	"github.com/Zondax/zindexer"
	"github.com/Zondax/zindexer/common"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

func main() {
	// Initialize logger
	common.InitGlobalLogger()
	defer func() { _ = zap.S().Sync() }()
	common.SetupCloseHandler(nil)

	var rootCmd = &cobra.Command{
		Use:              "zindexer-example",
		Short:            "zindexer-example collects information about Oasis ROSE chain",
		PersistentPreRun: common.SetupConfiguration,
	}

	common.SetupDefaultConfiguration(func() {
		viper.AddConfigPath("$HOME/.zindexer/example") // search path
		viper.SetEnvPrefix("zindexer_example")
	})

	// Add commands and activate CLI
	rootCmd.AddCommand(zindexer.VersionCmd)
	rootCmd.AddCommand(StartCmd)
	common.RunCLI(rootCmd)
}
