package zindexer

import (
	"fmt"
	"github.com/spf13/cobra"
)

var (
	// Versions info to be injected on build time
	GitVersion  = "Unknown"
	GitRevision = "Unknown"
)

var VersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("%s %s\n", GitVersion, GitRevision)
	},
}
