package cmd

import (
    "fmt"

    "github.com/spf13/cobra"
)

var (
    Version   = "2.1.0"
    BuildTime = "unknown"
    GitCommit = "unknown"
)

var versionCmd = &cobra.Command{
    Use:   "version",
    Short: "打印版本信息",
    Run: func(cmd *cobra.Command, args []string) {
        fmt.Printf("redis-probe\n")
        fmt.Printf("  Version:    %s\n", Version)
        fmt.Printf("  Build Time: %s\n", BuildTime)
        fmt.Printf("  Git Commit: %s\n", GitCommit)
    },
}

func init() {
    rootCmd.AddCommand(versionCmd)
}
