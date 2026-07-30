package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/aoagents/agent-orchestrator/backend/internal/daemonmeta"
)

// VersionString renders the shared build identity for the human CLI surface.
func VersionString() string {
	build := daemonmeta.Current().Build
	parts := []string{build.Version}
	if build.Commit != "" && build.Commit != "unknown" {
		parts = append(parts, "commit "+build.Commit)
	}
	return strings.Join(parts, " ")
}

func newVersionCommand() *cobra.Command {
	var jsonOutput bool
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  noArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if jsonOutput {
				return writeJSON(cmd.OutOrStdout(), daemonmeta.Current())
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), VersionString())
			return err
		},
	}
	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print machine-readable build and compatibility attestation")
	return cmd
}
