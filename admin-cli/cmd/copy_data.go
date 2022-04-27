package cmd

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {

	shell.AddCommand(&grumble.Command{
		Name:     "copy_data",
		Help:     "copy data",
		Run: func(c *grumble.Context) error {
			return executor.CopyData(pegasusClient, c.Flags.String("from"), c.Flags.String("target"), c.Flags.String("start"),c.Flags.String("end"), c.Flags.Bool("copy"))
		},
		Flags: func(f *grumble.Flags) {
			f.String("f", "from", "", "the number of partitions")
			f.String("t", "target", "", "the number of replicas")
			f.String("s", "start", "2022-02-29_00:00:00", "the number of partitions")
			f.String("e", "end", "2022-06-01_00:00:00", "the number of replicas")
			f.BoolL("copy", false, "2022-04-27_00:00:00")
		},
	})
}
