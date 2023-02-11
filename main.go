package main

import (
	"github.com/evgeny/consul-replicate/cmd"
	"os"

	"github.com/sirupsen/logrus"
)

func init() {

}

func main() {
	log := logrus.New()
	log.SetFormatter(&logrus.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(logrus.WarnLevel)

	cli := cmd.NewCLI(log, os.Stdout, os.Stderr)
	os.Exit(cli.Run(os.Args))
}
