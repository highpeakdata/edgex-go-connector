package main

import (
	"fmt"
	"os"

	v1beta1 "github.com/Nexenta/edgex-go-connector/pkg/s3xclient/v1beta1"
	nested "github.com/antonfisher/nested-logrus-formatter"
	logrus "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	defaultS3XEndpoint = "localhost:4000"
)

var (
	secure   = false
	authKey  = ""
	secret   = ""
	endpoint = defaultS3XEndpoint
	verbose  = "Info"
)

func main() {

	cmd := &cobra.Command{
		Use:   "s3xclient",
		Short: "HPD Cluster S3X client",
		Run: func(cmd *cobra.Command, args []string) {

			formatter := &nested.Formatter{
				HideKeys:    true,
				FieldsOrder: []string{"Scope", "Direction", "req"},
			}

			logger := &logrus.Logger{
				Out:       os.Stderr,
				Formatter: formatter,
			}

			l := logger.WithFields(logrus.Fields{
				"Scope": "Service",
			})

			loggerLevel, err := logrus.ParseLevel(verbose)
			if err != nil {
				l.Infof("Logger level unknown. Set default to Info. %s", err)
				l.Logger.SetLevel(logrus.InfoLevel)
			} else {
				l.Logger.SetLevel(loggerLevel)
			}

			l.Info("HPD S3X service options:")
			l.Infof("- Secure connection    : '%t'", secure)
			l.Infof("- S3X service endpoint : '%s'", endpoint)
			if secure {
				l.Infof("- Auth key             : '%s'", authKey)
				l.Infof("- Secret               : '%s'", secret)
			}

			_, err = v1beta1.CreateEdgex(endpoint, authKey, secret)
			if err != nil {
				l.Error(err)
				os.Exit(1)
			}

			//TODO: Print bucket list
		},
	}

	cmd.Flags().StringVarP(&endpoint, "endpoint", "e", endpoint, "S3X service endpoint. Format: `scheme://service-ip:service-port`. Default is "+defaultS3XEndpoint)
	cmd.Flags().StringVarP(&authKey, "authKey", "k", authKey, "S3X service auth file path")
	cmd.PersistentFlags().BoolVarP(&secure, "secure", "s", secure, "Use TLS/SSL secure connection")
	cmd.PersistentFlags().StringVarP(&verbose, "verbose", "v", verbose, "S3xClient log verbose level")

	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}
