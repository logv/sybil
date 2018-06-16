package sybil

import "log"
import "fmt"
import "os"

// extracted from and influenced by
// https://groups.google.com/forum/#!topic/golang-nuts/ct99dtK2Jo4
// use env variable DEBUG=1 to turn on debug output
var ENV_FLAG = os.Getenv("DEBUG")

func Print(args ...interface{}) {
	fmt.Println(args...)
}

func Warn(args ...interface{}) {
	fmt.Fprintln(os.Stderr, append([]interface{}{"Warning:"}, args...)...)
}

func Debug(args ...interface{}) {
	if FLAGS.DEBUG || ENV_FLAG != "" {
		log.Println(args...)
	}
}
