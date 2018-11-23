package main

import (
	"fmt"
	"github.com/shelmesky/gms/log"
)

func main() {
	log := disklog.DiskLog{}
	log.Init()
	
	fmt.Println("hello world")
}
