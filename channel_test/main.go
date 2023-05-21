package main

import (
	"fmt"
	"time"
)

func main() {
	ic := make(chan int)

	go func() {
		for {
			ic <- 100
			time.Sleep(time.Second)
		}
	}()

	for i := range ic {
		fmt.Println(i)
	}
	//r, ok := <-ic
	//fmt.Printf("r = %d, ok = %t", r, ok)
}
