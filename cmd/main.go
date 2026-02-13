package main

import (
	"fmt"

	"github.com/baxromumarov/skiphash"
)

func main() {
	sh := skiphash.New[int, int]()
	sh.Insert(1, -12)
	sh.Insert(2, -12)
	sh.Insert(3, -12)
	sh.Insert(4, -12)
	sh.Insert(5, -12)
	sh.Insert(6, -12)
	sh.Insert(7, -12)
	sh.Insert(8, -12)
	sh.Insert(9, -12)
	sh.Insert(10, -13)
	fmt.Println(sh.Get(1))
}
