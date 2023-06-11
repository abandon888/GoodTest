package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func main() {
	start := time.Now()
	outFile, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	defer outFile.Close()
	for i := 1; i <= 3; i++ {
		file, err := os.Open(fmt.Sprintf("file6_%d.txt", i))
		if err != nil {
			panic(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		scanner.Split(bufio.ScanWords)

		freq := make(map[string]int)
		for scanner.Scan() {
			word := scanner.Text()
			freq[word]++
		}
		for word, count := range freq {
			fmt.Fprintf(outFile, "%s: %d\n", word, count)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("\n运行时间为: %s", elapsed)
}
