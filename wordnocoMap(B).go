package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// KeyValue 结构体表示键值对
type KeyValue struct {
	Key   string
	Value int
}

// wordCountMapFunc 函数实现了 Map 阶段的逻辑，将字符串切分成单词，并构造成键值对数组返回
func wordCountMapFunc(filename string, contents string) []KeyValue {
	var kvs []KeyValue
	words := strings.FieldsFunc(contents, func(c rune) bool {
		return !unicode.IsLetter(c)
	})
	for _, w := range words {
		kvs = append(kvs, KeyValue{w, 1})
	}
	return kvs
}

// wordCountReduceFunc 函数实现了 Reduce 阶段的逻辑，计算相同键的值的和并返回结果
func wordCountReduceFunc(key string, values []int) KeyValue {
	return KeyValue{key, len(values)}
}

// groupByKey 函数将键值对按照键分组，返回一个以键为索引的 map，值为该键对应的值数组
func groupByKey(kvs []KeyValue) map[string][]int {
	groups := make(map[string][]int)
	for _, kv := range kvs {
		if _, ok := groups[kv.Key]; !ok {
			groups[kv.Key] = []int{}
		}
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}
	return groups
}

// runMapReduce 函数实现了整个 MapReduce 框架，包括读取输入文件、Map、Reduce、排序、输出结果等步骤
func runMapReduce(inputPath string, outputPath string, mapFunc func(string, string) []KeyValue, reduceFunc func(string, []int) KeyValue) {
	// 打开输入文件
	start := time.Now()
	inputFile, err := os.Open(inputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()
	// 创建输出文件
	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()
	// 读取输入文件内容
	scanner := bufio.NewScanner(inputFile)
	var contents []string
	for scanner.Scan() {
		contents = append(contents, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	// Map 阶段
	var kvs []KeyValue
	for _, c := range contents {
		kvs = append(kvs, mapFunc(inputPath, c)...)
	}
	// Reduce 阶段
	groups := groupByKey(kvs)
	var result []KeyValue
	for k, v := range groups {
		result = append(result, reduceFunc(k, v))
	}
	// 对结果按照键排序
	sort.Slice(result, func(i, j int) bool {
		return result[i].Key < result[j].Key
	})
	// 将结果写入输出文件
	for _, kv := range result {
		line := kv.Key + " " + strconv.Itoa(kv.Value) + "\n"
		_, err := outputFile.WriteString(line)
		if err != nil {
			log.Fatal(err)
		}
	}
	defer fmt.Printf("程序运行时间为 %s\n", time.Since(start))
}

func main() {
	start := time.Now()
	// 定义输入文件路径和输出文件路径
	inputPaths := []string{"file6_1.txt", "file6_2.txt", "file6_3.txt"}
	outputPath := "output.txt"

	// 启动多个 MapReduce 任务，每个任务处理一个输入文件
	for _, inputPath := range inputPaths {
		runMapReduce(inputPath, inputPath+".out", wordCountMapFunc, wordCountReduceFunc)
	}

	// 合并输出文件
	outputFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()
	for _, inputPath := range inputPaths {
		inputFile, err := os.Open(inputPath + ".out")
		if err != nil {
			log.Fatal(err)
		}
		defer inputFile.Close()
		scanner := bufio.NewScanner(inputFile)
		for scanner.Scan() {
			_, err := outputFile.WriteString(scanner.Text() + "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("程序运行时间为 %s\n", elapsed)
}
