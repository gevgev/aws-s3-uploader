package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

var (
	inExtension string
)

func main() {

	searchDir := "/Users/ggevorgyan/git/go/src/github.com/gevgev/cdwdatagetter/cdw-data-reports/"

	flagSearchDir := flag.String("p", searchDir, "`path` to traverse")
	flagFilterExt := flag.String("f", "csv", "`Extention` to filter")

	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(-1)
	}

	searchDir = *flagSearchDir
	inExtension = *flagFilterExt

	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		fileList = append(fileList, path)
		return nil
	})

	if err != nil {
		log.Fatalln("Error walking the provided path: ", err)
	}

	for _, file := range fileList {
		if isFileToPush(file) {
			log.Println("Pushing: ", file)
		}
		fmt.Println(file)
	}
}

func isFileToPush(fileName string) bool {
	return filepath.Ext(fileName) == "."+inExtension
}
