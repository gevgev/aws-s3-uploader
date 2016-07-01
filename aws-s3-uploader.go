package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	inExtension string
	bucket      string
)

func main() {

	searchDir := "/Users/ggevorgyan/git/go/src/github.com/gevgev/cdwdatagetter/cdw-data-reports/"

	flagSearchDir := flag.String("p", searchDir, "`path` to traverse")
	flagFilterExt := flag.String("f", "csv", "`Extention` to filter")
	flagBucketname := flag.String("b", "rovi-daap-test", "AWS S3 `bucket` name")

	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(-1)
	}

	searchDir = *flagSearchDir
	inExtension = *flagFilterExt
	bucket = *flagBucketname

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
			log.Println("Key: ", filepath.Dir(file))
			log.Println("File Name:", filepath.Base(file))
			uploadFile(file, bucket)
		}
		fmt.Println(file)
	}
}

func isFileToPush(fileName string) bool {
	return filepath.Ext(fileName) == "."+inExtension
}

func uploadFile(fileName, bucket string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("Failed to open file", err)
		return
	}

	defer file.Close()

	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String("us-west-2")}))
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   file,
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
	})
	if err != nil {
		log.Println("Failed to upload", err)
		return
	}

	log.Println("Successfully uploaded to", result.Location)

}
