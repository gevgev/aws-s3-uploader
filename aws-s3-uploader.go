package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var (
	inExtension string
	bucket      string
	searchDir   string
	msoNames    map[string]string
)

func main() {

	flagSearchDir := flag.String("p", ".", "`path` to traverse")
	flagFilterExt := flag.String("f", "csv", "`Extention` to filter")
	flagBucketname := flag.String("b", "rovi-daap-test", "AWS S3 `bucket` name")
	flagMsoNames := flag.String("m", "mso-list.csv", "`MSO` ID to names lookup file")

	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(-1)
	}

	searchDir = *flagSearchDir
	inExtension = *flagFilterExt
	bucket = *flagBucketname
	msoNames = getMsoNamesList(*flagMsoNames)

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
		Key:    aws.String(replaceIdByMSOName(fileName)),
	})
	if err != nil {
		log.Println("Failed to upload", err)
		return
	}

	log.Println("Successfully uploaded to", result.Location)

}

type MsoType struct {
	Code string
	Name string
}

func replaceIdByMSOName(str string) string {
	segments := strings.Split(str, "/")
	msoId := segments[2]

	return strings.Replace(str, msoId, msoNames[msoId], 2)
}

func getMsoNamesList(msoListFilename string) map[string]string {
	msoList := make(map[string]string)

	msoFile, err := os.Open(msoListFilename)
	if err != nil {
		log.Fatalf("Could not open Mso List file: %s, Error: %s\n", msoListFilename, err)
	}

	r := csv.NewReader(msoFile)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", msoListFilename, err)
	}

	for _, record := range records {
		msoList[record[0]] = record[1]
	}
	return msoList
}
