package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
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
	MSONames    map[string]string
	zipFiles    bool
)

func main() {

	flagSearchDir := flag.String("p", ".", "`path` to traverse")
	flagFilterExt := flag.String("f", "csv", "`Extention` to filter")
	flagBucketname := flag.String("b", "rovi-daap-test", "AWS S3 `bucket` name")
	flagMSONames := flag.String("m", "mso-list.csv", "`MSO` ID to names lookup file")
	flagZipFiles := flag.Bool("z", false, "`Zip` files before uploading to AWS S3")

	flag.Parse()
	if !flag.Parsed() {
		flag.Usage()
		os.Exit(-1)
	}

	searchDir = *flagSearchDir
	inExtension = *flagFilterExt
	bucket = *flagBucketname
	MSONames = getMSONamesList(*flagMSONames)
	zipFiles = *flagZipFiles

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
			if zipFiles {
				zipUploadFile(file, bucket)
			} else {
				uploadFile(file, bucket)
			}
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
		Key:    aws.String(replaceIDByMSOName(fileName)),
	})
	if err != nil {
		log.Println("Failed to upload", err)
		return
	}

	log.Println("Successfully uploaded to", result.Location)

}

func replaceExtensionToZip(filename string) string {
	return strings.Replace(filename, ".csv", ".zip", 1)
}

func zipUploadFile(fileName, bucket string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Println("Failed to open file", err)
		return
	}

	// Not required, but you could zip the file before uploading it
	// using io.Pipe read/writer to stream gzip'd file contents.
	reader, writer := io.Pipe()
	go func() {
		gw := gzip.NewWriter(writer)
		io.Copy(gw, file)

		file.Close()
		gw.Close()
		writer.Close()
	}()
	uploader := s3manager.NewUploader(session.New(&aws.Config{Region: aws.String("us-west-2")}))
	result, err := uploader.Upload(&s3manager.UploadInput{
		Body:   reader,
		Bucket: aws.String(bucket),
		Key:    aws.String(replaceIDByMSOName(replaceExtensionToZip(fileName))),
	})
	if err != nil {
		log.Fatalln("Failed to upload", err)
	}

	log.Println("Successfully uploaded to", result.Location)
}

// MSOType structure for MSO name and ID
type MSOType struct {
	Code string
	Name string
}

func replaceIDByMSOName(str string) string {
	segments := strings.Split(str, "/")
	MSOID := segments[2]

	return strings.Replace(str, MSOID, MSONames[MSOID], 2)
}

func getMSONamesList(MSOListFilename string) map[string]string {
	MSOList := make(map[string]string)

	MSOFile, err := os.Open(MSOListFilename)
	if err != nil {
		log.Fatalf("Could not open MSO List file: %s, Error: %s\n", MSOListFilename, err)
	}

	r := csv.NewReader(MSOFile)
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", MSOListFilename, err)
	}

	for _, record := range records {
		MSOList[record[0]] = record[1]
	}
	return MSOList
}
