package main

import (
	"fmt"
	"io/ioutil"
	"log"

	rolling "github.com/jakub-gawlas/go-rolling-hash-diff"
)

const chunkSize = 5

func main() {
	// calculate signature
	originalData, err := ioutil.ReadFile("./testdata/original.txt")
	if err != nil {
		log.Fatal(err)
	}

	signatureCalc := rolling.NewSignatureCalculator(chunkSize)
	if _, err := signatureCalc.Write(originalData); err != nil {
		panic(err)
	}

	originalSignature, err := signatureCalc.Signature()
	if err != nil {
		log.Fatal(err)
	}

	// calculate delta
	data, err := ioutil.ReadFile("./testdata/data.txt")
	if err != nil {
		log.Fatal(err)
	}

	deltaCalc := rolling.NewDeltaCalculator(originalSignature)
	if _, err := deltaCalc.Write(data); err != nil {
		log.Fatal(err)
	}

	delta, err := deltaCalc.Delta()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%+v\n", delta)
}
