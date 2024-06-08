package main

import (
	"fmt"
	"github.com/gocarina/gocsv"
	gg "github.com/jasonmerecki/gopriceoptions"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

func getlistOfFiles(path string) []string {
	var files []string
	err := filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		fmt.Println("Error walking the path", err)
		return nil
	}

	return files
}

type OptionData struct {
	StrikePrice        float64 `csv:"Strike Price"`
	CALL_LTP           float64 `csv:"CALL_LTP"`
	PUT_LTP            float64 `csv:"PUT_LTP"`
	GAMMA_CALL         float64 `csv:"GAMMA_CALL"`
	GAMMA_PUT          float64 `csv:"GAMMA_PUT"`
	IV_CALL            float64 `csv:"IV_CALL"`
	IV_PUT             float64 `csv:"IV_PUT"`
	VOLUME_CALL        int     `csv:"VOLUME_CALL"`
	VOLUME_PUT         int     `csv:"VOLUME_PUT"`
	DELTA_CALL         float64 `csv:"DELTA_CALL"`
	DELTA_PUT          float64 `csv:"DELTA_PUT"`
	THETA_CALL         float64 `csv:"THETA_CALL"`
	THETA_PUT          float64 `csv:"THETA_PUT"`
	RHO_CALL           float64 `csv:"RHO_CALL"`
	RHO_PUT            float64 `csv:"RHO_PUT"`
	UnderlyingLTP      float64 `csv:"Underlying LTP"`
	CALL_OPEN_INTEREST int     `csv:"CALL_OPEN_INTEREST"`
	PUT_OPEN_INTEREST  int     `csv:"PUT_OPEN_INTEREST"`
	DaysToExpiry       float64 `csv:"Days to Expiry"`
	Vega_Call          float64 `csv:"Vega_Call"`
	Vega_Put           float64 `csv:"Vega_Put"`
}

// ParseCSV function is used to parse the CSV file and return a slice of slices containing the data
func ParseOptionData(filename string) ([]*OptionData, error) {
	// Implementation of CSV parsing goes here
	optionFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer optionFile.Close()

	clients := []*OptionData{}

	if err := gocsv.UnmarshalFile(optionFile, &clients); err != nil { // Load clients from file
		return nil, err
	}
	return clients, nil
}

func getGreeks(data *OptionData) {
	// Implementation of greek calculation goes here
	strikePrice := data.StrikePrice
	underlyingPrice := data.UnderlyingLTP
	daysToExpiry := data.DaysToExpiry / 365.0
	callLTP := data.CALL_LTP
	putLTP := data.PUT_LTP
	intrestRate := 0.0675
	if underlyingPrice == 0.0 {
		fmt.Println("Underlying Price is zero")
		return
	}
	ivCall := gg.BSImpliedVol(true, callLTP, underlyingPrice, strikePrice, daysToExpiry, 0.15, intrestRate, 0.013)
	ivPut := gg.BSImpliedVol(false, putLTP, underlyingPrice, strikePrice, daysToExpiry, 0.15, intrestRate, 0.013)

	callDelta := gg.BSDelta(true, underlyingPrice, strikePrice, daysToExpiry, ivCall, intrestRate, 0.013)
	putDelta := gg.BSDelta(false, underlyingPrice, strikePrice, daysToExpiry, ivPut, intrestRate, 0.013)

	gamma_c := gg.BSGamma(underlyingPrice, strikePrice, daysToExpiry, ivCall, intrestRate, 0.013)
	vega_c := gg.BSVega(underlyingPrice, strikePrice, daysToExpiry, ivCall, intrestRate, 0.013)

	gamma_p := gg.BSGamma(underlyingPrice, strikePrice, daysToExpiry, ivPut, intrestRate, 0.013)
	vega_p := gg.BSVega(underlyingPrice, strikePrice, daysToExpiry, ivPut, intrestRate, 0.013)

	callTheata := gg.BSTheta(true, underlyingPrice, strikePrice, daysToExpiry, ivCall, intrestRate, 0.013)
	putTheata := gg.BSTheta(false, underlyingPrice, strikePrice, daysToExpiry, ivPut, intrestRate, 0.013)

	data.THETA_CALL = callTheata
	data.THETA_PUT = putTheata
	data.IV_CALL = ivCall
	data.IV_PUT = ivPut
	data.DELTA_CALL = callDelta
	data.DELTA_PUT = putDelta
	data.GAMMA_CALL = gamma_c
	data.GAMMA_PUT = gamma_p
	data.Vega_Call = vega_c
	data.Vega_Put = vega_p

}

type OptionMeta struct {
	path    string
	Options []*OptionData
}

func worker(id int, ch <-chan OptionMeta) {
	for data := range ch {
		path := data.path
		options := data.Options
		fmt.Println("Worker", id, "started processing: ", path)
		for _, option := range options {
			getGreeks(option)
		}
		fmt.Printf("finished Processing %s\n", path)
		f, err := os.Create(path)
		if err != nil {
			fmt.Println("Error creating file:  ", err,  path)
			continue
		}
		gocsv.MarshalFile( &options, f)
		f.Close()
	}
}

func dumpFiles( )  {
	path := "..\\dataset"
	files := getlistOfFiles(path)
	wg :=  sync.WaitGroup{}
	ch  := make(chan OptionMeta, len(files))
	for  i := 0; i < 8; i++ {
		 wg.Add(1)
		go func() {
			defer wg.Done()
			worker(i, ch)
		}()
	}
	for i, file := range files {
		data, err := ParseOptionData(file)
		if err != nil {
			fmt.Println(err, file)
		}
		// Use the data from
		ch <- OptionMeta{path: file, Options: data}
		if i% 100 == 0 {
			 fmt.Printf("Processed %d files\n", i)
		}
	}
	close(ch)
	wg.Wait()
	return 
}


// main function to test the getlistOfFiles function
func main() {
	
}

