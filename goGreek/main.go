package main

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/gocarina/gocsv"
	gg "github.com/jasonmerecki/gopriceoptions"
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

func deletionRoutine(ch <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	counters := make(map[string]int)
	defer fmt.Printf("%+v\n", counters)
	list := make(map[string]int)
	for file := range ch {
		dir := filepath.Dir(file)
		folder := filepath.Base(dir)
		counters[folder]++
		if counters[folder] > 7 {
			list[dir] += 1
			println(dir, " the folder will be deleted")
			delete(counters, folder)
		}
	}

	for dir, _ := range list {
		err := os.RemoveAll(dir)
		if err != nil {
			fmt.Printf("Error deleting directory: %v\n", err)
		} else {
			fmt.Printf("Deleted directory: %s\n", dir)
		}
	}
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
			fmt.Println("Error creating file:  ", err, path)
			continue
		}
		gocsv.MarshalFile(&options, f)
		f.Close()
	}
}

// workerInputter is a function that takes in the number of workers and imputes the Nan va;ues

func workerInputter(id int, ch <-chan OptionMeta, delchan chan string) {
	for data := range ch {
		path := data.path
		options := data.Options
		delCtr := 0
		change := false
		// fmt.Println("Worker", id, "started inputting: ", path)
		for i := 1; i < len(options)-1; i++ {
			prev := options[i-1]
			curr := options[i]
			next := options[i+1]
			ctr := 0

			if curr.StrikePrice >= curr.UnderlyingLTP-400 && curr.StrikePrice <= curr.UnderlyingLTP+400 {
				// for all the fields of curr, if the field is NaN and the same field of prev or next is not NaN, then input average of prev and next to curr
				if math.IsNaN(curr.CALL_LTP) && (!math.IsNaN(prev.CALL_LTP) || !math.IsNaN(next.CALL_LTP)) {
					curr.CALL_LTP = (prev.CALL_LTP + next.CALL_LTP) / 2
					change = true
				} else if math.IsNaN(prev.CALL_LTP) && math.IsNaN(curr.CALL_LTP) && math.IsNaN(next.CALL_LTP) {
					// println("Error: All CALL_LTP values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.PUT_LTP) && (!math.IsNaN(prev.PUT_LTP) || !math.IsNaN(next.PUT_LTP)) {
					curr.PUT_LTP = (prev.PUT_LTP + next.PUT_LTP) / 2
					change = true
				} else if math.IsNaN(prev.PUT_LTP) && math.IsNaN(curr.PUT_LTP) && math.IsNaN(next.PUT_LTP) {
					// println("Error: All PUT_LTP values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.GAMMA_CALL) && (!math.IsNaN(prev.GAMMA_CALL) || !math.IsNaN(next.GAMMA_CALL)) {
					curr.GAMMA_CALL = (prev.GAMMA_CALL + next.GAMMA_CALL) / 2
					change = true
				} else if math.IsNaN(prev.GAMMA_CALL) && math.IsNaN(curr.GAMMA_CALL) && math.IsNaN(next.GAMMA_CALL) {
					// println("Error: All GAMMA_CALL values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.GAMMA_PUT) && (!math.IsNaN(prev.GAMMA_PUT) || !math.IsNaN(next.GAMMA_PUT)) {
					curr.GAMMA_PUT = (prev.GAMMA_PUT + next.GAMMA_PUT) / 2
					change = true
				} else if math.IsNaN(prev.GAMMA_PUT) && math.IsNaN(curr.GAMMA_PUT) && math.IsNaN(next.GAMMA_PUT) {
					// println("Error: All GAMMA_PUT values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.IV_CALL) && (!math.IsNaN(prev.IV_CALL) || !math.IsNaN(next.IV_CALL)) {
					curr.IV_CALL = (prev.IV_CALL + next.IV_CALL) / 2
					change = true
				} else if math.IsNaN(prev.IV_CALL) && math.IsNaN(curr.IV_CALL) && math.IsNaN(next.IV_CALL) {
					// println("Error: All IV_CALL values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.IV_PUT) && (!math.IsNaN(prev.IV_PUT) || !math.IsNaN(next.IV_PUT)) {
					curr.IV_PUT = (prev.IV_PUT + next.IV_PUT) / 2
					change = true
				} else if math.IsNaN(prev.IV_PUT) && math.IsNaN(curr.IV_PUT) && math.IsNaN(next.IV_PUT) {
					// println("Error: All IV_PUT values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}

				if math.IsNaN(curr.DELTA_CALL) && (!math.IsNaN(prev.DELTA_CALL) || !math.IsNaN(next.DELTA_CALL)) {
					curr.DELTA_CALL = (prev.DELTA_CALL + next.DELTA_CALL) / 2
					change = true
				} else if math.IsNaN(prev.DELTA_CALL) && math.IsNaN(curr.DELTA_CALL) && math.IsNaN(next.DELTA_CALL) {
					// println("Error: All DELTA_CALL values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.DELTA_PUT) && (!math.IsNaN(prev.DELTA_PUT) || !math.IsNaN(next.DELTA_PUT)) {
					curr.DELTA_PUT = (prev.DELTA_PUT + next.DELTA_PUT) / 2
					change = true
				} else if math.IsNaN(prev.DELTA_PUT) && math.IsNaN(curr.DELTA_PUT) && math.IsNaN(next.DELTA_PUT) {
					// println("Error: All DELTA_PUT values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.THETA_CALL) && (!math.IsNaN(prev.THETA_CALL) || !math.IsNaN(next.THETA_CALL)) {
					curr.THETA_CALL = (prev.THETA_CALL + next.THETA_CALL) / 2
					change = true
				} else if math.IsNaN(prev.THETA_CALL) && math.IsNaN(curr.THETA_CALL) && math.IsNaN(next.THETA_CALL) {
					// println("Error: All THETA_CALL values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.THETA_PUT) && (!math.IsNaN(prev.THETA_PUT) || !math.IsNaN(next.THETA_PUT)) {
					curr.THETA_PUT = (prev.THETA_PUT + next.THETA_PUT) / 2
					change = true
				} else if math.IsNaN(prev.THETA_PUT) && math.IsNaN(curr.THETA_PUT) && math.IsNaN(next.THETA_PUT) {
					// println("Error: All THETA_PUT values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.RHO_CALL) && (!math.IsNaN(prev.RHO_CALL) || !math.IsNaN(next.RHO_CALL)) {
					curr.RHO_CALL = (prev.RHO_CALL + next.RHO_CALL) / 2
					change = true
				} else if math.IsNaN(prev.RHO_CALL) && math.IsNaN(curr.RHO_CALL) && math.IsNaN(next.RHO_CALL) {
					// println("Error: All RHO_CALL values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.RHO_PUT) && (!math.IsNaN(prev.RHO_PUT) || !math.IsNaN(next.RHO_PUT)) {
					curr.RHO_PUT = (prev.RHO_PUT + next.RHO_PUT) / 2
					change = true
				} else if math.IsNaN(prev.RHO_PUT) && math.IsNaN(curr.RHO_PUT) && math.IsNaN(next.RHO_PUT) {
					// println("Error: All RHO_PUT values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.UnderlyingLTP) && (!math.IsNaN(prev.UnderlyingLTP) || !math.IsNaN(next.UnderlyingLTP)) {
					curr.UnderlyingLTP = (prev.UnderlyingLTP + next.UnderlyingLTP) / 2
					change = true
				} else if math.IsNaN(prev.UnderlyingLTP) && math.IsNaN(curr.UnderlyingLTP) && math.IsNaN(next.UnderlyingLTP) {
					// println("Error: All UnderlyingLTP values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}

				if math.IsNaN(curr.DaysToExpiry) && (!math.IsNaN(prev.DaysToExpiry) || !math.IsNaN(next.DaysToExpiry)) {
					curr.DaysToExpiry = (prev.DaysToExpiry + next.DaysToExpiry) / 2
				} else if math.IsNaN(prev.DaysToExpiry) && math.IsNaN(curr.DaysToExpiry) && math.IsNaN(next.DaysToExpiry) {
					// println("Error: All DaysToExpiry values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
				if math.IsNaN(curr.Vega_Put) && (!math.IsNaN(prev.Vega_Put) || !math.IsNaN(next.Vega_Put)) {
					curr.Vega_Put = (prev.Vega_Put + next.Vega_Put) / 2
				} else if math.IsNaN(curr.Vega_Put) && (!math.IsNaN(prev.Vega_Put) || !math.IsNaN(next.Vega_Put)) {
					// println("Error: All Vega_Put values are NaN",path, curr.StrikePrice,)
					ctr += 1
				}
			}
			if ctr > 0 {
				delCtr += 1
			}
			if delCtr > 6 || curr.UnderlyingLTP == 0 {
				delchan <- path
				break
			}
		}
		if delCtr > 4 {

			continue
		}
		if change {
			f, err := os.Create(path)
			if err != nil {
				fmt.Println("Error creating file:  ", err, path)
				continue
			}
			gocsv.MarshalFile(&options, f)
			f.Close()
		}

	}
}

func dumpFiles() {
	path := "..\\dataset"
	files := getlistOfFiles(path)
	wg := sync.WaitGroup{}
	ch := make(chan OptionMeta, len(files))

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(int(i), ch)
		}()
	}

	for i, file := range files {
		data, err := ParseOptionData(file)
		if err != nil {
			fmt.Println(err, file)
		}
		// Use the data from
		ch <- OptionMeta{path: file, Options: data}
		if i%100 == 0 {
			fmt.Printf("Processed %d files\n", i)
		}
	}
	close(ch)

	wg.Wait()
	return
}

// main function to test the getlistOfFiles function
func main() {
	path := "..\\dataset"
	files := getlistOfFiles(path)
	wg := sync.WaitGroup{}
	ch := make(chan OptionMeta, len(files))
	delCh := make(chan string, 1000)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			workerInputter(i, ch, delCh)
		}()
	}
	wg.Add(1)
	go deletionRoutine(delCh, &wg)
	for i, file := range files {
		data, err := ParseOptionData(file)
		if err != nil {
			fmt.Println(err, file)
		}
		// Use the data from
		ch <- OptionMeta{path: file, Options: data}
		if i%100 == 0 {
			fmt.Printf("Processed %d files\n", i)
		}
	}
	close(ch)
	close(delCh)
	wg.Wait()
}
