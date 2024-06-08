package ppo

import (
	"fmt"
	"log"
	"os"

	"github.com/gocarina/gocsv"
)

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

func processCSV(fileLocation string, columns []string) (float64, *OptionData) {
	// Open the file
	file, err := os.Open(fileLocation)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new CSV reader
	var rows []OptionData
	if err := gocsv.UnmarshalFile(file, &rows); err != nil {
		log.Fatal(err)
	}

	// Find the maximum UnderlyingLTP value
	var maxUnderlyingLTP float64
	for _, row := range rows {
		if row.UnderlyingLTP > maxUnderlyingLTP {
			maxUnderlyingLTP = row.UnderlyingLTP
		}
	}

	// Find the row with the nearest strike price to maxUnderlyingLTP
	var nearestRow *OptionData
	nearestIndex := 0
	for i, row := range rows {
		if row.StrikePrice >= maxUnderlyingLTP-50 && row.StrikePrice <= maxUnderlyingLTP+50 {
			nearestRow = &row
			nearestIndex = i
			break
		}
	}

	// sums := make(map[string]float64)
	resultRow := &OptionData{}

	// Select 4 rows above and 4 rows below the nearest strike price
	var selectedRows []OptionData
	for i := -4; i <= 4; i++ {
		index := nearestIndex + i
		if index >= 0 && index < len(rows) {
			selectedRows = append(selectedRows, rows[index])
		}
	}

	// Sum up the selected columns
	for _, row := range selectedRows {
		resultRow.GAMMA_CALL += row.GAMMA_CALL
		resultRow.GAMMA_PUT += row.GAMMA_PUT
		resultRow.THETA_CALL += row.THETA_CALL
		resultRow.THETA_PUT += row.THETA_PUT
		resultRow.DELTA_CALL += row.DELTA_CALL
		resultRow.DELTA_PUT += row.DELTA_PUT
		resultRow.Vega_Call += row.Vega_Call
		resultRow.Vega_Put += row.Vega_Put
	}

	return nearestRow.StrikePrice, resultRow
}

func processRefCsvData(ref []OptionData, strikePrice float64) *OptionData {
	nearestIndex := 0
	for i, row := range ref {
		if row.StrikePrice >= strikePrice-50 && row.StrikePrice <= strikePrice+50 {
			nearestIndex = i
			break
		}
	}

	// sums := make(map[string]float64)
	resultRow := &OptionData{}

	// Select 4 rows above and 4 rows below the nearest strike price
	var selectedRows []OptionData
	for i := -4; i <= 4; i++ {
		index := nearestIndex + i
		if index >= 0 && index < len(ref) {
			selectedRows = append(selectedRows, ref[index])
		}
	}

	// Sum up the selected columns
	for _, row := range selectedRows {
		resultRow.GAMMA_CALL += row.GAMMA_CALL
		resultRow.GAMMA_PUT += row.GAMMA_PUT
		resultRow.THETA_CALL += row.THETA_CALL
		resultRow.THETA_PUT += row.THETA_PUT
		resultRow.DELTA_CALL += row.DELTA_CALL
		resultRow.DELTA_PUT += row.DELTA_PUT
		resultRow.Vega_Call += row.Vega_Call
		resultRow.Vega_Put += row.Vega_Put
	}

	return resultRow
}

func getOptionDataByPrice(ref []OptionData, strikePrice float64) (*OptionData, error) {

	for _, row := range ref {
		if row.StrikePrice >= strikePrice {
			return &row, nil

		}
	}
	return nil, fmt.Errorf("data not found")
}
