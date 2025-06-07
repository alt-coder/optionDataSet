package regression

import (
	"fmt"
	"math"

	"gonum.org/v1/gonum/mat"
)

// GenerateWeights creates exponentially decaying weights.
func GenerateWeights(length int, decayRate float64) []float64 {
	weights := make([]float64, length)
	for i := 0; i < length; i++ {
		weights[i] = math.Exp(-decayRate * float64(i))
	}
	return weights
}

// Function to create a diagonal matrix from a slice of weights
func NewDiagonalMatrix(weights []float64) *mat.Dense {
	n := len(weights)
	data := make([]float64, n*n)
	for i := 0; i < n; i++ {
		data[i*n+i] = weights[i]
	}
	return mat.NewDense(n, n, data)
}

// Function to perform weighted linear regression
func WeightedLinearRegression(x, y, weights []float64) (float64, float64) {
	n := len(x)
	if len(y) != n || len(weights) != n {
		panic("All input slices must have the same length")
	}

	// Create matrices X and Y
	X := mat.NewDense(n, 2, nil)
	for i := 0; i < n; i++ {
		X.Set(i, 0, 1.0) // Intercept term
		X.Set(i, 1, x[i])
	}

	Y := mat.NewDense(n, 1, y)

	// Create the diagonal weight matrix W
	W := NewDiagonalMatrix(weights)

	// Compute X^T W X
	var XT mat.Dense
	XT.Mul(X.T(), W)

	var XTWX mat.Dense
	XTWX.Mul(&XT, X)

	// Compute X^T W Y
	var XTWY mat.Dense
	XTWY.Mul(&XT, Y)

	// Solve (X^T W X) beta = X^T W Y for beta
	var beta mat.VecDense
	if err := beta.SolveVec(&XTWX, XTWY.ColView(0)); err != nil {
		panic("Solving linear system failed")
	}

	// Extract coefficients
	b := beta.AtVec(0)
	m := beta.AtVec(1)
	return m, b
}

func run() {
	// Example data
	x := []float64{0, 1, 2, 3, 4, 5}
	y := []float64{0, 2, 4, 6, 8, 10}

	// Generate weights
	decayRate := 0.1
	weights := GenerateWeights(len(x), decayRate)

	// Fit the weighted linear regression model
	m, b := WeightedLinearRegression(x, y, weights)

	// Print the fitted model parameters
	fmt.Printf("Fitted model: y = %.4fx + %.4f\n", m, b)
}
