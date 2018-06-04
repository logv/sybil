package sybil

import "math"

// Using an analysis of variance, calculate the intra class correlation co-efficient
// The ICC is defined as: (mean square between) / (mean square between + mean square within)
// or: (mean square between - mean square within) / (mean square between + mean square within)
// For this function, I'm using the first definition because it stops at 0 (and we don't have
// to interpret negative correlations)

// to calculate MSB and MSW, we use sum of squares between and sum of squares within
// MSB = SSB / (K - 1)
// MSW = SSW / (N - len(groups))
// K = len(groups)
// N = total sample count

// to calculate SSW and SSB, we do:
// SSW = sum of squares within groups. Take each group and calculate its
// variance, then add all those variances together
// SSB = sum of square between groups. Take each group's averages and calculate their
// variance against the overall average.
func (querySpec *QuerySpec) CalculateICC(dir string, tableName string, params HistogramParameters) map[string]float64 {
	iccs := make(map[string]float64)
	t := GetTable(dir, tableName)

	for _, agg := range querySpec.Aggregations {
		cumulative, ok := querySpec.Cumulative.Hists[agg.Name]
		if !ok {
			continue
		}

		// start by assuming the overall population mean and variance are already calculated
		stdDev := cumulative.StdDev()
		totalVariance := stdDev * stdDev

		// find out the min and max avg of each group by row so we can use a
		// Histogram for calculating variance between groups
		minAvg := totalVariance
		maxAvg := 0.0
		for _, res := range querySpec.Results {
			hist, ok := res.Hists[agg.Name]
			if !ok {
				// TODO: count the missing values, so we can subtract them from the degrees of freedom later
				continue
			}

			minAvg = math.Min(hist.Mean(), minAvg)
			maxAvg = math.Max(hist.Mean(), maxAvg)
		}

		// CALCULATE THE VARIANCE BETWEEN GROUPS AND WITHIN GROUPS
		// for calculating within groups, we already have histograms constructed
		// for each group, so we just consult the histogram for the variance and
		// add it to our total
		//
		// for calculating between groups, we create a new histogram and insert
		// each group's average into the histogram (+ it's count as a weight) and
		// then take the variance of that
		info := IntInfo{}
		info.Min = int64(minAvg)
		info.Max = int64(maxAvg)

		betweenGroups := newBasicHist(params, t, &info, querySpec.WeightColumn != "")

		sumOfSquaresWithin := float64(0.0)
		for _, res := range querySpec.Results {
			hist, ok := res.Hists[agg.Name]
			if !ok {
				continue
			}

			// for calculating ss within groups
			stdDev := cumulative.StdDev()
			variance := stdDev * stdDev
			sumOfSquaresWithin += float64(variance)

			// for calculating ss between groups
			betweenGroups.AddWeightedValue(int64(hist.Mean()), hist.TotalCount())
		}

		icc := 1.0
		K := len(querySpec.Results)
		if K > 1 {
			meanBetweenVariance := betweenGroups.GetVariance() / float64(K-1)

			ssWithinCount := float64(cumulative.TotalCount() - int64(K))
			meanWithinVariance := sumOfSquaresWithin / ssWithinCount

			icc = (meanBetweenVariance) / (meanBetweenVariance + meanWithinVariance)

		}

		iccs[agg.Name] = icc

		Debug(agg.Name, "ICC", int(icc*100))
	}

	return iccs
}
