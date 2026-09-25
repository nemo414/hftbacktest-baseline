package engine

import "math/bits"

// priceIndex is a hierarchical bitmap over a bounded integer-tick range.
//
// A plain map cannot answer "highest active bid" without scanning or a tree.
// This index keeps one bit per active price and recursively summarizes groups
// of 64 words. Set/clear/find therefore touch a fixed number of machine words
// (log_64(range)); for the bounded range selected at process start that is a
// small constant and independent of the number of resting orders. The level
// queues themselves still use intrusive O(1) operations.
type priceIndex struct {
	levels [][]uint64 // levels[0] contains price bits; the final level has one word
	n      int
}

func newPriceIndex(n int) *priceIndex {
	if n <= 0 {
		panic("price index must have at least one tick")
	}
	p := &priceIndex{n: n}
	words := (n + 63) / 64
	for {
		p.levels = append(p.levels, make([]uint64, words))
		if words == 1 {
			break
		}
		words = (words + 63) / 64
	}
	return p
}

func (p *priceIndex) set(i int) {
	for level := 0; level < len(p.levels); level++ {
		wordIndex := i >> 6
		mask := uint64(1) << uint(i&63)
		old := p.levels[level][wordIndex]
		if old&mask != 0 {
			return
		}
		p.levels[level][wordIndex] = old | mask
		// If this word was already represented by its parent, no higher
		// summary changes are necessary.
		if old != 0 {
			return
		}
		i = wordIndex
	}
}

func (p *priceIndex) clear(i int) {
	for level := 0; level < len(p.levels); level++ {
		wordIndex := i >> 6
		mask := uint64(1) << uint(i&63)
		old := p.levels[level][wordIndex]
		if old&mask == 0 {
			return
		}
		updated := old &^ mask
		p.levels[level][wordIndex] = updated
		// A non-empty word remains represented by its parent.
		if updated != 0 {
			return
		}
		i = wordIndex
	}
}

func (p *priceIndex) min() (int, bool) {
	root := p.levels[len(p.levels)-1][0]
	if root == 0 {
		return 0, false
	}
	wordIndex := bits.TrailingZeros64(root)
	for level := len(p.levels) - 2; level >= 0; level-- {
		word := p.levels[level][wordIndex]
		bit := bits.TrailingZeros64(word)
		wordIndex = wordIndex*64 + bit
	}
	if wordIndex >= p.n { // protects the unused bits in the final word
		return 0, false
	}
	return wordIndex, true
}

func (p *priceIndex) max() (int, bool) {
	root := p.levels[len(p.levels)-1][0]
	if root == 0 {
		return 0, false
	}
	wordIndex := 63 - bits.LeadingZeros64(root)
	for level := len(p.levels) - 2; level >= 0; level-- {
		word := p.levels[level][wordIndex]
		bit := 63 - bits.LeadingZeros64(word)
		wordIndex = wordIndex*64 + bit
	}
	if wordIndex >= p.n {
		return 0, false
	}
	return wordIndex, true
}
