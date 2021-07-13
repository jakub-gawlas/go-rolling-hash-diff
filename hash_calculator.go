package rolling_hash_diff

import "crypto/sha256"

var (
	defaultHashCalculator = sha256.New()
)
