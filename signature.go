package rolling_hash_diff

import (
	"errors"
)

// Signature is used to calculate delta for updated data
type Signature struct {
	ChunkSize    int
	ChunksHashes [][]byte

	//TODO: for backward compatibility must be stored version of used algorithm
}

type SignatureCalculator struct {
	chunkSize      int
	hashCalculator HashCalculator

	currentChunkSize int
	chunksHashes     [][]byte
}

type HashCalculator interface {
	Write(p []byte) (n int, err error)
	Sum(b []byte) []byte
	Reset()
}

var (
	ErrCalculateSignatureInsufficientData = errors.New("insufficient data to calculate signature")
)

func NewSignatureCalculator(chunkSize int) SignatureCalculator {
	return newSignatureCalculator(chunkSize, defaultHashCalculator)
}

func newSignatureCalculator(chunkSize int, hashCalc HashCalculator) SignatureCalculator {
	return SignatureCalculator{
		chunkSize:      chunkSize,
		hashCalculator: hashCalc,
	}
}

func (s *SignatureCalculator) Write(data []byte) (int, error) {
	fromIndex := 0
	for {
		maxChunkPartSize := s.chunkSize - s.currentChunkSize
		toIndex := min(fromIndex+maxChunkPartSize, len(data))

		chunkPart := data[fromIndex:toIndex]
		if _, err := s.hashCalculator.Write(chunkPart); err != nil {
			return 0, err
		}

		chunkPartSize := toIndex - fromIndex
		s.currentChunkSize += chunkPartSize

		if s.currentChunkSize == s.chunkSize {
			s.calculateChunkHash()
		}

		fromIndex += chunkPartSize
		if fromIndex >= len(data) {
			break
		}
	}
	return len(data), nil
}

// Returns calculated signature for written data, it's not safe to reuse SignatureCalculator after call this method
func (s *SignatureCalculator) Signature() (Signature, error) {
	if s.currentChunkSize > 0 {
		s.calculateChunkHash()
	}

	if len(s.chunksHashes) < 2 {
		return Signature{}, ErrCalculateSignatureInsufficientData
	}

	return Signature{
		ChunkSize: s.chunkSize,

		//TODO: should be returned a deep copy of slice
		ChunksHashes: s.chunksHashes,
	}, nil
}

func (s *SignatureCalculator) calculateChunkHash() {
	hash := s.hashCalculator.Sum(nil)
	s.chunksHashes = append(s.chunksHashes, hash)

	s.hashCalculator.Reset()
	s.currentChunkSize = 0
}
