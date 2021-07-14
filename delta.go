package rolling_hash_diff

import (
	"bytes"
)

type Delta struct {
	Operations []DeltaOperation
}

type DeltaOperation struct {
	Type       OperationType
	ChunkIndex int
	Data       []byte
}

type OperationType int

const (
	OperationTypeAddition OperationType = iota
	OperationTypeDeletion
)

type DeltaCalculator struct {
	origin         Signature
	hashCalculator HashCalculator

	operations             []DeltaOperation
	operationData          []byte
	chunkData              []byte
	lastMatchingChunkIndex int
}

func NewDeltaCalculator(originSignature Signature) DeltaCalculator {
	return newDeltaCalculator(originSignature, defaultHashCalculator)
}

func newDeltaCalculator(originSignature Signature, hashCalc HashCalculator) DeltaCalculator {
	return DeltaCalculator{
		origin:         originSignature,
		hashCalculator: hashCalc,

		operations:             make([]DeltaOperation, 0),
		operationData:          make([]byte, 0),
		chunkData:              make([]byte, 0),
		lastMatchingChunkIndex: -1,
	}
}

func (d *DeltaCalculator) Write(data []byte) (int, error) {
	fromIndex := 0
	for {
		// if reached end of origin chunk just append data to operationData
		if d.lastMatchingChunkIndex+1 >= len(d.origin.ChunksHashes) {
			d.operationData = append(d.operationData, data[fromIndex:]...)
			return len(data), nil
		}

		currentChunkSize := len(d.chunkData)
		maxChunkPartSize := d.origin.ChunkSize - currentChunkSize
		toIndex := min(fromIndex+maxChunkPartSize, len(data))

		chunkPart := data[fromIndex:toIndex]
		if _, err := d.hashCalculator.Write(chunkPart); err != nil {
			return 0, err
		}

		chunkPartSize := toIndex - fromIndex
		currentChunkSize += chunkPartSize

		d.chunkData = append(d.chunkData, chunkPart...)
		if currentChunkSize == d.origin.ChunkSize {
			d.calculateDeltaOperation(d.chunkData)
			d.chunkData = make([]byte, 0)
		}

		fromIndex += chunkPartSize
		if fromIndex >= len(data) {
			break
		}
	}
	return len(data), nil
}

func (d *DeltaCalculator) Delta() (Delta, error) {
	if len(d.chunkData) > 0 {
		d.calculateDeltaOperation(d.chunkData)
	}

	for i := d.lastMatchingChunkIndex + 1; i < len(d.origin.ChunksHashes); i++ {
		d.operations = append(d.operations, DeltaOperation{
			Type:       OperationTypeDeletion,
			ChunkIndex: i,
		})
	}

	if len(d.operationData) > 0 {
		d.operations = append(d.operations, DeltaOperation{
			Type:       OperationTypeAddition,
			ChunkIndex: d.lastMatchingChunkIndex + 1,
			Data:       d.operationData,
		})
	}

	return Delta{
		Operations: d.operations,
	}, nil
}

func (d *DeltaCalculator) calculateDeltaOperation(chunkData []byte) {
	hash := d.hashCalculator.Sum(nil)
	d.hashCalculator.Reset()

	matchingIndex := d.nextMatchingChunkIndex(hash)
	// not found matching chunk
	if matchingIndex == -1 {
		d.operationData = append(d.operationData, chunkData...)
		return
	}

	// found matching chunk
	// delete operations for not matching chunks between last and current found matching index
	for i := d.lastMatchingChunkIndex + 1; i < matchingIndex; i++ {
		d.operations = append(d.operations, DeltaOperation{
			Type:       OperationTypeDeletion,
			ChunkIndex: i,
		})
	}
	// add operation for not matched data since last matching
	if len(d.operationData) > 0 {
		d.operations = append(d.operations, DeltaOperation{
			Type:       OperationTypeAddition,
			ChunkIndex: d.lastMatchingChunkIndex + 1,
			Data:       d.operationData,
		})
		d.operationData = make([]byte, 0)
	}

	d.lastMatchingChunkIndex = matchingIndex
}

// returns matching origin chunk index or -1 if not found, starting after lastMatchingChunkIndex
func (d *DeltaCalculator) nextMatchingChunkIndex(expectedHash []byte) int {
	for i := d.lastMatchingChunkIndex + 1; i < len(d.origin.ChunksHashes); i++ {
		hash := d.origin.ChunksHashes[i]
		if bytes.Compare(expectedHash, hash) == 0 {
			return i
		}
	}
	return -1
}
