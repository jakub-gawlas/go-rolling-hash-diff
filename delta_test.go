package rolling_hash_diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDeltaCalculator_Delta(t *testing.T) {
	cases := map[string]struct {
		mock        func(*deltaCalculatorMock)
		givenOrigin Signature
		givenData   [][]byte
		expected    Delta
	}{
		"no delta": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}},
			},
			givenData: [][]byte{
				{1, 1, 2, 2},
			},
			expected: Delta{
				Operations: []DeltaOperation{},
			},
		},
		"suffix added": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}},
			},
			givenData: [][]byte{
				{1, 1, 2, 2, 3, 3, 4, 5},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeAddition,
						ChunkIndex: 2,
						Data:       []byte{3, 3, 4, 5},
					},
				},
			},
		},
		"prefix added": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{3, 3}).Once()
				m.On("Sum", nil).Return([]byte{3}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{4, 5}).Once()
				m.On("Sum", nil).Return([]byte{45}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}},
			},
			givenData: [][]byte{
				{3, 3, 4, 5, 1, 1, 2, 2},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeAddition,
						ChunkIndex: 0,
						Data:       []byte{3, 3, 4, 5},
					},
				},
			},
		},
		"inner added": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{3, 3}).Once()
				m.On("Sum", nil).Return([]byte{3}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{4, 5}).Once()
				m.On("Sum", nil).Return([]byte{45}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}},
			},
			givenData: [][]byte{
				{1, 1, 3, 3, 4, 5, 2, 2},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeAddition,
						ChunkIndex: 1,
						Data:       []byte{3, 3, 4, 5},
					},
				},
			},
		},
		"suffix deleted": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}, {3}},
			},
			givenData: [][]byte{
				{1, 1, 2, 2},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeDeletion,
						ChunkIndex: 2,
					},
				},
			},
		},
		"prefix deleted": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{2, 2}).Once()
				m.On("Sum", nil).Return([]byte{2}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{3, 3}).Once()
				m.On("Sum", nil).Return([]byte{3}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}, {3}},
			},
			givenData: [][]byte{
				{2, 2, 3, 3},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeDeletion,
						ChunkIndex: 0,
					},
				},
			},
		},
		"inner deleted": {
			mock: func(m *deltaCalculatorMock) {
				m.On("Write", []byte{1, 1}).Once()
				m.On("Sum", nil).Return([]byte{1}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{3, 3}).Once()
				m.On("Sum", nil).Return([]byte{3}).Once()
				m.On("Reset").Once()
			},
			givenOrigin: Signature{
				ChunkSize:    2,
				ChunksHashes: [][]byte{{1}, {2}, {3}},
			},
			givenData: [][]byte{
				{1, 1, 3, 3},
			},
			expected: Delta{
				Operations: []DeltaOperation{
					{
						Type:       OperationTypeDeletion,
						ChunkIndex: 1,
					},
				},
			},
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := &deltaCalculatorMock{}
			c.mock(m)

			calc := newDeltaCalculator(c.givenOrigin, m)
			for _, d := range c.givenData {
				_, err := calc.Write(d)
				assert.NoError(t, err)
			}
			actual, err := calc.Delta()
			assert.NoError(t, err)
			assert.Equal(t, c.expected, actual)

			m.AssertExpectations(t)
		})
	}
}

type deltaCalculatorMock struct {
	mock.Mock
}

func (m *deltaCalculatorMock) Write(p []byte) (n int, err error) {
	m.Called(p)
	return 0, nil
}

func (m *deltaCalculatorMock) Sum([]byte) []byte {
	args := m.Called(nil)
	return args.Get(0).([]byte)
}

func (m *deltaCalculatorMock) Reset() {
	m.Called()
}