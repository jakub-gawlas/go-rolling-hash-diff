package rolling_hash_diff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSignatureCalculator_Signature(t *testing.T) {
	cases := map[string]struct {
		mock           func(*signatureCalculatorMock)
		givenChunkSize int
		givenWrites    [][]byte
		expected       Signature
		expectedErr    error
	}{
		"ok, chunk size = 2, one write": {
			mock: func(m *signatureCalculatorMock) {
				m.On("Write", []byte{1, 2}).Once()
				m.On("Sum", nil).Return([]byte{11}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{3}).Once()
				m.On("Sum", nil).Return([]byte{22}).Once()
				m.On("Reset").Once()
			},
			givenChunkSize: 2,
			givenWrites: [][]byte{
				{1, 2, 3},
			},
			expected: Signature{
				ChunkSize: 2,
				ChunksHashes: [][]byte{
					{11},
					{22},
				},
			},
		},
		"ok, chunk size = 3, one write": {
			mock: func(m *signatureCalculatorMock) {
				m.On("Write", []byte{1, 2, 3}).Once()
				m.On("Sum", nil).Return([]byte{111}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{4}).Once()
				m.On("Sum", nil).Return([]byte{222}).Once()
				m.On("Reset").Once()
			},
			givenChunkSize: 3,
			givenWrites: [][]byte{
				{1, 2, 3, 4},
			},
			expected: Signature{
				ChunkSize: 3,
				ChunksHashes: [][]byte{
					{111},
					{222},
				},
			},
		},
		"ok, chunk size = 2, many writes": {
			mock: func(m *signatureCalculatorMock) {
				m.On("Write", []byte{1, 2}).Once()
				m.On("Sum", nil).Return([]byte{11}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{3}).Once()
				m.On("Write", []byte{4}).Once()
				m.On("Sum", nil).Return([]byte{22}).Once()
				m.On("Reset").Once()

				m.On("Write", []byte{5}).Once()
				m.On("Sum", nil).Return([]byte{33}).Once()
				m.On("Reset").Once()
			},
			givenChunkSize: 2,
			givenWrites: [][]byte{
				{1, 2, 3},
				{4, 5},
			},
			expected: Signature{
				ChunkSize: 2,
				ChunksHashes: [][]byte{
					{11},
					{22},
					{33},
				},
			},
		},
		"err insufficient data, no writes, zero chunks": {
			mock:           func(m *signatureCalculatorMock) {},
			givenChunkSize: 2,
			givenWrites:    [][]byte{},
			expectedErr:    ErrCalculateSignatureInsufficientData,
		},
		"err insufficient data, one chunk": {
			mock: func(m *signatureCalculatorMock) {
				m.On("Write", []byte{1, 2}).Once()
				m.On("Sum", nil).Return([]byte{11}).Once()
				m.On("Reset").Once()
			},
			givenChunkSize: 10,
			givenWrites: [][]byte{
				{1, 2},
			},
			expectedErr: ErrCalculateSignatureInsufficientData,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			m := &signatureCalculatorMock{}
			c.mock(m)

			s := newSignatureCalculator(c.givenChunkSize, m)

			for _, data := range c.givenWrites {
				_, err := s.Write(data)
				assert.NoError(t, err)
			}

			actual, err := s.Signature()

			assert.Equal(t, c.expected, actual)
			if c.expectedErr != nil {
				assert.Equal(t, err, c.expectedErr)
			} else {
				assert.NoError(t, err)
			}

			m.AssertExpectations(t)
		})
	}
}

type signatureCalculatorMock struct {
	mock.Mock
}

func (m *signatureCalculatorMock) Write(p []byte) (n int, err error) {
	m.Called(p)
	return 0, nil
}

func (m *signatureCalculatorMock) Sum([]byte) []byte {
	args := m.Called(nil)
	return args.Get(0).([]byte)
}

func (m *signatureCalculatorMock) Reset() {
	m.Called()
}
