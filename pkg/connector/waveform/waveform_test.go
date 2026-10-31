// mautrix-telegram - A Matrix-Telegram puppeting bridge.
// Copyright (C) 2025 Sumner Evans
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package waveform_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.mau.fi/mautrix-telegram/pkg/connector/waveform"
)

func TestNormalizeWaveform(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input []int
		want  []byte
	}{
		{"empty", nil, []byte{}},
		{"five-bit values", []int{0, 1, 2, 8, 16, 31}, []byte{0, 1, 2, 8, 16, 31}},
		{"128 peak and silent neighbor", []int{128, 0}, []byte{31, 0}},
		{"Matrix peak and silent neighbor", []int{1024, 0}, []byte{31, 0}},
		{"relative envelope", []int{0, 256, 512, 768, 1024}, []byte{0, 8, 16, 23, 31}},
		{"smaller envelope", []int{32, 64, 128}, []byte{8, 16, 31}},
		{"negative sample", []int{-128, 128, 0}, []byte{0, 31, 0}},
		{"negative only", []int{-1, math.MinInt}, []byte{0, 0}},
		{"silence", []int{0, 0, 0}, []byte{0, 0, 0}},
		{"extreme integers", []int{math.MaxInt, math.MaxInt / 2, math.MinInt, -1, 0}, []byte{31, 16, 0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, waveform.NormalizeWaveform(tc.input))
		})
	}
}

func TestEncode(t *testing.T) {
	assert.Equal(t, []byte{0x01}, waveform.Encode([]int{1}))
	assert.Equal(t, []byte{0xff, 0x03}, waveform.Encode([]int{31, 31}))
	assert.Equal(t, []byte{0x41, 0x0c, 0x52, 0xcc, 0x41}, waveform.Encode([]int{1, 2, 3, 4, 5, 6, 7, 8}))
	assert.Equal(t, []byte{0xff, 0xff, 0xff, 0xff, 0xff}, waveform.Encode([]int{31, 31, 31, 31, 31, 31, 31, 31}))
	// A sample above five bits must not spill into its silent neighbor.
	assert.Equal(t, []byte{0x1f, 0x00}, waveform.Encode([]int{128, 0}))
	assert.Equal(t, []byte{0x1f, 0x00}, waveform.Encode([]int{1024, 0}))
	assert.Equal(t, []byte{0x00, 0x00}, waveform.Encode([]int{-1, math.MinInt}))
}

func TestDecode(t *testing.T) {
	assert.Equal(t, []int{}, waveform.Decode(nil))
	assert.Equal(t, []int{1}, waveform.Decode([]byte{1}))
	assert.Equal(t, []int{1, 16, 0}, waveform.Decode([]byte{1, 2}))
	assert.Equal(t, []int{1, 16, 0, 6, 0, 2, 20, 0}, waveform.Decode([]byte{1, 2, 3, 4, 5}))
}

func TestPackedLengthsAndPadding(t *testing.T) {
	for count := 0; count <= 32; count++ {
		input := make([]int, count)
		for i := range input {
			input[i] = i % 32
		}
		encoded := waveform.Encode(input)
		assert.Len(t, encoded, (count*5+7)/8)
		if usedBits := (count * 5) % 8; usedBits != 0 {
			assert.Zero(t, encoded[len(encoded)-1]>>usedBits, "nonzero padding with %d samples", count)
		}
		decoded := waveform.Decode(encoded)
		assert.Equal(t, input, decoded[:count])
		for _, sample := range decoded[count:] {
			assert.Zero(t, sample, "nonzero decoded padding with %d samples", count)
		}
	}
}

func FuzzRoundtrip(f *testing.F) {
	f.Add([]byte{0x01})
	f.Add([]byte{128, 0})
	f.Add([]byte{0, 31, 32, 127, 128, 255})

	f.Fuzz(func(t *testing.T, w []byte) {
		wf := make([]int, len(w))
		for i, v := range w {
			wf[i] = int(v)
		}
		normalized := waveform.NormalizeWaveform(wf)
		expected := make([]int, len(normalized))
		for i, v := range normalized {
			assert.LessOrEqual(t, v, byte(31))
			expected[i] = int(v)
		}
		encoded := waveform.Encode(wf)
		decoded := waveform.Decode(encoded)

		// Sometimes, the decoded waveform might have an extra value if the
		// last value of the encoded waveform is packed into the 3
		// least-significant bits of the last byte. In that case, it's unclear
		// whether the waveform contains a 0b00000 as the last byte or if there
		// shouldn't have been anything there.
		if len(expected) != len(decoded) {
			assert.Len(t, decoded, len(expected)+1)
			expected = append(expected, 0x00)
		}
		assert.Equal(t, expected, decoded)
	})
}
