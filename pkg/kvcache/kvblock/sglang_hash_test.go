/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kvblock

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func i64Hash(v int64) BlockHash { return BlockHash(uint64(v)) }

func TestComputeSGLangBlockHashes_UnigramGoldens(t *testing.T) {
	got, err := ComputeSGLangBlockHashes([]uint32{1, 2, 3, 4}, 4)
	require.NoError(t, err)
	d0 := chainSGLangBlock(nil, []uint32{1, 2, 3, 4})
	require.Equal(t, []BlockHash{sha256ToBlockHash(&d0)}, got)

	got, err = ComputeSGLangBlockHashes([]uint32{1, 2, 3, 4, 5}, 4)
	require.NoError(t, err)
	d1 := chainSGLangBlock(&d0, []uint32{5})
	require.Equal(t, []BlockHash{sha256ToBlockHash(&d0), sha256ToBlockHash(&d1)}, got)
}

func TestComputeSGLangBlockHashesBigram_Goldens(t *testing.T) {
	// Values from sgl-router hash.rs, produced by SGLang RadixKey(is_bigram=True).
	cases := []struct {
		name      string
		tokens    []uint32
		blockSize int
		want      []int64
	}{
		{"single_block_full", []uint32{10, 20, 30, 40}, 4, []int64{-2735951481331064195}},
		{"multi_block", []uint32{10, 20, 30, 40, 50}, 2, []int64{-8847804484166691499, 4989791362144317498}},
		{"partial_last", []uint32{1, 2, 3, 4, 5, 6}, 4, []int64{-638950109823820341, 3604587133525381017}},
		{"longer_multi", []uint32{5, 6, 7, 8, 9, 10, 11, 12, 13}, 4, []int64{-2900568514773989563, -322435596280658912}},
		{"single_bigram", []uint32{10, 20}, 4, []int64{978178666101069530}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ComputeSGLangBlockHashesBigram(tc.tokens, tc.blockSize)
			require.NoError(t, err)
			want := make([]BlockHash, len(tc.want))
			for i, v := range tc.want {
				want[i] = i64Hash(v)
			}
			require.Equal(t, want, got)
		})
	}
}

func TestComputeSGLangBlockHashesBigram_Empty(t *testing.T) {
	got, err := ComputeSGLangBlockHashesBigram([]uint32{10}, 4)
	require.NoError(t, err)
	require.Empty(t, got)
	got, err = ComputeSGLangBlockHashesBigram(nil, 4)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestComputeSGLangBlockHashesBigram_DiffersFromUnigram(t *testing.T) {
	toks := []uint32{10, 20, 30, 40}
	uni, err := ComputeSGLangBlockHashes(toks, 4)
	require.NoError(t, err)
	bi, err := ComputeSGLangBlockHashesBigram(toks, 4)
	require.NoError(t, err)
	require.NotEqual(t, uni, bi)
}

func TestSGLangTokenProcessor_RequestPathMatchesEngineHashes(t *testing.T) {
	proc, err := NewSGLangTokenProcessor(&TokenProcessorConfig{
		BlockSizeTokens: 4,
		HashAlgo:        HashAlgoSGLang,
		Bigram:          true,
	})
	require.NoError(t, err)
	require.True(t, proc.IndexesEngineHashes())
	require.Equal(t, 4, proc.BlockSize())

	keys, err := proc.TokensToKVBlockKeys(EmptyBlockHash, []uint32{10, 20, 30, 40}, "unused", nil)
	require.NoError(t, err)
	require.Equal(t, []BlockHash{i64Hash(-2735951481331064195)}, keys)

	_, err = proc.TokensToKVBlockKeys(BlockHash(1), []uint32{10, 20, 30, 40}, "", nil)
	require.Error(t, err)
}

func TestNewTokenProcessor_SelectsSGLang(t *testing.T) {
	proc, err := NewTokenProcessor(&TokenProcessorConfig{
		BlockSizeTokens: 64,
		HashAlgo:        HashAlgoSGLang,
		Bigram:          true,
	})
	require.NoError(t, err)
	require.True(t, proc.IndexesEngineHashes())

	_, err = NewTokenProcessor(&TokenProcessorConfig{HashAlgo: "nope"})
	require.Error(t, err)
}
