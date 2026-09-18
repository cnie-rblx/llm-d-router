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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// ComputeSGLangBlockHashes hashes a token sequence the way SGLang's radix_cache
// and sgl-router hash.rs do for a chain that starts with no parent page.
//
// Each page of blockSize tokens (the last page may be short) is SHA256 of
// optional parent 32-byte digest || little-endian uint32 tokens. The published
// block hash is the top 64 bits of that digest, big-endian, stored as BlockHash
// (the same two's-complement bit pattern SGLang puts on the wire as i64).
func ComputeSGLangBlockHashes(tokens []uint32, blockSize int) ([]BlockHash, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("blockSize must be positive, got %d", blockSize)
	}
	if len(tokens) == 0 {
		return nil, nil
	}

	out := make([]BlockHash, 0, (len(tokens)+blockSize-1)/blockSize)
	var prev [32]byte
	hasPrev := false
	for start := 0; start < len(tokens); start += blockSize {
		end := start + blockSize
		if end > len(tokens) {
			end = len(tokens)
		}
		var parent *[32]byte
		if hasPrev {
			parent = &prev
		}
		digest := chainSGLangBlock(parent, tokens[start:end])
		out = append(out, sha256ToBlockHash(&digest))
		prev = digest
		hasPrev = true
	}
	return out, nil
}

// ComputeSGLangBlockHashesBigram hashes EAGLE overlapping pairs, matching
// RadixKey.hash_page(is_bigram=True). N tokens yield N-1 bigrams; fewer than
// two tokens yield no blocks. Each page feeds both tokens of every bigram.
func ComputeSGLangBlockHashesBigram(tokens []uint32, blockSize int) ([]BlockHash, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("blockSize must be positive, got %d", blockSize)
	}
	logicalLen := 0
	if len(tokens) > 0 {
		logicalLen = len(tokens) - 1
	}
	if logicalLen == 0 {
		return nil, nil
	}

	out := make([]BlockHash, 0, (logicalLen+blockSize-1)/blockSize)
	var prev [32]byte
	hasPrev := false
	for start := 0; start < logicalLen; start += blockSize {
		end := start + blockSize
		if end > logicalLen {
			end = logicalLen
		}
		var parent *[32]byte
		if hasPrev {
			parent = &prev
		}
		digest := chainSGLangBlockBigram(parent, tokens, start, end)
		out = append(out, sha256ToBlockHash(&digest))
		prev = digest
		hasPrev = true
	}
	return out, nil
}

func chainSGLangBlock(parentDigest *[32]byte, blockTokens []uint32) [32]byte {
	h := sha256.New()
	if parentDigest != nil {
		_, _ = h.Write(parentDigest[:])
	}
	var buf [4]byte
	for _, t := range blockTokens {
		binary.LittleEndian.PutUint32(buf[:], t)
		_, _ = h.Write(buf[:])
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func chainSGLangBlockBigram(parentDigest *[32]byte, tokens []uint32, start, end int) [32]byte {
	h := sha256.New()
	if parentDigest != nil {
		_, _ = h.Write(parentDigest[:])
	}
	var buf [4]byte
	for j := start; j < end; j++ {
		binary.LittleEndian.PutUint32(buf[:], tokens[j])
		_, _ = h.Write(buf[:])
		binary.LittleEndian.PutUint32(buf[:], tokens[j+1])
		_, _ = h.Write(buf[:])
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func sha256ToBlockHash(digest *[32]byte) BlockHash {
	return BlockHash(binary.BigEndian.Uint64(digest[:8]))
}
