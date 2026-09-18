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

import "fmt"

// sglangTokenProcessor hashes full prompts the way SGLang workers hash pages.
// KV-event ingest does not use TokensToKVBlockKeys: it indexes engine hashes
// directly because the parent SHA256 digest cannot be recovered from the i64
// on the wire.
type sglangTokenProcessor struct {
	blockSize int
	bigram    bool
}

var _ TokenProcessor = &sglangTokenProcessor{}

// NewSGLangTokenProcessor creates a SGLang-native token processor.
func NewSGLangTokenProcessor(config *TokenProcessorConfig) (TokenProcessor, error) {
	var cfg TokenProcessorConfig
	if config == nil {
		cfg = *DefaultTokenProcessorConfig()
	} else {
		cfg = *config
	}
	if cfg.BlockSizeTokens == 0 && cfg.BlockSize == 0 {
		cfg.BlockSizeTokens = defaultBlockSize
	}
	if cfg.BlockSizeTokens == 0 && cfg.BlockSize > 0 {
		cfg.BlockSizeTokens = cfg.BlockSize
	}
	if cfg.BlockSizeTokens <= 0 {
		invalid := cfg.BlockSizeTokens
		if invalid == 0 {
			invalid = cfg.BlockSize
		}
		return nil, fmt.Errorf("blockSizeTokens must be greater than 0, got %d", invalid)
	}
	return &sglangTokenProcessor{
		blockSize: cfg.BlockSizeTokens,
		bigram:    cfg.Bigram,
	}, nil
}

func (p *sglangTokenProcessor) BlockSize() int { return p.blockSize }

func (p *sglangTokenProcessor) IndexesEngineHashes() bool { return true }

// TokensToKVBlockKeys hashes the full token sequence from scratch. parentKey
// must be empty: SGLang chains on the parent page's 32-byte digest, which is
// not recoverable from a truncated BlockHash. extraFeatures and modelName are
// unused; SGLang page hashes do not include them.
func (p *sglangTokenProcessor) TokensToKVBlockKeys(
	parentKey BlockHash, tokens []uint32, _ string,
	_ []*BlockExtraFeatures,
) ([]BlockHash, error) {
	if parentKey != EmptyBlockHash {
		return nil, fmt.Errorf("sglang native hashing cannot continue from a truncated parent hash")
	}
	if p.bigram {
		return ComputeSGLangBlockHashesBigram(tokens, p.blockSize)
	}
	return ComputeSGLangBlockHashes(tokens, p.blockSize)
}
