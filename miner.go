package loggingdrain

import (
	"encoding/json"
	"errors"
	"strconv"
)

type TemplateMiner struct {
	drain  *drain
	masker *logMasker
}

type templateMinerMarshalStruct struct {
	Drain  *drain
	Masker *logMasker
}

func (miner *TemplateMiner) MarshalJSON() ([]byte, error) {
	return json.Marshal(templateMinerMarshalStruct{
		Drain:  miner.drain,
		Masker: miner.masker,
	})
}

func (miner *TemplateMiner) UnmarshalJSON(data []byte) error {
	var marshalStruct templateMinerMarshalStruct
	err := json.Unmarshal(data, &marshalStruct)
	if err != nil {
		return err
	}
	miner.drain = marshalStruct.Drain
	miner.masker = marshalStruct.Masker
	return nil
}

type LogMessageResponse struct {
	ChangeType    ClusterUpdateType
	Cluster       *LogCluster
	TemplateId    string
	TemplateMined string
	ClusterCount  int
}

func NewTemplateMiner(options ...MinerOption) (*TemplateMiner, error) {
	c := newTemplateMinerConfig(options)
	return newTemplateMinerWithConfig(c)
}

func newTemplateMinerWithConfig(config *minerConfig) (*TemplateMiner, error) {
	drain := newDrainWithConfig(config.Drain)
	masker, err := newLogMaskerWithConfig(config.Mask)
	if err != nil {
		return nil, err
	}
	return &TemplateMiner{
		drain:  drain,
		masker: masker,
	}, nil
}

func newTemplateMinerConfig(options []MinerOption) *minerConfig {
	drainConfig := drainConfig{
		Depth:       default_max_depth,
		Similarity:  default_sim,
		MaxChildren: default_max_children,
		MaxCluster:  default_max_clusters,
	}
	maskConfig := maskConfig{
		Prefix:           default_masking_prefix,
		Suffix:           default_masking_suffix,
		MaskInstructions: make([]maskInstruction, 0),
	}
	conf := minerConfig{
		Mask:  maskConfig,
		Drain: drainConfig,
	}
	for _, o := range options {
		conf = o.apply(conf)
	}
	return &conf
}

func (miner *TemplateMiner) AddLogMessage(message string) *LogMessageResponse {
	maskedMessage := miner.masker.mask(message)
	logCluster, updateType := miner.drain.addLogMessage(maskedMessage)
	return &LogMessageResponse{
		ChangeType:    updateType,
		Cluster:       logCluster,
		TemplateId:    strconv.FormatInt(logCluster.id, 10),
		TemplateMined: logCluster.getTemplate(),
		ClusterCount:  len(miner.drain.idToCluster.Keys()),
	}
}

// LoadMinerData 加载miner数据, 不改变配置
func (miner *TemplateMiner) LoadMinerData(minerData []byte) error {
	var newMiner *TemplateMiner
	err := json.Unmarshal(minerData, &newMiner)
	if err != nil {
		return err
	}
	if newMiner.drain == nil || newMiner.drain.idToCluster == nil {
		return errors.New("drain is nil or idToCluster is nil")
	}

	miner.drain.mu.Lock()
	defer miner.drain.mu.Unlock()
	// 清空当前数据
	miner.drain.idToCluster.Purge()
	miner.drain.rootNode = newRootTreeNode()
	miner.drain.clusterCounter = 0

	for _, cluster := range newMiner.drain.idToCluster.Values() {
		miner.drain.clusterCounter++
		miner.drain.idToCluster.Add(cluster.id, cluster)
		miner.drain.addSeqToPrefixTree(miner.drain.rootNode, cluster)
	}
	return nil
}

func (miner *TemplateMiner) Match(message string) *LogCluster {
	maskedMessage := miner.masker.mask(message)
	return miner.drain.match(maskedMessage, SEARCH_STRATEGY_NEVER)
}

func WithDrainDepth(depth int) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Drain.Depth = depth
		return conf
	})
}

func WithDrainSim(sim float32) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Drain.Similarity = sim
		return conf
	})
}

func WithDrainMaxChildren(maxChildren int) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Drain.MaxChildren = maxChildren
		return conf
	})
}

func WithDrainMaxCluster(maxCluster int) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Drain.MaxCluster = maxCluster
		return conf
	})
}

func WithMaskPrefix(prefix string) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Mask.Prefix = prefix
		return conf
	})
}

func WithMaskSuffix(suffix string) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Mask.Suffix = suffix
		return conf
	})
}

func WithMaskInsturction(pattern, maskWith string) MinerOption {
	return minerOptionFunc(func(conf minerConfig) minerConfig {
		conf.Mask.MaskInstructions = append(conf.Mask.MaskInstructions, maskInstruction{
			Pattern:  pattern,
			MaskWith: maskWith,
		})
		return conf
	})
}

func (miner *TemplateMiner) Status() string {
	return miner.drain.status()
}

type MinerOption interface {
	apply(minerConfig) minerConfig
}

type minerOptionFunc func(minerConfig) minerConfig

func (o minerOptionFunc) apply(conf minerConfig) minerConfig {
	return o(conf)
}
