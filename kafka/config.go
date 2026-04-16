package kafka

type Config struct {
	Brokers []string `json:"brokers" yaml:"brokers" mapstructure:"brokers"`
	GroupID string   `json:"group_id" yaml:"group_id" mapstructure:"group_id"`
}
