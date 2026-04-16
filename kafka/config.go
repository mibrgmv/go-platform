package kafka

type Config struct {
	Brokers []string `mapstructure:"brokers"`
	GroupID string   `mapstructure:"group_id"`
	Topics  Topics   `mapstructure:"topics"`
}

type Topics struct {
	QuizCompleted string `mapstructure:"quiz_completed"`
	GameCompleted string `mapstructure:"game_completed"`
}

func (c *Config) GetBrokers() []string {
	return c.Brokers
}
