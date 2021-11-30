package protocol

import "regexp"

// TopicName  ChannelName 都要遵守命名规则
// 后面要是出现 #ephemeral 表示要连Registration(map中的key)一起删除
var validTopicChannelNameRegex = regexp.MustCompile(`^[\.a-zA-Z0-9_-]+(#ephemeral)?$`)

func IsValidTopicName(name string) bool {
	return isValidName(name)
}

func IsValidChannelName(name string) bool {
	return isValidName(name)
}

func isValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}
	return validTopicChannelNameRegex.MatchString(name)
}
