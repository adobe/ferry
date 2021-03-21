module github.com/adobe/ferry

go 1.16

require (
	github.com/adobe/blackhole v0.1.4
	github.com/apple/foundationdb/bindings/go v0.0.0-20210301215213-98a8f3e30802
	github.com/google/uuid v1.2.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/pkg/profile v1.5.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/viper v1.7.1
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20210316092652-d523dce5a7f4 // indirect
	golang.org/x/sys v0.0.0-20210317225723-c4fcb01b228e // indirect

)

// replace github.com/adobe/blackhole => ../blackhole
