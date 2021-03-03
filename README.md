# ferry

	This utility will export all (or filtered) data from FoundationDB 
	to one of the possible stores - a local file-system folder, Azure blobstore or Amazon S3
	Export is not done in a single transaction and that implies you should only do this
	if your data is static or you don't care for it being a point-in-time snapshot

	Usage:
	  ferry [flags]
	  ferry [command]

	Available Commands:
	  export      Export all (or filtered set of) keys and values from FoundationDB
	  help        Help about any command
	  import      Import all (or filtered set of) keys and values from an export

	Flags:
	      --config string    config file (default is $HOME/.ferry.yaml)
	  -h, --help             help for ferry
	      --profile string   mem|cpu (Go performance profiling)
	  -v, --verbose          Verbose logging

	Use "ferry [command] --help" for more information about a command.
