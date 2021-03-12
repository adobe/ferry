# ferry (Work in Progress)

## Goals

Provide a management utility to import and export data out of FoundationDB.

## Non-Goals

The utility is not designed to replace the wonderful `fdbbackup` tool.
We do not attempt to copy or follow the foundationdb mutation log.
We are also unable to fetch the entire DB in one transaction because
of the 5-second transaction limit. See also `Differences from fdbbackup`
section

### Installation

(Work in progress - Build repo using `go build`)

### Usage


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

# Differences from `fdbbackup`

1. This isn't a backup tool, but mainly an export tool with more convenience options that may not apply to `fdbbackup` . We don't even attempt to read the mutation log, so the data will be stale if the DB is being modified at the same time. But for some applications, this may be enough.

1. When a `file://` url is used with `fdbbackup`, the request is sent to all nodes which run `backup_agent` and each node will write to its local file system. This may not be what you want.
