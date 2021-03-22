# ferry (Work in Progress)

## Goals

Provide a management utility to import and export data out of FoundationDB.
There is experimental support for transparent write to `file://`, `s3://`, or `az://` urls. 
`az://` is a non-standard shorthand for azure blobstore urls.

The utility can only run from a single client node at this point, but that isn't scalable long term.

## Non-Goals

The utility is not designed to replace the wonderful `fdbbackup` tool.
We do not attempt to copy or follow the foundationdb mutation log.
We are also unable to fetch the entire DB in one transaction because
of the 5-second transaction limit. 

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

## Export format

Export format, in oversimplied term is a length-prefixed binary dump of the form

```
[[ length ] [ key-bytes ] [ value-bytes ]] . . . .
length = 4 bytes in little-endian format.
Once read as an Unsigned Int 32, split them as

key-length = length >> 18 & ((1 << 14) - 1)
value-length = length & (1 << 18) - 1

```

We may look at flatbuffer for output format, but that depends on subsequent usage needs

