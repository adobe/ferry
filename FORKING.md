# How to `fork` our Golang project on github without headache

Forking a Go project is a[[1]](https://www.reddit.com/r/golang/comments/eeocn6/how_to_fork_a_golang_project/) bit[[2]](https://stackoverflow.com/questions/56122066/go-get-on-forked-github-repo-got-unexpected-module-path-error) non-trivial[[3]](https://github.com/golang/go/issues/39889). Go requires absolute path (including repo URL) for imports. This, coupled with the fact that Github's PR system [does not allow one to exclude certain changes](https://stackoverflow.com/questions/28703140/pull-request-ignore-some-file-changes) from being included, it appears **forking a Golang project and contributing back is *slightly* harder** than what one assumes it to be.

# Temporary fork
If your goal is to have a temporary fork and contribute changes back to this repo, (Thank you!), follow these steps

## Easiest option

   1. Fork on github
   1. mkdir $GOPATH/src/github.com/adobe  # Note: this says `adobe`, not your github username
   1. `git clone github.com/your-name/ferry` # to this `adobe` directory
   1. Make improvements and send PRs back to `adobe/ferry`

In this method
   1. Your changes work both in the local checkout of your branch as well as in the upstream repo *as is* without any changes.
   2. This makes it very easy for us to accept your changes. And you don't have to create [a special branch](https://stackoverflow.com/questions/28703140/pull-request-ignore-some-file-changes) to deal with it.

### Disadvantages

  1. Your repository requires the special checkout procedure described above to make it compile/build.
  2. If your changes are not accepted in upstream repository soon, it is harder to remember this convoluted setup.

## Golang official way

   1. Fork on github
   1. Checkout to any directory, following Go recommendations. Typically this is $GOPATH/src/github.com/your-name/ferry
   1. Use `go mod`'s `replace` directive
      ```
	  replace github.com/adobe/ferry => /path/to/my/fork/ferry
                               OR
	  replace github.com/adobe/ferry => github.com/your-name/ferry
	  ```
   1. See also [this github issue comment](https://github.com/golang/go/issues/39889#issuecomment-651344768)

   1. When you send a PR, [remember to avoid sending](https://stackoverflow.com/questions/28703140/pull-request-ignore-some-file-changes) the `go.mod` override.

# Permanent fork

If your goal is to have a permanent fork and never contribute back, you will need to

   1. Fork on github
   1. Change the imports in *all* go files based on compile errors

At this point, you will have tons of changes in many files and it would be cumbersome (although not impossible) to send changes back to our (upstream) repository.