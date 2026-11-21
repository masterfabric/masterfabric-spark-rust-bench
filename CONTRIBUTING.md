# Contributing

This is a benchmark repository. Changes are welcome when they keep it fair, runnable on a laptop,
and keep every number in the README reproducible.

## Ground rules

- **One SQL text for every engine.** Queries live in `bench/queries.sql` and run unchanged on all
  engines. If an engine needs a different dialect, say so in the README instead of quietly
  rewriting the query for it.
- **Equal limits.** Every engine runs in its own container with the same `--cpus` and `--memory`.
  Host runs are reference numbers only and are labelled as such.
- **Same answers first.** `bench/collect.py` must report `all_results_match: true`. A faster engine
  that returns different rows does not count.
- **Measured numbers only.** Re-run `make bench collect charts` and commit the new
  `results/summary.json` and images. Never edit a results file by hand.
- No generated data or Parquet files in git (`data/` and `*.parquet` are ignored).
- One logical change per commit, Conventional Commit subject, and a body that explains why.

## License of contributions

By contributing you agree that your contribution is licensed under the AGPL-3.0, as described in
[LICENSE](LICENSE) and [NOTICE](NOTICE).
