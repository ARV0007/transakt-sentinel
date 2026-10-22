`./sentinel.sh retrain` writes analyst decisions here as `feedback.json` (from the
investigator's `GET /feedback`) and rebuilds the scorer, which trains on them.
The JSON holds merchant and payment ids from your local runs, so it's gitignored.
