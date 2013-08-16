#!/usr/bin/env bash
export filename="$1"
md5sum - | perl -pe 's/-/$ENV{"filename"}/' >> $TMPDIR/omnipitr-helper-dst-pipe.out
